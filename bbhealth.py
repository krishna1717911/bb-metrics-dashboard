"""BB health: one builder, connector or relay at a time, and whether it is
getting better or worse.

Every number on the page is computed twice, for the window being looked at
and for a baseline window of the same length (the window before it, the same
hours a day earlier, or a week earlier), and shown as a KPI tile with the
change between the two coloured by whether it is good news. Every chart
draws the window as a solid line and the baseline, shifted onto the same
clock, as a dashed one.

What each view reads:

  builder    the relay's round log (offers, wins, reward per round, the gap
             to the winner, rejections, time to first offer), the simulator's
             own timings in Influx (replay, commit/promote, extend, probe),
             and our event log (restarts, relay disconnects, extend and probe
             drops, flow). The live cards of the old connector & relay health
             page stay at the top.
  connector  the relay's rounds for that validator, the chain (blocks it
             produced, skipped leader slots, fallback blocks we did not
             offer on), its heartbeat gaps and relay handshakes, and the
             Firedancer report's daily reward figures.
  relay      the relay's rounds, its connectors, our and Titan's win rates,
             our rejections by reason, and connector handshakes.

Query helpers are injected (see page()), so this module holds no
credentials and opens no connections of its own.
"""

import concurrent.futures as cf
import datetime as dt
import html
import json
import re
import threading
import time
import urllib.parse
import urllib.request

import accuracy
import health
import ui
import valcomp
from rewards import PLOTLY_CDN

T = "relay.mini_block_events_v2"
ACC = "('best', 'not_best', 'tail')"
TIMING = "('index_mismatch', 'slot_mismatch', 'no_round_open')"
# Our production builders: house prefix, never a mock (a mock's offers are
# graded builder_not_eligible by policy and would pollute every rate).
OURS = "(builder_id LIKE 'ASTRALANE%' AND builder_id NOT LIKE '%MOCK%')"
TITAN = "(builder_id LIKE 'TITAN%')"
F = "%Y-%m-%d %H:%M:%S"
SLOTS_PER_EPOCH = 432_000         # getEpochSchedule: no warmup on mainnet

KINDS = [("builder", "builder health"), ("connector", "connector health"),
         ("relay", "relay health")]
WINDOWS = [(0, "today (since 00:00 UTC)"), (1, "last 1 hour"), (6, "last 6 hours"), (24, "last 24 hours"),
           (72, "last 3 days"), (168, "last 7 days")]
VS = [("prev", "the window before it"), ("day", "the same hours a day earlier"),
      ("week", "the same hours a week earlier")]
BUCKETS = [5, 15, 30, 60, 120, 360]     # all divide a day: buckets align
BUCKET_MIN = {0: 60, 1: 5, 6: 15, 24: 60, 72: 120, 168: 360}

_cache, _lock = {}, threading.Lock()


def _cached(key, fn, ttl=60):
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _lock:
        _cache[key] = (now, val)
    return val


def _q(s):
    return str(s).replace("\\", "").replace("'", "")


def _i(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ================================================================ params

def parse(qs):
    one = lambda k: (qs.get(k, [""])[0] or "").strip()
    kind = one("kind") if one("kind") in dict(KINDS) else "builder"
    try:
        win = int(one("win") or 24)
    except ValueError:
        win = 24
    if win not in dict(WINDOWS):
        win = 24
    vs = one("vs") if one("vs") in dict(VS) else "prev"
    try:
        bm = int(one("bucket") or 0)
    except ValueError:
        bm = 0
    auto = bm not in BUCKETS
    bm = BUCKET_MIN[win] if auto else bm
    end, err = None, ""
    if one("end"):
        try:
            end = dt.datetime.strptime(one("end")[:16].replace("T", " "),
                                       "%Y-%m-%d %H:%M")
        except ValueError:
            err = "the end time must look like 2026-11-09 12:00 (UTC)"
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, second=0, microsecond=0)
    end = min(end or now, now)
    # whole buckets only: a window ending mid-bucket draws its last point
    # from a fraction of the time, which reads as a cliff on every count
    end = end.replace(hour=0, minute=0) + dt.timedelta(
        minutes=(end.hour * 60 + end.minute) // bm * bm)
    if win == 0:
        # today: midnight UTC to now; "the window before" is yesterday's
        # same hours, so it is the same thing as "a day earlier"
        cur = (end.replace(hour=0, minute=0), end)
        shift = {"prev": 24, "day": 24, "week": 168}[vs]
    else:
        cur = (end - dt.timedelta(hours=win), end)
        shift = {"prev": win, "day": 24, "week": 168}[vs]
    if cur[1] <= cur[0]:                       # just after midnight
        cur = (cur[0], cur[0] + dt.timedelta(minutes=bm))
    base = (cur[0] - dt.timedelta(hours=shift), cur[1] - dt.timedelta(hours=shift))
    return {"kind": kind, "id": one("id"), "win": win, "vs": vs,
            "end_in": one("end"), "cur": cur, "base": base, "shift": shift,
            "bucket": bm, "bucket_auto": auto, "err": err}


def _w(col, win):
    return f"{col} >= '{win[0]:{F}}' AND {col} < '{win[1]:{F}}'"


def _pad(col, win, minutes=5):
    return (f"{col} >= '{win[0] - dt.timedelta(minutes=minutes):{F}}' "
            f"AND {col} < '{win[1] + dt.timedelta(minutes=minutes):{F}}'")


# ================================================================ entities

def builders(ch):
    return [i["id"] for i in health.instances(ch)]


def connectors(rq):
    def go():
        _, rows = rq(f"SELECT connector_identity, uniqExact(slot) AS n FROM {T} "
                     "WHERE timestamp > now() - INTERVAL 7 DAY "
                     "  AND event = 'round_opened' AND connector_identity != '' "
                     "GROUP BY connector_identity ORDER BY n DESC")
        return [(r[0], _i(r[1])) for r in rows if r[0]]
    return _cached("conns", go, ttl=600)


def relays(rq):
    def go():
        _, rows = rq(f"SELECT instance_id, count() AS n FROM {T} "
                     "WHERE timestamp > now() - INTERVAL 7 DAY "
                     "  AND event = 'round_opened' AND instance_id != '' "
                     "GROUP BY instance_id ORDER BY instance_id")
        return [r[0] for r in rows if r[0]]
    return _cached("relays", go, ttl=600)


# ================================================================ buckets
#
# Every query returns one row per time bucket of additive quantities (counts
# and sums). A window's KPI is the sum over its buckets, so the tile and the
# chart can never disagree; a mean is a sum over a count, never a mean of
# means.

def _rows_to_buckets(cols, rows):
    out = {}
    for r in rows:
        d = dict(zip(cols, r))
        k = str(d.pop("b"))[:19].replace("T", " ")
        out[k] = {c: _f(v) or 0.0 for c, v in d.items()}
    return out


def _sum(buckets, col):
    return sum(b.get(col, 0.0) for b in buckets.values())


def _ratio(buckets, num, den, scale=1.0):
    d = _sum(buckets, den)
    return scale * _sum(buckets, num) / d if d else None


def _bucket(col, minutes):
    return f"toStartOfInterval({col}, INTERVAL {int(minutes)} MINUTE) AS b"


# ---------------------------------------------------- relay, round level

def _chosen(win):
    return (f"SELECT mini_block_uuid FROM {T} WHERE {_w('timestamp', win)} "
            "AND event = 'round_chosen'")


def builder_relay(rq, score, b, win, bm):
    """One row per round the builder offered in: its offers, rejections,
    best accepted score, whether it won (chosen or tail), the winner's
    score, and how long after the round opened its first offer came."""
    B = _q(b)
    me = f"event = 'submitted' AND builder_id = '{B}'"
    sql = (
        f"SELECT {_bucket('r.first', bm)}, count() AS rounds, "
        " sum(r.offers) AS offers, sum(r.rejected) AS rejected, "
        " sum(r.timing) AS timing, sum(greatest(r.won, r.tail)) AS wins, "
        " countIf(r.best > 0) AS best_n, sumIf(r.best, r.best > 0) AS best_sum, "
        " countIf(w.win > 0) AS win_n, sumIf(w.win, w.win > 0) AS win_sum, "
        " countIf(w.win > 0 AND r.best > 0 AND greatest(r.won, r.tail) = 0) AS lost_n, "
        " sumIf(toInt64(w.win) - toInt64(r.best), w.win > 0 AND r.best > 0 "
        "       AND greatest(r.won, r.tail) = 0) AS gap_sum, "
        " countIf(r.lat >= 0) AS lat_n, sumIf(r.lat, r.lat >= 0) AS lat_sum "
        "FROM (SELECT connector_identity AS c, slot AS s, index_in_slot AS i, "
        f"  minIf(timestamp, {me}) AS first, "
        f"  countIf({me}) AS offers, "
        f"  countIf({me} AND reason NOT IN {ACC}) AS rejected, "
        f"  countIf({me} AND reason IN {TIMING}) AS timing, "
        f"  maxIf({score}, {me} AND reason IN {ACC}) AS best, "
        f"  maxIf(1, {me} AND reason = 'tail') AS tail, "
        f"  maxIf(1, {me} AND mini_block_uuid IN ({_chosen(win)})) AS won, "
        "   if(minIf(timestamp, event = 'round_opened') > toDateTime64('2001-01-01', 9), "
        "      (toUnixTimestamp64Micro(minIf(timestamp, " + me + ")) - "
        "       toUnixTimestamp64Micro(minIf(timestamp, event = 'round_opened'))) / 1000.0, "
        "      -1) AS lat "
        f" FROM {T} WHERE {_w('timestamp', win)} "
        f"  AND (event = 'round_opened' OR ({me})) "
        " GROUP BY c, s, i HAVING offers > 0) AS r "
        "LEFT JOIN (SELECT connector_identity AS c, slot AS s, index_in_slot AS i, "
        f"  max({score}) AS win FROM {T} WHERE {_pad('timestamp', win)} "
        f"  AND event = 'submitted' AND mini_block_uuid IN ({_chosen(win)}) "
        "  GROUP BY c, s, i) AS w ON r.c = w.c AND r.s = w.s AND r.i = w.i "
        "GROUP BY b ORDER BY b")
    return _rows_to_buckets(*rq(sql))


def scope_relay(rq, score, cond, win, bm):
    """The same round-level view for a connector or a relay (`cond` picks
    which): every round it opened, whether one of OUR builders offered and
    won, Titan likewise, the winner's score and our best."""
    ours = f"event = 'submitted' AND {OURS}"
    titan = f"event = 'submitted' AND {TITAN}"
    won = f"(mini_block_uuid IN ({_chosen(win)}) OR reason = 'tail')"
    sql = (
        f"SELECT {_bucket('r.t', bm)}, count() AS rounds, "
        " countIf(r.our_offers > 0) AS our_rounds, sum(r.our_won) AS our_wins, "
        " countIf(r.titan_offers > 0) AS titan_rounds, sum(r.titan_won) AS titan_wins, "
        " sum(r.any_won) AS decided, sum(r.our_offers) AS our_offers, "
        " sum(r.our_rej) AS our_rej, sum(r.our_timing) AS our_timing, "
        " countIf(r.win > 0) AS win_n, sumIf(r.win, r.win > 0) AS win_sum, "
        " countIf(r.our_best > 0) AS best_n, sumIf(r.our_best, r.our_best > 0) AS best_sum "
        "FROM (SELECT connector_identity AS c, slot AS s, index_in_slot AS i, "
        "  min(timestamp) AS t, "
        "  countIf(event = 'round_opened') AS opened, "
        f"  countIf({ours}) AS our_offers, "
        f"  countIf({ours} AND reason NOT IN {ACC}) AS our_rej, "
        f"  countIf({ours} AND reason IN {TIMING}) AS our_timing, "
        f"  maxIf({score}, {ours} AND reason IN {ACC}) AS our_best, "
        f"  maxIf(1, {ours} AND {won}) AS our_won, "
        f"  countIf({titan}) AS titan_offers, "
        f"  maxIf(1, {titan} AND {won}) AS titan_won, "
        f"  maxIf(1, event = 'submitted' AND {won}) AS any_won, "
        f"  maxIf({score}, event = 'submitted' AND mini_block_uuid IN ({_chosen(win)})) AS win "
        f" FROM {T} WHERE {_w('timestamp', win)} AND {cond} "
        " GROUP BY c, s, i HAVING opened > 0) AS r "
        "GROUP BY b ORDER BY b")
    return _rows_to_buckets(*rq(sql))


def relay_extra(rq, relay_id, win, bm):
    """Connectors the relay served, and connector handshakes (reconnects)."""
    R = _q(relay_id)
    a = _rows_to_buckets(*rq(
        f"SELECT {_bucket('timestamp', bm)}, uniqExact(connector_identity) AS conns, "
        " uniqExact(slot) AS slots "
        f"FROM {T} WHERE {_w('timestamp', win)} AND instance_id = '{R}' "
        " AND event = 'round_opened' GROUP BY b ORDER BY b"))
    try:
        hs = _rows_to_buckets(*rq(
            f"SELECT {_bucket('timestamp', bm)}, count() AS handshakes "
            f"FROM relay.connector_handshakes WHERE {_w('timestamp', win)} "
            f" AND instance_id = '{R}' GROUP BY b ORDER BY b"))
    except Exception:
        hs = {}
    for k, v in hs.items():
        a.setdefault(k, {}).update(v)
    return a


def relay_reasons(rq, relay_id, win):
    _, rows = rq(
        "SELECT builder_id, reason, count() AS n "
        f"FROM {T} WHERE {_w('timestamp', win)} AND instance_id = '{_q(relay_id)}' "
        f" AND event = 'submitted' AND reason NOT IN {ACC} AND {OURS} "
        "GROUP BY builder_id, reason ORDER BY n DESC LIMIT 40")
    return [(r[0], r[1] or "(none)", _i(r[2])) for r in rows]


# ---------------------------------------------------- simulator (Influx)

SIM = [("replay", "sim-commit", '"winner" = 2',
        "replaying another builder's winning mini-block onto the real bank"),
       ("commit", "sim-commit", '"winner" = 1',
        "promoting our own winning mini-block (the commit of a round we won)"),
       ("extend", "sim-extend", "", "one extend batch on the held prefix"),
       ("probe", "sim-probe", "", "one bundle probe")]


def _ix_time(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def sim(ixs, host, win, bm):
    """{name: {"buckets": {...}, "kpi": {...}}} for the four timings, the
    four measurements asked in parallel."""
    with cf.ThreadPoolExecutor(len(SIM)) as ex:
        got = ex.map(lambda spec: _sim_one(ixs, host, win, bm, spec), SIM)
        return dict(zip([spec[0] for spec in SIM], got))


def _sim_one(ixs, host, win, bm, spec):
    _name, meas, cond, _ = spec
    where = (f"\"host_id\" = '{_q(host)}' AND time >= '{_ix_time(win[0])}' "
             f"AND time < '{_ix_time(win[1])}'" + (f" AND {cond}" if cond else ""))
    sel = ('count("body_us") AS n, sum("body_us") AS s, '
           'percentile("body_us", 50) AS p50, percentile("body_us", 99) AS p99')
    bk = {}
    for ser in ixs(f'SELECT {sel} FROM "{meas}" WHERE {where} '
                   f"GROUP BY time({int(bm)}m)"):
        for row in ser["values"]:
            d = dict(zip(ser["columns"], row))
            if not d.get("n"):
                continue
            k = d["time"][:19].replace("T", " ")
            bk[k] = {"n": _f(d["n"]) or 0, "s": _f(d["s"]) or 0,
                     "p50": _f(d["p50"]), "p99": _f(d["p99"])}
    kp = {}
    for ser in ixs(f'SELECT {sel} FROM "{meas}" WHERE {where}'):
        for row in ser["values"]:
            kp = dict(zip(ser["columns"], row))
    return {"buckets": bk, "kpi": kp}


# ---------------------------------------------------- our event log

def builder_events(ch, b, win, bm):
    B = _q(b)
    bk = _rows_to_buckets(*ch(
        f"SELECT {_bucket('ts', bm)}, "
        " countIf(stage = 'scheduler' AND event = 'received') AS recv, "
        " countIf(event = 'dropped' AND reason = 'extend_refused') AS ext_ref, "
        " countIf(event = 'dropped' AND reason = 'probe_failed') AS probe_fail, "
        " countIf(stage = 'network' AND event = 'disconnected') AS relay_disc "
        f"FROM bifrost_events WHERE instance_id = '{B}' AND {_w('ts', win)} "
        " AND ((stage = 'scheduler' AND event IN ('received', 'dropped')) "
        "      OR (stage = 'network' AND event = 'disconnected')) "
        "GROUP BY b ORDER BY b"))
    # A run is a process lifetime (a fresh run_id per start). One whose
    # first event falls inside the window is a restart in the window.
    _, runs = ch(
        "SELECT run_id, toString(min(ts)) AS first FROM bifrost_events "
        f"WHERE instance_id = '{B}' "
        f"  AND ts >= '{win[0] - dt.timedelta(hours=1):{F}}' AND ts < '{win[1]:{F}}' "
        f"GROUP BY run_id HAVING min(ts) >= '{win[0]:{F}}' ORDER BY first")
    return bk, [(r[0], r[1][:19]) for r in runs]


# Every builder panic is posted to #bb-alerts as one line:
#   [ASTRALANE_AMS_1 e68b5c8a5] ERROR panic: thread=NetworkTile
#   location=bifrost-builder/src/relay.rs:74:9 message=latest relay frame ...
# That channel is the only live record of the panic text since the otel
# logins were retired (2026-10-17), so it is read through Slack's API.
PANIC_RE = re.compile(
    r"^\[(?P<inst>[A-Za-z0-9_]+) (?P<build>[0-9a-f]{6,40})\] ERROR panic: "
    r"(?:thread=(?P<thread>\S+) )?(?:location=(?P<loc>\S+) )?"
    r"(?:message=)?(?P<msg>.*)$", re.S)


def parse_panic(text):
    """One #bb-alerts message -> dict, or None if it is not a panic line."""
    m = PANIC_RE.match(html.unescape(text or "").strip())
    if not m:
        return None
    d = m.groupdict()
    d["msg"] = d["msg"].strip()
    return d


def snapshot_panics(path, b, win):
    """Panics from panics.json, a snapshot of every panic line in #bb-alerts
    (pulled by hand; see its `note`). Returns (rows, covered_to)."""
    def load():
        with open(path) as fh:
            return json.load(fh)
    doc = _cached(f"snap:{path}", load, ttl=300)
    lo, hi = f"{win[0]:{F}}", f"{win[1]:{F}}"
    rows = [(r["at"], r["build"], r["thread"], r["location"], r["message"])
            for r in doc.get("panics") or []
            if r["instance"] == b and lo <= r["at"] < hi]
    return sorted(rows), doc.get("covers_to") or ""


# A start this soon after an ERROR line means that error killed the process:
# panic -> 1 s abort -> restart lands 2-3 s later in every one of the 54
# panics 10-17 -> 11-09; a planned restart after "simulator gone" took 48 s.
PANIC_GAP_S = 10


def otel_panics(ot, b, win):
    """Panics from otel_logs, read as the builder actually leaves them.

    The panic hook's own "panic: ..." line (common/src/panic_hook.rs) does
    not survive: the process aborts a second later, before the log shipper
    drains. What does survive is the panic site's own ERROR line -- the
    panic message itself ("latest relay frame rejected: ...", "winner order
    bytes invariant failed: ...") -- and the next process's "starting
    bifrost builder" line, which carries the build. So a panic is the last
    ERROR line within PANIC_GAP_S before a start; a start with no ERROR
    line before it is an exit without a logged error (a deploy or a kill).
    Checked against #bb-alerts 10-17 -> 11-09.

    Returns (panics, quiet_starts): panics as (time, build, thread,
    location, message), quiet starts as (time, build)."""
    B = _q(b)
    lo = f"'{win[0]:{F}}'"
    starts = ot(
        "SELECT toString(Timestamp), LogAttributes['build'] FROM otel_logs "
        f"WHERE ServiceName = '{B}' AND Timestamp >= {lo} "
        f"  AND Timestamp < toDateTime64('{win[1]:{F}}', 9) + INTERVAL {PANIC_GAP_S} SECOND "
        "  AND Body = 'starting bifrost builder' ORDER BY Timestamp")
    errors = ot(
        "SELECT toString(Timestamp), Body FROM otel_logs "
        f"WHERE ServiceName = '{B}' "
        f"  AND Timestamp >= toDateTime64({lo}, 9) - INTERVAL {PANIC_GAP_S} SECOND "
        f"  AND Timestamp < '{win[1]:{F}}' "
        "  AND SeverityText IN ('error', 'ERROR') ORDER BY Timestamp")
    ts = lambda x: dt.datetime.strptime(x[:19], F)
    errs = [(ts(t), t, body) for t, body in errors]
    panics, quiet = [], []
    prev_build = ""
    for t, build in starts:
        st = ts(t)
        before = [e for e in errs if st - dt.timedelta(seconds=PANIC_GAP_S) <= e[0] < st]
        if before:
            et, raw, body = before[-1]
            if win[0] <= et < win[1]:
                hook = body.startswith("panic: ")
                panics.append((raw[:19], prev_build or build or "", "", "",
                               body.split("\n", 1)[0][7 if hook else 0:][:400]))
        elif win[0] <= st < win[1]:
            quiet.append((t[:19], build or ""))
        prev_build = build or prev_build
    return panics, quiet


def panic_source(ot, token, channel, snapshot_path):
    """`fn(builder, window) -> (rows, label, covered_to)`, from the first
    source that answers: otel_logs (the panic hook's own log, once a
    working ClickStack login is configured), #bb-alerts live (with a Slack
    bot token), else the panics.json snapshot. `covered_to` is where the
    snapshot ends (empty for a live source); a restart after it cannot be
    told apart from a deploy and is shown as an exit not yet classified."""
    def fn(b, win):
        why = []
        try:
            rows, quiet = otel_panics(ot, b, win)
            return rows, "otel_logs, live", quiet
        except Exception as exc:
            why.append("otel: " + str(exc)[:90])
        if token:
            try:
                return slack_panics(token, channel, b, win), "#bb-alerts, live", ""
            except Exception as exc:
                why.append("Slack: " + str(exc)[:90])
        rows, upto = snapshot_panics(snapshot_path, b, win)
        return rows, (f"#bb-alerts snapshot (to {upto} UTC) &mdash; no live "
                      "source answered: " + html.escape("; ".join(why))), upto
    return fn


def slack_panics(token, channel, b, win):
    """[(utc time, build, thread, location, message)] for builder `b` from
    #bb-alerts over `win`. Needs a bot token with channels:history (or
    groups:history) in the channel."""
    if not token:
        raise RuntimeError(
            "panics are read from #bb-alerts, and no SLACK_BOT_TOKEN is set "
            "for this dashboard (a bot token with channels:history, invited "
            "to the channel). The otel store that used to hold the panic "
            "text has had no working login since 2026-10-17.")
    lo = win[0].replace(tzinfo=dt.UTC).timestamp()
    hi = win[1].replace(tzinfo=dt.UTC).timestamp()

    def fetch():
        msgs, cursor = [], ""
        for _ in range(50):                       # 50 pages x 200 messages
            qs = urllib.parse.urlencode(
                {"channel": channel, "oldest": f"{lo:.6f}", "latest": f"{hi:.6f}",
                 "limit": 200, "inclusive": "true", **({"cursor": cursor} if cursor else {})})
            req = urllib.request.Request(
                "https://slack.com/api/conversations.history?" + qs,
                headers={"Authorization": "Bearer " + token})
            with urllib.request.urlopen(req, timeout=30) as resp:
                doc = json.load(resp)
            if not doc.get("ok"):
                raise RuntimeError("Slack: " + str(doc.get("error")))
            msgs += doc.get("messages") or []
            cursor = (doc.get("response_metadata") or {}).get("next_cursor") or ""
            if not cursor:
                break
        return msgs
    out = []
    for m in _cached(f"bba:{channel}:{lo:.0f}:{hi:.0f}", fetch, ttl=120):
        d = parse_panic(m.get("text"))
        if d and d["inst"] == b:
            t = dt.datetime.fromtimestamp(float(m.get("ts") or 0), dt.UTC)
            out.append((t.strftime(F), d["build"], d["thread"] or "",
                        d["loc"] or "", d["msg"]))
    return sorted(out)


# ---------------------------------------------------- connector, chain side

def _rpc(url, method, params):
    req = urllib.request.Request(url, data=json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        doc = json.load(resp)
    if doc.get("error"):
        raise RuntimeError(str(doc["error"])[:200])
    return doc.get("result")


def leader_slots(rpc_url, ident, lo, hi):
    """The validator's leader slots in [lo, hi], from the leader schedule of
    every epoch the range touches."""
    out = []
    for ep in range(lo // SLOTS_PER_EPOCH, hi // SLOTS_PER_EPOCH + 1):
        start = ep * SLOTS_PER_EPOCH

        def go(start=start):
            got = _rpc(rpc_url, "getLeaderSchedule", [start, {"identity": ident}])
            return [start + i for i in (got or {}).get(ident, [])]
        out += [s for s in _cached(f"ls:{ident}:{start}", go, ttl=3600)
                if lo <= s <= hi]
    return out


def connector_chain(gq, rq, rpc_url, ident, win, bm):
    """Blocks produced, skipped leader slots and fallback blocks, per bucket.

    A fallback block is one this validator produced in a slot none of our
    builders offered on: either the relay opened no round for it at all
    (the connector was off GBX) or it did and we never bid."""
    C = _q(ident)
    _, span = gq("SELECT min(slot), max(slot) FROM solana.block_summary "
                 f"WHERE {_w('estimated_slot_time', win)}")
    lo, hi = (_i(span[0][0]), _i(span[0][1])) if span else (0, 0)
    _, blocks = gq(
        "SELECT slot, toString(estimated_slot_time), builder_tips + bifrost_tip "
        f"FROM solana.block_summary WHERE slot BETWEEN {lo} AND {hi} "
        f"AND leader = '{C}'")
    _, auction = rq(
        "SELECT slot, countIf(event = 'round_opened') > 0, "
        f" countIf(event = 'submitted' AND {OURS}) > 0 "
        f"FROM {T} WHERE {_pad('timestamp', win, 2)} AND connector_identity = '{C}' "
        "GROUP BY slot")
    opened = {_i(s) for s, o, _ in auction if str(o) in ("1", "true", "True")}
    offered = {_i(s) for s, _, x in auction if str(x) in ("1", "true", "True")}
    produced = {}
    for s, t, tips in blocks:
        produced[_i(s)] = (t[:19], _i(tips))
    lead = leader_slots(rpc_url, ident, lo, hi) if lo and rpc_url else []
    skipped = sorted(s for s in lead if s not in produced and s <= hi)

    def bucket_of(t):
        x = dt.datetime.strptime(t, F)
        m = (x.hour * 60 + x.minute) // bm * bm
        return (x.replace(hour=0, minute=0, second=0)
                + dt.timedelta(minutes=m)).strftime(F)
    bk = {}
    fb_rows = []
    for s, (t, tips) in produced.items():
        k = bk.setdefault(bucket_of(t), {})
        k["blocks"] = k.get("blocks", 0) + 1
        k["gbx"] = k.get("gbx", 0) + (1 if tips > 0 else 0)
        if s not in offered:
            k["fallback"] = k.get("fallback", 0) + 1
            why = ("relay opened no round" if s not in opened
                   else "rounds opened, none of our builders offered")
            k["noround" if s not in opened else "notoffered"] = (
                k.get("noround" if s not in opened else "notoffered", 0) + 1)
            fb_rows.append((s, t, why, tips))
    # skipped slots carry no block time; place them by slot distance from
    # the window's first slot at the nominal 200 ms (good enough to bucket)
    for s in skipped:
        t = (win[0] + dt.timedelta(milliseconds=200 * (s - lo))).strftime(F)
        k = bk.setdefault(bucket_of(t), {})
        k["skipped"] = k.get("skipped", 0) + 1
    k_all = {"leader": len(lead), "range": (lo, hi)}
    return bk, sorted(fb_rows), skipped, k_all


def heartbeat(gq, ident, win, gap_s=15):
    """Stretches where the connector's heartbeat (solana.slot_info, one row
    per slot while it is connected to a relay) went silent for more than
    `gap_s` seconds -- the connector disconnecting or its process dying."""
    _, rows = gq(
        "SELECT toUnixTimestamp(toStartOfInterval(start_at, INTERVAL 5 SECOND)) AS b "
        f"FROM solana.slot_info WHERE {_w('start_at', win)} "
        f"AND CAST(connector_pubkey AS String) = base58Decode('{_q(ident)}') "
        "GROUP BY b ORDER BY b")
    ts = [_i(r[0]) for r in rows]
    lo = int(win[0].replace(tzinfo=dt.UTC).timestamp())
    hi = int(win[1].replace(tzinfo=dt.UTC).timestamp())
    gaps = []
    prev = None
    for t in ts:
        if prev is not None and t - prev > gap_s:
            gaps.append((prev, t, t - prev))
        prev = t
    now = int(time.time())
    if ts and min(hi, now) - ts[-1] > gap_s + 60:
        gaps.append((ts[-1], None, min(hi, now) - ts[-1]))     # still silent
    if not ts:
        gaps.append((lo, None, min(hi, now) - lo))
    return gaps, len(ts)


def handshakes(rq, ident, win):
    _, rows = rq(
        "SELECT toString(timestamp), instance_id, conn_version "
        f"FROM relay.connector_handshakes WHERE {_w('timestamp', win)} "
        f"AND connector_identity = '{_q(ident)}' ORDER BY timestamp")
    return [(r[0][:23], r[1], r[2]) for r in rows]


def fd_day(date):
    """One day's Firedancer report (reports.firedancer.io), cached."""
    def go():
        doc = valcomp._get(f"{valcomp.FD_API}?period=daily&date={date}")
        return {v.get("leader"): v for v in doc.get("validators") or []}
    return _cached(f"fd:{date}", go, ttl=3600)


# ================================================================ render

def _sol(lamports, digits=4):
    return "&mdash;" if lamports is None else f"{lamports / 1e9:,.{digits}f}"


def _pct(v):
    return "&mdash;" if v is None else f"{v:.1f}%"


def _ms(v):
    return "&mdash;" if v is None else f"{v:,.1f} ms"


def _n(v):
    return "&mdash;" if v is None else f"{v:,.0f}"


def _per_h(total, win):
    h = (win[1] - win[0]).total_seconds() / 3600
    return total / h if h else None


def tile(label, cur, base, fmt, better, help_="", sub=""):
    return ui.kpi(label, fmt(cur), sub=sub or f"was {fmt(base)}",
                  d=ui.delta(cur, base, better), help_=help_)


def _series(buckets, fn):
    xs, ys = [], []
    for k in sorted(buckets):
        try:
            y = fn(buckets[k])
        except (ZeroDivisionError, TypeError):
            y = None
        xs.append(k)
        ys.append(y)
    return xs, ys


def _shift(xs, delta_h):
    out = []
    for x in xs:
        t = dt.datetime.strptime(x[:19], F) + dt.timedelta(hours=delta_h)
        out.append(t.strftime(F))
    return out


def fig(title, cur_b, base_b, fn, shift_h, unit="", kind="lines"):
    """Window (solid) against baseline (dashed), on the window's clock."""
    xc, yc = _series(cur_b, fn)
    xb, yb = _series(base_b, fn)
    xb = _shift(xb, shift_h)
    tr = lambda x, y, name, dash, col: {
        "x": x, "y": y, "name": name, "type": "scatter", "mode": "lines+markers",
        "connectgaps": False, "line": {"color": col, "width": 2, "dash": dash},
        "marker": {"size": 4}, "hovertemplate": "%{y:,.2f}" + unit + "<extra>" + name + "</extra>"}
    return {"data": [tr(xb, yb, f"baseline ({shift_h:g} h earlier, shifted)",
                        "dot", "#64748b"),
                     tr(xc, yc, "this window", "solid", "#2dd4bf")],
            "layout": {"title": {"text": title, "font": {"size": 13, "color": "#dbe4ee"},
                                 "x": 0.02},
                       "paper_bgcolor": "rgba(0,0,0,0)", "plot_bgcolor": "rgba(0,0,0,0)",
                       "font": {"color": "#8fa6bf", "size": 11},
                       "margin": {"l": 52, "r": 12, "t": 34, "b": 34},
                       "xaxis": {"gridcolor": "#16212c", "zeroline": False},
                       "yaxis": {"gridcolor": "#16212c", "zeroline": False,
                                 "ticksuffix": unit, "rangemode": "tozero"},
                       "legend": {"orientation": "h", "y": 1.13, "x": 1, "xanchor": "right",
                                  "font": {"size": 10}},
                       "hovermode": "x unified"}}


def _div(a, b, scale=1.0):
    return (scale * a / b) if b else None


def _table(cols, rows, num=()):
    head = "".join(f'<th{" class=n" if i in num else ""}>{c}</th>'
                   for i, c in enumerate(cols))
    body = "".join("<tr>" + "".join(
        f'<td{" class=n" if i in num else ""}>{v}</td>' for i, v in enumerate(r))
        + "</tr>" for r in rows)
    return f'<table class="utab"><tr>{head}</tr>{body}</table>'


def win_after(win, t):
    return f"{win[1]:{F}}" > t


def _err(name, exc):
    return ui.info(f"<b>{name}</b> could not be read: "
                   f"{html.escape(str(exc)[:240])}", "err")


def _run(jobs):
    """Run the named jobs in parallel; a failure is kept as the exception."""
    out = {}
    with cf.ThreadPoolExecutor(max_workers=min(12, len(jobs) or 1)) as ex:
        futs = {ex.submit(fn): k for k, fn in jobs.items()}
        for f in cf.as_completed(futs):
            try:
                out[futs[f]] = f.result()
            except Exception as exc:
                out[futs[f]] = exc
    return out


# ---------------------------------------------------------------- builder

def view_builder(p, ch, ixs, panic_src, rq, score, b, live_fn):
    bm, cur, base, sh = p["bucket"], p["cur"], p["base"], p["shift"]
    host = health.INSTANCE_SIM_HOST.get(b)
    jobs = {
        "rc": lambda: builder_relay(rq, score, b, cur, bm),
        "rb": lambda: builder_relay(rq, score, b, base, bm),
        "ec": lambda: builder_events(ch, b, cur, bm),
        "eb": lambda: builder_events(ch, b, base, bm),
        "pc": lambda: panic_src(b, cur),
        "pb": lambda: panic_src(b, base),
        "live": live_fn,
    }
    if host:
        jobs["sc"] = lambda: sim(ixs, host, cur, bm)
        jobs["sb"] = lambda: sim(ixs, host, base, bm)
    r = _run(jobs)
    out = [r["live"] if not isinstance(r["live"], Exception)
           else _err("the live cards", r["live"])]

    # ---- auction
    out.append(ui.section("auction", "the relay's round log, this builder's "
                          "offers only"))
    rc, rb = r["rc"], r["rb"]
    if isinstance(rc, Exception) or isinstance(rb, Exception):
        out.append(_err("the relay", rc if isinstance(rc, Exception) else rb))
    else:
        g = lambda bk: {
            "rounds": _sum(bk, "rounds"), "wins": _sum(bk, "wins"),
            "win": _ratio(bk, "wins", "rounds", 100),
            "best": _ratio(bk, "best_sum", "best_n"),
            "winr": _ratio(bk, "win_sum", "win_n"),
            "gap": _ratio(bk, "gap_sum", "lost_n"),
            "rej": _ratio(bk, "rejected", "offers", 100),
            "tim": _ratio(bk, "timing", "offers", 100),
            "lat": _ratio(bk, "lat_sum", "lat_n"), "offers": _sum(bk, "offers")}
        c, bb = g(rc), g(rb)
        out.append(ui.kpi_row([
            tile("win rate (contested)", c["win"], bb["win"], _pct, "up",
                 "rounds won (chosen, or a tail it owned) / rounds it offered in"),
            tile("rounds won", c["wins"], bb["wins"], _n, "up"),
            tile("rounds offered in", c["rounds"], bb["rounds"], _n, "up"),
            tile("avg reward / round offered (SOL)", c["best"], bb["best"], _sol, "up",
                 "its best accepted offer per round, scored as the relay scores it "
                 "(fees + Jito x the connector's weight + builder payment)"),
            tile("avg winning reward / round (SOL)", c["winr"], bb["winr"], _sol, None,
                 "what won the rounds it offered in, whoever won them"),
            tile("avg gap to winner, lost rounds (SOL)", c["gap"], bb["gap"], _sol, "down",
                 "winner's score minus our best, over rounds we offered in and lost"),
            tile("rejected offers", c["rej"], bb["rej"], _pct, "down"),
            tile("timing rejections", c["tim"], bb["tim"], _pct, "down",
                 "index_mismatch + slot_mismatch + no_round_open, of all offers"),
            tile("first offer after round open", c["lat"], bb["lat"], _ms, "down",
                 "mean, relay clock both sides"),
        ]))
        out.append(ui.chart_grid([
            fig("win rate", rc, rb, lambda k: _div(k["wins"], k["rounds"], 100), sh, "%"),
            fig("avg reward per round offered (SOL)", rc, rb,
                lambda k: _div(k["best_sum"], k["best_n"], 1e-9), sh),
            fig("avg gap to winner in lost rounds (SOL)", rc, rb,
                lambda k: _div(k["gap_sum"], k["lost_n"], 1e-9), sh),
            fig("rounds offered in", rc, rb, lambda k: k["rounds"], sh),
            fig("timing rejections", rc, rb,
                lambda k: _div(k["timing"], k["offers"], 100), sh, "%"),
            fig("first offer after round open (ms)", rc, rb,
                lambda k: _div(k["lat_sum"], k["lat_n"]), sh, " ms"),
        ]))

    # ---- simulator
    out.append(ui.section("simulator", "replay, commit, extend and probe "
                          "timings, body_us from Influx"))
    if not host:
        out.append(ui.info(f"No simulator host is mapped for {html.escape(b)} "
                           "(health.INSTANCE_SIM_HOST).", "warn"))
    elif isinstance(r.get("sc"), Exception) or isinstance(r.get("sb"), Exception):
        out.append(_err("Influx", r["sc"] if isinstance(r["sc"], Exception) else r["sb"]))
    else:
        sc, sb = r["sc"], r["sb"]
        tiles, figs = [], []
        for name, _meas, _c, what in SIM:
            kc, kb = sc[name]["kpi"], sb[name]["kpi"]
            mean = lambda k: _div(_f(k.get("s")) or 0, _f(k.get("n")) or 0, 1e-3)
            tiles.append(tile(f"{name} p50", _f(kc.get("p50")) and kc["p50"] / 1e3,
                              _f(kb.get("p50")) and kb["p50"] / 1e3, _ms, "down", what,
                              sub=f"{_n(_f(kc.get('n')))} runs &middot; was "
                                  f"{_ms(_f(kb.get('p50')) and kb['p50'] / 1e3)}"))
            tiles.append(tile(f"{name} p99", _f(kc.get("p99")) and kc["p99"] / 1e3,
                              _f(kb.get("p99")) and kb["p99"] / 1e3, _ms, "down", what))
            tiles.append(tile(f"{name} mean", mean(kc), mean(kb), _ms, "down", what))
            bc, bbk = sc[name]["buckets"], sb[name]["buckets"]
            figs.append(fig(f"{name} p50 / p99 (ms)", bc, bbk,
                            lambda k: (k["p50"] or 0) / 1e3, sh, " ms"))
            figs[-1]["data"] += [
                dict(figs[-1]["data"][1], y=_series(bc, lambda k: (k["p99"] or 0) / 1e3)[1],
                     name="p99 this window", line={"color": "#f59e0b", "width": 1.5}),
                dict(figs[-1]["data"][0], y=_series(bbk, lambda k: (k["p99"] or 0) / 1e3)[1],
                     name="p99 baseline", line={"color": "#7c5a1e", "width": 1.2, "dash": "dot"})]
            figs.append(fig(f"{name} runs per bucket", bc, bbk, lambda k: k["n"], sh))
        out.append(ui.kpi_row(tiles))
        out.append(ui.chart_grid(figs))

    # ---- stability and flow
    out.append(ui.section("stability and flow", "our event log"))
    ec, eb = r["ec"], r["eb"]
    if isinstance(ec, Exception) or isinstance(eb, Exception):
        out.append(_err("ClickHouse", ec if isinstance(ec, Exception) else eb))
    else:
        (bc, runs_c), (bbk, runs_b) = ec, eb
        ph = lambda bk, col, w: _per_h(_sum(bk, col), w)
        out.append(ui.kpi_row([
            tile("restarts", len(runs_c), len(runs_b), _n, "down",
                 "process starts (a new run_id) inside the window"),
            tile("relay disconnects", _sum(bc, "relay_disc"), _sum(bbk, "relay_disc"),
                 _n, "down", "network/disconnected events"),
            tile("extend refusals / h", ph(bc, "ext_ref", cur), ph(bbk, "ext_ref", base),
                 _n, "down"),
            tile("probe failures / h", ph(bc, "probe_fail", cur), ph(bbk, "probe_fail", base),
                 _n, "down"),
            tile("orders received / h", ph(bc, "recv", cur), ph(bbk, "recv", base), _n, None),
        ]))
        out.append(ui.chart_grid([
            fig("extend refusals", bc, bbk, lambda k: k["ext_ref"], sh),
            fig("probe failures", bc, bbk, lambda k: k["probe_fail"], sh),
            fig("orders received", bc, bbk, lambda k: k["recv"], sh),
            fig("relay disconnects", bc, bbk, lambda k: k["relay_disc"], sh),
        ]))
        if runs_c:
            out.append(ui.expander(
                f"{len(runs_c)} restart(s) in the window",
                _table(["run_id", "first event (UTC)"],
                       [(html.escape(a), html.escape(t)) for a, t in runs_c])))
    pc, pb = r["pc"], r["pb"]
    if isinstance(pc, Exception):
        out.append(ui.section("panics", "#bb-alerts, one line per panic"))
        out.append(ui.info("<b>Panics</b>: " + html.escape(str(pc)[:400]), "warn"))
    else:
        (pc, label, upto) = pc
        pbn = None if isinstance(pb, Exception) else len(pb[0])
        out.append(ui.section("panics", label))
        by_loc = {}
        for row in pc:
            # otel carries no location; the message's leading phrase names
            # the site just as well ("latest relay frame rejected")
            site = row[3] or row[4].split(":", 1)[0][:60]
            by_loc[site] = by_loc.get(site, 0) + 1
        top = max(by_loc.items(), key=lambda kv: kv[1]) if by_loc else None
        runs_c = ec[1] if not isinstance(ec, Exception) else []
        if isinstance(upto, list):          # live otel: starts with no error
            late, upto = upto, ""
        else:
            late = [x for x in runs_c if upto and x[1] > upto]
        out.append(ui.kpi_row([
            tile("panics", len(pc), pbn, _n, "down",
                 "lines '[instance build] ERROR panic: ...' in #bb-alerts"),
            ui.kpi("distinct panic sites", _n(len(by_loc)),
                   sub=html.escape(f"most: {top[0]} ({top[1]})") if top else
                   "no panic in the window"),
            ui.kpi("restarts with no error logged" if not upto else
                   "exits not yet classified", _n(len(late)),
                   sub=(f"restarts after the snapshot ends ({html.escape(upto)} UTC); "
                        "a panic or a deploy") if upto else
                   "a deploy or a kill, not a panic"
                   + (" &middot; builds " + html.escape(", ".join(
                       sorted({x[1] for x in late if x[1]})))
                      if late and not upto else "")),
        ]))
        if upto and win_after(p["cur"], upto):
            out.append(ui.info(
                f"The panic snapshot ends at {html.escape(upto)} UTC. With a "
                "working otel login (OTEL_URL / OTEL_USER / OTEL_PASS) panics "
                "are read live from otel_logs, or with <b>SLACK_BOT_TOKEN</b> "
                "from #bb-alerts; until then a restart after that time is "
                "counted as an unclassified exit.",
                "info"))
        if pc:
            out.append(ui.expander(f"{len(pc)} panic line(s)", _table(
                ["at (UTC)", "build", "thread", "location", "message"],
                [(html.escape(t), html.escape(bd), html.escape(th), html.escape(lc),
                  html.escape(m[:300])) for t, bd, th, lc, m in pc]), open_=True))
    return "".join(out)


# ---------------------------------------------------------------- connector

def view_connector(p, rq, gq, score, rpc_url, ident, names):
    bm, cur, base, sh = p["bucket"], p["cur"], p["base"], p["shift"]
    cond = f"connector_identity = '{_q(ident)}'"
    jobs = {
        "rc": lambda: scope_relay(rq, score, cond, cur, bm),
        "rb": lambda: scope_relay(rq, score, cond, base, bm),
        "cc": lambda: connector_chain(gq, rq, rpc_url, ident, cur, bm),
        "cb": lambda: connector_chain(gq, rq, rpc_url, ident, base, bm),
        "hc": lambda: heartbeat(gq, ident, cur),
        "hb": lambda: heartbeat(gq, ident, base),
        "kc": lambda: handshakes(rq, ident, cur),
        "kb": lambda: handshakes(rq, ident, base),
        "fd": lambda: valcomp.fd_report(),
    }
    r = _run(jobs)
    out = []
    name = names.get(ident) or ""

    # ---- connection
    out.append(ui.section("connection", "heartbeat gaps (solana.slot_info) "
                          "and relay handshakes"))
    hc, hb, kc, kb = r["hc"], r["hb"], r["kc"], r["kb"]
    if any(isinstance(x, Exception) for x in (hc, hb)):
        out.append(_err("the heartbeat", hc if isinstance(hc, Exception) else hb))
    else:
        (gc, nc), (gb, nb) = hc, hb
        down = lambda g: sum(x[2] for x in g)
        kcn = len(kc) if not isinstance(kc, Exception) else None
        kbn = len(kb) if not isinstance(kb, Exception) else None
        silent_now = [g for g in gc if g[1] is None]
        out.append(ui.kpi_row([
            tile("disconnects (heartbeat gaps > 15 s)", len(gc), len(gb), _n, "down"),
            tile("time disconnected", down(gc) / 60, down(gb) / 60,
                 lambda v: "&mdash;" if v is None else f"{v:,.1f} min", "down"),
            tile("relay handshakes (reconnects)", kcn, kbn, _n, "down",
                 "connector_handshakes: every (re)connect to a relay"),
            ui.kpi("right now",
                   ('<span class="udelta bad">silent</span>' if silent_now
                    else '<span class="udelta good">connected</span>'),
                   sub=("no heartbeat since "
                        + dt.datetime.fromtimestamp(silent_now[0][0], dt.UTC)
                        .strftime(F) + " UTC") if silent_now else
                   f"{nc:,} heartbeat buckets in the window"),
        ]))
        rows = [(dt.datetime.fromtimestamp(a, dt.UTC).strftime(F),
                 (dt.datetime.fromtimestamp(z, dt.UTC).strftime(F) if z else
                  '<span class="udelta bad">still silent</span>'),
                 f"{d / 60:,.1f} min") for a, z, d in gc]
        out.append(ui.expander(
            f"when it disconnected &mdash; {len(gc)} gap(s) in the window",
            _table(["last heartbeat (UTC)", "next heartbeat (UTC)", "silent for"],
                   rows, num=(2,)) if rows else
            '<div class="ks">no gap longer than 15 s</div>', open_=bool(gc)))
        if kcn:
            out.append(ui.expander(f"{kcn} relay handshake(s)", _table(
                ["at (UTC)", "relay", "connector version"],
                [(html.escape(a), html.escape(b), html.escape(v or ""))
                 for a, b, v in kc])))

    # ---- production
    out.append(ui.section("blocks", "the chain (solana.block_summary) against "
                          "the leader schedule and our offers"))
    cc, cb = r["cc"], r["cb"]
    if isinstance(cc, Exception) or isinstance(cb, Exception):
        out.append(_err("the chain", cc if isinstance(cc, Exception) else cb))
    else:
        (bkc, fb, skc, allc), (bkb, _fbb, skb, allb) = cc, cb
        out.append(ui.kpi_row([
            tile("leader slots", allc["leader"], allb["leader"], _n, None),
            tile("blocks produced", _sum(bkc, "blocks"), _sum(bkb, "blocks"), _n, None),
            tile("skipped slots", len(skc), len(skb), _n, "down",
                 "leader slots with no block on chain"),
            tile("fallback blocks (we did not offer)", _sum(bkc, "fallback"),
                 _sum(bkb, "fallback"), _n, "down",
                 "blocks it produced in slots none of our builders offered on"),
            tile("&hellip; relay opened no round", _sum(bkc, "noround"),
                 _sum(bkb, "noround"), _n, "down", "the connector was off GBX"),
            tile("&hellip; rounds, but we did not bid", _sum(bkc, "notoffered"),
                 _sum(bkb, "notoffered"), _n, "down"),
            tile("blocks carrying builder tips", _sum(bkc, "gbx"), _sum(bkb, "gbx"),
                 _n, "up", "Titan or Bifrost tip accounts paid in the block"),
        ]))
        out.append(ui.chart_grid([
            fig("blocks produced", bkc, bkb, lambda k: k.get("blocks", 0), sh),
            fig("fallback blocks", bkc, bkb, lambda k: k.get("fallback", 0), sh),
            fig("skipped slots", bkc, bkb, lambda k: k.get("skipped", 0), sh),
        ]))
        if fb:
            out.append(ui.expander(
                f"{len(fb)} fallback block(s) in the window", _table(
                    ["slot", "block time (UTC)", "why", "builder tips (lamports)"],
                    [(s, t, w, f"{tips:,}") for s, t, w, tips in fb[:500]],
                    num=(3,))))
        if skc:
            out.append(ui.expander(f"{len(skc)} skipped slot(s)",
                                   '<div class="ks m">' + ", ".join(
                                       str(s) for s in skc[:2000]) + "</div>"))

    # ---- auction
    out.append(ui.section("auction", "the relay's rounds for this validator"))
    rc, rb = r["rc"], r["rb"]
    if isinstance(rc, Exception) or isinstance(rb, Exception):
        out.append(_err("the relay", rc if isinstance(rc, Exception) else rb))
    else:
        out.append(_auction_tiles(rc, rb))
        out.append(_auction_charts(rc, rb, sh))

    # ---- Firedancer report
    out.append(ui.section("daily report", "reports.firedancer.io, one day "
                          "behind the chain"))
    fd = r["fd"]
    if isinstance(fd, Exception):
        out.append(_err("the Firedancer report", fd))
    else:
        day = fd.get("_date")
        days = {"prev": 1, "day": 1, "week": 7}[p["vs"]]
        bday = (dt.date.fromisoformat(day) - dt.timedelta(days=days)).isoformat()
        me = next((v for v in fd.get("validators") or [] if v.get("leader") == ident), None)
        try:
            them = fd_day(bday).get(ident)
        except Exception:
            them = None
        if not me:
            out.append(ui.info(f"{html.escape(ident)} is not in the {day} report.", "warn"))
        else:
            g = lambda v, k: _f((v or {}).get(k))
            out.append(ui.kpi_row([
                tile("avg reward / block (SOL)", g(me, "avg_reward"), g(them, "avg_reward"),
                     _sol, "up", "fees + tips per produced block, from the report"),
                tile("avg fee / block (SOL)", g(me, "avg_fee"), g(them, "avg_fee"), _sol, "up"),
                tile("avg tips / block (SOL)", g(me, "avg_tips"), g(them, "avg_tips"), _sol, "up"),
                tile("blocks", g(me, "blocks"), g(them, "blocks"), _n, None),
                tile("skipped", g(me, "skipped"), g(them, "skipped"), _n, "down"),
                tile("skip rate", (g(me, "skip_rate") or 0) * 100,
                     (g(them, "skip_rate") or 0) * 100 if them else None, _pct, "down"),
            ]))
            out.append(ui.info(f"Report day <b>{day}</b> against <b>{bday}</b>"
                               + ("" if them else " (no baseline report for that day)")
                               + f". {html.escape(me.get('client') or '')} "
                               f"{html.escape(me.get('version') or '')} &middot; "
                               f"{html.escape(me.get('data_center_key') or '')}."))
    return "".join(out)


def _auction_tiles(rc, rb, titan=False):
    g = lambda bk: {
        "rounds": _sum(bk, "rounds"), "our": _sum(bk, "our_rounds"),
        "wins": _sum(bk, "our_wins"),
        "win": _ratio(bk, "our_wins", "our_rounds", 100),
        "share": _ratio(bk, "our_wins", "decided", 100),
        "twin": _ratio(bk, "titan_wins", "titan_rounds", 100),
        "winr": _ratio(bk, "win_sum", "win_n"),
        "best": _ratio(bk, "best_sum", "best_n"),
        "rej": _ratio(bk, "our_rej", "our_offers", 100),
        "tim": _ratio(bk, "our_timing", "our_offers", 100)}
    c, b = g(rc), g(rb)
    tiles = [
        tile("rounds opened", c["rounds"], b["rounds"], _n, None),
        tile("rounds we offered in", c["our"], b["our"], _n, "up"),
        tile("our win rate", c["win"], b["win"], _pct, "up",
             "our wins / rounds one of our builders offered in"),
        tile("our share of decided rounds", c["share"], b["share"], _pct, "up"),
        tile("Titan win rate", c["twin"], b["twin"], _pct, None),
        tile("avg winning reward / round (SOL)", c["winr"], b["winr"], _sol, "up"),
        tile("our avg reward / round (SOL)", c["best"], b["best"], _sol, "up",
             "our best accepted offer per round we offered in"),
        tile("our rejected offers", c["rej"], b["rej"], _pct, "down"),
        tile("our timing rejections", c["tim"], b["tim"], _pct, "down"),
    ]
    return ui.kpi_row(tiles)


def _auction_charts(rc, rb, sh):
    return ui.chart_grid([
        fig("our win rate", rc, rb, lambda k: _div(k["our_wins"], k["our_rounds"], 100), sh, "%"),
        fig("Titan win rate", rc, rb,
            lambda k: _div(k["titan_wins"], k["titan_rounds"], 100), sh, "%"),
        fig("avg winning reward per round (SOL)", rc, rb,
            lambda k: _div(k["win_sum"], k["win_n"], 1e-9), sh),
        fig("our avg reward per round (SOL)", rc, rb,
            lambda k: _div(k["best_sum"], k["best_n"], 1e-9), sh),
        fig("rounds opened", rc, rb, lambda k: k["rounds"], sh),
        fig("our timing rejections", rc, rb,
            lambda k: _div(k["our_timing"], k["our_offers"], 100), sh, "%"),
    ])


# ---------------------------------------------------------------- relay

def view_relay(p, rq, score, relay_id, live_html):
    bm, cur, base, sh = p["bucket"], p["cur"], p["base"], p["shift"]
    cond = f"instance_id = '{_q(relay_id)}'"
    r = _run({
        "rc": lambda: scope_relay(rq, score, cond, cur, bm),
        "rb": lambda: scope_relay(rq, score, cond, base, bm),
        "xc": lambda: relay_extra(rq, relay_id, cur, bm),
        "xb": lambda: relay_extra(rq, relay_id, base, bm),
        "why": lambda: relay_reasons(rq, relay_id, cur),
    })
    out = [live_html]
    out.append(ui.section("auction", "every round this relay opened"))
    rc, rb, xc, xb = r["rc"], r["rb"], r["xc"], r["xb"]
    if any(isinstance(x, Exception) for x in (rc, rb)):
        out.append(_err("the relay", rc if isinstance(rc, Exception) else rb))
    else:
        if not any(isinstance(x, Exception) for x in (xc, xb)):
            peak = lambda bk, c: max((k.get(c, 0) for k in bk.values()), default=0)
            out.append(ui.kpi_row([
                tile("connectors served (peak per bucket)", peak(xc, "conns"),
                     peak(xb, "conns"), _n, None),
                tile("leader slots auctioned", _sum(xc, "slots"), _sum(xb, "slots"), _n, None),
                tile("connector handshakes", _sum(xc, "handshakes"),
                     _sum(xb, "handshakes"), _n, "down",
                     "connector (re)connects to this relay"),
            ]))
        out.append(_auction_tiles(rc, rb))
        out.append(_auction_charts(rc, rb, sh))
    why = r["why"]
    if isinstance(why, Exception):
        out.append(_err("rejection reasons", why))
    elif why:
        out.append(ui.expander("our rejected offers on this relay, by reason", _table(
            ["builder", "reason", "offers"],
            [(html.escape(b), html.escape(x), f"{n:,}") for b, x, n in why], num=(2,)),
            open_=True))
    return "".join(out)


# ================================================================ freshness
#
# The BI dashboard's Freshness tab, scoped to what this view reads: how old
# the newest row is in each store. A window that ends "now" against a store
# that stopped ten minutes ago looks like a collapse that never happened.

def freshness(kind, ent, ch, ixs, rq, gq):
    E = _q(ent)
    day = "now() - INTERVAL 1 DAY"
    jobs = {}
    if kind == "builder":
        jobs["relay round log (its offers)"] = lambda: rq(
            f"SELECT toString(max(timestamp)) FROM {T} WHERE timestamp > {day} "
            f"AND builder_id = '{E}'")[1][0][0]
        jobs["our event log"] = lambda: ch(
            f"SELECT toString(max(ts)) FROM bifrost_events WHERE ts > {day} "
            f"AND instance_id = '{E}'")[1][0][0]
        host = health.INSTANCE_SIM_HOST.get(ent)
        if host:
            def ix_last():
                for ser in ixs('SELECT last("body_us") FROM "sim-commit" WHERE '
                               f"\"host_id\" = '{_q(host)}' AND time > now() - 1d"):
                    return ser["values"][-1][0]
                return ""
            jobs["simulator (Influx)"] = ix_last
    elif kind == "connector":
        jobs["relay rounds"] = lambda: rq(
            f"SELECT toString(max(timestamp)) FROM {T} WHERE timestamp > {day} "
            f"AND connector_identity = '{E}'")[1][0][0]
        jobs["heartbeat (slot_info)"] = lambda: gq(
            f"SELECT toString(max(start_at)) FROM solana.slot_info WHERE start_at > {day} "
            f"AND CAST(connector_pubkey AS String) = base58Decode('{E}')")[1][0][0]
        jobs["chain (block_summary)"] = lambda: gq(
            "SELECT toString(max(estimated_slot_time)) FROM solana.block_summary "
            f"WHERE estimated_slot_time > {day}")[1][0][0]
    else:
        jobs["relay round log"] = lambda: rq(
            f"SELECT toString(max(timestamp)) FROM {T} WHERE timestamp > {day} "
            f"AND instance_id = '{E}'")[1][0][0]
    got = _run(jobs)
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    tiles = []
    for name in jobs:
        v = got[name]
        if isinstance(v, Exception) or not v or str(v).startswith("1970"):
            tiles.append(ui.kpi(name, '<span class="udelta na">no rows</span>',
                                sub=html.escape(str(v)[:80]) if isinstance(v, Exception)
                                else "nothing in the last day"))
            continue
        t = str(v)[:19].replace("T", " ")
        try:
            age = (now - dt.datetime.strptime(t, F)).total_seconds()
        except ValueError:
            age = None
        cls = ("na" if age is None else "good" if age < 120 else
               "flat" if age < 900 else "bad")
        tiles.append(ui.kpi(
            name, f'<span class="udelta {cls}">{health._dur(max(int(age or 0), 0))} ago</span>',
            sub=f"newest row {html.escape(t)} UTC"))
    return ui.kpi_row(tiles)


# ================================================================ the page

def form(p, entities, names):
    e = html.escape
    kinds = "".join(f'<option value="{k}"{" selected" if k == p["kind"] else ""}>'
                    f"{e(label)}</option>" for k, label in KINDS)
    opts = []
    for ent in entities:
        nm = names.get(ent) or ""
        lab = f"{nm} — {ent[:8]}…" if nm else ent
        opts.append(f'<option value="{e(ent)}"{" selected" if ent == p["id"] else ""}>'
                    f"{e(lab)}</option>")
    wins = "".join(f'<option value="{h}"{" selected" if h == p["win"] else ""}>'
                   f"{e(label)}</option>" for h, label in WINDOWS)
    vss = "".join(f'<option value="{k}"{" selected" if k == p["vs"] else ""}>'
                  f"{e(label)}</option>" for k, label in VS)
    bks = ('<option value="">auto</option>' + "".join(
        f'<option value="{m}"{" selected" if not p["bucket_auto"] and m == p["bucket"] else ""}>'
        f'{m if m < 60 else m // 60} {"min" if m < 60 else "h"}</option>' for m in BUCKETS))
    return (
        '<form class="aqform bbform" method="get" action="' + e(PATH) + '">'
        f'<label>view<select name="kind" onchange="this.form.id.value=\'\';'
        f'this.form.submit()">{kinds}</select></label>'
        f'<label>{e(p["kind"])}<select name="id" onchange="this.form.submit()">'
        f'{"".join(opts)}</select></label>'
        f'<label>window<select name="win">{wins}</select></label>'
        f'<label>compare with<select name="vs">{vss}</select></label>'
        f'<label>bucket<select name="bucket">{bks}</select></label>'
        '<label>ending (UTC, blank = now)<input name="end" size="16" '
        f'value="{e(p["end_in"])}" placeholder="2026-11-09 12:00"></label>'
        "<button type=submit>run</button></form>")


PATH = "/tech/health"

CSS = """
.bbform{margin:14px 28px 4px}
.bbform select{max-width:340px}
.ks.m{font:11px ui-monospace,Menlo,monospace;color:#8fa6bf;word-break:break-all}
"""


def page(qs, ch, ixs, panic_src, rq, gq, rpc_url, names):
    """`ch` our ClickHouse, `ixs` Influx (all series), `panic_src(builder,
    window)` the panic lines (slack_panics, bound to a token), `rq` the
    relay's store (direct login), `gq` the relay's Grafana datasource (the
    only path to the solana db), `names` identity -> validator name."""
    p = parse(qs)
    try:
        if p["kind"] == "builder":
            ents = builders(ch)
        elif p["kind"] == "connector":
            ents = [c for c, _ in connectors(rq)]
        else:
            ents = relays(rq)
    except Exception as exc:
        return form(p, [], names) + _err("the entity list", exc)
    if p["kind"] == "builder" and not p["id"]:
        p["id"] = health.DEFAULT_INSTANCE if health.DEFAULT_INSTANCE in ents else (
            ents[0] if ents else "")
    if not p["id"] and ents:
        p["id"] = ents[0]
    if p["id"] and p["id"] not in ents:
        ents = [p["id"]] + ents
    out = [form(p, ents, names)]
    if p["err"]:
        return out[0] + ui.info(html.escape(p["err"]), "err")
    if not p["id"]:
        return out[0] + ui.info("Nothing to show: no entity of this kind was "
                                "active in the last 7 days.", "warn")
    cur, base = p["cur"], p["base"]
    label = dict(VS)[p["vs"]]
    out.append(ui.info(
        f"<b>{html.escape(p['id'])}</b>"
        + (f" ({html.escape(names[p['id']])})" if names.get(p["id"]) else "")
        + f" &middot; window <b>{cur[0]:%Y-%m-%d %H:%M}</b> &rarr; "
        f"<b>{cur[1]:%Y-%m-%d %H:%M}</b> UTC, compared with {label} "
        f"({base[0]:%m-%d %H:%M} &rarr; {base[1]:%m-%d %H:%M}). "
        "Every tile shows the window's value, how far it moved, and the "
        "baseline; <span class='udelta good'>green</span> is better, "
        "<span class='udelta bad'>red</span> worse, grey is no direction or "
        f"under 1%. Charts bucket by {p['bucket']} min; the dashed line is "
        "the baseline moved onto the window's clock."))
    out.append(ui.section("data freshness", "newest row in each store this "
                          "view reads; a builder's own clock can drift"))
    try:
        out.append(freshness(p["kind"], p["id"], ch, ixs, rq, gq))
    except Exception as exc:
        out.append(_err("freshness", exc))
    try:
        # the per-connector Jito weights behind it take ~5 s to read
        score = _cached("score", lambda: accuracy.score_sql(rq), ttl=900)
    except Exception:
        score = ("(protocol_fees + intDiv(jito_tips * "
                 f"{accuracy.DEFAULT_JITO_WEIGHT}, 10000) + builder_payment)")
    if p["kind"] == "builder":
        live = lambda: health.builder_live(ch, ixs, rq, rpc_url, p["id"])
        out.append(view_builder(p, ch, ixs, panic_src, rq, score, p["id"], live))
    elif p["kind"] == "connector":
        out.append(view_connector(p, rq, gq, score, rpc_url, p["id"], names))
    else:
        live = (ui.section("relay liveness", "all relays, today")
                + health.relay_section(rq))
        out.append(view_relay(p, rq, score, p["id"], live))
    out.append("<script>" + ui.CHART_JS.replace("__PLOTLY_CDN__", PLOTLY_CDN)
               + "</script>")
    return "".join(out)
