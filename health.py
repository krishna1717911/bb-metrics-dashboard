"""Connector & relay health -- three questions, one builder at a time.

Keyed by `instance_id` read live from ClickHouse rather than by the
deployment config. That is the point of the rewrite: the old page could only
show the four builders someone had put in a .env, so ASTRALANE_FRA_1 -- the
busiest instance in the store -- and the city-client hosts were invisible.
Anything writing rows now appears in the picker.

The three panels, and what each actually proves:

  building      the builder produced a miniblock for a slot that really
                happened recently. The slot number comes from our own store;
                the WALL-CLOCK time it happened comes from the chain, via
                getBlockTime. Deliberately not our own `ts` column -- that is
                the builder's clock, which drifts, and a stuck clock would
                otherwise read as a healthy builder.

  bank feed     how long after the connector says "sequencing" the bank for
                that slot actually freezes. This is the validator->builder
                feed, and it is the thing that goes slow before it goes dead.

  relay stream  whether the relay has this builder connected, and whether it
                has a connector of its own. A builder can be perfectly
                connected to a relay that is serving nobody, which is exactly
                the state the fleet has been in since 2026-10-08.

Query helpers (ch / ix / ot / rpc) are injected by app.py so this module
holds no credentials and opens no connections of its own.
"""

import datetime as dt
import html
import json
import os
import re
import concurrent.futures as cf
import threading
import time
import urllib.request


# ======================================================== CONFIG -- SWAP ME
#
# Every value here is overridable from the environment, so these can change
# without a code edit. The two thresholds are the ones that will want tuning
# first; they are in MILLISECONDS and they grade the bank feed.

def _int_env(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


# Bank feed: amber at or above MED, red at or above HIGH.
MED_BANK_FEED_TH_MS = _int_env("MED_BANK_FEED_TH_MS", 400)
HIGH_BANK_FEED_TH_MS = _int_env("HIGH_BANK_FEED_TH_MS", 900)

# How far back the bank-feed average looks.
BANK_FEED_WINDOW_MIN = _int_env("BANK_FEED_WINDOW_MIN", 180)

# "building" = the newest slot we hold a miniblock for happened less than
# this many minutes ago, measured on the chain's clock.
BUILDING_MAX_AGE_MIN = _int_env("BUILDING_MAX_AGE_MIN", 60)

# How far back to look for that newest miniblock at all. Only bounds the
# query; it does not loosen the rule above.
BUILDING_LOOKBACK_H = _int_env("BUILDING_LOOKBACK_H", 48)

# Relay health: how long a relay may log nothing before it stops counting
# as healthy. These relays emit every one to four seconds, so a quiet
# stretch is not idleness.
RELAY_HEALTHY_TH_S = _int_env("RELAY_HEALTHY_TH_S", 30)

# Which builder the page opens on. One line to change.
DEFAULT_INSTANCE = "ASTRALANE_AMS_2"

# How long a panel's answer is reused before it is asked again.
HEALTH_TTL_S = _int_env("HEALTH_TTL_S", 60)



def _map_env(name, default):
    """A instance->value map, overridable as `A=1,B=2` in one variable."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return dict(default)
    out = dict(default)
    for part in raw.split(","):
        k, _, v = part.partition("=")
        if k.strip():
            out[k.strip()] = v.strip()
    return out


# instance_id -> InfluxDB host_id.
#
# PROVENANCE. FRA_1 and JP_1 were DERIVED, not copied: sim-extend is each
# builder's own work, so its slot set identifies the host. Over 2026-10-08
# 10:00-11:00 UTC, H7JUzGxs matched FRA_1 on 288 of 288 slots and 0% against
# every other instance; 68a15jm6 matched JP_1 on 32 of 32, likewise 0%
# elsewhere.
#
# The three AMS hosts cannot be separated this way and remain copied from
# the box's .env: they serve the same connectors, so 4J1cfRfM, 5Gt3y6D6 and
# 5u4r7pVt all score 100% against MOCK, AMS_1 and AMS_2 alike. The repo's
# notes say that mapping was settled by clock domain, and a wrong pair there
# would silently report another builder's bank as this one's.
#
# The rest of this note stood before any of that was checked: these four
# pairs are copied from the dashboard box's .env (DEPLOY_*_SIM). They were
# never derived here. bank_frozen is tagged by host_id alone, bifrost_events
# carries no host_id, and the two stores share no other key -- so there is
# nothing to join on and nothing to verify against. The repo's own notes
# call this mapping "inferred, not labelled", settled originally by matching
# clock domains, and a wrong pair here would silently report another
# builder's bank as this one's. Treat it as an assumption with a blast
# radius, not as a lookup.
#
# Four further hosts write bank_frozen and belong to nobody here:
# 2YvrJK2o..., 6ZK2VNWv..., DaRKu4L3..., H7JUzGxs...
INSTANCE_SIM_HOST = _map_env("INSTANCE_SIM_HOST", {
    "ASTRALANE_AMS_1": "5u4r7pVt1vuKqxXx4QPd5H9mvw3PXS5VRZjPHDng8hRY",
    "ASTRALANE_AMS_2": "5Gt3y6D6xcrViHPc4hFyQqvcQQr64oCKm49UARzt2u3X",
    "ASTRALANE_JP_1": "68a15jm6T4q5D5aTE3xg5zAuLyBXP6ihCkgqpHPKJbB7",
    "ASTRALANE_AMS_MOCK": "4J1cfRfMPhD55iSxtvtZVXM67Qow8dHUpppEy6HSytLm",
    "ASTRALANE_FRA_1": "H7JUzGxs2iGx3Ntg7mF7aVhKziM9CBp57nL41ZYsLXpK",
    # Confirmed by the operator. Both spellings are carried because the store
    # holds each: ASTRALANE_FRA_MOCK_1 is the one still writing events, and
    # ASTRALANE_FRA_MOCK appears on a single older row.
    "ASTRALANE_FRA_MOCK_1": "2YvrJK2oq1xDZM74GRaZKeDPKKsEj7bdYVv5YooZJMgG",
    "ASTRALANE_FRA_MOCK": "2YvrJK2oq1xDZM74GRaZKeDPKKsEj7bdYVv5YooZJMgG",
})

# The relay is NOT configured. It is discovered -- see relay_service_for().
# An earlier cut of this file mapped instance -> ServiceName by hand, which
# got ASTRALANE_AMS_2 wrong: it was pointed at `ams-mainnet-solana-relay`
# purely because both names say "ams", and that relay has never once
# reported AMS_2. Guessing from a name is not a correlation.

# ========================================================== end of config

_cache = {}
_lock = threading.Lock()


def _cached(key, fn, cache_empty=True):
    """Memoise for HEALTH_TTL_S.

    `cache_empty=False` refuses to remember a falsy answer. clickhouse()
    turns an empty HTTP body into ([], []) with no error, so a single blink
    from the store is indistinguishable from a real "nothing here" -- and
    caching it pins a wrong, confident-looking page up for a full minute."""
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < HEALTH_TTL_S:
        return hit[1]
    val = fn()
    if not val and not cache_empty:
        return val
    with _lock:
        if len(_cache) > 64:
            _cache.clear()
        _cache[key] = (now, val)
    return val


def copy_val(value):
    """The value itself as the copy control -- see app.py's copy_val. The
    `.cpv` stylesheet and click handler are global (shell ships COPY_JS on
    every page), so matching the markup is all that is needed."""
    v = html.escape(str(value), quote=True)
    return (f'<span class="cpv" data-c="{v}" role="button" tabindex="0" '
            f'title="click to copy {v}">{v}</span>')


def _q(s):
    """Single-quote escape for a value going into SQL."""
    return str(s).replace("\\", "").replace("'", "")


def instances(ch):
    """Every instance_id writing events, newest activity first.

    From bifrost_events rather than bifrost_miniblocks: a builder that is up
    but building nothing still has to appear, since that is precisely the
    state worth seeing."""
    def once():
        # Our builders only. The city-client hosts -- frankfurt-rakurai,
        # london-jito, limburg-rakurai, frankfurt-jito -- write events but
        # are connector side, and none of the three panels has a source for
        # them, so they would only ever render three blank cards.
        _, rows = ch(
            "SELECT instance_id, dateDiff('second', max(ts), now()) AS lag_s "
            "FROM bifrost_events WHERE ts > now() - INTERVAL 24 HOUR "
            "  AND instance_id LIKE 'ASTRALANE%' "
            # Alphabetical, not by recency: the picker is a fixed list the
            # reader learns the shape of, and ordering by lag reshuffled it
            # between loads for no visible reason.
            "GROUP BY instance_id ORDER BY instance_id")
        return [{"id": r[0], "lag": int(r[1] or 0)} for r in rows if r[0]]

    def go():
        # An empty answer here is far more often a blink than an empty fleet
        # -- something is always writing events. Ask twice before believing
        # it, and (via cache_empty) never remember the empty one.
        got = once()
        return got if got else once()
    return _cached("instances", go, cache_empty=False)


# Everything below fetches the WHOLE fleet in one shot and is keyed by
# instance. The page used to query per builder on demand, which meant every
# change of the dropdown was a fresh page load and a fresh wait on three
# stores. One fleet-wide query per store costs barely more than one
# builder's -- the expensive part is the scan, not the GROUP BY -- and it
# makes switching instant.


def _in_list(ids):
    return ", ".join("'" + _q(i) + "'" for i in ids)


# ------------------------------------------------------------- 1. building

def block_time(rpc_url, slot):
    """getBlockTime -- the chain's own wall clock for a slot.

    getBlock would also carry it, but that response is megabytes and this
    needs eight bytes of it."""
    if not rpc_url:
        raise RuntimeError("SOLANA_RPC is not set, so a slot cannot be dated")
    req = urllib.request.Request(
        rpc_url, headers={"Content-Type": "application/json"},
        data=json.dumps({"jsonrpc": "2.0", "id": 1,
                         "method": "getBlockTime",
                         "params": [int(slot)]}).encode())
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read())
    if body.get("error"):
        raise RuntimeError(str(body["error"].get("message", ""))[:120])
    if body.get("result") is None:
        raise RuntimeError(f"no block time for slot {slot}")
    return int(body["result"])


def building_all(ch, rpc_url, ids):
    """Newest slot each builder holds a miniblock for, dated on the chain."""
    def go():
        _, rows = ch(
            "SELECT instance_id, max(slot) AS slot, count() AS n "
            "FROM bifrost_miniblocks "
            f"WHERE ts > now() - INTERVAL {BUILDING_LOOKBACK_H} HOUR "
            f"  AND instance_id IN ({_in_list(ids)}) "
            "GROUP BY instance_id")
        seen = {}
        for r in rows:
            if r[0] and r[1] and int(r[1]):
                seen[r[0]] = {"slot": int(r[1]), "rows": int(r[2] or 0)}
        # One RPC per distinct slot, in parallel: sequentially this was the
        # slowest thing on the page by a wide margin.
        slots = {v["slot"] for v in seen.values()}
        times, errs = {}, {}
        if slots:
            with cf.ThreadPoolExecutor(max_workers=8) as pool:
                futs = {pool.submit(block_time, rpc_url, sl): sl
                        for sl in slots}
                for f in cf.as_completed(futs):
                    sl = futs[f]
                    try:
                        times[sl] = f.result()
                    except Exception as exc:
                        errs[sl] = str(exc)[:120]
        now = dt.datetime.now(dt.UTC).timestamp()
        out = {}
        for i in ids:
            hit = seen.get(i)
            if not hit:
                out[i] = {"state": "no", "slot": None, "age_s": None,
                          "why": "no miniblock in the last "
                                 f"{BUILDING_LOOKBACK_H}h"}
                continue
            d = dict(hit)
            sl = d["slot"]
            if sl in times:
                age = now - times[sl]
                d.update(age_s=age, at=times[sl], why="",
                         state="yes" if age < BUILDING_MAX_AGE_MIN * 60
                         else "no")
            else:
                d.update(state="unknown", age_s=None,
                         why=f"slot {sl} could not be dated: "
                             f'{errs.get(sl, "no result")}')
            out[i] = d
        return out
    return _cached("building", go, cache_empty=False)


# ------------------------------------------------------------ 2. bank feed

# shred -> bank_frozen is a WITHIN-HOST delta: both rows are written by
# the same node, on the same clock, for the same slot. No reference host,
# no cross-host subtraction, nothing to argue about.
#
# It also answers a different question than the cross-host version did.
# JP_1 reads ~100 ms behind the fleet on bank_frozen alone, but 2.1 ms
# here -- the whole gap was shreds arriving later, not replay being slow.
SHRED_LAG_WINDOW_MIN = _int_env("SHRED_LAG_WINDOW_MIN", 30)
SHRED_LAG_MED_TH_MS = _int_env("SHRED_LAG_MED_TH_MS", 8)
SHRED_LAG_HIGH_TH_MS = _int_env("SHRED_LAG_HIGH_TH_MS", 20)
SHRED_REPAIR_DAYS = _int_env("SHRED_REPAIR_DAYS", 5)


def _by_host_slot(ix, meas, win):
    """{host: {slot: epoch_ms}} for one measurement."""
    out = {}
    for ser in ix(f'SELECT "slot" FROM "{meas}" '
                  f"WHERE time > now() - {win}m GROUP BY \"host_id\""):
        h = (ser.get("tags") or {}).get("host_id")
        if not h:
            continue
        col = {c: n for n, c in enumerate(ser["columns"])}
        m = {}
        for row in ser["values"]:
            sl = row[col["slot"]]
            if sl is None:
                continue
            m[int(sl)] = dt.datetime.fromisoformat(
                row[0].replace("Z", "+00:00")).timestamp() * 1000.0
        out[h] = m
    return out


def bank_feed_all(ix, ids):
    """Replay time: slot complete -> bank frozen, per host.

    `shred_insert_is_full` fires when the node has every shred for a slot;
    `bank_frozen` when it has finished replaying it. Both carry `slot` and
    are tagged `host_id`, so the subtraction stays inside one host and one
    clock. That is the whole point -- it measures the node's own work,
    where a cross-host comparison measures work plus distance."""
    def go():
        w = SHRED_LAG_WINDOW_MIN
        full = _by_host_slot(ix, "shred_insert_is_full", w)
        frozen = _by_host_slot(ix, "bank_frozen", w)
        out = {}
        for i in ids:
            host = INSTANCE_SIM_HOST.get(i, "")
            if not host:
                out[i] = {"n": 0, "why": "no simulator host_id is mapped "
                                         "(set INSTANCE_SIM_HOST)"}
                continue
            a, b = full.get(host) or {}, frozen.get(host) or {}
            shared = set(a) & set(b)
            if len(shared) < 20:
                out[i] = {"n": len(shared),
                          "why": f"only {len(shared)} slots have both a "
                                 f"shred-complete and a freeze in {w}m"}
                continue
            d = sorted(b[sl] - a[sl] for sl in shared)
            q = lambda f: d[min(len(d) - 1, int(len(d) * f))]
            out[i] = {"n": len(d), "p10": q(.10), "p50": q(.50),
                      "p90": q(.90), "lo": d[0], "hi": d[-1], "host": host}
        return out
    return _cached("shredlag", go, cache_empty=False)


def shred_repairs_all(ix, ids):
    """Average repaired and recovered shreds per slot, per day.

    Two different things and the difference matters. `num_recovered` is
    erasure recovery -- the node rebuilt the shred locally from the coding
    shreds it did get. `num_repaired` means it had to go and ASK a peer for
    the shred, which is the slower path and the one that signals a weak
    connection to the leader."""
    def go():
        days = SHRED_REPAIR_DAYS
        series = ix('SELECT mean("num_repaired"), mean("num_recovered"), '
                    'count("slot") FROM "shred_insert_is_full" '
                    f"WHERE time > now() - {days}d "
                    'GROUP BY time(1d), "host_id" fill(none)')
        per = {}
        for ser in series:
            h = (ser.get("tags") or {}).get("host_id")
            if not h:
                continue
            col = {c: n for n, c in enumerate(ser["columns"])}
            for row in ser["values"]:
                per.setdefault(h, []).append(
                    {"day": str(row[0])[:10],
                     "repaired": row[col["mean"]],
                     "recovered": row[col["mean_1"]],
                     "slots": row[col["count"]]})
        out = {}
        for i in ids:
            host = INSTANCE_SIM_HOST.get(i, "")
            rows = sorted(per.get(host) or [], key=lambda r: r["day"])
            if not host:
                out[i] = {"days": [], "why": "no simulator host_id is mapped"}
            elif not rows:
                out[i] = {"days": [], "why": f"no shred rows in {days}d"}
            else:
                rep = [r["repaired"] for r in rows if r["repaired"] is not None]
                rec = [r["recovered"] for r in rows
                       if r["recovered"] is not None]
                out[i] = {"days": rows, "host": host,
                          "avg_repaired": sum(rep) / len(rep) if rep else None,
                          "avg_recovered": sum(rec) / len(rec) if rec else None}
        return out
    return _cached("shredrepair", go, cache_empty=False)


def bank_grade(ms):
    """Graded on the median replay time."""
    if ms is None:
        return "dim"
    if ms >= SHRED_LAG_HIGH_TH_MS:
        return "dead"
    if ms >= SHRED_LAG_MED_TH_MS:
        return "warn"
    return "good"


PANIC_BODY = "winner order bytes invariant failed:"
PANIC_LOOKBACK_D = _int_env("PANIC_LOOKBACK_D", 14)

# The histogram window, and how many panics per builder carry their full
# detail. The counts are exact whatever the cap -- they come from a separate
# aggregate -- so a capped day still plots at its true height and says so.
PANIC_DAYS = _int_env("PANIC_DAYS", 5)
PANIC_DETAIL_CAP = _int_env("PANIC_DETAIL_CAP", 60)


def _parse_panic(body):
    """The message is flat `k=v` pairs, two of which are comma lists.

    Shape:
      winner order bytes invariant failed: slot=N index=N uuid=U
      resolution_attempts=N resolution_wait_us=N
      missing_tx_sig_prefixes=a,b,c missing_bundle_ids=x,y,z

    Both lists can be absent, and the sig list runs to hundreds of entries
    -- the longest body seen is 12 KB."""
    out = {}
    for key in ("slot", "index", "uuid", "resolution_attempts",
                "resolution_wait_us"):
        m = re.search(rf"\b{key}=(\S+)", body)
        out[key] = m.group(1) if m else ""
    for key, dest in (("missing_tx_sig_prefixes", "sigs"),
                      ("missing_bundle_ids", "bundles")):
        m = re.search(rf"\b{key}=([^\s]+)", body)
        out[dest] = [x for x in m.group(1).split(",") if x] if m else []
    return out


# ---------------------------------------------------- restarts, not panics
#
# The otel login died on 2026-10-17 and the panic TEXT exists nowhere else
# -- CLAUDE.md notes the panic outruns the async event sink, so
# bifrost_events never carried it. What bifrost_events DOES carry is
# `run_id`, a fresh UUID per process start, so a death is still visible
# even though its cause is not.
#
# This answers the question the panic card was really asked -- did this
# builder die -- and is strictly WIDER: it catches every exit, panic or
# not. It cannot say why, and it must not pretend to.
#
# A run is counted only above a floor of events, so a partial first/last
# bucket at the window edge does not read as a restart.
RUN_MIN_EVENTS = 50
# A process that died this fast did not do useful work; that is a crash
# loop, not a deploy, and it is the shape most worth surfacing.
CRASHLOOP_MIN = 15


def _runs(ch, ids, days):
    """-> {instance: [{run_id, started, ended, events, mins}, ...]} oldest first."""
    _, rows = ch(
        "SELECT instance_id, run_id, toString(min(ts)) AS started, "
        "  toString(max(ts)) AS ended, count() AS n "
        f"FROM bifrost_events WHERE ts > now() - INTERVAL {int(days)} DAY "
        f"  AND instance_id IN ({_in_list(ids)}) "
        f"GROUP BY instance_id, run_id HAVING n > {RUN_MIN_EVENTS} "
        "ORDER BY instance_id, started")
    out = {}
    for inst, rid, started, ended, n in rows:
        a, b = _parse_utc_str(started), _parse_utc_str(ended)
        out.setdefault(inst, []).append({
            "run_id": rid, "started": started, "ended": ended,
            "events": int(n or 0),
            "mins": ((b - a).total_seconds() / 60.0) if a and b else None})
    return out


def restarts_all(ch, ids):
    """Per builder: restarts today, over the window, and the most recent.

    Same shape as the panic card it replaces, so the renderer is unchanged.
    The FIRST run in the window is not a restart -- it is just the oldest
    run still visible -- so the count is len(runs) - 1."""
    def go():
        runs = _runs(ch, ids, PANIC_LOOKBACK_D)
        today = dt.datetime.now(dt.UTC).date()
        got = {}
        for i in ids:
            rs = runs.get(i, [])
            starts = [r["started"] for r in rs[1:]]        # restarts only
            n_today = sum(1 for t in starts
                          if (_parse_utc_str(t) or dt.datetime.now(dt.UTC)
                              ).date() == today)
            short = [r for r in rs if r["mins"] is not None
                     and r["mins"] < CRASHLOOP_MIN]
            got[i] = {"today": n_today, "recent": len(starts),
                      "last": starts[-1] if starts else "",
                      "up_since": rs[-1]["started"] if rs else "",
                      "crashloop": len(short)}
        return got
    return _cached("restarts", go, cache_empty=False)


def restart_history(ch, ids):
    """Per-day restart counts plus the run list, for the chart."""
    def go():
        runs = _runs(ch, ids, PANIC_DAYS)
        today = dt.datetime.now(dt.UTC).date()
        days = [(today - dt.timedelta(days=k)).isoformat()
                for k in range(PANIC_DAYS - 1, -1, -1)]
        out = {}
        for i in ids:
            rs = runs.get(i, [])
            counts, detail = {}, {}
            for r in rs[1:]:
                at = _parse_utc_str(r["started"])
                if not at:
                    continue
                d = at.date().isoformat()
                counts[d] = counts.get(d, 0) + 1
                detail.setdefault(d, []).append({
                    "ts": r["started"], "run_id": r["run_id"],
                    "events": r["events"],
                    "lived": (_dur(r["mins"] * 60) if r["mins"] is not None
                              else ""),
                    "short": (r["mins"] is not None
                              and r["mins"] < CRASHLOOP_MIN)})
            out[i] = {"days": days, "counts": counts, "detail": detail}
        return out
    return _cached("restarthist", go, cache_empty=False)


def restart_card(d):
    n = d["today"]
    rows = []
    if d.get("up_since"):
        at = _parse_utc_str(d["up_since"])
        age = (dt.datetime.now(dt.UTC) - at).total_seconds() if at else None
        rows.append(("up since",
                     f'{_local_dt(at)} <span class="dim">&middot; '
                     f'{_dur(age)}</span>' if at else "&mdash;"))
    if d.get("last"):
        at = _parse_utc_str(d["last"])
        age = (dt.datetime.now(dt.UTC) - at).total_seconds() if at else None
        rows.append(("last restart",
                     f'{_local_dt(at)} <span class="dim">&middot; '
                     f'{_dur(age)} ago</span>' if at else "&mdash;"))
    rows.append((f'in the last {PANIC_LOOKBACK_D}d', f'{d["recent"]:,}'))
    if d.get("crashloop"):
        rows.append(("died inside %d min" % CRASHLOOP_MIN,
                     f'<b class="bad">{d["crashloop"]}</b>'))
    state = "good" if n == 0 and not d.get("crashloop") else "dead"
    return _card(
        "restarts today", f"{n:,}", state,
        "process starts seen in bifrost_events, since 00:00 UTC", rows,
        note="A fresh run_id means the process restarted. This replaces the "
             "panic card: the otel login that held the panic TEXT was "
             "retired on 2026-10-17, and the panic outruns the event sink "
             "so bifrost_events never carried it. This is wider than a "
             "panic count &mdash; it catches every exit, deploy included "
             "&mdash; and it cannot tell you which was which. A run that "
             "died inside %d minutes is the one to open." % CRASHLOOP_MIN)



def panic_history(ot, ids):
    """Per-day counts over the window, plus each panic's missing refs.

    Counts and detail are two queries on purpose. The detail is capped per
    builder because one panic body can be 12 KB and a bad day ran twenty of
    them; the counts are a plain aggregate, so the bars stay honest even
    when the list under them is clipped."""
    def go():
        since = f"toStartOfDay(now() - INTERVAL {PANIC_DAYS - 1} DAY)"
        rows = ot("SELECT ServiceName AS s, toString(toDate(Timestamp)) AS d, "
                  "  count() AS n FROM otel_logs "
                  f"WHERE ServiceName IN ({_in_list(ids)}) "
                  f"  AND Body ILIKE '{PANIC_BODY}%' "
                  f"  AND Timestamp >= {since} "
                  "GROUP BY s, d")
        counts = {}
        for r in rows:
            if r[0]:
                counts.setdefault(r[0], {})[r[1]] = int(r[2] or 0)
        rows = ot("SELECT ServiceName AS s, toString(Timestamp) AS ts, "
                  "  toString(toDate(Timestamp)) AS d, Body AS b "
                  "FROM otel_logs "
                  f"WHERE ServiceName IN ({_in_list(ids)}) "
                  f"  AND Body ILIKE '{PANIC_BODY}%' "
                  f"  AND Timestamp >= {since} "
                  f"ORDER BY ts DESC LIMIT {PANIC_DETAIL_CAP} BY ServiceName")
        detail = {}
        for r in rows:
            if not r[0]:
                continue
            ev = _parse_panic(r[3])
            ev["ts"] = r[1]
            detail.setdefault(r[0], {}).setdefault(r[2], []).append(ev)
        today = dt.datetime.now(dt.UTC).date()
        days = [(today - dt.timedelta(days=k)).isoformat()
                for k in range(PANIC_DAYS - 1, -1, -1)]
        return {i: {"days": days, "counts": counts.get(i, {}),
                    "detail": detail.get(i, {})} for i in ids}
    return _cached("panichist", go, cache_empty=False)


def panics_all(ot, ids):
    """`winner order bytes invariant failed` per builder, today and recent.

    The builder hits this when a winner's order_refs name bytes it cannot
    resolve from OrderBytesCache and the assert in queue_winner_commit
    fires. A crash, not a warning, so any non-zero value is worth a look.

    The lookback figure rides along so a quiet day still says when the last
    one was: `0 today` alone cannot tell a fixed bug from a builder that has
    run no winner commit all day, which is the state everything but the mock
    is in right now."""
    def go():
        rows = ot(
            "SELECT ServiceName AS s, "
            "  countIf(Timestamp >= toStartOfDay(now())) AS today, "
            "  count() AS recent, toString(max(Timestamp)) AS last "
            "FROM otel_logs "
            f"WHERE ServiceName IN ({_in_list(ids)}) "
            f"  AND Body ILIKE '{PANIC_BODY}%' "
            f"  AND Timestamp > now() - INTERVAL {PANIC_LOOKBACK_D} DAY "
            "GROUP BY s")
        got = {}
        for r in rows:
            if not r[0]:
                continue
            n = int(r[2] or 0)
            got[r[0]] = {"today": int(r[1] or 0), "recent": n,
                         "last": r[3] if n else ""}
        return {i: got.get(i, {"today": 0, "recent": 0, "last": ""})
                for i in ids}
    return _cached("panics", go, cache_empty=False)


# ---------------------------------------------------------- 4. win rate
#
# The method is builder_scorecard.py report 1, ported to the relay's CURRENT
# table. That script still reads `relay.mini_block_events`, which stopped
# being written on 2026-10-09 when the relay moved to `_v2`, so running it
# unchanged now returns zeros for every builder.
#
# What changed in v2, and why this query looks different:
#   * `offer_best` / `offer_not_best` / `offer_rejected` are gone -- the
#     verdict is the `reason` column on the `submitted` row.
#   * `tail_offer_accepted` is gone -- a tail win is `submitted` with
#     reason='tail'. Still exclusive: checked over 6h, every tail round had
#     exactly one builder, which is what made it a win marker in v1.
#   * `round_chosen` no longer carries `builder_id` (blank on 100% of rows),
#     so the winner is recovered by joining `mini_block_uuid` back to the
#     `submitted` row that produced it.
#
# The headline is win% CONTESTED -- wins over the rounds that builder
# actually bid on. The scorecard is explicit that this is the only figure
# comparable across regions, because a window-wide denominator counts rounds
# on connectors a builder never sees.
WINRATE_HOURS = _int_env("WINRATE_HOURS", 24)

_REGION_ALIASES = {"JP": "TYO"}


def _region(builder_id):
    m = re.search(r"_(AMS|FRA|TYO|JP|LON|NY|SG)(?:_|$)", builder_id or "")
    return _REGION_ALIASES.get(m.group(1), m.group(1)) if m else "?"


def win_rates(relay):
    """builder -> offers, contested rounds, round wins, tail wins."""
    def go():
        win = f"timestamp > now() - INTERVAL {WINRATE_HOURS} HOUR"
        _, rows = relay(
            "SELECT builder_id AS b, countIf(event = 'submitted') AS offered, "
            "  uniqExactIf((slot, index_in_slot), event = 'submitted') "
            "    AS contested, "
            "  uniqExactIf((slot, index_in_slot), event = 'submitted' "
            "    AND reason = 'tail') AS tail_wins "
            f"FROM relay.mini_block_events_v2 WHERE {win} AND builder_id != '' "
            "GROUP BY b")
        out = {}
        for b, offered, contested, tail in rows:
            if not b:
                continue
            out[b] = {"offered": int(offered or 0),
                      "contested": int(contested or 0),
                      "tail_wins": int(tail or 0), "round_wins": 0,
                      "region": _region(b)}
        _, rows = relay(
            "SELECT w.builder_id AS b, count() AS round_wins "
            "FROM relay.mini_block_events_v2 AS c "
            "INNER JOIN (SELECT mini_block_uuid, any(builder_id) AS builder_id "
            "            FROM relay.mini_block_events_v2 "
            f"           WHERE {win} AND event = 'submitted' "
            "            GROUP BY mini_block_uuid) AS w "
            "  ON c.mini_block_uuid = w.mini_block_uuid "
            f"WHERE c.{win} AND c.event = 'round_chosen' GROUP BY b")
        for b, wins in rows:
            if b in out:
                out[b]["round_wins"] = int(wins or 0)
        for v in out.values():
            v["wins"] = v["round_wins"] + v["tail_wins"]
            v["pct"] = (100.0 * v["wins"] / v["contested"]
                        if v["contested"] else None)
        return out
    return _cached("winrate", go, cache_empty=False)


# The relays, and only the relays. ServiceName is 'relay' for all five;
# what tells sol-relay-lat-fra from sol-relay-2-lat-ams is host.name, so
# that is the identity used throughout.
RELAY_SVC = "ResourceAttributes['service.name'] = 'relay'"
RELAY_ID = "ResourceAttributes['host.name']"


def relay_health(rq):
    """Per relay: last event, and the longest it went silent today.

    Reads the relay's OWN event stream, not otel. Two reasons, and the
    second is why this is an improvement rather than a workaround:

      * otel died with the relay_analyst credential on 2026-10-17 and
        there is no replacement login.
      * the otel verdict was never trustworthy. Its logs do not enumerate
        every connected builder, which is how this page once concluded no
        relay served ASTRALANE_FRA_1 while FRA_1 was submitting 715,509
        offers. mini_block_events_v2 IS the relay's work, so a relay that
        is carrying rounds cannot be absent from it.

    `instance_id` is the relay host (sol-relay-lat-fra, sol-relay-2-lat-ams
    and so on), which is the identity this table is keyed by -- the same
    role host.name played in otel.

    Bucketing to the second before the window function keeps this to ~86k
    rows per relay instead of the raw event stream. `lagInFrame(sec, 1,
    sec)` defaults to the row's own value, so the first row of each
    partition yields gap 0 rather than the whole Unix epoch."""
    def go():
        rows = rq(
            "SELECT relay, toString(max(sec)) AS last_sec, "
            "  dateDiff('second', toDateTime(max(sec)), now()) AS last_age, "
            "  max(gap) AS max_gap "
            "FROM (SELECT relay, sec, "
            "        sec - lagInFrame(sec, 1, sec) OVER ("
            "          PARTITION BY relay ORDER BY sec "
            "          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS gap "
            "      FROM (SELECT instance_id AS relay, "
            "              toUnixTimestamp(timestamp) AS sec "
            "            FROM relay.mini_block_events_v2 "
            "            WHERE timestamp >= toStartOfDay(now()) "
            "            GROUP BY relay, sec)) "
            "GROUP BY relay ORDER BY relay")[1]
        out = {}
        for r in rows:
            if not r[0]:
                continue
            out[r[0]] = {"relay": r[0], "last_epoch": int(r[1] or 0),
                         "last_age": int(r[2] or 0),
                         "max_gap": int(r[3] or 0)}
        return out
    return _cached("relayhealth", go, cache_empty=False)


def relay_names(rq):
    """Every relay seen in the last 24h.

    Kept separate from the stats so a relay that has logged NOTHING today
    still gets a row saying so, rather than silently vanishing from the
    table -- which is the one case the table most needs to show."""
    def go():
        rows = rq("SELECT DISTINCT instance_id AS relay "
                  "FROM relay.mini_block_events_v2 "
                  "WHERE timestamp > now() - INTERVAL 24 HOUR "
                  "ORDER BY relay")[1]
        return [r[0] for r in rows if r and r[0]]
    return _cached("relaynames", go, cache_empty=False)


# ------------------------------------------------------------------ render

CSS = """
.hsel{display:flex;gap:10px;align-items:center;padding:14px 28px;
  border-bottom:1px solid #1e2937;flex-wrap:wrap}
.hsel label{color:#6b7f96;font-size:11px;text-transform:uppercase;
  letter-spacing:.04em}
.hsel select{background:#0e151d;border:1px solid #22303f;border-radius:6px;
  color:#cfe0f0;padding:6px 11px;font:12px ui-monospace,Menlo,monospace;
  min-width:260px}
.hsel select:focus{outline:none;border-color:#5eead4}
.hsel button{background:#0f766e;border:1px solid #14b8a6;color:#eafffb;
  border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer}
.bpanel[hidden]{display:none}
.hsel .lagn{color:#4d5c70;font-size:11px}
/* Four cards, one row, on any screen wide enough to hold them. The grid was
   capped at 1180px with a 330px minimum, so the fourth card always wrapped
   onto a line of its own and left most of the width empty. auto-fit still
   reflows to 2 or 1 column on a narrow viewport. */
/* Fixed column counts, so each row keeps its shape: three on the first,
   two on the second. They collapse to one column on a narrow viewport. */
.hgrid{display:grid;gap:14px;padding:20px 28px 0}
.hgrid.g3{grid-template-columns:repeat(3,minmax(0,1fr))}
.hgrid.g2{grid-template-columns:repeat(2,minmax(0,1fr));padding-top:14px;
  padding-bottom:50px}
@media (max-width:1100px){
  .hgrid.g3,.hgrid.g2{grid-template-columns:repeat(2,minmax(0,1fr))}
}
@media (max-width:720px){
  .hgrid.g3,.hgrid.g2{grid-template-columns:minmax(0,1fr)}
}
.hcard{border:1px solid #22303f;border-radius:10px;background:#0e151d;
  padding:16px 18px}
.hcard h3{margin:0;font-size:11px;text-transform:uppercase;
  letter-spacing:.05em;color:#6b7f96;font-weight:650}
.hbig{font-size:27px;font-weight:650;margin-top:9px;letter-spacing:-.02em}
.hbig.good{color:#5eead4}
.hbig.warn{color:#fbbf24}
.hbig.dead{color:#fca5a5}
.hbig.dim{color:#6b7f96}
.hsub{color:#8fa6bf;font-size:12px;margin-top:7px;line-height:1.55}
.hrow{display:flex;justify-content:space-between;gap:12px;margin-top:6px;
  font-size:12px;border-top:1px solid #16202b;padding-top:6px}
.hrow span:first-child{color:#6b7f96}
.hrow span:last-child{color:#cfe0f0;font:11.5px ui-monospace,Menlo,monospace}
.hnote{margin-top:11px;color:#6b7f96;font-size:11px;line-height:1.55;
  font-style:italic}
.herr{margin-top:9px;color:#fca5a5;font-size:12px;line-height:1.55}
.hth{color:#4d5c70;font-size:10.5px;margin-top:9px;
  font:10.5px ui-monospace,Menlo,monospace}
.hsec{margin:26px 28px 0;font-size:11px;text-transform:uppercase;
  letter-spacing:.06em;color:#8fa6bf;font-weight:650;
  border-bottom:1px solid #1e2937;padding-bottom:9px}
.rtab{margin:14px 28px 4px;border-collapse:collapse;max-width:1180px;
  width:calc(100% - 56px);font-size:12.5px}
.rtab th{text-align:left;color:#6b7f96;font-size:10.5px;font-weight:600;
  text-transform:uppercase;letter-spacing:.04em;padding:0 14px 7px 0;
  border-bottom:1px solid #1e2937;vertical-align:bottom}
.rtab th.n,.rtab td.n{text-align:right}
.rtab td{padding:9px 14px 9px 0;border-bottom:1px solid #16202b;
  color:#cfe0f0}
.rtab td.m{font:11.5px ui-monospace,Menlo,monospace}
.rtab .dim{color:#6b7f96}
/* The deployment table carries prose in its last column, so it drops the
   monospace and gets room to wrap -- a branch description squeezed into a
   code-width cell is unreadable. */
.dtab{max-width:1180px}
.dtab td.dnote{font:12px/1.55 inherit;color:#8fa6bf;min-width:240px;
  padding-right:0}
.dtab tr.now td{color:#cfe0f0}
.dtab tr.now td:first-child::after{content:"current";margin-left:8px;
  font:9.5px ui-sans-serif,sans-serif;text-transform:uppercase;
  letter-spacing:.04em;color:#5eead4;border:1px solid #14b8a655;
  border-radius:4px;padding:1px 5px}
.dbuild{margin:12px 28px;border:1px solid #22303f;border-radius:10px;
  background:#0e151d;max-width:1180px}
.dbuild>summary{cursor:pointer;padding:13px 17px;display:flex;gap:12px;
  align-items:baseline;flex-wrap:wrap;list-style:none}
.dbuild>summary::-webkit-details-marker{display:none}
.dbuild>summary::before{content:"\25b8";color:#4d5c70;font-size:11px}
.dbuild[open]>summary::before{content:"\25be"}
.dbuild>summary b{color:#5eead4;font:12.5px ui-monospace,Menlo,monospace}
.dbuild>summary:hover b{text-decoration:underline}
.dsum{color:#8fa6bf;font-size:12px}
.dcount{margin-left:auto;color:#4d5c70;font-size:11px}
/* Branch links had no rule at all, so they fell back to the browser's
   default link blue -- the one colour on the page that belongs to no part
   of this palette. Teal is the accent everything else uses, with a faint
   rule under it so it still reads as a link rather than as emphasis. */
.dtab a,.dsum a{color:#5eead4;text-decoration:none;
  border-bottom:1px solid #14b8a644}
.dtab a:hover,.dsum a:hover,.dtab a:focus,.dsum a:focus{
  border-bottom-color:#5eead4;outline:none}
.dbuild .rtab{margin:0 17px 14px;width:calc(100% - 34px)}
.dapi{margin:16px 28px;border:1px solid #22303f;border-radius:10px;
  background:#0e151d;max-width:1180px}
.dapi>summary{cursor:pointer;padding:12px 17px;color:#5eead4;font-size:12px}
.dapib{padding:0 17px 14px;color:#8fa6bf;font-size:12.5px;line-height:1.6}
.dapib pre{background:#0b1016;border:1px solid #1e2937;border-radius:7px;
  padding:11px 13px;overflow:auto;color:#cfe0f0;
  font:11.5px/1.65 ui-monospace,Menlo,monospace}
.dapib code{color:#5eead4;font:11.5px ui-monospace,Menlo,monospace}
.dwarn{border-left:2px solid #78350f;padding-left:11px;color:#fcd9a4}
.rv{font-weight:650;font-size:11px;text-transform:uppercase;
  letter-spacing:.04em}
.rv.good{color:#5eead4}
.rv.warn{color:#fbbf24}
.rv.dead{color:#fca5a5}

/* ---- panic histogram ---- */
.pchart{margin:16px 28px 10px;max-width:760px}
.pchart svg{width:100%;height:118px;display:block;overflow:visible}
/* One series, so one hue. #fca5a5 is the dashboard's existing "bad" step
   and clears 3:1 against this surface; panics are never good news, so a
   magnitude ramp in a neutral hue would understate what is being read. */
.pbar{fill:#fca5a5;fill-opacity:.62;transition:fill-opacity .12s}
.pbar.has{cursor:pointer}
.pbar.has:hover{fill-opacity:.92}
.pbar.on{fill-opacity:1;stroke:#0e151d;stroke-width:.5}
.pbar.zero{fill:#22303f;fill-opacity:1}
.pnum{fill:#cfe0f0;font:4.4px ui-monospace,Menlo,monospace;
  text-anchor:middle}
.paxis{display:flex;margin-top:5px}
.pax{flex:1;text-align:center;display:flex;flex-direction:column;gap:1px}
.pax b{color:#8fa6bf;font:11px ui-monospace,Menlo,monospace;font-weight:500}
.pax span{color:#4d5c70;font-size:10px}
.pday{margin-top:14px;border-top:1px solid #1e2937;padding-top:12px}
.pdayh{font-size:12px;color:#cfe0f0;font-weight:650;margin-bottom:9px}
.pev{border:1px solid #22303f;border-radius:8px;background:#0b1016;
  padding:11px 13px;margin-bottom:9px}
.pevh{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;
  margin-bottom:8px}
.pevh b{color:#fca5a5;font:11.5px ui-monospace,Menlo,monospace}
.pevh span{color:#6b7f96;font-size:11px}
.plist{margin-top:7px}
.plh{font-size:10.5px;text-transform:uppercase;letter-spacing:.04em;
  color:#6b7f96;display:flex;gap:8px;align-items:center;margin-bottom:3px}
.plist code{display:block;color:#8fa6bf;font:10.5px ui-monospace,Menlo,monospace;
  background:#0e151d;border:1px solid #1e2937;border-radius:6px;
  padding:7px 9px;max-height:76px;overflow:auto;word-break:break-all;
  line-height:1.5}
.pcopy{background:#0f766e22;border:1px solid #14b8a655;color:#5eead4;
  border-radius:5px;padding:1px 8px;font-size:10px;cursor:pointer}
.pcopy:hover{background:#0f766e44}
"""


def _dur(sec):
    if sec is None:
        return "&mdash;"
    sec = int(sec)
    if sec < 90:
        return f"{sec}s"
    if sec < 5400:
        return f"{sec // 60}m {sec % 60}s"
    return f"{sec // 3600}h {(sec % 3600) // 60}m"


def picker(insts, cur):
    opts = []
    for i in insts:
        sel = " selected" if i["id"] == cur else ""
        opts.append(f'<option value="{html.escape(i["id"])}"{sel}>'
                    f'{html.escape(i["id"])}</option>')
    # A bare select, not a form. Every builder's cards are already in the
    # page, so choosing one is page state rather than a new address -- there
    # is nothing to put in a URL and nothing to go and fetch.
    return ('<div class="hsel"><label for=i>builder</label>'
            '<select id=i>' + "".join(opts) + "</select>"
            f'<span class="lagn">{len(insts)} builders &middot; all loaded'
            "</span></div>")


HEALTH_JS = r'''
(function(){
  // Every builder's cards are already in the DOM -- one fleet-wide query
  // per store built them all. Switching is show/hide: no request, and
  // nothing that can wait on a store.
  var sel = document.getElementById('i');
  if (!sel) return;
  sel.addEventListener('change', function(){
    var panels = document.querySelectorAll('.bpanel');
    for (var k = 0; k < panels.length; k++)
      panels[k].hidden = panels[k].getAttribute('data-inst') !== sel.value;
  });

  // One delegated listener on the document: the panels for every builder
  // are already in the DOM, so binding per bar would wire up six charts'
  // worth of handlers for the one chart on screen.
  document.addEventListener('click', function(ev){
    var bar = ev.target.closest && ev.target.closest('.pbar.has');
    if (bar) {
      var inst = bar.getAttribute('data-inst'),
          day = bar.getAttribute('data-day');
      var panes = document.querySelectorAll('.pday[data-inst="' + inst + '"]');
      var bars = document.querySelectorAll('.pbar[data-inst="' + inst + '"]');
      var open = false;
      for (var k = 0; k < panes.length; k++) {
        var mine = panes[k].getAttribute('data-day') === day;
        // Clicking the open day closes it, so the chart is never stuck
        // showing a day you have finished with.
        var show = mine && panes[k].hidden;
        if (mine) open = show;
        panes[k].hidden = !show;
      }
      for (var j = 0; j < bars.length; j++)
        bars[j].classList.toggle(
          'on', open && bars[j].getAttribute('data-day') === day);
      return;
    }
    var btn = ev.target.closest && ev.target.closest('.pcopy');
    if (!btn) return;
    var text = btn.getAttribute('data-copy') || '';
    var done = function(){
      var was = btn.textContent;
      btn.textContent = 'copied';
      setTimeout(function(){ btn.textContent = was; }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, function(){});
    } else {
      // file:// and older browsers have no async clipboard; this path is
      // the only one that works there.
      var ta = document.createElement('textarea');
      ta.value = text; document.body.appendChild(ta); ta.select();
      try { document.execCommand('copy'); done(); } catch (e) {}
      document.body.removeChild(ta);
    }
  });
})();
'''


def _card(title, big, grade, sub, rows=(), note="", err=""):
    out = [f'<div class="hcard"><h3>{title}</h3>'
           f'<div class="hbig {grade}">{big}</div>']
    if sub:
        out.append(f'<div class="hsub">{sub}</div>')
    for k, v in rows:
        out.append(f'<div class="hrow"><span>{k}</span><span>{v}</span></div>')
    if err:
        out.append(f'<div class="herr">{err}</div>')
    if note:
        out.append(f'<div class="hnote">{note}</div>')
    return "".join(out) + "</div>"


def building_card(d):
    if d.get("state") == "yes":
        grade, big = "good", "yes"
    elif d.get("state") == "unknown":
        grade, big = "dim", "?"
    else:
        grade, big = "dead", "no"
    rows = []
    if d.get("slot"):
        # A slot is an identifier, not a quantity: no thousands separator, so
        # it pastes straight into a query, and a copy control beside it.
        rows.append(("newest miniblock slot", copy_val(d["slot"])))
    if d.get("age_s") is not None:
        rows.append(("that slot happened", _dur(d["age_s"]) + " ago"))
    if d.get("rows"):
        rows.append((f'miniblocks, last {BUILDING_LOOKBACK_H}h',
                     f'{d["rows"]:,}'))
    return _card(
        "building", big, grade,
        f'yes when the newest slot we hold a miniblock for is less than '
        f"{BUILDING_MAX_AGE_MIN} minutes old",
        rows, err=html.escape(d.get("why") or ""),
        note="Dated with getBlockTime, on the chain's clock &mdash; not our "
             "own ts column, which is the builder's clock and drifts.")


def bank_card(d):
    """Replay time, within one host. No reference, no cross-host clock."""
    title = "replay &mdash; slot complete &rarr; bank frozen"
    if not d.get("n") or d.get("why"):
        return _card(title, "&mdash;", "dim", "",
                     err=html.escape(d.get("why") or "no data"))
    return _card(
        title, f'{d["p50"]:,.1f} <span style="font-size:15px">ms</span>',
        bank_grade(d["p50"]),
        f'median over the last {SHRED_LAG_WINDOW_MIN} minutes, '
        f'{d["n"]:,} slots',
        [("p10 &hellip; p90", f'{d["p10"]:,.1f} &hellip; {d["p90"]:,.1f} ms'),
         ("range", f'{d["lo"]:,.1f} &hellip; {d["hi"]:,.1f} ms'),
         ("slots", f'{d["n"]:,}')],
        note="Both rows come from this host, so the subtraction never "
             "crosses a clock: shred_insert_is_full is the moment it held "
             "every shred for the slot, bank_frozen the moment it finished "
             "replaying. That isolates the node's own work &mdash; a "
             "comparison against another host would fold in distance, "
             "which is how JP_1 reads ~100 ms behind on freeze time yet "
             f"replays in {d["p50"]:,.1f} ms. Amber at "
             f"{SHRED_LAG_MED_TH_MS} ms, red at {SHRED_LAG_HIGH_TH_MS} ms.")


def repair_card(d):
    """Repaired and recovered shreds per slot, by day."""
    title = f"shreds &mdash; repaired per slot, {SHRED_REPAIR_DAYS}d"
    if not d.get("days"):
        return _card(title, "&mdash;", "dim", "",
                     err=html.escape(d.get("why") or "no data"))
    avg = d.get("avg_repaired")
    rec = d.get("avg_recovered")
    rows = [(r["day"][5:].replace("-", "/"),
             (f'{r["repaired"]:.2f} repaired'
              if r["repaired"] is not None else "&mdash;")
             + (f' &middot; {r["recovered"]:,.0f} recovered'
                if r["recovered"] is not None else ""))
            for r in d["days"]]
    if rec is not None:
        rows.append(("recovered, avg", f"{rec:,.0f} per slot"))
    return _card(
        title, "&mdash;" if avg is None else f"{avg:.2f}",
        "good" if (avg or 0) < 1 else "warn",
        f"average shreds this host had to ASK a peer for, per slot",
        rows,
        note="Repaired is not recovered. A recovered shred was rebuilt "
             "locally from the coding shreds the node already had; a "
             "repaired one had to be requested from a peer, which is the "
             "slow path and the one that says the node is poorly connected "
             "to the leader. Recovery volume is the better read on how much "
             "of the block arrived indirectly.")


def winrate_card(me, peers):
    """win% contested, with the region's best for scale.

    No pass/fail colour: 3% and 23% are both "winning", and what makes one
    of them bad is the builders next to it, not a threshold. The region's
    leader is on the card instead."""
    if not me:
        return _card(
            f"win rate &mdash; last {WINRATE_HOURS}h", "&mdash;", "dim", "",
            err="the relay recorded no submissions from this builder")
    rows = [("offered", f'{me["offered"]:,}'),
            ("rounds bid on", f'{me["contested"]:,}'),
            ("round wins", f'{me["round_wins"]:,}'),
            ("tail wins", f'{me["tail_wins"]:,}')]
    same = sorted((v for b, v in peers.items()
                   if v["region"] == me["region"] and v["pct"] is not None),
                  key=lambda v: -v["pct"])
    note = ""
    if same and same[0] is not me:
        best = same[0]
        note = (f'Best in {html.escape(me["region"])} over the same window is '
                f'{html.escape(best["builder"])} at {best["pct"]:.1f}%, over '
                f'{best["contested"]:,} contested rounds.')
    big = "&mdash;" if me["pct"] is None else f'{me["pct"]:.1f}%'
    return _card(
        f"win rate &mdash; last {WINRATE_HOURS}h", big,
        "dead" if me["wins"] == 0 else "good",
        f'{me["wins"]:,} of {me["contested"]:,} rounds this builder bid on',
        rows, note=note or
        "Contested denominator: rounds this builder actually bid on. A "
        "window-wide one counts rounds on connectors it never sees, which "
        "makes a smaller market look worse than it is.")


def panic_card(d):
    n = d["today"]
    rows = []
    if d.get("last"):
        at = _parse_utc_str(d["last"])
        age = (dt.datetime.now(dt.UTC) - at).total_seconds() if at else None
        rows.append(("most recent",
                     f'{_local_dt(at)} <span class="dim">&middot; '
                     f'{_dur(age)} ago</span>' if at else "&mdash;"))
    rows.append((f'in the last {PANIC_LOOKBACK_D}d', f'{d["recent"]:,}'))
    return _card(
        "panics today", f"{n:,}", "good" if n == 0 else "dead",
        "winner order bytes invariant failed, since 00:00 UTC", rows,
        note="The assert in queue_winner_commit, fired when a winner names "
             "order bytes this builder cannot resolve. A crash, so any "
             "non-zero count is worth opening. Zero only means zero winner "
             "commits went wrong &mdash; it does not mean any happened.")


def _parse_utc_str(ts):
    """ClickHouse hands back 9 fractional digits; datetime takes 6."""
    ts = (ts or "").strip()
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts[:26]).replace(tzinfo=dt.UTC)
    except ValueError:
        return None


def _local_dt(at):
    """UTC -> this machine's local clock, named so the reader knows which."""
    if at is None:
        return "&mdash;"
    loc = at.astimezone()
    return loc.strftime("%H:%M:%S ") + (loc.tzname() or "")


def _copy(label, values):
    """A copy control carrying the FULL list, however much is displayed.

    The visible <code> is clipped by CSS, not by truncation, so what lands
    on the clipboard is the whole list -- a half-copied signature list is
    worse than none."""
    if not values:
        return ""
    joined = "\n".join(values)
    return (f'<div class="plist"><div class="plh">{label} '
            f'<span class="dim">({len(values):,})</span>'
            f'<button class="pcopy" type="button" '
            f'data-copy="{html.escape(joined)}">copy</button></div>'
            f'<code>{html.escape(", ".join(values))}</code></div>')


def panic_chart(inst, hist):
    """Panics per day, and each day's missing refs behind a click.

    A bar chart because the question is magnitude across a handful of
    discrete days. One series, so no legend -- the heading names it -- and
    every bar is directly labelled, which at five bars is clearer than
    making the reader hover for the number."""
    days, counts = hist["days"], hist["counts"]
    detail = hist["detail"]
    top = max([counts.get(d, 0) for d in days] + [1])
    if not any(counts.get(d, 0) for d in days):
        return ('<div class="pchart"><div class="none">no panic in the last '
                f"{PANIC_DAYS} days</div></div>")
    W, H, PAD = 100.0, 70.0, 14.0
    slot = W / len(days)
    bw = slot * 0.52
    bars, labels = [], []
    for k, d in enumerate(days):
        n = counts.get(d, 0)
        x = slot * k + (slot - bw) / 2
        h = (H - PAD) * (n / top) if n else 0
        y = H - h
        has = " has" if detail.get(d) else ""
        if n:
            # 4px-equivalent rounded data-end, anchored to the baseline: the
            # radius is in user units scaled by the viewBox.
            bars.append(
                f'<rect class="pbar{has}" data-inst="{html.escape(inst)}" '
                f'data-day="{d}" x="{x:.2f}" y="{y:.2f}" width="{bw:.2f}" '
                f'height="{h:.2f}" rx="1.1" />')
            labels.append(f'<text class="pnum" x="{x + bw / 2:.2f}" '
                          f'y="{y - 3:.2f}">{n}</text>')
        else:
            bars.append(f'<rect class="pbar zero" x="{x:.2f}" y="{H - 1:.2f}" '
                        f'width="{bw:.2f}" height="1" />')
    axis = "".join(
        f'<div class="pax"><b>{d[8:]}/{d[5:7]}</b>'
        f'<span>{counts.get(d, 0) or "&mdash;"}</span></div>' for d in days)
    panes = []
    for d in days:
        evs = detail.get(d) or []
        if not evs:
            continue
        shown = "".join(
            '<div class="pev"><div class="pevh">'
            f'<b>{_local_dt(_parse_utc_str(e["ts"]))}</b>'
            f'<span>slot {copy_val(e["slot"])}'
            f' &middot; round {html.escape(e["index"])} &middot; '
            f'{html.escape(e["resolution_attempts"])}'
            " resolution attempts</span></div>"
            + _copy("missing tx sig prefixes", e["sigs"])
            + _copy("missing bundle ids", e["bundles"])
            + "</div>" for e in evs)
        n = counts.get(d, 0)
        more = ("" if len(evs) >= n else
                f'<div class="hnote">showing the {len(evs)} most recent of '
                f"{n}; the rest are past the per-builder detail cap.</div>")
        panes.append(f'<div class="pday" data-inst="{html.escape(inst)}" '
                     f'data-day="{d}" hidden><div class="pdayh">{d} '
                     f"&mdash; {n} panic{'' if n == 1 else 's'}</div>"
                     f"{more}{shown}</div>")
    return (f'<div class="pchart"><svg viewBox="0 0 {W:g} {H:g}" '
            'preserveAspectRatio="none" role="img" '
            f'aria-label="panics per day for {html.escape(inst)}">'
            + "".join(bars) + "".join(labels) + "</svg>"
            f'<div class="paxis">{axis}</div>'
            '<div class="hnote">Click a bar for that day&rsquo;s panics and '
            "the refs the builder could not resolve.</div>"
            + "".join(panes) + "</div>")



def restart_chart(inst, hist):
    """Restarts per day, and each day's runs behind a click.

    Same chart as the panic version it replaces -- the question is still
    magnitude across a handful of days. The PANE differs: a restart has no
    slot, round or resolution attempt to show, because those only ever
    existed in the panic body, which went with the otel login.

    A bar chart because the question is magnitude across a handful of
    discrete days. One series, so no legend -- the heading names it -- and
    every bar is directly labelled, which at five bars is clearer than
    making the reader hover for the number."""
    days, counts = hist["days"], hist["counts"]
    detail = hist["detail"]
    top = max([counts.get(d, 0) for d in days] + [1])
    if not any(counts.get(d, 0) for d in days):
        return ('<div class="pchart"><div class="none">no restart in the last '
                f"{PANIC_DAYS} days</div></div>")
    W, H, PAD = 100.0, 70.0, 14.0
    slot = W / len(days)
    bw = slot * 0.52
    bars, labels = [], []
    for k, d in enumerate(days):
        n = counts.get(d, 0)
        x = slot * k + (slot - bw) / 2
        h = (H - PAD) * (n / top) if n else 0
        y = H - h
        has = " has" if detail.get(d) else ""
        if n:
            # 4px-equivalent rounded data-end, anchored to the baseline: the
            # radius is in user units scaled by the viewBox.
            bars.append(
                f'<rect class="pbar{has}" data-inst="{html.escape(inst)}" '
                f'data-day="{d}" x="{x:.2f}" y="{y:.2f}" width="{bw:.2f}" '
                f'height="{h:.2f}" rx="1.1" />')
            labels.append(f'<text class="pnum" x="{x + bw / 2:.2f}" '
                          f'y="{y - 3:.2f}">{n}</text>')
        else:
            bars.append(f'<rect class="pbar zero" x="{x:.2f}" y="{H - 1:.2f}" '
                        f'width="{bw:.2f}" height="1" />')
    axis = "".join(
        f'<div class="pax"><b>{d[8:]}/{d[5:7]}</b>'
        f'<span>{counts.get(d, 0) or "&mdash;"}</span></div>' for d in days)
    panes = []
    for d in days:
        evs = detail.get(d) or []
        if not evs:
            continue
        shown = "".join(
            '<div class="pev"><div class="pevh">'
            f'<b>{_local_dt(_parse_utc_str(e["ts"]))}</b>'
            f'<span>run {copy_val(e["run_id"])}'
            + (f' &middot; lived <b class="{"bad" if e["short"] else ""}">'
               f'{e["lived"]}</b>' if e["lived"] else "")
            + f' &middot; {e["events"]:,} events</span></div></div>'
            for e in evs)
        n = counts.get(d, 0)
        more = ("" if len(evs) >= n else
                f'<div class="hnote">showing the {len(evs)} most recent of '
                f"{n}.</div>")
        panes.append(f'<div class="pday" data-inst="{html.escape(inst)}" '
                     f'data-day="{d}" hidden><div class="pdayh">{d} '
                     f"&mdash; {n} restart{'' if n == 1 else 's'}</div>"
                     f"{more}{shown}</div>")
    return (f'<div class="pchart"><svg viewBox="0 0 {W:g} {H:g}" '
            'preserveAspectRatio="none" role="img" '
            f'aria-label="panics per day for {html.escape(inst)}">'
            + "".join(bars) + "".join(labels) + "</svg>"
            f'<div class="paxis">{axis}</div>'
            '<div class="hnote">Click a bar for that day&rsquo;s panics and '
            "the refs the builder could not resolve.</div>"
            + "".join(panes) + "</div>")


def relay_section(rq):
    """Name, last event, healthy, longest silence today. One row per relay.

    `rq` is the relay transport now, not otel -- see relay_health."""
    try:
        names = relay_names(rq)
        stats = relay_health(rq)
    except Exception as exc:
        return ('<div class="herr" style="padding:0 28px">relay health '
                f"unavailable: {html.escape(str(exc)[:200])}</div>")
    if not names:
        return ('<div class="herr" style="padding:0 28px">no relay has '
                "logged anything in the last 24h</div>")
    out = []
    for name in names:
        st = stats.get(name)
        if not st:
            out.append(f'<tr><td class="m">{html.escape(name)}</td>'
                       '<td class="m dim">&mdash;</td>'
                       '<td class="rv dead">no</td>'
                       '<td class="n m dim">nothing today</td></tr>')
            continue
        ok = st["last_age"] <= RELAY_HEALTHY_TH_S
        last = dt.datetime.fromtimestamp(st["last_epoch"], dt.UTC)
        # A relay silent right now is silent for that long whether or not
        # the gap has closed, so the running silence counts too -- without
        # it a relay dead for six hours still reports its best quiet minute
        # from this morning.
        worst = max(st["max_gap"], st["last_age"])
        out.append(
            f'<tr><td class="m">{html.escape(name)}</td>'
            f'<td class="m">{_local_dt(last)} '
            f'<span class="dim">&middot; {_dur(st["last_age"])} ago</span></td>'
            f'<td class="rv {"good" if ok else "dead"}">'
            f'{"yes" if ok else "no"}</td>'
            f'<td class="n m">{_dur(worst)}</td></tr>')
    return (
        '<table class="rtab"><tr><th>relay</th><th>last event</th>'
        '<th>healthy</th><th class=n>longest silence today</th></tr>'
        + "".join(out) + "</table>"
        '<div class="hnote" style="padding:0 28px 8px">'
        "Healthy means the relay logged something in the last "
        f"{_dur(RELAY_HEALTHY_TH_S)}. Longest silence is the biggest gap "
        "between events since 00:00 UTC, and counts the silence still "
        "running if the relay is quiet now. Times are this machine's local "
        "clock.</div>")


def body(ch, ix, ot, relay, rpc_url, path):
    try:
        insts = instances(ch)
    except Exception as exc:
        return ('<div class="herr" style="margin:28px">the instance list '
                "could not be read from ClickHouse: "
                f"{html.escape(str(exc)[:200])}</div>")
    if not insts:
        return ('<div class="herr" style="margin:28px">The instance list came '
                "back empty twice. bifrost_events returned no rows for the "
                "last 24h, which is almost always the store blinking "
                "rather than every builder being silent &mdash; "
                f'<a href="{html.escape(path)}">try again</a>.</div>')
    ids = [i["id"] for i in insts]
    inst = DEFAULT_INSTANCE if DEFAULT_INSTANCE in ids else ids[0]

    # Three fleet-wide fetches, whatever is on screen. Each store is asked
    # once per page rather than once per builder per click.
    def safe(fn):
        try:
            return fn(), None
        except Exception as exc:
            return {}, str(exc)[:200]
    build, e1 = safe(lambda: building_all(ch, rpc_url, ids))
    bank, e2 = safe(lambda: bank_feed_all(ix, ids))
    rep, e6 = safe(lambda: shred_repairs_all(ix, ids))
    pan, e3 = safe(lambda: restarts_all(ch, ids))
    wr, e5 = safe(lambda: win_rates(relay))
    hist, e4 = safe(lambda: restart_history(ch, ids))

    def fail(name, err):
        return _card(name, "&mdash;", "dead", "", err=html.escape(err))

    panels = []
    for i in ids:
        # Two rows, fixed columns rather than auto-fit: is-it-working on
        # top, the slot-timing pair underneath. auto-fit would reflow them
        # into one ragged line the moment the window was wide enough.
        row1 = [
            build.get(i) and building_card(build[i]) or
            fail("building", e1 or "no data for this builder"),
            pan.get(i) and restart_card(pan[i]) or
            fail("restarts today", e3 or "no data for this builder"),
            (winrate_card(dict(wr[i], builder=i) if wr.get(i) else None,
                          {b: dict(v, builder=b) for b, v in wr.items()})
             if wr or not e5 else
             fail(f"win rate &mdash; last {WINRATE_HOURS}h", e5)),
        ]
        row2 = [
            bank.get(i) and bank_card(bank[i]) or
            fail("replay", e2 or "no data for this builder"),
            rep.get(i) and repair_card(rep[i]) or
            fail(f"shreds &mdash; repaired per slot, {SHRED_REPAIR_DAYS}d",
                 e6 or "no data for this builder"),
        ]
        panels.append(
            f'<div class="bpanel" data-inst="{html.escape(i)}"'
            + ("" if i == inst else " hidden") + ">"
            + '<h2 class="hsec">builder health</h2>'
            + f'<div class="hgrid g3">{"".join(row1)}</div>'
            + f'<div class="hgrid g2">{"".join(row2)}</div>'
            + '<h2 class="hsec">restarts, last '
            + f"{PANIC_DAYS} days</h2>"
            + (restart_chart(i, hist[i]) if hist.get(i) else
               f'<div class="herr" style="padding:0 28px">{html.escape(e4 or "")}'
               "</div>")
            + "</div>")
    return (picker(insts, inst) + "".join(panels)
            + '<h2 class="hsec">relay health</h2>' + relay_section(relay)
            + f"<script>{HEALTH_JS}</script>")
