#!/usr/bin/env python3
"""
simbench — rebuild, step 1: the produced-slot strip.

Every slot we actually produced the block for, newest on the left, scrolling
right into the past. Nothing is selected and nothing is plotted yet; panels get
added back one at a time.

    python3 app.py                 # http://127.0.0.1:8899
    python3 app.py --port 9000

The InfluxDB client is kept below so the next steps can use it; right now only
ClickHouse is read, for the produced-slot list.
"""

import argparse
import base64
import datetime as dt
import html
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ------------------------------------------------------------------- clients

# Comma-separated and tried in order, so a DNS name can carry an IP fallback.
INFLUX_HOSTS = [h.strip() for h in os.environ.get("INFLUX_HOSTS", "").split(",")
                if h.strip()]
INFLUX_PORT = int(os.environ.get("INFLUX_PORT", "8086"))
INFLUX_DB = os.environ.get("INFLUX_DB", "solana")
INFLUX_USER = os.environ.get("INFLUX_USER", "")
INFLUX_PASS = os.environ.get("INFLUX_PASS", "")

CH_URL = os.environ.get("CH_URL", "")
CH_USER = os.environ.get("CH_USER", "")
CH_PASS = os.environ.get("CH_PASS", "")
CH_DB = os.environ.get("CH_DB", "block_builder")
CH_BUILDER = os.environ.get("CH_BUILDER", "")

_auth = base64.b64encode(f"{INFLUX_USER}:{INFLUX_PASS}".encode()).decode()
_good_host = None


def influx_series(query):
    """Run InfluxQL and return every series.

    One query can span several measurements and GROUP BY host_id, which yields
    one series per (measurement, host) pair -- `name` and `tags` identify it.
    Taking only series[0] would silently drop all but one host."""
    global _good_host
    hosts = ([_good_host] if _good_host else []) + [
        h for h in INFLUX_HOSTS if h != _good_host]
    last = None
    for host in hosts:
        try:
            body = urllib.parse.urlencode({"db": INFLUX_DB, "q": query}).encode()
            req = urllib.request.Request(f"http://{host}:{INFLUX_PORT}/query", data=body)
            req.add_header("Authorization", "Basic " + _auth)
            with urllib.request.urlopen(req, timeout=180) as resp:
                payload = json.load(resp)
            _good_host = host
            result = payload["results"][0]
            if "error" in result:
                raise RuntimeError(result["error"])
            return result.get("series") or []
        except (urllib.error.URLError, socket.timeout, OSError) as err:
            last = err
    raise RuntimeError(f"no influx host reachable ({last})")


def influx(query):
    """Single-series convenience wrapper -> (columns, rows)."""
    series = influx_series(query)
    return (series[0]["columns"], series[0]["values"]) if series else ([], [])


def clickhouse(sql):
    """-> (columns, rows). No CTEs: bb_read is readonly=1 and WITH needs a temp table."""
    qs = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASS,
                                 "database": CH_DB,
                                 "query": sql + " FORMAT TabSeparatedWithNames"})
    with urllib.request.urlopen(CH_URL + "?" + qs, timeout=120) as resp:
        text = resp.read().decode()
    lines = [ln for ln in text.split("\n") if ln]
    if not lines:
        return [], []
    return lines[0].split("\t"), [ln.split("\t") for ln in lines[1:]]


TRUEISH = {"1", "true", "True"}


def ch_int(v, default=0):
    """ClickHouse TabSeparated writes NULL as the two characters \\N."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ------------------------------------------------------------ produced slots

_cache = {"windows": None, "at": 0.0}

WINDOW = 4  # a Solana leader window is four consecutive slots


def window_of(slot):
    return (slot // WINDOW) * WINDOW


def produced_windows(days=30):
    """Contested slots folded into their leader windows, newest window first.

    A leader owns four consecutive slots, so four rows in the strip were always
    four views of one assignment. We fold them into one entry keyed by the
    window's first slot and keep the individual slots for the expansion.

    Scope is every slot we *competed* in — at least one kind='selected' offer —
    not only the ones we produced. Winning is rare and connector-specific: over
    30 days a single connector identity accounts for every slot we have ever
    won, so a won-only list shows one leader and hides all the others.

    `won_by_us` (set on kind='winner' rows) marks the ones we did produce.
    `connector_identity` is the validator identity holding the window, i.e. the
    leader."""
    import time as _time

    now = _time.time()
    if _cache["windows"] is not None and now - _cache["at"] < 300:
        return _cache["windows"]
    _, rows = clickhouse(
        "SELECT slot, toString(min(ts)) AS first_ts, any(connector_identity) AS leader, "
        "       max(won_by_us) AS won, countIf(kind = 'selected') AS offers "
        "FROM bifrost_miniblocks "
        f"WHERE ts > now() - INTERVAL {int(days)} DAY "
        f"  AND local_builder_id = '{CH_BUILDER}' "
        "GROUP BY slot HAVING offers > 0 OR won ORDER BY slot DESC")
    windows = {}
    for r in rows:
        slot = int(r[0])
        win = windows.setdefault(window_of(slot),
                                 {"win": window_of(slot), "slots": [], "leaders": set()})
        win["slots"].append({"slot": slot, "ts": r[1], "won": r[3] in TRUEISH,
                             "offers": ch_int(r[4])})
        if r[2]:
            win["leaders"].add(r[2])
    out = []
    for win in (windows[k] for k in sorted(windows, reverse=True)):
        win["slots"].sort(key=lambda s: s["slot"])
        win["ts"] = min(s["ts"] for s in win["slots"])
        win["won"] = sum(1 for s in win["slots"] if s["won"])
        leaders = sorted(win.pop("leaders"))
        # one leader per window is the invariant; surface a violation rather than
        # silently picking one with any().
        win["leader"] = leaders[0] if leaders else ""
        win["leader_split"] = len(leaders) > 1
        out.append(win)
    _cache.update(windows=out, at=now)
    return out


def copy_btn(value):
    """A copy affordance that lives inside the chip's <a>. It is a span, not a
    button, because a button nested in an anchor is invalid HTML; the click is
    intercepted in JS so copying never navigates."""
    return (f'<span class="cp" data-c="{html.escape(str(value), quote=True)}" '
            f'role="button" tabindex="0" title="copy {html.escape(str(value))}"'
            f'>copy</span>')


def short_id(identity):
    """Chip-sized rendering. The full identity is always in the title attribute
    and is printed in full in the expanded panel."""
    return identity if len(identity) <= 14 else f"{identity[:6]}…{identity[-4:]}"


def epoch(ts_utc):
    """Epoch seconds from a ClickHouse naive-UTC string, or None."""
    try:
        return dt.datetime.fromisoformat(ts_utc[:26]).replace(tzinfo=dt.UTC).timestamp()
    except (ValueError, TypeError):
        return None


def age_html(ts_utc, suffix=" ago"):
    """A live-ticking age. The rendered text is the server's answer at render
    time and stands on its own without JS; `data-t` lets the browser keep
    counting from there."""
    at = epoch(ts_utc)
    stamp = "" if at is None else f' data-t="{at:.0f}"'
    return f'<span class="age"{stamp}>{ago(ts_utc)}{suffix}</span>'


def ago(ts_utc):
    """'3h 12m' style age. ClickHouse hands back naive UTC strings."""
    at = epoch(ts_utc)
    if at is None:
        return "?"
    secs = int(dt.datetime.now(dt.UTC).timestamp() - at)
    if secs < 0:
        return "now"
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    mins, sec = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {mins}m"
    if mins:
        return f"{mins}m {sec}s"
    return f"{sec}s"


# --------------------------------------------------------------------- rounds

def slot_rounds(slot):
    """One entry per auction round: our offers, and the winner echo.
    Rounds where the relay never echoed a winner simply have winner=None."""
    _, rows = clickhouse(
        "SELECT index_in_slot, kind, toString(ts) AS ts, order_count, transaction_count, "
        "       bundle_count, reward, execution_cost, selected_cu, is_last, "
        "       won_by_us, uuid, connector_identity, run_id, instance_id, seq_id "
        f"FROM bifrost_miniblocks WHERE slot = {int(slot)} ORDER BY index_in_slot, kind, ts")
    rounds = {}
    for r in rows:
        idx = ch_int(r[0])
        item = {"ts": r[2], "orders": ch_int(r[3]), "txs": ch_int(r[4]),
                "bundles": ch_int(r[5]), "reward": ch_int(r[6]),
                "exec_cost": ch_int(r[7]), "sel_cu": ch_int(r[8]),
                "is_last": r[9] in TRUEISH, "won": r[10] in TRUEISH,
                "uuid": r[11], "connector": r[12], "run_id": r[13],
                "instance": r[14], "seq_id": ch_int(r[15])}
        slot_round = rounds.setdefault(idx, {"round": idx, "offers": [], "winner": None})
        if r[1] == "winner":
            slot_round["winner"] = item
        else:
            slot_round["offers"].append(item)
    return [rounds[k] for k in sorted(rounds)]


def sol(lamports):
    return f"{lamports/1e9:.6f}"


# ------------------------------------------------------- extends, per round

# Status enum as the simulator reports it on the wire.
EXT_STATUS = {0: "UNKNOWN", 1: "SUCCESS", 2: "NOT_READY", 3: "REJECTED",
              4: "THROTTLED", 5: "UNAVAILABLE"}


def _rfc3339_ns(stamp):
    """InfluxDB RFC3339 -> epoch nanoseconds, kept integral so sub-ms survives."""
    import calendar
    date, _, rest = stamp.partition("T")
    clock, _, frac = rest.rstrip("Z").partition(".")
    y, mo, d = (int(x) for x in date.split("-"))
    h, mi, s = (int(x) for x in clock.split(":"))
    return (calendar.timegm((y, mo, d, h, mi, s, 0, 0, 0)) * 10**9
            + int((frac + "0" * 9)[:9]))


def _p50(values):
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2] if ordered else 0


def slot_extends(slot, stamps):
    """sim-extend points for one slot, bucketed by auction round.

    Exact attribution, not a timestamp join: workers.rs emits
    ("index", round.index_in_slot) on every point.

    `slot` and `index` are FIELDS, not tags, so InfluxQL cannot GROUP BY them
    and an unbounded predicate would walk the whole 720h retention -- hence the
    window, taken from the slot's own miniblock timestamps.

    Refused extends emit no datapoint at all: a THROTTLED request is rejected
    before the worker runs, so `n` counts ACCEPTED calls only."""
    if not stamps:
        return {}
    # The window is derived from ClickHouse timestamps but applied to InfluxDB,
    # and the two stores are not always in step: normally within +-50ms, but
    # an 11.5s divergence has been observed in practice. The margin has to
    # cover that or the panel silently reports "0 ext" for a round that
    # really did extend.
    pad = dt.timedelta(seconds=150)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%dT%H:%M:%SZ")
    cols, rows = influx(
        'SELECT "index","body_us","queue_us","orders","applied","status",'
        '"exec_wall_us","program_cache_us","program_cache_clone_us",'
        '"program_cache_compile_us","program_cache_compiles",'
        '"program_cache_entries","program_cache_entries_cloned",'
        '"program_cache_loaded" '
        f'FROM "sim-extend" WHERE slot = {int(slot)} '
        + "".join(f"AND \"host_id\" != '{h}' " for h in OFF_CHAIN)
        + f"AND time >= '{lo}' AND time <= '{hi}'")
    if not rows:
        return {}
    at = {name: i for i, name in enumerate(cols)}
    buckets = {}
    for r in rows:
        def num(field):
            try:
                return int(r[at[field]] or 0)
            except (TypeError, ValueError):
                return 0
        buckets.setdefault(num("index"), []).append({
            "t": _rfc3339_ns(r[at["time"]]), "body": num("body_us"),
            "queue": num("queue_us"), "orders": num("orders"),
            "applied": num("applied"), "status": num("status"),
            "exec": num("exec_wall_us"),
            "pc_us": num("program_cache_us"),
            "pc_clone_us": num("program_cache_clone_us"),
            "pc_compile_us": num("program_cache_compile_us"),
            "pc_compiles": num("program_cache_compiles"),
            "pc_entries": num("program_cache_entries"),
            "pc_cloned": num("program_cache_entries_cloned"),
            "pc_loaded": num("program_cache_loaded")})
    out = {}
    for idx, calls in buckets.items():
        calls.sort(key=lambda c: c["t"])
        body = [c["body"] for c in calls]
        # a point is written AFTER the body finishes, so its time is the
        # extend's completion; back out the first call's start to get the span
        start = calls[0]["t"] - calls[0]["body"] * 1000
        wall = max(1, (calls[-1]["t"] - start) // 1000)
        out[idx] = {
            "n": len(calls), "body_sum": sum(body), "body_max": max(body),
            "body_p50": _p50(body), "queue_sum": sum(c["queue"] for c in calls),
            "exec_sum": sum(c["exec"] for c in calls), "wall": wall,
            "busy": 100.0 * sum(body) / wall,
            "orders": sum(c["orders"] for c in calls),
            "applied": sum(c["applied"] for c in calls),
            "statuses": sorted(
                ((EXT_STATUS.get(s, str(s)),
                  sum(1 for c in calls if c["status"] == s))
                 for s in {c["status"] for c in calls}), key=lambda kv: -kv[1]),
            # Program cache. Costs are per-call and sum; sizes are snapshots
            # taken inside one call and do not, so they are reported as maxima.
            "pc_us": sum(c["pc_us"] for c in calls),
            "pc_compile_us": sum(c["pc_compile_us"] for c in calls),
            "pc_compiles": sum(c["pc_compiles"] for c in calls),
            "pc_loaded": sum(c["pc_loaded"] for c in calls),
            "pc_clone_us": sum(c["pc_clone_us"] for c in calls),
            "pc_forks": sum(1 for c in calls if c["pc_clone_us"] or c["pc_cloned"]),
            "pc_cloned_max": max(c["pc_cloned"] for c in calls),
            "pc_entries_max": max(c["pc_entries"] for c in calls),
        }
    return out


# ---------------------------------------------------------- builder runs

def slot_runs(rounds):
    """The builder process(es) that served this slot.

    `run_id` is one process lifetime, so it changes on every restart. It is
    constant within a (slot, instance) pair, but a slot can carry more than one
    run when two instances were live at once -- so this returns a list, not a
    single run.

    What makes it worth showing is position: how far into the run this slot
    fell. A slot served minutes after a restart ran against a cold program
    cache and a cold account overlay, and its timings are not comparable to one
    served hours in."""
    seen = {}
    for r in rounds:
        for item in r["offers"] + ([r["winner"]] if r["winner"] else []):
            rid = item.get("run_id")
            if not rid:
                continue
            slot_run = seen.setdefault(rid, {
                "run_id": rid, "instance": item.get("instance", ""),
                "first_ts": item["ts"], "last_ts": item["ts"], "rows": 0,
                "seq_lo": item["seq_id"], "seq_hi": item["seq_id"]})
            slot_run["rows"] += 1
            slot_run["first_ts"] = min(slot_run["first_ts"], item["ts"])
            slot_run["last_ts"] = max(slot_run["last_ts"], item["ts"])
            slot_run["seq_lo"] = min(slot_run["seq_lo"], item["seq_id"])
            slot_run["seq_hi"] = max(slot_run["seq_hi"], item["seq_id"])
    if not seen:
        return []
    quoted = ",".join("'" + rid.replace("'", "") + "'" for rid in seen)
    _, rows = clickhouse(
        "SELECT run_id, any(instance_id) AS instance, toString(min(ts)) AS started, "
        "       toString(max(ts)) AS ended, uniqExact(slot) AS slots, "
        "       min(slot) AS slot_lo, max(slot) AS slot_hi, "
        "       uniqExactIf(slot, won_by_us) AS won "
        f"FROM bifrost_miniblocks WHERE run_id IN ({quoted}) "
        f"  AND local_builder_id = '{CH_BUILDER}' GROUP BY run_id")
    for r in rows:
        if r[0] in seen:
            seen[r[0]].update(instance=r[1], started=r[2], ended=r[3],
                              slots=ch_int(r[4]), slot_lo=ch_int(r[5]),
                              slot_hi=ch_int(r[6]), won=ch_int(r[7]))
    return sorted(seen.values(), key=lambda s: s["first_ts"])


def since(a, b):
    """b - a as a coarse duration, both naive-UTC ClickHouse strings."""
    start, end = epoch(a), epoch(b)
    if start is None or end is None:
        return "?"
    secs = max(0, int(end - start))
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# A slot served shortly after a restart ran against cold caches.
COLD_RUN_SECONDS = 600


def runs_html(slot, runs):
    if not runs:
        return ""
    rows = []
    for run in runs:
        started = run.get("started")
        into = since(started, run["first_ts"]) if started else "?"
        age = epoch(run["first_ts"]) - epoch(started) if started else None
        cold = age is not None and age < COLD_RUN_SECONDS
        span = (f'{run.get("slot_lo","?")} &ndash; {run.get("slot_hi","?")}'
                if started else "?")
        num = lambda key: (f"{run[key]:,}" if isinstance(run.get(key), int)
                           else "?")
        rows.append(
            "<tr>"
            f'<td class=m>{html.escape(run["run_id"])}{copy_btn(run["run_id"])}</td>'
            f'<td class=m>{html.escape(run.get("instance", ""))}</td>'
            f'<td class=m>{html.escape((started or "?")[:19])}</td>'
            f'<td class="n m{" bad" if cold else ""}">{into}'
            + ("<span class='warnpill'>cold start</span>" if cold else "")
            + "</td>"
            f'<td class="n m">{num("slots")}</td>'
            f'<td class="n m">{num("won")}</td>'
            f'<td class="n m">{span}</td>'
            f'<td class="n m">{run["rows"]} rows, seq '
            f'{run["seq_lo"]}&ndash;{run["seq_hi"]}</td></tr>')
    head = ("<tr><th>run_id</th><th>instance</th><th>run started (UTC)</th>"
            "<th class=n>slot is this far in</th><th class=n>run slots</th>"
            "<th class=n>run wins</th><th class=n>run slot span</th>"
            "<th class=n>this slot</th></tr>")
    return ('<div class="panel"><div class="dtlhead">builder run'
            + ("s" if len(runs) > 1 else "")
            + "<span class='note'>run_id is one builder process lifetime, so it "
              "changes on every restart &middot; a slot served soon after a "
              "restart ran against cold caches and its timings are not "
              "comparable to one served hours in</span></div>"
            f"<table><thead>{head}</thead><tbody>{''.join(rows)}</tbody></table>"
            "</div>")


# ----------------------------------------- shred path: leader vs simulator

# host_id is the validator identity. Several nodes write the same measurement
# names, so anything unbound silently blends machines. These are deployment
# facts, not code -- supply them via the environment.
#   SHRED_LEADER_ID   the leader whose slots you are studying
#   SHRED_SIM_ID      the host running the simulator
#   SHRED_EXCLUDE_IDS comma-separated identities to drop entirely. Use this for
#                     any node on a different chain or replaying history: such a
#                     node can write the same measurement names at a wildly
#                     different slot height and will pollute unpinned queries.
LEADER = os.environ.get("SHRED_LEADER_ID", "")
SIM = os.environ.get("SHRED_SIM_ID", "")
OFF_CHAIN = [h.strip() for h in os.environ.get("SHRED_EXCLUDE_IDS", "").split(",")
             if h.strip()]
HOST_NAME = {LEADER: "leader", SIM: "our simulator"}
# measurement -> (row label, sort key). The timestamp IS the datum for all four.
SHRED_STAGES = [("retransmit-first-shred", "first shred received"),
                ("shred_insert_is_full", "slot complete"),
                ("bank_frozen", "bank frozen"),
                ("optimistic_slot", "optimistic confirmed")]


def slot_shreds(slot, stamps):
    """Per-host shred-path timeline for one slot, in two queries.

    Joined on `slot`, never on time: the stores are not always in step -- we
    have seen ClickHouse run 11.5s ahead of three Influx hosts that agreed with
    each other -- so a time join would be quietly wrong.

    `retransmit-stage-slot-stats` is flushed minutes after the slot -- its row
    time is meaningless, so it gets its own wide window and its `outset_timestamp`
    field (epoch ms) is the real anchor."""
    if not stamps:
        return {}
    lo = (dt.datetime.fromisoformat(min(stamps)[:26])
          - dt.timedelta(seconds=150)).strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26])
          + dt.timedelta(seconds=150)).strftime("%Y-%m-%dT%H:%M:%SZ")
    series = influx_series(
        'SELECT "slot","total_time_ms","num_repaired","num_recovered" FROM '
        + ",".join(f'"{m}"' for m, _ in SHRED_STAGES)
        + f" WHERE slot = {int(slot)} AND time >= '{lo}' AND time <= '{hi}' "
        'GROUP BY "host_id"')
    out = {}
    for ser in series:
        host = ser["tags"]["host_id"]
        if host in OFF_CHAIN or host not in (LEADER, SIM):
            continue
        col = {name: i for i, name in enumerate(ser["columns"])}
        row = ser["values"][0]

        def val(name):
            v = row[col[name]] if name in col else None
            return None if v is None else int(v)

        out.setdefault(ser["name"], {})[host] = {
            "t": _rfc3339_ns(row[0]), "total_time_ms": val("total_time_ms"),
            "num_repaired": val("num_repaired"), "num_recovered": val("num_recovered")}
    return out


def shred_html(slot, shreds):
    """A stage x host grid, with the sim-minus-leader delta called out."""
    if not shreds:
        return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs '
                'our simulator</div><div class="none">no shred-path rows for this '
                "slot</div></div>")
    hosts = [h for h in (LEADER, SIM) if h]   # the leader and us, nothing else
    head = ("<tr><th>stage</th>"
            + "".join(f"<th class=n>{html.escape(HOST_NAME[h])}</th>" for h in hosts)
            + "<th class=n>sim &minus; leader</th><th>detail</th></tr>")
    body = []
    for meas, label in SHRED_STAGES:
        seen = {h: e for h, e in (shreds.get(meas) or {}).items() if h in hosts}
        if not seen:                    # neither side reported this stage
            continue
        cells = []
        for h in hosts:
            e = seen.get(h)
            cells.append(f"<td class='n m'>{dt.datetime.fromtimestamp(e['t']/1e9, dt.UTC).strftime('%H:%M:%S.%f')[:-3]}</td>"
                         if e else "<td class='n m dim'>&mdash;</td>")
        if LEADER in seen and SIM in seen:
            d = (seen[SIM]["t"] - seen[LEADER]["t"]) / 1e6
            cls = "bad" if d > 0 else "good"
            delta = f"<td class='n m {cls}'>{d:+.1f} ms</td>"
        else:
            delta = "<td class='n m dim'>&mdash;</td>"
        note = "; ".join(
            f"{HOST_NAME[h]}: {e['total_time_ms']} ms"
            + (f", {e['num_recovered']} recovered" if e.get("num_recovered") else "")
            + (f", {e['num_repaired']} repaired" if e.get("num_repaired") else "")
            for h in hosts if (e := seen.get(h)) and e.get("total_time_ms") is not None)
        body.append(f"<tr><td class=m>{label}</td>{''.join(cells)}{delta}"
                    f"<td class='m dim'>{note}</td></tr>")
    if not body:
        return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs '
                'our simulator</div><div class="none">neither the leader nor our '
                "simulator reported the shred path for this slot</div></div>")
    missing = ""
    if not any(LEADER in v for v in shreds.values()):
        # A leader's metric submission can be intermittent while peers report
        # continuously, so an empty leader column is a reporting gap rather
        # than evidence of a slow node. Say so, rather than let it read as a
        # measurement.
        missing = ('<span class="warn">the leader wrote no shred metrics for this slot '
                   "&mdash; its submission is intermittent, so this is a gap in "
                   "reporting rather than a slow node</span>")
    elif not any(SIM in v for v in shreds.values()):
        missing = '<span class="note">our simulator host wrote nothing here</span>'
    return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs our '
            'simulator'
            '<span class="note">positive delta = our simulator was later &middot; '
            'joined on slot, not on time &middot; our host does not emit '
            "retransmit-first-shred</span>" + missing + "</div>"
            f"<table><thead>{head}</thead><tbody>{''.join(body)}</tbody></table></div>")


# ------------------------------------------------ one fetch per slot, cached

_slot_cache = {}                       # slot -> (monotonic_at, data)
_slot_lock = threading.Lock()
SLOT_CACHE_TTL = 300
SLOT_CACHE_MAX = 64


def slot_data(slot):
    """Everything the rounds view needs for one slot: two queries, all rounds.

    Neither query was ever per-round -- ClickHouse returns the whole slot and
    is bucketed by index_in_slot here, and the InfluxDB one is bucketed by
    `index`. What cost us was that opening a round re-ran both. This caches the
    pair, so clicking through the rounds of a slot is free after the first hit.

    A finished slot is immutable, so the TTL only matters for one still in
    flight. A failed sim-extend fetch is not cached -- it should be retried,
    not pinned for five minutes."""
    now = time.monotonic()
    with _slot_lock:
        hit = _slot_cache.get(slot)
        if hit and now - hit[0] < SLOT_CACHE_TTL:
            return hit[1]

    rounds = slot_rounds(slot)
    stamps = [i["ts"] for r in rounds
              for i in r["offers"] + ([r["winner"]] if r["winner"] else [])]
    # The two InfluxDB queries are independent and each costs seconds against a
    # 300s window, so run them together rather than back to back.
    import concurrent.futures as cf

    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        fut = {"ext": pool.submit(slot_extends, slot, stamps),
               "shred": pool.submit(slot_shreds, slot, stamps)}
    try:
        extends, ext_err = fut["ext"].result(), None
    except Exception as exc:
        extends, ext_err = {}, str(exc)[:140]
    try:
        shreds, shred_err = fut["shred"].result(), None
    except Exception as exc:
        shreds, shred_err = {}, str(exc)[:140]
    try:
        runs, runs_err = slot_runs(rounds), None
    except Exception as exc:
        runs, runs_err = [], str(exc)[:140]
    data = {"rounds": rounds, "extends": extends, "ext_err": ext_err,
            "shreds": shreds, "shred_err": shred_err,
            "runs": runs, "runs_err": runs_err}

    if ext_err is None and shred_err is None and runs_err is None:
        with _slot_lock:
            if len(_slot_cache) >= SLOT_CACHE_MAX:
                _slot_cache.pop(min(_slot_cache, key=lambda k: _slot_cache[k][0]),
                                None)
            _slot_cache[slot] = (now, data)
    return data


def ms(micros):
    return f"{micros/1000:.1f} ms" if micros >= 1000 else f"{micros} &micro;s"


def extend_table(stat):
    if stat is None:
        return ('<div class="dtl"><div class="dtlhead">mutation lane &mdash; '
                'extends</div><div class="none">no sim-extend points for this '
                'round &mdash; every extend was refused, or none was attempted'
                "</div></div>")
    head = ("<tr><th class=n>extends</th><th class=n>orders</th>"
            "<th class=n>applied</th><th class=n>body sum</th>"
            "<th class=n>body max</th><th class=n>body p50</th>"
            "<th class=n>queue sum</th><th class=n>exec sum</th>"
            "<th class=n>wall</th><th class=n>busy</th><th>status</th></tr>")
    row = (f"<tr><td class='n m'>{stat['n']}</td>"
           f"<td class='n m'>{stat['orders']:,}</td>"
           f"<td class='n m'>{stat['applied']:,}</td>"
           f"<td class='n m'>{ms(stat['body_sum'])}</td>"
           f"<td class='n m'>{ms(stat['body_max'])}</td>"
           f"<td class='n m'>{ms(stat['body_p50'])}</td>"
           f"<td class='n m'>{ms(stat['queue_sum'])}</td>"
           f"<td class='n m'>{ms(stat['exec_sum'])}</td>"
           f"<td class='n m'>{ms(stat['wall'])}</td>"
           f"<td class='n m'>{stat['busy']:.0f}%</td>"
           f"<td class='m dim'>"
           + ", ".join(f"{name}&times;{count}" for name, count in stat["statuses"])
           + "</td></tr>")
    return ('<div class="dtl"><div class="dtlhead">mutation lane &mdash; extends'
            '<span class="note">body = the extend itself (sanitize, check, DAG '
            'execute, overlay commit) &middot; queue = wait on the single '
            'mutation lane &middot; wall = round span, so busy% is how much of '
            'it the lane was working &middot; refused extends emit nothing'
            "</span></div>"
            f"<table><thead>{head}</thead><tbody>{row}</tbody></table></div>")


def detail_table(title, items, show_won=False):
    if not items:
        return f'<div class="dtl"><div class="dtlhead">{title}</div>' \
               '<div class="none">none</div></div>'
    head = ("<tr><th>time (UTC)</th><th class=n>orders</th><th class=n>txs</th>"
            "<th class=n>bundles</th><th class=n>reward SOL</th>"
            "<th class=n>exec cost</th><th class=n>selected cu</th><th>uuid</th>"
            + ("<th>won</th>" if show_won else "") + "</tr>")
    body = "".join(
        f"<tr><td class=m>{html.escape(i['ts'][11:23])}</td>"
        f"<td class='n m'>{i['orders']:,}</td><td class='n m'>{i['txs']:,}</td>"
        f"<td class='n m'>{i['bundles']:,}</td><td class='n m'>{sol(i['reward'])}</td>"
        f"<td class='n m'>{i['exec_cost']:,}</td><td class='n m'>{i['sel_cu']:,}</td>"
        f"<td class='m dim'>{html.escape(i['uuid'][:8])}&hellip;</td>"
        + (f"<td class=m>{'YES' if i['won'] else '-'}</td>" if show_won else "")
        + "</tr>"
        for i in items)
    return (f'<div class="dtl"><div class="dtlhead">{title}</div>'
            f"<table><thead>{head}</thead><tbody>{body}</tbody></table></div>")


def pcache_table(stat):
    """Program cache, per round.

    Two kinds of number, which is why they are not one row of sums:

      costs   program_cache_us, compile_us and clone_us are per extend and add
              up across the round. The first two are accumulated across replay
              workers, so like the other stage timings they are CPU time and
              can exceed wall clock -- read them against exec sum, never
              subtract them from body.

      sizes   entries and entries_cloned are snapshots taken inside a single
              extend (cache length at fork time, and at end of batch), so
              summing them would be meaningless. Reported as maxima.

    The fork is copy-on-write and only happens when an admitted order actually
    MODIFIES a program -- a deploy or upgrade landing in the batch. So `forks`
    is normally 0 and clone cost with it; a non-zero fork is the interesting
    case, not the default one."""
    if stat is None:
        return ""
    hot = stat["pc_compiles"] > 0 or stat["pc_forks"] > 0
    head = ("<tr><th class=n>cache us</th><th class=n>compiles</th>"
            "<th class=n>compile us</th><th class=n>programs loaded</th>"
            "<th class=n>forks</th><th class=n>clone us</th>"
            "<th class=n>entries cloned</th><th class=n>entries</th></tr>")
    cell = lambda v, warn=False: (
        f"<td class='n m{' bad' if warn and v else ''}'>{v}</td>")
    row = ("<tr>"
           + cell(ms(stat["pc_us"]))
           + cell(f"{stat['pc_compiles']:,}", warn=True)
           + cell(ms(stat["pc_compile_us"]), warn=stat["pc_compiles"] > 0)
           + cell(f"{stat['pc_loaded']:,}")
           + cell(stat["pc_forks"], warn=True)
           + cell(ms(stat["pc_clone_us"]), warn=stat["pc_forks"] > 0)
           + cell(f"{stat['pc_cloned_max']:,}")
           + cell(f"{stat['pc_entries_max']:,}")
           + "</tr>")
    note = ("<span class='note'>costs are per-extend sums and accumulate across "
            "replay workers, so they are CPU time, not wall &middot; entries and "
            "entries cloned are snapshots inside one extend, so they are maxima "
            "&middot; a fork happens only when an admitted order modifies a "
            "program, so 0 is the normal case</span>")
    return ('<div class="dtl"><div class="dtlhead">program cache'
            + ("<span class='warnpill'>compiled</span>" if stat["pc_compiles"]
               else "")
            + ("<span class='warnpill'>forked</span>" if stat["pc_forks"] else "")
            + note + "</div>"
            f"<table><thead>{head}</thead><tbody>{row}</tbody></table></div>")


def rounds_html(slot, sel_round):
    try:
        data = slot_data(slot)
    except Exception as exc:
        return f'<div class="err">rounds unavailable: {html.escape(str(exc))[:150]}</div>'
    rounds, extends, ext_err = data["rounds"], data["extends"], data["ext_err"]
    if not rounds:
        return f'<div class="empty">no miniblock rows for slot <b>{slot}</b></div>'

    out = []
    if data["runs_err"]:
        out.append('<div class="err" style="margin:0 28px 8px">builder run '
                   f"unavailable: {html.escape(data['runs_err'])}</div>")
    else:
        out.append(runs_html(slot, data["runs"]))
    if data["shred_err"]:
        out.append('<div class="err" style="margin:0 28px 8px">shred path '
                   f"unavailable: {html.escape(data['shred_err'])}</div>")
    else:
        out.append(shred_html(slot, data["shreds"]))
    if ext_err:
        out.append('<div class="err" style="margin:0 28px 8px">sim-extend '
                   f"unavailable: {html.escape(ext_err)}</div>")
    for r in rounds:
        w = r["winner"]
        stat = extends.get(r["round"])
        open_ = r["round"] == sel_round
        base = f"/?win={window_of(slot)}&slot={slot}"
        href = base if open_ else f"{base}&round={r['round']}"
        summary = (
            f'<a class="rnd {"open" if open_ else ""}" href="{href}">'
            f'<span class="rid">round {r["round"]}</span>'
            f'<span class="cnt">{len(r["offers"])} offer'
            f'{"" if len(r["offers"]) == 1 else "s"}</span>'
            + (f'<span class="won">won</span>' if w and w["won"]
               else '<span class="lost">not ours</span>' if w
               else '<span class="nowin">no winner echo</span>')
            + (f'<span class="last">is_last</span>' if w and w["is_last"] else "")
            + (f'<span class="ext">{stat["n"]} ext &middot; '
               f'{ms(stat["body_sum"])}</span>' if stat
               else '<span class="noext">0 ext</span>' if not ext_err else "")
            + f'<span class="chev">{"&minus;" if open_ else "+"}</span></a>')
        detail = ""
        if open_:
            detail = ('<div class="detail">'
                      + extend_table(stat)
                      + pcache_table(stat)
                      + detail_table(f"our offers &mdash; {len(r['offers'])}", r["offers"])
                      + detail_table("winner miniblock", [w] if w else [], show_won=True)
                      + "</div>")
        out.append(f'<div class="rndwrap">{summary}{detail}</div>')
    return "".join(out)


# ------------------------------------------------------------------ rendering

CSS = """
*{box-sizing:border-box}
body{margin:0;background:#0b1016;color:#dbe4ee;
  font:14px/1.55 ui-sans-serif,-apple-system,"SF Pro Text",Segoe UI,sans-serif}
header{padding:20px 28px 14px;display:flex;gap:16px;align-items:baseline;
  border-bottom:1px solid #1e2937}
h1{margin:0;font-size:17px;font-weight:650;letter-spacing:-.01em}
h1 span{color:#5eead4}
.sub{color:#6b7f96;font-size:12.5px}
.striphead{display:flex;gap:12px;align-items:baseline;padding:12px 28px 0}
.cap{color:#61748b;font-size:10.5px;text-transform:uppercase;letter-spacing:.07em}
.hint{color:#4d5c70;font-size:11.5px;margin-left:auto}

/* one row, scrolls horizontally: newest at the left, older to the right */
.strip{display:flex;gap:8px;padding:12px 28px 18px;overflow-x:auto;overflow-y:hidden;
  white-space:nowrap;border-bottom:1px solid #1e2937;scrollbar-width:thin;
  scrollbar-color:#22303f #0b1016}
.strip::-webkit-scrollbar{height:9px}
.strip::-webkit-scrollbar-track{background:#0c121a}
.strip::-webkit-scrollbar-thumb{background:#22303f;border-radius:5px}
.strip::-webkit-scrollbar-thumb:hover{background:#2f4256}

.chip{flex:0 0 auto;background:#141d28;border:1px solid #22303f;border-radius:8px;
  padding:8px 12px;min-width:136px;user-select:none;text-decoration:none;display:block}
.chip:hover{border-color:#14b8a6}
.chip.on{background:#0f766e;border-color:#5eead4}
.chip.on .slot,.chip.on .ageline,.chip.on .meta{color:#eafffb}
.chip .slot{font-family:ui-monospace,Menlo,monospace;font-size:12.5px;color:#cfe0f0}
.chip .ageline{font-size:11px;color:#6b7f96;margin-top:3px}
.age{font-variant-numeric:tabular-nums}

/* copy-on-hover; hidden until the row is hovered or the control is focused */
.cp{opacity:0;margin-left:7px;padding:0 5px;border:1px solid #2b3a4b;border-radius:4px;
  color:#6b7f96;font:9.5px/1.7 ui-sans-serif,sans-serif;text-transform:uppercase;
  letter-spacing:.05em;cursor:pointer;vertical-align:1px;
  transition:opacity .1s;display:inline-block}
.chip:hover .cp,.hascp:hover .cp,.cp:focus{opacity:1}
.cp:hover{color:#5eead4;border-color:#14b8a6;background:#0f766e22}
.cp.ok{opacity:1;color:#5eead4;border-color:#14b8a6;background:#0f766e33}
.cp.fail{opacity:1;color:#fca5a5;border-color:#7f1d1d}
.chip.on .cp{color:#c8fff7;border-color:#7fe9dd88}
@media (hover:none){.cp{opacity:.75}}
.chip .meta{font-size:10.5px;color:#4d5c70;margin-top:2px;
  font-family:ui-monospace,Menlo,monospace}
.chip .slot .n{color:#4d5c70;font-size:11px}
.chip.on .slot .n{color:#a7f3ea}

/* the open window's own slots, one step in */
.winbar{display:flex;gap:14px;align-items:baseline;padding:14px 28px 0;flex-wrap:wrap}
.winbar code{font-size:11.5px}
.strip.sub2{padding:8px 28px 16px;background:#0a0f15}
.chip.sm{min-width:150px;padding:7px 11px;background:#111823}
.warn{color:#fbbf24}
.okc{color:#5eead4}

/* won == we produced the block; everything else we bid on and lost */
.chip .tag{font-size:10px;color:#4d5c70;margin-top:3px}
.chip.won{border-color:#14b8a655;background:#0f1c1f}
.chip.won .tag{color:#5eead4}
.chip.won:hover{border-color:#5eead4}
.chip.on .tag{color:#c8fff7}

main{padding:44px 28px 60px}
main.rounds{padding:10px 0 60px}
.empty{color:#4d5c70;font-size:13px;border:1px dashed #1e2937;border-radius:10px;
  padding:40px;text-align:center}
.empty b{color:#6b7f96}
.err{background:#1e0d0d;border:1px solid #7f1d1d;color:#fca5a5;border-radius:9px;
  padding:11px 15px;margin:16px 28px;font-size:13px}
code{font-family:ui-monospace,Menlo,monospace;color:#8fa6bf}

/* rounds */
.slotbar{padding:20px 28px 4px;color:#9fb2c8;font-size:13px}
.slotbar b{color:#5eead4;font-family:ui-monospace,Menlo,monospace}
.rndwrap{margin:0 28px 8px}
a.rnd{display:flex;gap:14px;align-items:center;background:#111823;
  border:1px solid #1e2937;border-radius:9px;padding:10px 15px;text-decoration:none}
a.rnd:hover{border-color:#2f4256}
a.rnd.open{border-color:#14b8a6;background:#0f1a22;border-bottom-left-radius:0;
  border-bottom-right-radius:0}
.rid{font-family:ui-monospace,Menlo,monospace;font-size:13px;color:#cfe0f0;min-width:78px}
.cnt{color:#7d90a6;font-size:12.5px;min-width:74px}
.won{color:#5eead4;font-size:11px;font-weight:650;letter-spacing:.04em;
  background:#0f766e33;border:1px solid #14b8a655;border-radius:5px;padding:1px 7px}
.lost{color:#fbbf24;font-size:11px;background:#78350f33;border:1px solid #78350f;
  border-radius:5px;padding:1px 7px}
.nowin{color:#5b6b80;font-size:11px;border:1px solid #22303f;border-radius:5px;
  padding:1px 7px}
.last{color:#93c5fd;font-size:11px;border:1px solid #1d4ed855;border-radius:5px;
  padding:1px 7px}
.ext{color:#a5b4fc;font-size:11px;border:1px solid #3730a355;background:#312e8122;
  border-radius:5px;padding:1px 7px;font-family:ui-monospace,Menlo,monospace}
.noext{color:#7f5a5a;font-size:11px;border:1px solid #7f1d1d55;border-radius:5px;
  padding:1px 7px;font-family:ui-monospace,Menlo,monospace}
.chev{margin-left:auto;color:#5b6b80;font-family:ui-monospace,Menlo,monospace}
.dtlhead .note{text-transform:none;letter-spacing:0;color:#4d5c70;font-size:10px;
  margin-left:10px}
.warnpill{margin-left:8px;color:#fbbf24;font-size:9.5px;letter-spacing:.04em;
  border:1px solid #78350f;background:#78350f22;border-radius:4px;padding:1px 6px}
.detail .bad{color:#fbbf24}

/* shred-path panel */
.panel{margin:0 28px 14px;background:#0c141b;border:1px solid #1e2937;
  border-radius:9px;padding:12px 15px 14px}
.panel table{width:100%;border-collapse:collapse;font-size:11.5px}
.panel th{color:#61748b;font-weight:500;text-align:left;padding:4px 9px;
  border-bottom:1px solid #1e2937;font-size:10px;text-transform:uppercase}
.panel td{padding:4px 9px;border-bottom:1px solid #141d27}
.panel .m{font-family:ui-monospace,Menlo,monospace}
.panel .n{text-align:right}
.panel .dim{color:#5b6b80}
.panel .good{color:#5eead4}
.panel .bad{color:#fbbf24}
.panel .none{color:#4d5c70;font-size:12px;padding:6px 0 0}
.detail{border:1px solid #14b8a6;border-top:none;border-radius:0 0 9px 9px;
  background:#0c141b;padding:12px 15px 14px}
.dtl{margin-bottom:12px}
.dtl:last-child{margin-bottom:0}
.dtlhead{color:#61748b;font-size:10.5px;text-transform:uppercase;letter-spacing:.06em;
  margin-bottom:6px}
.detail table{width:100%;border-collapse:collapse;font-size:11.5px}
.detail th{color:#61748b;font-weight:500;text-align:left;padding:4px 9px;
  border-bottom:1px solid #1e2937;font-size:10px;text-transform:uppercase}
.detail td{padding:4px 9px;border-bottom:1px solid #141d27}
.detail .m{font-family:ui-monospace,Menlo,monospace}
.detail .n{text-align:right}
.detail .dim{color:#5b6b80}
.none{color:#4d5c70;font-size:12px;padding:6px 9px}
"""


# Ages keep counting after the page is rendered. The browser clock is not
# trusted on its own -- a machine several minutes off would show ages that
# disagree with the server's own numbers -- so we take the server's timestamp
# once at load and carry the difference as a fixed offset.
TICK_JS = """
(function(){
  var skew = __SERVER_NOW__ * 1000 - Date.now();   // server clock minus ours
  var nodes = [].slice.call(document.querySelectorAll('.age[data-t]'));
  if (!nodes.length) return;
  function fmt(s){
    if (s < 0) return 'now';
    var d = Math.floor(s/86400), h = Math.floor(s%86400/3600),
        m = Math.floor(s%3600/60), sec = s%60;
    if (d) return d+'d '+h+'h';
    if (h) return h+'h '+m+'m';
    if (m) return m+'m '+sec+'s';
    return sec+'s';
  }
  function tick(){
    var now = (Date.now() + skew) / 1000;
    for (var i = 0; i < nodes.length; i++){
      var n = nodes[i], suffix = n.dataset.suffix;
      if (suffix === undefined){
        // whatever trailed the server's number, usually ' ago'
        suffix = n.dataset.suffix = (n.textContent.match(/(\\s*ago)$/) || ['',''])[1];
      }
      n.textContent = fmt(Math.floor(now - (+n.dataset.t))) + suffix;
    }
  }
  tick();
  setInterval(tick, 1000);
  // a backgrounded tab throttles timers; resync the moment it comes back
  document.addEventListener('visibilitychange', function(){
    if (!document.hidden) tick();
  });
})();

// Copy-on-hover. The controls sit inside chip anchors, so the click must be
// swallowed or copying would also navigate. navigator.clipboard needs a secure
// context -- localhost qualifies, a bare LAN IP does not -- hence the fallback.
(function(){
  function flash(el, cls, label){
    var was = el.textContent;
    el.textContent = label; el.classList.add(cls);
    setTimeout(function(){ el.textContent = was; el.classList.remove(cls); }, 1100);
  }
  function legacy(text){
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.cssText = 'position:fixed;top:-1000px;opacity:0';
    document.body.appendChild(ta); ta.select();
    var ok = false;
    try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
    document.body.removeChild(ta);
    return ok;
  }
  function fallback(el, text){          // one attempt, one verdict
    var ok = legacy(text);
    flash(el, ok ? 'ok' : 'fail', ok ? 'copied' : 'failed');
  }
  function copy(el){
    var text = el.dataset.c;
    if (navigator.clipboard && window.isSecureContext){
      navigator.clipboard.writeText(text).then(
        function(){ flash(el, 'ok', 'copied'); },
        function(){ fallback(el, text); });
    } else {
      fallback(el, text);
    }
  }
  document.addEventListener('click', function(ev){
    var el = ev.target.closest('.cp');
    if (!el) return;
    ev.preventDefault(); ev.stopPropagation();
    copy(el);
  });
  document.addEventListener('keydown', function(ev){
    if (ev.key !== 'Enter' && ev.key !== ' ') return;
    var el = ev.target.closest && ev.target.closest('.cp');
    if (!el) return;
    ev.preventDefault(); ev.stopPropagation();
    copy(el);
  });
})();
"""


def strip_html(windows, sel_win, sel_slot):
    """One chip per leader window, newest at the left."""
    def chip(w):
        on = w["win"] == sel_win
        href = "/" if on else f"/?win={w['win']}"
        cls = "chip" + (" on" if on else "") + (" won" if w["won"] else "")
        return (f'<a class="{cls}" href="{href}" '
                f'title="{html.escape(w["ts"][:19])} UTC &mdash; leader '
                f'{html.escape(w["leader"])} &mdash; '
                f'{w["won"]}/{len(w["slots"])} slots won">'
                f'<div class="slot">{w["win"]}<span class="n">'
                f'&nbsp;+{len(w["slots"]) - 1}</span>{copy_btn(w["win"])}</div>'
                f'<div class="ageline">{age_html(w["ts"])}</div>'
                f'<div class="meta">{html.escape(short_id(w["leader"]))}'
                f'{" &#9888;" if w["leader_split"] else ""}</div>'
                f'<div class="tag">{w["won"]}/{len(w["slots"])} won</div></a>')

    chips = "".join(chip(w) for w in windows)
    slots = sum(len(w["slots"]) for w in windows)
    won = sum(w["won"] for w in windows)
    leaders = len({w["leader"] for w in windows if w["leader"]})
    return (f'<div class="striphead"><span class="cap">leader windows</span>'
            f'<span class="sub">{len(windows)} windows &middot; {slots} slots contested '
            f'&middot; <b class="okc">{won} won</b> &middot; {leaders} leaders '
            f'&middot; last 30d on <code>{CH_BUILDER}</code></span>'
            f'<span class="hint">newest at the left &mdash; scroll right for older</span>'
            f'</div><div class="strip">{chips}</div>'
            + window_html(windows, sel_win, sel_slot))


def window_html(windows, sel_win, sel_slot):
    """The expansion under the strip: the individual slots of the open window."""
    if sel_win is None:
        return ""
    win = next((w for w in windows if w["win"] == sel_win), None)
    if win is None:
        return (f'<div class="err">window {sel_win} is not in the produced list</div>')
    missing = WINDOW - len(win["slots"])
    def slotchip(s):
        on = s["slot"] == sel_slot
        href = f"/?win={sel_win}" if on else f"/?win={sel_win}&slot={s['slot']}"
        cls = "chip sm" + (" on" if on else "") + (" won" if s["won"] else "")
        return (f'<a class="{cls}" href="{href}">'
                f'<div class="slot">{s["slot"]}{copy_btn(s["slot"])}</div>'
                f'<div class="ageline">+{s["slot"] - sel_win} &middot; '
                f'{age_html(s["ts"])}</div>'
                f'<div class="tag">{"we produced" if s["won"] else "lost"} '
                f'&middot; {s["offers"]} offers</div></a>')

    return (f'<div class="winbar"><span class="cap hascp">window {sel_win}'
            f'{copy_btn(sel_win)}</span>'
            f'<span class="sub">slots {sel_win}&ndash;{sel_win + WINDOW - 1} &middot; '
            f'{len(win["slots"])} contested &middot; '
            f'<b class="okc">{win["won"]} won</b>'
            + (f' &middot; <span class="warn">{missing} with no offers from us</span>'
               if missing else "")
            + f'</span><span class="sub hascp">leader '
              f'<code>{html.escape(win["leader"])}</code>{copy_btn(win["leader"])}'
            + ('<span class="warn"> &#9888; more than one leader identity in this '
               'window</span>' if win["leader_split"] else "")
            + f'</span></div><div class="strip sub2">'
            + "".join(slotchip(s) for s in win["slots"]) + "</div>")


def page(sel_win=None, sel_slot=None, sel_round=None):
    try:
        windows = produced_windows()
    except Exception as exc:
        strip = (f'<div class="err">produced-window list unavailable: '
                 f"{html.escape(str(exc))[:160]}</div>")
        windows = []
    else:
        if not windows:
            strip = '<div class="err">no produced slots in the last 30 days</div>'
        else:
            if sel_slot is not None and sel_win is None:
                sel_win = window_of(sel_slot)
            strip = strip_html(windows, sel_win, sel_slot)

    if sel_slot is None:
        note = ("Pick a leader window above, then a slot inside it."
                if sel_win is None else "Pick a slot from this window.")
        body = (f'<main><div class="empty"><b>No slot selected.</b><br>{note}'
                "</div></main>")
    else:
        body = (f'<div class="slotbar hascp">slot <b>{sel_slot}</b>'
                f'{copy_btn(sel_slot)} &mdash; auction rounds. '
                f"Click a round for its offers and the winning miniblock.</div>"
                f"<main class=rounds>{rounds_html(sel_slot, sel_round)}</main>")
    title = f" &middot; slot {sel_slot}" if sel_slot else \
            f" &middot; window {sel_win}" if sel_win else ""
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">block-builder slot explorer</div>
</header>
{strip}
{body}
<script>{TICK_JS.replace("__SERVER_NOW__", f"{dt.datetime.now(dt.UTC).timestamp():.3f}")}</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        qs = urllib.parse.parse_qs(parsed.query)
        def as_int(name):
            try:
                return int(qs.get(name, [""])[0])
            except ValueError:
                return None
        try:
            body = page(as_int("win"), as_int("slot"), as_int("round")).encode()
        except Exception as exc:
            body = f"<pre>{html.escape(str(exc))}</pre>".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8899)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    print(f"simbench -> http://{args.host}:{args.port}")
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
