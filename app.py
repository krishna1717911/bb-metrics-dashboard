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
# instance_id as it appears in bifrost_events / otel ServiceName
CH_INSTANCE = os.environ.get("CH_INSTANCE", "")

# ClickStack: otel structured logs. A separate cluster from block_builder.
# Optional -- without it the health header keeps its InfluxDB and ClickHouse
# rows and marks the connector/scheduler rows unconfigured.
OTEL_URL = os.environ.get("OTEL_URL", "")
OTEL_USER = os.environ.get("OTEL_USER", "")
OTEL_PASS = os.environ.get("OTEL_PASS", "")
OTEL_DB = os.environ.get("OTEL_DB", "default")
OTEL_SERVICE = os.environ.get("OTEL_SERVICE", "")

# The relay's own ClickHouse, reached through its Grafana datasource proxy.
# Unlike bifrost_miniblocks this sees EVERY builder's submissions, and its
# round_chosen event names the winner -- which our own data structurally
# cannot, since won_by_us=false only ever means "not us".
RELAY_URL = os.environ.get("RELAY_URL", "")
RELAY_USER = os.environ.get("RELAY_USER", "")
RELAY_PASS = os.environ.get("RELAY_PASS", "")
RELAY_DS_UID = os.environ.get("RELAY_DS_UID", "")
# our builder_id as the relay knows it; usually the same as CH_BUILDER
RELAY_OUR_BUILDER = os.environ.get("RELAY_OUR_BUILDER", "") or CH_BUILDER

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


def relay(sql):
    """-> (columns, rows) from the relay ClickHouse via Grafana's datasource
    proxy. Cloudflare fronts that host and rejects a default urllib
    user-agent with a 403 "error code 1010" that reads like an auth failure,
    hence the explicit header."""
    if not (RELAY_URL and RELAY_DS_UID):
        raise RuntimeError("RELAY_URL / RELAY_DS_UID not configured")
    body = json.dumps({"queries": [{
        "refId": "A", "format": 1,
        "datasource": {"type": "grafana-clickhouse-datasource",
                       "uid": RELAY_DS_UID},
        "rawSql": sql}], "from": "now-24h", "to": "now"}).encode()
    token = base64.b64encode(f"{RELAY_USER}:{RELAY_PASS}".encode()).decode()
    req = urllib.request.Request(
        RELAY_URL.rstrip("/") + "/api/ds/query", data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": "Basic " + token,
                 "User-Agent": "simbench/1.0"})
    with urllib.request.urlopen(req, timeout=90) as resp:
        payload = json.load(resp)
    result = payload.get("results", {}).get("A", {})
    if result.get("error"):
        raise RuntimeError(result["error"][:200])
    frames = result.get("frames") or []
    if not frames:
        return [], []
    frame = frames[0]
    return ([f["name"] for f in frame["schema"]["fields"]],
            list(zip(*frame["data"]["values"])))


def otel(sql):
    """-> rows. ClickStack is a different cluster with different credentials."""
    if not (OTEL_URL and OTEL_SERVICE):
        raise RuntimeError("OTEL_URL / OTEL_SERVICE not configured")
    qs = urllib.parse.urlencode({"user": OTEL_USER, "password": OTEL_PASS,
                                 "database": OTEL_DB})
    req = urllib.request.Request(OTEL_URL + "?" + qs,
                                 data=(sql + " FORMAT TabSeparated").encode())
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode()
    return [ln.split("\t") for ln in text.split("\n") if ln]


TRUEISH = {"1", "true", "True"}


def ch_int(v, default=0):
    """ClickHouse TabSeparated writes NULL as the two characters \\N."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return default



# --------------------------------------------------------------- health

# A process being up is not the same as it building. The status heartbeat keeps
# ticking on a stalled builder, so the WORK stream is the only honest liveness
# signal: sim-extend / sim-commit / sim-context counts. Work is bursty by leader
# window, so zero between windows is normal and only a long zero run is a stall.
WORK_WINDOW_MIN = 20
# The pool only fills during a leader window, so this must span several.
SCHED_WINDOW_H = 6
_health_cache = {"at": 0.0, "data": None}
HEALTH_TTL = 30


def health_probe():
    """Six independent checks. Every one degrades on its own -- a dead otel
    cluster must not blank the InfluxDB rows, and vice versa."""
    now = time.monotonic()
    if _health_cache["data"] is not None and now - _health_cache["at"] < HEALTH_TTL:
        return _health_cache["data"]
    out = {}

    def run(name, fn):
        try:
            out[name] = {"ok": True, "v": fn()}
        except Exception as exc:
            out[name] = {"ok": False, "err": str(exc)[:120]}

    def work():
        got = {}
        for meas, field in (("sim-extend", "body_us"), ("sim-commit", "body_us"),
                            ("sim-context", "parent_slot"), ("sim-probe", "body_us")):
            series = influx_series(
                f'SELECT count("{field}") FROM "{meas}" '
                f"WHERE \"host_id\" = '{SIM}' AND time > now() - {WORK_WINDOW_MIN}m")
            got[meas] = int(series[0]["values"][0][1]) if series else 0
        return got

    def ingest():
        _, rows = clickhouse(
            "SELECT instance_id, dateDiff('second', max(ts), now()) AS lag_s, "
            "count() AS rows FROM bifrost_events "
            "WHERE ts > now() - INTERVAL 15 MINUTE "
            "GROUP BY instance_id ORDER BY lag_s")
        return [{"instance": r[0], "lag": ch_int(r[1]), "rows": ch_int(r[2])}
                for r in rows]

    def restart():
        _, rows = clickhouse(
            "SELECT toString(ts) AS at, attrs['peer_addr'] AS peer "
            "FROM bifrost_events WHERE ts > now() - INTERVAL 48 HOUR "
            f"  AND instance_id = '{CH_INSTANCE}' AND event = 'connected' "
            "  AND attrs['peer_addr'] LIKE '127.0.0.1%' ORDER BY ts DESC LIMIT 1")
        return {"at": rows[0][0], "peer": rows[0][1]} if rows else None

    def connector():
        # MUST pin Body='connector status'. 'builder network status' rows carry
        # no leader_state/slot attrs and would count as inactive, making a
        # healthy feed look dead.
        r = otel("SELECT countIf(LogAttributes['leader_state'] != 'Inactive') AS active, "
                 "countIf(LogAttributes['slot'] != '0') AS live_slot, count() AS total "
                 "FROM otel_logs WHERE Timestamp > now() - INTERVAL 10 MINUTE "
                 f"AND ServiceName = '{OTEL_SERVICE}' AND Body = 'connector status'")
        a, l, t = (int(x) for x in r[0])
        return {"active": a, "live_slot": l, "total": t}

    def network():
        r = otel("SELECT LogAttributes['connectors'] AS c, LogAttributes['relays'] AS s, "
                 "count() AS n FROM otel_logs "
                 "WHERE Timestamp > now() - INTERVAL 10 MINUTE "
                 f"AND ServiceName = '{OTEL_SERVICE}' AND Body = 'builder network status' "
                 "GROUP BY c, s ORDER BY n DESC")
        return [{"connectors": x[0], "relays": x[1], "n": ch_int(x[2])} for x in r]

    def scheduler():
        # The pool only fills during a leader window. Measured over an hour it
        # is non-zero in ~6 of 360 samples, so a short zero run is the normal
        # off-window resting state, not a fault -- judging it over 15 minutes
        # sat on amber permanently. Peak over a span covering several windows.
        r = otel("SELECT max(toUInt64OrZero(LogAttributes['pool'])) AS pool_max, "
                 "argMax(LogAttributes['pool'], Timestamp) AS pool_last, "
                 "argMax(LogAttributes['prefix'], Timestamp) AS prefix_last, "
                 "argMax(LogAttributes['inflight_probes'], Timestamp) AS inflight, "
                 "max(toUInt64OrZero(LogAttributes['probe_queue'])) AS queue_max, "
                 "countIf(toUInt64OrZero(LogAttributes['pool']) > 0) AS busy, "
                 "count() AS samples, toString(max(Timestamp)) AS at "
                 "FROM otel_logs WHERE Timestamp > now() - INTERVAL "
                 f"{SCHED_WINDOW_H} HOUR "
                 f"AND ServiceName = '{OTEL_SERVICE}' AND Body = 'scheduler status'")
        if not r or not ch_int(r[0][6]):
            return None
        return {"pool_max": ch_int(r[0][0]), "pool": r[0][1], "prefix": r[0][2],
                "inflight": r[0][3], "queue_max": ch_int(r[0][4]),
                "busy": ch_int(r[0][5]), "samples": ch_int(r[0][6]),
                "at": r[0][7]}

    for name, fn in (("work", work), ("ingest", ingest), ("restart", restart),
                     ("connector", connector), ("network", network),
                     ("scheduler", scheduler)):
        run(name, fn)
    _health_cache.update(at=now, data=out)
    return out


def health_html():
    try:
        h = health_probe()
    except Exception as exc:
        return ('<div class="err">health probe failed: '
                f"{html.escape(str(exc))[:160]}</div>")
    cells = []

    def info(label):
        """The i affordance. Text comes from METRIC_DOCS, the same source the
        reference page renders, so the two cannot disagree."""
        d = DOC_BY_NAME.get(label)
        if not d:
            return ""
        rows = "".join(
            f'<div class="pr"><span class="pk {cls}">{k}</span>'
            f'<span class="pv">{v}</span></div>'
            for k, v, cls in (("healthy", d["good"], "greenc"),
                              ("amber", d["amber"], "amberc"),
                              ("red", d["red"], "redc"))
            if v and v != "&mdash;")
        cls, plabel = PROV[d["prov"]]
        return (f'<span class="info" tabindex="0" role="button" '
                f'aria-label="what {label} means">i'
                f'<span class="pop"><span class="pm">{d["meaning"]}</span>'
                f"{rows}"
                f'<span class="pg">{d["gotcha"]}</span>'
                f'<span class="pf">cutoffs: <span class="{cls}">{plabel}</span>'
                f' &middot; <a href="/reference">full reference</a></span>'
                "</span></span>")

    def cell(label, value, state, detail=""):
        cells.append(f'<div class="hc {state}"><div class="hl">{label}'
                     f"{info(label)}</div>"
                     f'<div class="hv">{value}</div>'
                     f'<div class="hd">{detail}</div></div>')

    w = h["work"]
    if w["ok"]:
        v = w["v"]
        building = v["sim-extend"] + v["sim-commit"]
        cell("building", "yes" if building else "idle",
             "good" if building else "warn",
             f'extend {v["sim-extend"]:,} &middot; commit {v["sim-commit"]:,} '
             f'&middot; ctx {v["sim-context"]:,} &middot; probe {v["sim-probe"]:,} '
             f"/ {WORK_WINDOW_MIN}m")
    else:
        cell("building", "?", "dead", html.escape(w["err"]))

    c = h["connector"]
    if c["ok"]:
        v = c["v"]
        # live_slot is the signal: an idle connector still reports the chain
        # head. active only rises when a served validator is actually leading,
        # so active=0 is normal and is NOT a fault.
        cell("connector feed", "live" if v["live_slot"] else "DEAD",
             "good" if v["live_slot"] else "dead",
             f'live_slot {v["live_slot"]}/{v["total"]} &middot; '
             f'active {v["active"]} (0 is normal off-window)')
    else:
        cell("connector feed", "n/a", "off", html.escape(c["err"]))

    n = h["network"]
    if n["ok"] and n["v"]:
        top = n["v"][0]
        flapping = len(n["v"]) > 1
        cell("topology", f'{top["connectors"]}c / {top["relays"]}r',
             "warn" if flapping else "good",
             "flapping: " + ", ".join(f'{x["connectors"]}c/{x["relays"]}r'
                                      for x in n["v"][:3]) if flapping
             else "stable over 10m")
    elif n["ok"]:
        cell("topology", "none", "warn", "no builder network status in 10m")
    else:
        cell("topology", "n/a", "off", html.escape(n["err"]))

    s = h["scheduler"]
    if s["ok"] and s["v"]:
        v = s["v"]
        # zero only counts as a fault across a span covering several leader
        # windows; off-window zero is the normal resting state
        idle = v["pool_max"] == 0
        cell("scheduler", f'pool {v["pool_max"]:,}', "warn" if idle else "good",
             f'peak over {SCHED_WINDOW_H}h &middot; latest {v["pool"]} &middot; '
             f'busy in {v["busy"]}/{v["samples"]} samples &middot; queue peak '
             f'{v["queue_max"]:,}'
             + (f" &mdash; nothing to build in {SCHED_WINDOW_H}h" if idle
                else " &mdash; 0 off-window is normal"))
    elif s["ok"]:
        cell("scheduler", "silent", "warn", "no scheduler status in 15m")
    else:
        cell("scheduler", "n/a", "off", html.escape(s["err"]))

    g = h["ingest"]
    if g["ok"] and g["v"]:
        mine = next((x for x in g["v"] if x["instance"] == CH_INSTANCE), None)
        best = g["v"][0]["lag"]
        if mine:
            state = "good" if mine["lag"] <= 60 else "warn" if mine["lag"] <= 300 else "dead"
            cell("event ingest", f'{mine["lag"]}s lag', state,
                 f'{mine["rows"]:,} rows/15m &middot; best peer {best}s '
                 f'({len(g["v"])} instances)')
        else:
            cell("event ingest", "absent", "dead",
                 f"{CH_INSTANCE} wrote nothing in 15m")
    else:
        cell("event ingest", "n/a", "off",
             html.escape(g.get("err", "no rows")))

    r = h["restart"]
    if r["ok"] and r["v"]:
        cell("last restart", ago(r["v"]["at"]) + " ago", "good",
             f'{html.escape(r["v"]["at"][:19])} UTC &middot; local sim reconnect '
             f'from {html.escape(r["v"]["peer"])}')
    elif r["ok"]:
        cell("last restart", "&gt;48h", "good", "no local-sim reconnect in 48h")
    else:
        cell("last restart", "n/a", "off", html.escape(r["err"]))

    return f'<div class="health">{"".join(cells)}</div>'


# ------------------------------------------------------------ produced slots

_cache = {"windows": None, "at": 0.0}

WINDOW = 4  # a Solana leader window is four consecutive slots

# run_id is one builder process lifetime, so it changes on every restart. A
# window served shortly after one ran against a cold program cache and a cold
# account overlay, and its timings are not comparable to a window served hours
# into the same run -- which is the whole reason the strip shows the run at all.
COLD_RUN_SECONDS = 600


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
        "       max(won_by_us) AS won, countIf(kind = 'selected') AS offers, "
        "       any(run_id) AS run_id "
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
        if len(r) > 5 and r[5]:
            win.setdefault("runs", set()).add(r[5])
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
        runs = sorted(win.pop("runs", set()))
        # a restart mid-window is rare but real; surface it rather than
        # silently showing one of the two
        win["run_id"] = runs[0] if runs else ""
        win["run_split"] = len(runs) > 1
        out.append(win)

    # One extra query for the whole strip: when each run began. That is what
    # makes run_id actionable -- a window served minutes after a restart ran
    # against a cold program cache and a cold account overlay, so its timings
    # are not comparable to one served hours in.
    run_ids = sorted({w["run_id"] for w in out if w["run_id"]})
    starts = {}
    if run_ids:
        try:
            quoted = ",".join("'" + r.replace("'", "") + "'" for r in run_ids)
            _, rrows = clickhouse(
                "SELECT run_id, toString(min(ts)) AS started, uniqExact(slot) AS slots "
                f"FROM bifrost_miniblocks WHERE run_id IN ({quoted}) "
                f"  AND local_builder_id = '{CH_BUILDER}' GROUP BY run_id")
            starts = {r[0]: (r[1], ch_int(r[2])) for r in rrows}
        except Exception:
            starts = {}
    for win in out:
        started, run_slots = starts.get(win["run_id"], (None, 0))
        win["run_started"] = started
        win["run_slots"] = run_slots
        age = None
        if started:
            a, b = epoch(started), epoch(win["ts"])
            age = None if (a is None or b is None) else b - a
        win["run_age"] = age
        win["run_cold"] = age is not None and age < COLD_RUN_SECONDS

    _cache.update(windows=out, at=now)
    return out


def copy_btn(value):
    """A copy affordance that lives inside the chip's <a>. It is a span, not a
    button, because a button nested in an anchor is invalid HTML; the click is
    intercepted in JS so copying never navigates."""
    return (f'<span class="cp" data-c="{html.escape(str(value), quote=True)}" '
            f'role="button" tabindex="0" title="copy {html.escape(str(value))}"'
            f'>copy</span>')


def cold_why(w):
    """Why this window is marked cold, in its own numbers.

    Escaped for an HTML attribute because it is read by the tooltip script,
    not rendered as markup."""
    age = w.get("run_age")
    mins = "" if age is None else (f"{int(age)}s" if age < 90
                                   else f"{int(age // 60)}m {int(age % 60)}s")
    started = (w.get("run_started") or "")[:19]
    return html.escape(
        f"This window began {mins} after builder run "
        f"{short_run(w.get('run_id',''))} started"
        + (f" at {started} UTC" if started else "")
        + f", inside the first {COLD_RUN_SECONDS // 60} minutes of the process. "
        "A fresh process starts with an empty program cache and an empty "
        "account overlay, so early extends pay for JIT compiles and account "
        "loads that a warmed process has already done. Treat this window's "
        "extend and commit timings as not comparable with one served hours "
        "into the same run.", quote=True)


def short_run(run_id):
    """run_id is a UUID; the chip has room for its first block only. The full
    value is in the chip title and is copyable from there."""
    return run_id.split("-")[0] if run_id else "?"


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


SOLANA_RPC = os.environ.get("SOLANA_RPC", "")
_block_cache = {}
_block_lock = threading.Lock()


def slot_votes(slot):
    """Which of this slot's transactions are votes, keyed by SigPrefix.

    Miniblocks DO carry votes -- measured on slot 439391881, 388 of the
    winner's 553 transaction refs were votes, and 680 of our 1,209 selected
    signatures. So "non-vote" is a real distinction and not a formality.

    Needs the block, hence an RPC call, hence optional: without SOLANA_RPC the
    non-vote columns read as unknown rather than as zero. Only the comparison
    tab asks for this."""
    if not SOLANA_RPC:
        return None
    with _block_lock:
        if slot in _block_cache:
            return _block_cache[slot]
    try:
        req = urllib.request.Request(
            SOLANA_RPC, headers={"Content-Type": "application/json"},
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getBlock",
                             "params": [int(slot), {
                                 "encoding": "json",
                                 "transactionDetails": "accounts",
                                 "rewards": False,
                                 "maxSupportedTransactionVersion": 0}]}).encode())
        blk = json.load(urllib.request.urlopen(req, timeout=120)).get("result")
    except Exception:
        blk = None
    votes = {}
    for t in (blk or {}).get("transactions", []):
        sig = t["transaction"]["signatures"][0]
        keys = [k["pubkey"] if isinstance(k, dict) else k
                for k in t["transaction"]["accountKeys"]]
        votes[b58decode(sig)[:16]] = (VOTE_PROGRAM in keys)
    with _block_lock:
        if len(_block_cache) > 32:
            _block_cache.clear()
        _block_cache[slot] = votes
    return votes


VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    n = 0
    for c in s:
        n = n * 58 + _B58.index(c)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\0" * (len(s) - len(s.lstrip("1"))) + body


def slot_our_sigs(slot):
    """Our offered signatures per round. `selected` rows are cumulative
    prefixes, so the union across a round is the final offer's membership."""
    out = {}
    try:
        _, rows = clickhouse(
            "SELECT index_in_slot, arrayDistinct(arrayFlatten(groupArray(signatures))) "
            "  AS sigs FROM bifrost_miniblocks "
            f"WHERE slot = {int(slot)} AND kind = 'selected' "
            f"  AND local_builder_id = '{CH_BUILDER}' GROUP BY index_in_slot")
    except Exception:
        return out
    for r in rows:
        raw = (r[1] or "").strip("[]")
        sigs = [x.strip().strip("'") for x in raw.split(",") if x.strip()]
        out[ch_int(r[0])] = sigs
    return out


def slot_winner_refs(slot):
    """Decode each foreign winner payload into its transaction and bundle refs.

    A foreign winner row reports transaction_count=0 and bundle_count=0 -- the
    counts are only populated for our own blocks -- but the payload carries the
    composition. Layout is a 55-byte header then tagged records:
    u32 0 + 16-byte SigPrefix for a transaction, u32 1 + 32-byte id for a
    bundle. Validated against order_count on every row."""
    out = {}
    try:
        _, rows = clickhouse(
            "SELECT index_in_slot, order_count, hex(payload) AS payload "
            f"FROM bifrost_miniblocks WHERE slot = {int(slot)} AND kind = 'winner' "
            f"  AND local_builder_id = '{CH_BUILDER}' AND length(payload) > 55")
    except Exception:
        return out
    for r in rows:
        try:
            idx, want, b = ch_int(r[0]), ch_int(r[1]), bytes.fromhex(r[2])
        except (ValueError, TypeError):
            continue
        off, txs, bundles = 55, [], 0
        while off + 4 <= len(b):
            tag = int.from_bytes(b[off:off + 4], "little")
            size = {0: 16, 1: 32}.get(tag)
            if size is None or off + 4 + size > len(b):
                break
            if tag == 0:
                txs.append(b[off + 4:off + 20])
            else:
                bundles += 1
            off += 4 + size
        if len(txs) + bundles != want:      # layout did not hold; do not guess
            continue
        out[idx] = {"txs": txs, "bundles": bundles}
    return out


def slot_relay(slot, stamps):
    """Per-round opponent view from the relay, keyed by index_in_slot.

    Two different things are collected for the opponent, and which one the
    table shows depends on who won:

      won_round   the round_chosen event -- their block AS ACCEPTED, the
                  authoritative reward/CU/orders for the round
      best_offer  their highest `submitted` offer, which is what to compare
                  against when the round did not go to them

    Also carries our own offer count and rejection reason as the relay saw
    them, which our own data cannot show: bifrost_miniblocks records what we
    sent, never the relay's verdict on it."""
    if not stamps or not (RELAY_URL and RELAY_DS_UID):
        return {}
    pad = dt.timedelta(minutes=10)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    ours = RELAY_OUR_BUILDER.replace("'", "")
    cols, rows = relay(
        "SELECT index_in_slot AS rnd,"
        " argMaxIf(assumeNotNull(builder_id), assumeNotNull(reward),"
        "          event = 'round_chosen') AS win_builder,"
        " maxIf(assumeNotNull(reward), event = 'round_chosen') AS win_reward,"
        " argMaxIf(assumeNotNull(execution_cost), assumeNotNull(reward),"
        "          event = 'round_chosen') AS win_cu,"
        " argMaxIf(order_count, assumeNotNull(reward),"
        "          event = 'round_chosen') AS win_orders,"
        f" argMaxIf(assumeNotNull(builder_id), assumeNotNull(reward),"
        f"          event = 'submitted' AND builder_id != '{ours}') AS opp_builder,"
        f" maxIf(assumeNotNull(reward),"
        f"       event = 'submitted' AND builder_id != '{ours}') AS opp_reward,"
        f" argMaxIf(assumeNotNull(execution_cost), assumeNotNull(reward),"
        f"          event = 'submitted' AND builder_id != '{ours}') AS opp_cu,"
        f" argMaxIf(order_count, assumeNotNull(reward),"
        f"          event = 'submitted' AND builder_id != '{ours}') AS opp_orders,"
        f" countIf(event = 'submitted' AND builder_id = '{ours}') AS our_subs,"
        f" anyIf(reason, event = 'offer_rejected'"
        f"       AND builder_id = '{ours}') AS our_reject"
        " FROM relay.mini_block_events"
        f" WHERE timestamp >= '{lo}' AND timestamp <= '{hi}'"
        f"   AND slot = {int(slot)}"
        " GROUP BY rnd ORDER BY rnd")
    if not rows:
        return {}
    at = {n: i for i, n in enumerate(cols)}
    out = {}
    for r in rows:
        def val(name):
            v = r[at[name]] if name in at else None
            return v

        def num(name):
            try:
                return int(val(name) or 0)
            except (TypeError, ValueError):
                return 0

        out[num("rnd")] = {
            "win_builder": val("win_builder") or "",
            "win_reward": num("win_reward"), "win_cu": num("win_cu"),
            "win_orders": num("win_orders"),
            "opp_builder": val("opp_builder") or "",
            "opp_reward": num("opp_reward"), "opp_cu": num("opp_cu"),
            "opp_orders": num("opp_orders"),
            "our_subs": num("our_subs"), "our_reject": val("our_reject") or ""}
    return out


def slot_commits(slot, stamps):
    """sim-commit for one slot, bucketed by round via the same `index` field.

    When we lose a round the commit is a REPLAY of the foreign winner's block,
    so body_us here is the cost of replaying someone else's composition before
    we can build on top of it. `replayed` is an ORDER COUNT, not a duration."""
    if not stamps:
        return {}
    pad = dt.timedelta(seconds=150)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%dT%H:%M:%SZ")
    # sim-context rides along: one query, and it is the timeline's t0 -- the
    # moment a leader-window context installed and sequencing could begin.
    series = influx_series(
        'SELECT "index","body_us","queue_us","replayed","promoted_len","applied",'
        '"winner","refusal","parent_slot","hold_us" '
        f'FROM "sim-commit","sim-context" WHERE slot = {int(slot)} '
        + "".join(f"AND \"host_id\" != '{h}' " for h in OFF_CHAIN)
        + f"AND time >= '{lo}' AND time <= '{hi}'")
    ctx, cols, rows = None, [], []
    for ser in series:
        if ser["name"] == "sim-context":
            c = {n: i for i, n in enumerate(ser["columns"])}
            v = ser["values"][0]
            ctx = {"t": _rfc3339_ns(v[0]),
                   "parent": int(v[c["parent_slot"]] or 0)
                   if "parent_slot" in c else 0}
        elif ser["name"] == "sim-commit":
            cols, rows = ser["columns"], ser["values"]
    if not rows:
        return {"_context": ctx} if ctx else {}
    at = {name: i for i, name in enumerate(cols)}
    out = {}
    if ctx:
        out["_context"] = ctx
    for r in rows:
        def num(field):
            try:
                return int(r[at[field]] or 0)
            except (TypeError, ValueError):
                return 0
        idx = num("index")
        e = out.setdefault(idx, {"n": 0, "body": 0, "replayed": 0,
                                 "promoted": 0, "queue": 0, "kind": 0,
                                 "refusal": 0, "t": _rfc3339_ns(r[at["time"]])})
        e["n"] += 1
        e["body"] += num("body_us")
        e["queue"] += num("queue_us")
        e["replayed"] += num("replayed")
        e["promoted"] += num("promoted_len")
        e["refusal"] += num("refusal")
        # workers.rs winner_kind(): 0 empty, 1 promote, 2 replay. Keep the
        # heaviest kind seen -- a replay is what actually costs.
        e["kind"] = max(e["kind"], num("winner"))
    return out


# workers.rs winner_kind()
COMMIT_KIND = {0: "empty", 1: "promote", 2: "replay"}


def commit_cell(com):
    """A commit is not always a replay, and calling it one hides the single
    biggest cost difference in the round.

      replay   we lost, so their winning block is re-executed on our bank
               before we can extend on top of it -- tens of milliseconds
      promote  we won, so the prefix we already hold is just pointed at -- a
               pointer move, tens of microseconds
      empty    a winnerless round; nothing to apply
    """
    if not com:
        return '<span class="dim">&mdash;</span>'
    kind = COMMIT_KIND.get(com["kind"], "?")
    if kind == "replay":
        return (f'{ms(com["body"])}<div class="basis">replay '
                f'{com["replayed"]:,} orders</div>')
    if kind == "promote":
        return (f'{ms(com["body"])}<div class="basis">promote, no replay</div>')
    return f'{ms(com["body"])}<div class="basis">empty round</div>' 


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
            "t_first": calls[0]["t"] - calls[0]["body"] * 1000,  # start, not completion
            "t_last": calls[-1]["t"],
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
# Comma-separated, in preference order. Two identities are needed because they
# are COMPLEMENTARY, not redundant: measured over 30 days one covers ~96% of
# contested slots but none of the ones we won, while the other covers only the
# ~800-slot stretches around our own leader windows. Whichever has data for a
# given slot is the leader for that slot.
LEADERS = [h.strip() for h in os.environ.get("SHRED_LEADER_ID", "").split(",")
           if h.strip()]
LEADER = LEADERS[0] if LEADERS else ""
SIM = os.environ.get("SHRED_SIM_ID", "")
OFF_CHAIN = [h.strip() for h in os.environ.get("SHRED_EXCLUDE_IDS", "").split(",")
             if h.strip()]
HOST_NAME = dict({h: "leader" for h in LEADERS}, **{SIM: "our simulator"})
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
        if host in OFF_CHAIN or (host not in LEADERS and host != SIM):
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
    # Pick the leader identity that actually reported this slot. They are
    # complementary, so at most one normally has it; ties break on the
    # configured order.
    present = {h for v in shreds.values() for h in v}
    leader = next((h for h in LEADERS if h in present), LEADER)
    hosts = [h for h in (leader, SIM) if h]
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
        if leader in seen and SIM in seen:
            d = (seen[SIM]["t"] - seen[leader]["t"]) / 1e6
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
    if not any(leader in v for v in shreds.values()):
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
            + (f'<span class="basis">leader identity {html.escape(leader)}</span>'
               if leader else "")
            + '<span class="note">positive delta = our simulator was later &middot; '
            'joined on slot, not on time &middot; our host does not emit '
            "retransmit-first-shred</span>" + missing + "</div>"
            f"<table><thead>{head}</thead><tbody>{''.join(body)}</tbody></table></div>")


# ------------------------------------------------ one fetch per slot, cached

_slot_cache = {}                       # slot -> (monotonic_at, data)
_slot_lock = threading.Lock()
SLOT_CACHE_TTL = 300
SLOT_CACHE_MAX = 64

# A leader window is the unit people actually inspect -- you open one slot and
# then walk the other three. Warming them in the background turns those clicks
# from a multi-second fetch into a cache hit. Bounded so a burst of navigation
# cannot pile up connections against ClickHouse and InfluxDB.
_prefetch = None
_prefetch_lock = threading.Lock()
_inflight = {}                         # slot -> Event, for single-flight


def _prefetch_pool():
    global _prefetch
    with _prefetch_lock:
        if _prefetch is None:
            import concurrent.futures as cf
            _prefetch = cf.ThreadPoolExecutor(max_workers=2,
                                              thread_name_prefix="warm")
        return _prefetch


def warm_window(win, skip=None):
    """Fetch the other slots of this window in the background.

    Never blocks the request that triggered it, and never duplicates work: a
    slot already cached or already being fetched is skipped, so a foreground
    request and a prefetch cannot both hit the datastores for the same slot."""
    if win is None:
        return
    try:
        windows = produced_windows()
    except Exception:
        return
    entry = next((w for w in windows if w["win"] == win), None)
    if entry is None:
        return
    now = time.monotonic()
    for s in entry["slots"]:
        slot = s["slot"]
        if slot == skip:
            continue
        with _slot_lock:
            hit = _slot_cache.get(slot)
            if hit and now - hit[0] < SLOT_CACHE_TTL:
                continue
        with _prefetch_lock:
            if slot in _inflight:
                continue

        def task(target=slot):
            try:
                slot_data(target)      # takes the single-flight lease itself
            except Exception:
                pass

        try:
            _prefetch_pool().submit(task)
        except Exception:
            pass


def slot_data(slot):
    """Everything the slot views need: rounds, extends, commits, shreds, runs.

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

    # Single-flight. Without it the ordinary path double-fetches: expanding a
    # window starts a prefetch, and clicking a slot a moment later starts a
    # second fetch for the same slot. The first caller does the work; the rest
    # wait on it and then read the cache.
    with _prefetch_lock:
        leader = slot not in _inflight
        if leader:
            _inflight[slot] = threading.Event()
        done = _inflight[slot]
    if not leader:
        done.wait(timeout=180)
        with _slot_lock:
            hit = _slot_cache.get(slot)
        if hit:
            return hit[1]
        return _slot_data_uncached(slot, time.monotonic())
    try:
        return _slot_data_uncached(slot, now)
    finally:
        with _prefetch_lock:
            _inflight.pop(slot, None)
        done.set()


def _slot_data_uncached(slot, now):
    rounds = slot_rounds(slot)
    stamps = [i["ts"] for r in rounds
              for i in r["offers"] + ([r["winner"]] if r["winner"] else [])]
    # The two InfluxDB queries are independent and each costs seconds against a
    # 300s window, so run them together rather than back to back.
    import concurrent.futures as cf

    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        fut = {"ext": pool.submit(slot_extends, slot, stamps),
               "commit": pool.submit(slot_commits, slot, stamps),
               "relay": pool.submit(slot_relay, slot, stamps),
               "shred": pool.submit(slot_shreds, slot, stamps)}
    try:
        extends, ext_err = fut["ext"].result(), None
    except Exception as exc:
        extends, ext_err = {}, str(exc)[:140]
    try:
        commits = fut["commit"].result()
    except Exception:
        commits = {}
    try:
        relay_rounds, relay_err = fut["relay"].result(), None
    except Exception as exc:
        relay_rounds, relay_err = {}, str(exc)[:140]
    try:
        shreds, shred_err = fut["shred"].result(), None
    except Exception as exc:
        shreds, shred_err = {}, str(exc)[:140]
    data = {"rounds": rounds, "extends": extends, "ext_err": ext_err,
            "commits": commits, "relay": relay_rounds,
            "relay_err": relay_err,
            "shreds": shreds, "shred_err": shred_err}

    if ext_err is None and shred_err is None:
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


# ---------------------------------------------------- per-round comparison

# The relay never tells us WHICH builder won a round it did not award us. The
# opponent is therefore named by exclusion, not identified: won_by_us=false
# only means someone else took it. Set OPPONENT_NAME when you know who that
# is in practice; the label is a convenience, not evidence.
OPPONENT = os.environ.get("OPPONENT_NAME", "other builder")
OURS_NAME = os.environ.get("OUR_NAME", "us")


def big(v):
    """Lamports and CU both run to millions; the table is unreadable raw."""
    if v is None:
        return "&mdash;"
    v = float(v)
    if abs(v) >= 1e6:
        return f"{v/1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"{v/1e3:.1f}k"
    return f"{v:.0f}"


def timeline_html(slot, extends, commits, shreds, rounds):
    """What happened in this slot, in order.

    ONE CLOCK ONLY. Every row here comes from InfluxDB, and the shred rows are
    pinned to our own host, so the ordering is trustworthy. Relay and
    ClickHouse timestamps are deliberately absent: those are separate clocks
    and have been observed 11.5s apart, which would silently reorder the
    sequence. The comparison table below carries the relay's side instead.

    Extends are collapsed per round -- twenty of them in a round is a burst,
    not twenty things worth reading."""
    ctx = (commits or {}).get("_context")
    ev = []
    if ctx:
        ev.append((ctx["t"], "context installed", "sequencing can begin",
                   f'parent slot {ctx["parent"]}', "start"))
    for idx in sorted(k for k in (extends or {}) if isinstance(k, int)):
        e = extends[idx]
        span = max(0, (e["t_last"] - e["t_first"]) / 1e6)
        ev.append((e["t_first"], f"round {idx} extends",
                   f'{e["n"]} accepted over {span:.0f} ms',
                   f'p50 {ms(e["body_p50"])} &middot; max {ms(e["body_max"])} '
                   f'&middot; {e["applied"]:,} of {e["orders"]:,} orders applied',
                   "ext"))
    for idx in sorted(k for k in (commits or {}) if isinstance(k, int)):
        c = commits[idx]
        kind = COMMIT_KIND.get(c["kind"], "?")
        detail = (f'replay {c["replayed"]:,} orders' if kind == "replay"
                  else "promote, no replay" if kind == "promote"
                  else "winnerless, nothing applied")
        ev.append((c["t"], f"round {idx} commit",
                   f'{kind} &middot; {ms(c["body"])}', detail, kind))
    for meas, label, note in (("retransmit-first-shred", "first shred received",
                               "turbine delivered the leader's first shred"),
                              ("shred_insert_is_full", "slot complete",
                               "every shred in the blockstore"),
                              ("bank_frozen", "bank frozen",
                               "state final, replay done"),
                              ("optimistic_slot", "optimistic confirmed",
                               "supermajority voted")):
        e = (shreds or {}).get(meas, {}).get(SIM)
        if not e:
            continue
        # only shred_insert_is_full carries a duration; the rest are instants
        extra = (f'insert took {e["total_time_ms"]:,} ms'
                 + (f', {e["num_recovered"]:,} recovered'
                    if e.get("num_recovered") else "")
                 + (f', {e["num_repaired"]:,} repaired'
                    if e.get("num_repaired") else "")
                 if e.get("total_time_ms") is not None else "")
        ev.append((e["t"], label, note, extra, "chain"))
    if not ev:
        return ('<div class="panel"><div class="dtlhead">slot timeline</div>'
                '<div class="none">no InfluxDB events for this slot</div></div>')
    ev.sort(key=lambda x: x[0])
    t0 = ev[0][0]
    total = max(1, ev[-1][0] - t0)
    rows = []
    for t, label, what, detail, cls in ev:
        off = (t - t0) / 1e6
        pct = 100.0 * (t - t0) / total
        rows.append(
            f'<tr class="tl-{cls}"><td class="n m">+{off:,.1f} ms</td>'
            f'<td class="m dim">{dt.datetime.fromtimestamp(t/1e9, dt.UTC).strftime("%H:%M:%S.%f")[:-3]}</td>'
            f'<td class="m"><b>{label}</b></td>'
            f'<td class="m">{what}</td>'
            f'<td class="m dim">{detail}</td>'
            f'<td class="bar"><span style="margin-left:{pct:.2f}%"></span></td></tr>')
    return ('<div class="panel"><div class="dtlhead">slot timeline'
            "<span class='note'>InfluxDB only, and shred rows pinned to our own "
            "host, so this is a single clock and the order is real &middot; "
            "relay and ClickHouse timestamps are excluded on purpose -- separate "
            "clocks, observed 11.5s apart &middot; extends are collapsed per "
            "round &middot; offsets are from the first event, not slot start"
            "</span></div>"
            "<table><thead><tr><th class=n>offset</th><th>UTC</th><th>event</th>"
            "<th>what</th><th>detail</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>")


_extra_cache = {}


def compare_extras(slot):
    """Bundle and non-vote inputs for the comparison tab only, so the rounds
    view never pays for the block fetch."""
    with _slot_lock:
        hit = _extra_cache.get(slot)
    if hit is not None:
        return hit
    extras = {"theirs": slot_winner_refs(slot), "ours": slot_our_sigs(slot),
              "votes": slot_votes(slot)}
    with _slot_lock:
        if len(_extra_cache) > 32:
            _extra_cache.clear()
        _extra_cache[slot] = extras
    return extras


def compare_html(slot, rounds, extends, commits, relay_rounds=None,
                 relay_err=None, extras=None):
    """Round-by-round: what won, what we offered, and what the lane spent.

    THEIR side comes from the relay when we have it, because the relay sees
    every builder and names the winner, while our own data only knows
    "won_by_us: false". Which of their numbers is shown depends on the result:

      they won   -> their round_chosen block, i.e. what was actually accepted
      we won     -> their best `submitted` offer, i.e. what they bid and lost

    Falling back to the bifrost_miniblocks winner row when the relay has
    nothing keeps slots off that relay (a different connector) readable.

    OUR offer is the FINAL cumulative offer, not a sum of the selected rows:
    those rows are cumulative prefixes, so summing double-counts. Verified on
    rounds we won, where the winner row IS our composition -- winner.order_count
    equals max of selected (128=128, 208=208, 360=360), never the sum.

    extends and commit are OURS only; there is no equivalent for them, so
    those cells read as a dash on their side by construction."""
    if not rounds:
        return '<div class="empty">no rounds for this slot</div>'
    relay_rounds = relay_rounds or {}
    extras = extras or {}
    their_refs = extras.get("theirs") or {}
    our_sigs = extras.get("ours") or {}
    votes = extras.get("votes")

    def nonvote(prefixes):
        """Counted only over transactions found in this slot's block; anything
        that did not land cannot be classified, so it is reported separately
        rather than silently counted as non-vote."""
        if votes is None:
            return None, 0
        seen = [votes[p] for p in prefixes if p in votes]
        return sum(1 for v in seen if not v), len(prefixes) - len(seen)

    body = []
    for r in rounds:
        w = r["winner"]
        offers = r["offers"]
        ours_reward = max((o["reward"] for o in offers), default=None)
        ours_orders = max((o["orders"] for o in offers), default=None)
        ours_cu = max((o["exec_cost"] for o in offers), default=None)
        we_won = bool(w and w["won"])
        rl = relay_rounds.get(r["round"])

        # who they are, and which of their numbers applies
        if rl and rl["win_builder"] and not we_won:
            them_name, basis = rl["win_builder"], "won"
            their_reward, their_cu = rl["win_reward"], rl["win_cu"]
            their_orders = rl["win_orders"]
        elif rl and rl["opp_builder"]:
            them_name, basis = rl["opp_builder"], "best offer"
            their_reward, their_cu = rl["opp_reward"], rl["opp_cu"]
            their_orders = rl["opp_orders"]
        elif w and not we_won:
            them_name, basis = (rl or {}).get("win_builder") or OPPONENT, "winner row"
            their_reward, their_cu = w["reward"], w["exec_cost"]
            their_orders = w["orders"]
        else:
            them_name, basis = None, ""
            their_reward = their_cu = their_orders = None

        winner = OURS_NAME if we_won else (them_name or OPPONENT if w else "&mdash;")
        if their_reward and ours_reward is not None and their_reward > 0:
            pct = 100.0 * (ours_reward - their_reward) / their_reward
            margin = (f'<span class="{"goodc" if pct >= 0 else "redc"}">'
                      f"{pct:+.4f}%</span>")
        else:
            margin = '<span class="dim">&mdash;</span>'
        ext = extends.get(r["round"])
        # the commit that had to finish before THIS round could extend
        com = commits.get(r["round"] - 1) if r["round"] > 0 else None
        # bundles and non-vote, both sides
        tr = their_refs.get(r["round"])
        their_bundles = tr["bundles"] if tr else None
        their_nv, their_unk = nonvote(tr["txs"]) if tr else (None, 0)
        ours_bundles = max((o["bundles"] for o in offers), default=None)
        mine = [b58decode(x)[:16] for x in our_sigs.get(r["round"], [])]
        our_nv, our_unk = nonvote(mine) if mine else (None, 0)

        def pair(a, b, unk_a=0, unk_b=0):
            fa = "&mdash;" if a is None else f"{a:,}"
            fb = "&mdash;" if b is None else f"{b:,}"
            tail = ""
            if unk_a or unk_b:
                tail = (f'<div class="basis">{unk_a}/{unk_b} did not land, '
                        "unclassified</div>")
            return f'<td class="n m">{fa} / {fb}{tail}</td>'

        note = ""
        if rl and rl["our_reject"]:
            note = (f'<div class="rejnote">our {rl["our_subs"]} offer'
                    f'{"" if rl["our_subs"] == 1 else "s"} rejected: '
                    f'{html.escape(rl["our_reject"])}</div>')
        body.append(
            f'<tr><td class="m"><a href="/?win={window_of(slot)}&slot={slot}'
            f'&round={r["round"]}">round {r["round"]}</a>'
            + (' <span class="last">is_last</span>' if w and w["is_last"] else "")
            + note + "</td>"
            f'<td class="m {"goodc" if we_won else ""}">{html.escape(winner)}'
            + (f'<div class="basis">their {basis}</div>' if basis else "")
            + "</td>"
            f'<td class="n m">{big(their_reward) if their_reward is not None else "&mdash;"}</td>'
            f'<td class="n m">{big(ours_reward)}</td>'
            f'<td class="n m">{margin}</td>'
            f'<td class="n m">{big(their_orders) if their_orders is not None else "&mdash;"}'
            f' / {big(ours_orders)}</td>'
            f'<td class="n m">{big(their_cu) if their_cu is not None else "&mdash;"}'
            f' / {big(ours_cu)}</td>'
            + pair(their_bundles, ours_bundles)
            + pair(their_nv, our_nv, their_unk, our_unk)
            + f'<td class="n m">{commit_cell(com)}</td>'
            + f'<td class="n m">{ext["n"] if ext else 0}</td>'
            f'<td class="n m">{ms(ext["body_p50"]) if ext else "&mdash;"}</td>'
            f'<td class="n m">{ms(ext["body_max"]) if ext else "&mdash;"}</td>'
            "</tr>")
    head = ("<tr><th>round</th><th>winner</th>"
            "<th class=n>their reward</th><th class=n>ours</th>"
            "<th class=n>margin</th><th class=n>orders (them/us)</th>"
            "<th class=n>CU (them/us)</th>"
            "<th class=n>bundles (them/us)</th>"
            "<th class=n>non-vote txs (them/us)</th>"
            "<th class=n>commit before this round (us)</th>"
            "<th class=n>extends n (us)</th><th class=n>extend p50 (us)</th>"
            "<th class=n>extend max (us)</th></tr>")
    src = ("relay" if relay_rounds else
           ("relay unavailable, using our own winner rows" if relay_err
            else "relay has no rows for this slot, using our own winner rows"))
    return ('<div class="panel"><div class="dtlhead">round comparison'
            f'<span class="note">their side from <b>{src}</b> &middot; when '
            "they won it is their accepted block, when we won it is their best "
            "losing offer &middot; <b>ours is the final cumulative offer, not a "
            "sum</b> &mdash; selected rows are cumulative prefixes &middot; "
            "commit and extends are OURS; there is no counterpart for them "
            "&middot; the commit column is the PREVIOUS round's commit, the "
            "cost that had to finish before this round could extend, so round "
            "0 has none &middot; `replayed` is replay.orders.len() from the "
            "builder's CommitRoundRequest, which per the proto EXCLUDES votes "
            "-- they are priced via votes_cu and never executed, which is why "
            "it sits well below the winner's order_count</span>"
            + (f'<span class="warn"> &#9888; {html.escape(relay_err)}</span>'
               if relay_err else "")
            + "</div>"
            f"<table><thead>{head}</thead><tbody>{''.join(body)}</tbody></table>"
            "</div>")


def rounds_html(slot, sel_round):
    try:
        data = slot_data(slot)
    except Exception as exc:
        return f'<div class="err">rounds unavailable: {html.escape(str(exc))[:150]}</div>'
    rounds, extends, ext_err = data["rounds"], data["extends"], data["ext_err"]
    if not rounds:
        return f'<div class="empty">no miniblock rows for slot <b>{slot}</b></div>'

    out = []
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
a.navlink{margin-left:auto;color:#5eead4;font-size:12px;text-decoration:none;
  border:1px solid #14b8a655;border-radius:6px;padding:4px 11px}
a.navlink:hover{background:#0f766e22;border-color:#5eead4}
.callout{margin:16px 28px;padding:12px 16px;border-radius:9px;
  background:#2a1f06;border:1px solid #78350f;color:#fcd9a4;font-size:12.5px;
  line-height:1.6}
.callout b{color:#fbbf24}
.mdesc{color:#8fa6bf;font-size:11px;margin-top:3px;font-weight:400;line-height:1.5}
.mgotcha{color:#6b7f96;font-size:10.5px;margin-top:4px;font-style:italic;
  line-height:1.5}
.amberc{color:#fbbf24}
.redc{color:#fca5a5}
.warnt{color:#fbbf24}
.prov-code{color:#5eead4;font-size:9.5px;border:1px solid #14b8a655;
  border-radius:4px;padding:1px 6px;text-transform:uppercase}
.prov-meas{color:#93c5fd;font-size:9.5px;border:1px solid #1d4ed855;
  border-radius:4px;padding:1px 6px;text-transform:uppercase}
.prov-prov{color:#fbbf24;font-size:9.5px;border:1px solid #78350f;
  background:#78350f33;border-radius:4px;padding:1px 6px;text-transform:uppercase}
.panel th{vertical-align:bottom}
.panel td{vertical-align:top}
/* the i affordance on a health cell */
.info{display:inline-flex;align-items:center;justify-content:center;width:13px;
  height:13px;margin-left:6px;border:1px solid #2f4256;border-radius:50%;
  color:#7d90a6;font-size:9px;font-style:italic;font-weight:700;cursor:help;
  position:relative;vertical-align:middle;font-family:Georgia,serif}
.info:hover,.info:focus{border-color:#5eead4;color:#5eead4;outline:none}
.info .pop{display:none;position:absolute;left:-8px;top:20px;z-index:60;
  width:310px;background:#0d151d;border:1px solid #2f4256;border-radius:9px;
  padding:11px 13px;box-shadow:0 10px 30px #000a;text-transform:none;
  letter-spacing:0;font-style:normal;font-weight:400;cursor:default}
.info:hover .pop,.info:focus .pop,.info:focus-within .pop{display:block}
.pm{display:block;color:#c3d3e6;font-size:11.5px;line-height:1.55}
.pr{display:flex;gap:8px;margin-top:6px;align-items:baseline}
.pk{flex:0 0 52px;font-size:9px;text-transform:uppercase;letter-spacing:.06em}
.pv{color:#9fb2c8;font-size:11px;font-family:ui-monospace,Menlo,monospace}
.pg{display:block;margin-top:9px;padding-top:8px;border-top:1px solid #1e2937;
  color:#7d90a6;font-size:10.5px;font-style:italic;line-height:1.5}
.pf{display:block;margin-top:8px;color:#5b6b80;font-size:10px}
.pf a{color:#5eead4;text-decoration:none}
.pf a:hover{text-decoration:underline}
.greenc{color:#5eead4}
.hc{overflow:visible}
.health{display:flex;gap:10px;padding:14px 28px 4px;flex-wrap:wrap}
.hc{flex:1 1 190px;background:#111823;border:1px solid #1e2937;border-radius:9px;
  padding:9px 13px;border-left-width:3px;position:relative;overflow:visible}
.hc.good{border-left-color:#14b8a6}
.hc.warn{border-left-color:#fbbf24}
.hc.dead{border-left-color:#dc2626;background:#1a1114}
.hc.off{border-left-color:#2b3a4b;opacity:.6}
.hl{color:#61748b;font-size:9.5px;text-transform:uppercase;letter-spacing:.07em}
.hv{font-size:15px;font-weight:600;margin-top:2px;color:#dbe4ee;
  font-family:ui-monospace,Menlo,monospace}
.hc.good .hv{color:#5eead4}
.hc.warn .hv{color:#fbbf24}
.hc.dead .hv{color:#fca5a5}
.hd{color:#5b6b80;font-size:10.5px;margin-top:3px;line-height:1.45}
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
.chip .runline{font-size:9.5px;color:#5b6b80;margin-top:3px;
  font-family:ui-monospace,Menlo,monospace}
.chip.on .runline{color:#a7f3ea}
.coldpill{color:#fbbf24;border:1px solid #78350f;background:#78350f33;
  border-radius:3px;padding:0 4px;font-size:8.5px;text-transform:uppercase;
  cursor:help;margin-left:4px}
.coldpill:hover,.coldpill:focus{background:#78350f66;border-color:#fbbf24;
  outline:none}
.tipbox{position:fixed;z-index:200;max-width:340px;background:#0d151d;
  border:1px solid #2f4256;border-radius:9px;padding:10px 13px;color:#c3d3e6;
  font:11.5px/1.55 ui-sans-serif,-apple-system,sans-serif;
  box-shadow:0 10px 30px #000a;pointer-events:none;white-space:normal}
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
a.tab{display:inline-block;padding:6px 15px;margin-right:6px;border-radius:8px 8px 0 0;
  border:1px solid #1e2937;border-bottom:none;background:#0e151d;color:#7d90a6;
  font-size:12.5px;text-decoration:none}
a.tab:hover{color:#cfe0f0;border-color:#2f4256}
a.tab.on{background:#0c141b;border-color:#14b8a6;color:#5eead4;font-weight:600}
.tabs{padding:6px 28px 0;border-bottom:1px solid #1e2937;margin-bottom:14px}
td.bar{width:22%;padding:0 9px}
td.bar span{display:block;width:7px;height:7px;border-radius:50%;background:#2f4256}
tr.tl-start td.bar span{background:#5eead4}
tr.tl-replay td.bar span{background:#fbbf24}
tr.tl-promote td.bar span{background:#5eead4}
tr.tl-chain td.bar span{background:#93c5fd}
tr.tl-ext td.bar span{background:#a5b4fc}
tr.tl-start td,tr.tl-chain td{background:#0e1720}
.basis{color:#5b6b80;font-size:9.5px;margin-top:2px;text-transform:none}
.rejnote{color:#fbbf24;font-size:9.5px;margin-top:3px}
.goodc{color:#5eead4}
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

// Tooltips for [data-tip]. Deliberately not a CSS popover: these live inside
// .strip, which is overflow:auto in both axes and would clip an absolutely
// positioned child. A fixed-position node attached to <body> escapes that.
(function(){
  var box = null;
  function hide(){
    if (box) { box.remove(); box = null; }
    unmute();
  }
  var muted = null;      // ancestor whose native title we suppressed
  function unmute(){
    if (muted) { muted.el.title = muted.title; muted = null; }
  }
  function show(el){
    hide();
    var tip = el.getAttribute('data-tip');
    if (!tip) return;
    // The chip carries its own title=. Left alone the browser pops it a second
    // later, on top of this tooltip. Suppress it while we are showing ours.
    var owner = el.closest('[title]');
    if (owner && owner.title) {
      muted = {el: owner, title: owner.title};
      owner.title = '';
    }
    box = document.createElement('div');
    box.className = 'tipbox';
    box.textContent = tip;
    document.body.appendChild(box);
    var r = el.getBoundingClientRect();
    var w = box.offsetWidth, h = box.offsetHeight;
    // prefer below-right, but stay inside the viewport
    var left = Math.min(Math.max(8, r.left), window.innerWidth - w - 8);
    var top = r.bottom + 8;
    if (top + h > window.innerHeight - 8) top = Math.max(8, r.top - h - 8);
    box.style.left = left + 'px';
    box.style.top = top + 'px';
  }
  document.addEventListener('mouseover', function(ev){
    var el = ev.target.closest && ev.target.closest('[data-tip]');
    if (el) show(el);
  });
  document.addEventListener('mouseout', function(ev){
    var el = ev.target.closest && ev.target.closest('[data-tip]');
    if (el) hide();
  });
  document.addEventListener('focusin', function(ev){
    var el = ev.target.closest && ev.target.closest('[data-tip]');
    if (el) show(el);
  });
  document.addEventListener('focusout', hide);
  window.addEventListener('scroll', hide, true);
  document.addEventListener('keydown', function(ev){
    if (ev.key === 'Escape') hide();
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
                f'{w["won"]}/{len(w["slots"])} slots won &mdash; run '
                f'{html.escape(w["run_id"] or "?")}'
                + (f' started {html.escape((w.get("run_started") or "")[:19])} '
                   f'UTC, {w.get("run_slots", 0)} slots'
                   if w.get("run_started") else "")
                + (f', this window {int(w["run_age"] // 60)}m in'
                   if w.get("run_age") is not None else "")
                + '">'
                f'<div class="slot">{w["win"]}<span class="n">'
                f'&nbsp;+{len(w["slots"]) - 1}</span>{copy_btn(w["win"])}</div>'
                f'<div class="ageline">{age_html(w["ts"])}</div>'
                f'<div class="meta">{html.escape(short_id(w["leader"]))}'
                f'{" &#9888;" if w["leader_split"] else ""}</div>'
                f'<div class="runline">run {html.escape(short_run(w["run_id"]))}'
                + (' <span class="warn">&#9888;</span>' if w["run_split"] else "")
                + (f'<span class="coldpill" tabindex="0" data-tip="{cold_why(w)}"'
                   f">cold</span>" if w.get("run_cold") else "")
                + "</div>"
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


# ------------------------------------------------------- metrics reference

# Every threshold below carries a PROVENANCE, because they are not equally
# trustworthy and reviewers need to know which is which:
#   code        forced by the source -- a slot is 400ms, the replay pool is 8
#   measured    taken from this deployment's own distribution
#   provisional an inference that has NOT been validated against an SLO or an
#               incident. These are the ones to argue with.
DOC_WINDOW_H = 24

def _doc(panel, name, meas, field, meaning, good, amber, red, prov, gotcha):
    return {"panel": panel, "name": name, "meas": meas, "field": field,
            "meaning": meaning, "good": good, "amber": amber, "red": red,
            "prov": prov, "gotcha": gotcha}


# `name` matches the health cell label exactly, so the header tooltip and the
# reference page are rendered from one source and cannot drift.
METRIC_DOCS = [
 _doc("health", "building", None, None,
  "Count of sim-extend + sim-commit points in the last 20 minutes. The only "
  "honest liveness signal: a status heartbeat keeps ticking on a stalled "
  "builder, but work does not.",
  "any non-zero count", "0 over a whole leader window", "0 for hours",
  "provisional",
  "Work is bursty by leader window, so a short zero run is normal. Nobody has "
  "pinned how long a zero run must be before it counts as a stall."),
 _doc("health", "connector feed", None, None,
  "Connector-status log lines whose slot attribute is non-zero. An idle "
  "connector still reports the live chain head, so this is the feed health "
  "signal.",
  "live_slot &gt; 0", "&mdash;", "live_slot 0 across all identities",
  "code",
  "active (leader_state != Inactive) is NOT the health signal -- it only rises "
  "while a served validator is leading, so active=0 is normal off-window and "
  "must not be read as a fault."),
 _doc("health", "topology", None, None,
  "Connector and relay counts from builder-network-status lines over 10 "
  "minutes.",
  "one stable pair matching the deployed topology",
  "more than one distinct pair in 10m (flapping)",
  "a sustained drop below the deployed count",
  "provisional",
  "There is no deployed-topology value wired in, so this cannot tell a "
  "legitimate scale-down from a degradation. It only flags instability."),
 _doc("health", "scheduler", None, None,
  "Peak scheduler pool depth over 6 hours, with how many samples were busy.",
  "any non-zero peak; 0 between leader windows is the normal resting state",
  "&mdash;", "peak 0 across the whole 6h span",
  "provisional",
  "The pool only fills during a leader window -- measured over an hour it is "
  "non-zero in about 6 of 360 samples. Judging it over 15 minutes flagged "
  "amber almost permanently, so the window has to span several windows."),
 _doc("health", "event ingest", None, None,
  "Seconds between the newest bifrost_events row for this instance and now.",
  "under 60 s", "&gt; 60 s", "&gt; 300 s",
  "provisional",
  "Always read against the best peer instance shown alongside: a global "
  "ClickHouse stall and a single-instance stall look identical otherwise."),
 _doc("health", "last restart", None, None,
  "Most recent builder reconnect to the local sim on 127.0.0.1, which is the "
  "restart signature.",
  "hours to days ago",
  "minutes ago -- caches are cold and timings are not comparable",
  "repeated restarts in a short span (crash loop)",
  "provisional",
  "A restart is not itself a fault; it is context. Slot timings taken shortly "
  "after one ran against a cold program cache and account overlay."),
 _doc("extends", "body_us", "sim-extend", "body_us",
  "The extend itself: sanitize, check, DAG execute, and the commit of account "
  "diffs into the round overlay. Wall time on the mutation lane.",
  "at or below the observed p90", "&gt; 25 ms", "&gt; 47 ms",
  "provisional",
  "47 ms is roughly one slot at 400 ms across the ~8 extends a round runs, so "
  "it is order-of-magnitude reasoning rather than a measured limit. Observed "
  "p99 is far below it."),
 _doc("extends", "queue_us", "sim-extend", "queue_us",
  "Wait before the work, on the single pinned mutation thread behind a "
  "bounded(1) channel shared with commit. Contention, not work.",
  "tens of microseconds", "&gt; 1 ms", "&gt; 5 ms",
  "provisional",
  "Refused extends never reach the worker and emit no datapoint at all, so "
  "queue_us understates contention by construction."),
 _doc("extends", "exec_wall_us", "sim-extend", "exec_wall_us",
  "Wall time of the DAG execution inside the extend.",
  "close to body_us", "&mdash;", "&mdash;",
  "measured",
  "Read as a ratio against body_us: a large gap means time went to "
  "sanitize/check/overlay rather than execution."),
 _doc("extends", "program_cache_us", "sim-extend", "program_cache_us",
  "Program-cache time accumulated across replay workers.",
  "microseconds", "&mdash;", "&mdash;",
  "code",
  "Accumulated across workers, so it is CPU time and can exceed wall clock. "
  "Read against exec_wall_us as a ratio; never subtract from body_us."),
 _doc("commits", "body_us", "sim-commit", "body_us",
  "Commit wall time. SIZE-CONFOUNDED: a commit of 400 orders is not slow "
  "relative to one of 40, it is bigger.",
  "n/a -- do not threshold raw", "&mdash;", "&mdash;",
  "code",
  "Never threshold this directly. Normalize by replayed; see per-order."),
 _doc("commits", "per-order (body_us / replayed)", None, None,
  "The size-matched commit cost. This is the number to threshold.",
  "111-149 us/order per the ops runbook",
  "&gt; 149 us/order", "&gt; 300 us/order",
  "provisional",
  "On this deployment only 32% of commits land inside the quoted band and p50 "
  "sits at 146, right on its upper edge -- so either the band is a p50 target "
  "we barely meet, or it is stale. This one most needs a second opinion."),
 _doc("commits", "replayed", "sim-commit", "replayed",
  "ORDER COUNT for the commit. Not a latency, despite the name.",
  "n/a", "&mdash;", "&mdash;",
  "code", "Used as the denominator for per-order cost."),
 _doc("shreds", "total_time_ms", "shred_insert_is_full", "total_time_ms",
  "Slot completion time, measured per node from its own first shred.",
  "at or below ~400 ms, one slot", "&gt; 600 ms", "&gt; 1000 ms",
  "provisional",
  "A slot is 400 ms, so sustained completion above that means falling behind. "
  "Never compare across a restart or deploy."),
 _doc("shreds", "sim - leader delta", None, None,
  "Our simulator's timestamp minus the leader's, per slot, per stage.",
  "at or below 0 -- we saw it no later than the leader",
  "&gt; 0 (we are later)", "&gt; 50 ms",
  "provisional",
  "Joined on slot, never on time. An absent leader column is a reporting gap, "
  "not a slow node."),
]

DOC_BY_NAME = {d["name"]: d for d in METRIC_DOCS}


_doc_cache = {"at": 0.0, "data": None}


def doc_baselines():
    """Live p50/p90/p99 for every documented field, so the proposed cutoffs can
    be judged against what this deployment actually does."""
    now = time.monotonic()
    if _doc_cache["data"] is not None and now - _doc_cache["at"] < 600:
        return _doc_cache["data"]
    out = {}
    wanted = {}
    for d in METRIC_DOCS:
        if d["meas"] and d["field"]:
            wanted.setdefault(d["meas"], []).append(d["field"])
    for meas, fields in wanted.items():
        try:
            sel = ", ".join(
                f'percentile("{f}",50), percentile("{f}",90), percentile("{f}",99), '
                f'max("{f}"), count("{f}")' for f in fields)
            cols, rows = influx(
                f'SELECT {sel} FROM "{meas}" WHERE "host_id" = {SIM!r} '
                f"AND time > now() - {DOC_WINDOW_H}h")
            if not rows:
                continue
            row = rows[0]
            for i, f in enumerate(fields):
                base = 1 + i * 5
                out[(meas, f)] = {
                    "p50": row[base], "p90": row[base + 1], "p99": row[base + 2],
                    "max": row[base + 3], "n": row[base + 4]}
        except Exception:
            continue
    # the derived one: per-order commit cost, computed per datapoint
    try:
        _, rows = influx(f'SELECT "body_us","replayed" FROM "sim-commit" '
                         f'WHERE "host_id" = {SIM!r} AND time > now() - {DOC_WINDOW_H}h')
        per = sorted(r[1] / r[2] for r in rows if r[1] and r[2])
        if per:
            pick = lambda p: per[min(len(per) - 1, int(round(p / 100 * (len(per) - 1))))]
            out[("derived", "per_order")] = {
                "p50": pick(50), "p90": pick(90), "p99": pick(99),
                "max": per[-1], "n": len(per),
                "in_band": sum(1 for x in per if 111 <= x <= 149)}
    except Exception:
        pass
    _doc_cache.update(at=now, data=out)
    return out


PROV = {"code": ("prov-code", "from code"),
        "measured": ("prov-meas", "measured"),
        "provisional": ("prov-prov", "PROVISIONAL")}


def reference_page():
    base = doc_baselines()
    # the health header already measures three of these; reuse rather than
    # claiming they cannot be measured
    live_health = {}
    try:
        h = health_probe()
        if h["work"]["ok"]:
            v = h["work"]["v"]
            live_health["building"] = (
                f'extend {v["sim-extend"]:,} &middot; commit {v["sim-commit"]:,} '
                f'&middot; ctx {v["sim-context"]:,} &middot; probe '
                f'{v["sim-probe"]:,} in {WORK_WINDOW_MIN}m')
        if h["connector"]["ok"]:
            v = h["connector"]["v"]
            live_health["connector live_slot"] = (
                f'live_slot {v["live_slot"]}/{v["total"]} &middot; active '
                f'{v["active"]} (10m)')
        if h["ingest"]["ok"] and h["ingest"]["v"]:
            mine = next((x for x in h["ingest"]["v"]
                         if x["instance"] == CH_INSTANCE), None)
            peers = ", ".join(f'{x["instance"]} {x["lag"]}s'
                              for x in h["ingest"]["v"][:3])
            if mine:
                live_health["event ingest lag"] = (
                    f'{mine["lag"]}s for {html.escape(CH_INSTANCE)} &middot; '
                    f"peers: {html.escape(peers)}")
    except Exception:
        pass

    # `replayed` is an ORDER COUNT, not a duration. Formatting it as time is
    # exactly the confusion this page exists to prevent.
    COUNT_FIELDS = {"replayed", "orders", "applied", "program_cache_compiles",
                    "program_cache_entries", "num_repaired", "num_recovered"}

    def fmt(v, field):
        if v is None:
            return "&mdash;"
        v = float(v)
        if field in COUNT_FIELDS:
            return f"{v:,.0f}"
        if field and field.endswith("_ms"):
            return f"{v:,.0f} ms"
        return f"{v/1000:,.1f} ms" if v >= 1000 else f"{v:,.0f} &micro;s"

    groups = {}
    for d in METRIC_DOCS:
        groups.setdefault(d["panel"], []).append(d)
    out = []
    for panel, items in groups.items():
        rows = []
        for d in items:
            name, meas, field = d["name"], d["meas"], d["field"]
            meaning, amber, red = d["meaning"], d["amber"], d["red"]
            prov, gotcha, good = d["prov"], d["gotcha"], d["good"]
            key = (meas, field) if meas else (
                ("derived", "per_order") if "per-order" in name else None)
            b = base.get(key) if key else None
            if b and key == ("derived", "per_order"):
                shown = (f'p50 {b["p50"]:.0f} &middot; p90 {b["p90"]:.0f} &middot; '
                         f'p99 {b["p99"]:.0f} &micro;s/order &middot; n={b["n"]:,}'
                         f'<br><span class="warnt">only '
                         f'{100*b["in_band"]/b["n"]:.0f}% inside the quoted '
                         f'111-149 band</span>')
            elif b:
                shown = (f'p50 {fmt(b["p50"], field)} &middot; p90 '
                         f'{fmt(b["p90"], field)} &middot; p99 {fmt(b["p99"], field)}'
                         f' &middot; max {fmt(b["max"], field)} &middot; n='
                         f'{int(b["n"]):,}' if b["p50"] is not None else "&mdash;")
            elif name in live_health:
                shown = live_health[name]
            else:
                shown = ('<span class="dim">per-slot only &mdash; see the slot'
                         " page</span>")
            cls, label = PROV[prov]
            src = f"<code>{meas}.{field}</code>" if meas else "&mdash;"
            rows.append(
                f"<tr><td><b>{name}</b><div class='mdesc'>{meaning}</div>"
                f"<div class='mgotcha'>{gotcha}</div></td>"
                f"<td class='m'>{src}</td>"
                f"<td class='m greenc'>{good}</td>"
                f"<td class='m amberc'>{amber}</td><td class='m redc'>{red}</td>"
                f"<td><span class='{cls}'>{label}</span></td>"
                f"<td class='m dim'>{shown}</td></tr>")
        out.append(
            f'<div class="panel"><div class="dtlhead">{panel}</div>'
            "<table><thead><tr><th>metric</th><th>source</th>"
            "<th>healthy</th><th>amber</th><th>red</th><th>cutoff basis</th>"
            f"<th>this deployment, last {DOC_WINDOW_H}h</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>")

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; metrics reference</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">metrics reference &mdash; what each number means and when it is bad</div>
  <a class="navlink" href="/">&larr; back to slots</a>
</header>
<div class="callout">
  <b>Thresholds marked PROVISIONAL are inferences, not agreed limits.</b>
  They have not been validated against an SLO or a real incident, and several
  are order-of-magnitude reasoning rather than measurement. The rightmost
  column shows what this deployment actually does over the last
  {DOC_WINDOW_H} hours, so you can judge each cutoff against its own
  distribution. Corrections welcome &mdash; especially on per-order commit
  cost, where the quoted healthy band and the observed distribution disagree.
</div>
{''.join(out)}
</body></html>"""

def page(sel_win=None, sel_slot=None, sel_round=None, tab="rounds"):
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
            # people walk a whole leader window, so warm its other slots in
            # the background as soon as the window is known
            warm_window(sel_win, skip=sel_slot)

    if sel_slot is None:
        note = ("Pick a leader window above, then a slot inside it."
                if sel_win is None else "Pick a slot from this window.")
        body = (f'<main><div class="empty"><b>No slot selected.</b><br>{note}'
                "</div></main>")
    else:
        base = f"/?win={window_of(sel_slot)}&slot={sel_slot}"
        tabs = "".join(
            f'<a class="tab{" on" if tab == key else ""}" '
            f'href="{base}{"" if key == "rounds" else "&tab=" + key}">{label}</a>'
            for key, label in (("rounds", "rounds"), ("compare", "comparison"),
                               ("timeline", "timeline")))
        if tab == "timeline":
            try:
                d = slot_data(sel_slot)
                inner = timeline_html(sel_slot, d["extends"], d["commits"],
                                      d.get("shreds"), d["rounds"])
            except Exception as exc:
                inner = ('<div class="err">timeline unavailable: '
                         f"{html.escape(str(exc))[:160]}</div>")
            blurb = ("everything that happened in this slot, in order, on a "
                     "single clock.")
        elif tab == "compare":
            try:
                d = slot_data(sel_slot)
                inner = compare_html(sel_slot, d["rounds"], d["extends"],
                                     d["commits"], d.get("relay"),
                                     d.get("relay_err"), compare_extras(sel_slot))
            except Exception as exc:
                inner = ('<div class="err">comparison unavailable: '
                         f"{html.escape(str(exc))[:160]}</div>")
            blurb = "what won each round, what we offered, and what the lane spent."
        else:
            inner = f"<main class=rounds>{rounds_html(sel_slot, sel_round)}</main>"
            blurb = ("auction rounds. Click a round for its offers and the "
                     "winning miniblock.")
        body = (f'<div class="slotbar hascp">slot <b>{sel_slot}</b>'
                f"{copy_btn(sel_slot)} &mdash; {blurb}</div>"
                f'<div class="tabs">{tabs}</div>{inner}')
    title = f" &middot; slot {sel_slot}" if sel_slot else \
            f" &middot; window {sel_win}" if sel_win else ""
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">block-builder slot explorer</div>
  <a class="navlink" href="/reference">metrics reference &mdash; what is bad?</a>
</header>
{health_html()}
{strip}
{body}
<script>{TICK_JS.replace("__SERVER_NOW__", f"{dt.datetime.now(dt.UTC).timestamp():.3f}")}</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/reference":
            try:
                body = reference_page().encode()
            except Exception as exc:
                body = f"<pre>{html.escape(str(exc))}</pre>".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
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
            tab = qs.get("tab", ["rounds"])[0]
            body = page(as_int("win"), as_int("slot"), as_int("round"),
                        tab if tab in ("compare", "timeline") else "rounds"
                        ).encode()
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
