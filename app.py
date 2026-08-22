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
import re
import socket
import struct
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
        "       any(run_id) AS run_id, "
        # A slot is several auction rounds and we can take some and lose the
        # rest, so "did we win this slot" throws the interesting part away:
        # over 7 days the slots we won at all ranged from 1 round to all 8.
        # One `winner` row per round, `won_by_us` on the ones we produced.
        "       countIf(kind = 'winner') AS rounds, "
        "       countIf(kind = 'winner' AND won_by_us) AS won_rounds "
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
                             "offers": ch_int(r[4]),
                             "rounds": ch_int(r[6]) if len(r) > 6 else 0,
                             "won_rounds": ch_int(r[7]) if len(r) > 7 else 0})
        if r[2]:
            win["leaders"].add(r[2])
        if len(r) > 5 and r[5]:
            win.setdefault("runs", set()).add(r[5])
    out = []
    for win in (windows[k] for k in sorted(windows, reverse=True)):
        win["slots"].sort(key=lambda s: s["slot"])
        win["ts"] = min(s["ts"] for s in win["slots"])
        win["won"] = sum(1 for s in win["slots"] if s["won"])
        win["rounds"] = sum(s["rounds"] for s in win["slots"])
        win["won_rounds"] = sum(s["won_rounds"] for s in win["slots"])
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
        "       won_by_us, uuid, connector_identity, run_id, instance_id, seq_id, "
        f"       countSubstrings(payload, unhex('{VOTE_ID_HEX}')) AS votes, "
        # Only the winner's payload comes back. A `selected` payload embeds
        # whole transactions and runs to a hundred kilobytes, so pulling every
        # one of them to count refs would cost megabytes a slot.
        "       if(kind = 'winner', hex(payload), '') AS wire "
        f"FROM bifrost_miniblocks WHERE slot = {int(slot)} ORDER BY index_in_slot, kind, ts")
    rounds = {}
    for r in rows:
        idx = ch_int(r[0])
        item = {"ts": r[2], "orders": ch_int(r[3]), "txs": ch_int(r[4]),
                "bundles": ch_int(r[5]), "reward": ch_int(r[6]),
                "exec_cost": ch_int(r[7]), "sel_cu": ch_int(r[8]),
                "is_last": r[9] in TRUEISH, "won": r[10] in TRUEISH,
                "uuid": r[11], "connector": r[12], "run_id": r[13],
                "instance": r[14], "seq_id": ch_int(r[15]),
                "votes": ch_int(r[16], -1)}
        slot_round = rounds.setdefault(idx, {"round": idx, "offers": [], "winner": None})
        if r[1] == "winner":
            # transaction_count and bundle_count are written from
            # BuilderOriginatedOrders, which holds only the orders the sending
            # builder ORIGINATED. A foreign winner attaches none, so both
            # columns read 0 -- or, when it attaches some, a number that has
            # nothing to do with the block it won. The refs are the truth, so
            # they are counted here and the columns ignored.
            #
            # Votes cannot come from the payload on this side: a winner tx ref
            # is a bare SigPrefix with no bytes, so there is nothing to find the
            # vote program id in. -1 means unknown; the comparison tab
            # classifies them against the block instead.
            item["votes"] = -1
            try:
                refs = _decode_refs(bytes.fromhex(r[17]))
            except Exception:
                item["wire_ok"] = False
            else:
                item["txs"] = sum(1 for k, _ in refs if k == "tx")
                item["bundles"] = sum(1 for k, _ in refs if k == "bundle")
                item["wire_ok"] = len(refs) == item["orders"]
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


def block_index(slot):
    """The slot's block indexed by SigPrefix: signature, account keys with
    their writability, and whether the transaction is a vote.

    `transactionDetails: accounts` already returns every key's `writable` flag
    with the address-lookup tables RESOLVED, so one call serves both the vote
    columns and the conflict DAG. Reconstructing writability by hand from the
    message header disagrees with the runtime on roughly one transaction in
    250 -- an account invoked as a program is demoted to readonly -- so the
    resolved flags are worth the heavier response."""
    if not SOLANA_RPC:
        return None
    with _block_lock:
        hit = _block_cache.get(slot)
    if hit is not None:
        return hit
    # A shared endpoint answers 429 under load and the block is 8 MB, so a
    # failure here is usually "come back in a moment", not "no such block".
    blk = None
    for attempt in range(4):
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
            break
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == 3:
                raise RuntimeError(f"getBlock({slot}) failed: HTTP {err.code}")
            time.sleep(1.5 * (attempt + 1))
        except Exception as err:
            raise RuntimeError(f"getBlock({slot}) failed: "
                               f"{type(err).__name__}: {str(err)[:80]}")
    if blk is None:
        raise RuntimeError(f"getBlock({slot}) returned nothing")
    index = {}
    for t in (blk or {}).get("transactions", []):
        sig = t["transaction"]["signatures"][0]
        keys = [(k["pubkey"], bool(k.get("writable"))) if isinstance(k, dict)
                else (k, False) for k in t["transaction"]["accountKeys"]]
        prefix = b58decode(sig)[:16]
        index[prefix] = {
            "sig": sig, "prefix": b58encode(prefix), "keys": keys,
            "vote": any(k == VOTE_PROGRAM for k, _ in keys)}
    with _block_lock:
        if len(_block_cache) > 8:
            _block_cache.clear()
        _block_cache[slot] = index
    return index


def slot_votes(slot):
    """Which of this slot's transactions are votes, keyed by SigPrefix.

    Miniblocks DO carry votes -- measured on slot 439391881, 388 of the
    winner's 553 transaction refs were votes, and 680 of our 1,209 selected
    signatures. So "non-vote" is a real distinction and not a formality.

    Needs the block, hence an RPC call, hence optional: without SOLANA_RPC, or
    when the call fails, the non-vote columns read as unknown rather than as
    zero."""
    try:
        index = block_index(slot)
    except RuntimeError:
        return None
    return None if index is None else {p: v["vote"] for p, v in index.items()}


VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"
# The 32-byte program id as it appears verbatim in a transaction's account
# keys. A `selected` payload carries whole transactions, so counting these
# occurrences classifies every offered transaction EXACTLY -- including ones
# that never landed, which a chain lookup cannot classify at all.
VOTE_ID_HEX = "0761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000"
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    n = 0
    for c in s:
        n = n * 58 + _B58.index(c)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\0" * (len(s) - len(s.lstrip("1"))) + body


def b58encode(raw):
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, rem = divmod(n, 58)
        out = _B58[rem] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + out


def slot_offer_sigs(slot):
    """Per OFFER (keyed by miniblock uuid) the 16-byte SigPrefix of every
    transaction in it, so each row of the offers table can be split into vote
    and non-vote.

    Prefixes rather than full signatures: a slot's selected rows carry 24k-37k
    signatures, ~2-3 MiB of base58, and only the first 16 bytes are needed to
    match the block. ClickHouse does the truncation, which cuts the transfer
    to roughly a third."""
    out = {}
    try:
        _, rows = clickhouse(
            "SELECT uuid, arrayStringConcat(arrayMap("
            "  s -> hex(substring(base58Decode(s), 1, 16)), signatures), ',') AS pfx "
            f"FROM bifrost_miniblocks WHERE slot = {int(slot)} "
            f"  AND kind = 'selected' AND local_builder_id = '{CH_BUILDER}'")
    except Exception:
        return out
    for r in rows:
        if not r[0]:
            continue
        out[r[0]] = [bytes.fromhex(h) for h in (r[1] or "").split(",") if h]
    return out


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
        try:
            refs = _decode_refs(b)
        except ValueError:                  # layout did not hold; do not guess
            continue
        if len(refs) != want:
            continue
        out[idx] = {"txs": [r for k, r in refs if k == "tx"],
                    "bundles": sum(1 for k, _ in refs if k == "bundle")}
    return out


def slot_logs(slot, stamps):
    """warn / error / fatal log lines for one slot, from ClickStack.

    Attribution is exact where it can be: 1,105 of the recent error rows carry
    LogAttributes['slot'], so those are matched on the slot itself and need no
    clock. Lines without that attribute -- ALT refresh failures, teardowns --
    are picked up by time window instead and flagged as such, because they are
    often the most interesting ones and dropping them would hide the cause.

    otel's clock agrees with InfluxDB: on slot 440027607 the tagged errors span
    09:00:30.087-.400 against sim-extend's 09:00:30.075-.400. That is unlike
    bifrost_events, which has been seen 899ms out, so the window is taken from
    InfluxDB times when we have them."""
    # Two different reasons for no logs, and they were reported identically:
    # an unconfigured ClickStack, and a slot with no miniblock rows to derive a
    # time window from (which happens exactly when the builder was down -- the
    # case you most want logs for).
    if not (OTEL_URL and OTEL_SERVICE):
        return {"unconfigured": True}
    if not stamps:
        return {"nowindow": True}
    pad = dt.timedelta(seconds=6)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        rows = otel(
            "SELECT toString(Timestamp) AS ts, SeverityText AS sev, Body AS body, "
            "  LogAttributes['slot'] AS s, LogAttributes['index'] AS idx "
            "FROM otel_logs "
            f"WHERE ServiceName = '{OTEL_SERVICE}' "
            f"  AND Timestamp >= '{lo}' AND Timestamp <= '{hi}' "
            "  AND SeverityText IN ('warn','error','fatal') "
            f"  AND (LogAttributes['slot'] = '{int(slot)}' "
            "       OR LogAttributes['slot'] = '') "
            "ORDER BY Timestamp LIMIT 4000")
    except Exception as exc:
        return {"err": str(exc)[:140]}
    out = []
    for r in rows:
        out.append({"t": _rfc3339_ns(r[0][:26].replace(" ", "T")),
                    "sev": r[1], "body": r[2],
                    "slot": ch_int(r[3], 0), "idx": r[4],
                    "tagged": bool(r[3])})
    return {"rows": out, "window": (lo, hi)}


def slot_builder_events(slot, stamps):
    """The builder's own view of the slot, from bifrost_events.

    This is ClickHouse, a different clock from InfluxDB, so it cannot simply be
    interleaved: the two have been observed 899ms apart on one slot and within
    5ms on another. But `round_committed` exists on BOTH sides -- the builder
    logs it here, the simulator emits sim-commit for the same round -- so the
    shift can be measured per slot and applied.

    It is a genuine clock offset rather than latency: across the rounds of a
    slot the per-round differences agree to 0.1ms while being 899ms from zero.
    Parallel, just displaced. The measured offset and its spread are reported
    alongside so the alignment is auditable rather than assumed."""
    if not stamps:
        return {}
    pad = dt.timedelta(minutes=10)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        _, rows = clickhouse(
            "SELECT event, toString(ts) AS at, "
            "  attrs['leader_state'] AS leader_state, attrs['identity'] AS identity, "
            "  attrs['source'] AS source, attrs['won_by_us'] AS won_by_us, "
            "  toInt64OrZero(toString(nums['index'])) AS idx, "
            "  toInt64OrZero(toString(nums['parent_slot'])) AS parent_slot, "
            "  toInt64OrZero(toString(nums['chosen_refs'])) AS chosen_refs, "
            "  toInt64OrZero(toString(nums['expected_refs'])) AS expected_refs "
            f"FROM bifrost_events WHERE ts >= '{lo}' AND ts <= '{hi}' "
            f"  AND instance_id = '{CH_INSTANCE}' AND nums['slot'] = {int(slot)} "
            "  AND event IN ('progress','bank_ready','round_winner',"
            "                'promote_mismatch','round_committed','dispatched') "
            "ORDER BY ts")
    except Exception:
        return {}
    out = []
    for r in rows:
        out.append({"event": r[0], "t": _rfc3339_ns(r[1][:26].replace(" ", "T")),
                    "leader_state": r[2], "identity": r[3], "source": r[4],
                    "won_by_us": r[5], "idx": ch_int(r[6]),
                    "parent_slot": ch_int(r[7]), "chosen_refs": ch_int(r[8]),
                    "expected_refs": ch_int(r[9])})
    return {"events": out}


def builder_offset(builder, commits):
    """Nanoseconds to add to a ClickHouse timestamp to put it on the InfluxDB
    clock. Anchored on round_committed, which both stores record."""
    if not builder or not commits:
        return None, None, 0
    ch_by_round = {e["idx"]: e["t"] for e in builder.get("events", [])
                   if e["event"] == "round_committed"}
    diffs = [commits[i]["t"] - ch_by_round[i]
             for i in ch_by_round if isinstance(i, int) and i in commits]
    if not diffs:
        return None, None, 0
    diffs.sort()
    median = diffs[len(diffs) // 2]
    spread = (max(diffs) - min(diffs)) / 1e6
    return median, spread, len(diffs)


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
        'SELECT "index","body_us","queue_us","replayed","critical_path","initial_width","promoted_len","applied",'
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
                                 "refusal": 0, "critical_path": 0,
                                 "initial_width": 0,
                                 "t": _rfc3339_ns(r[at["time"]])})
        e["n"] += 1
        e["body"] += num("body_us")
        e["queue"] += num("queue_us")
        e["replayed"] += num("replayed")
        e["promoted"] += num("promoted_len")
        e["refusal"] += num("refusal")
        # The DAG the simulator measured while replaying this round's winner.
        # Unlike the extend path's, this one covers the WHOLE miniblock in a
        # single call, so it is the number a round-level rebuild is checked
        # against. Keep the largest: a round commits once, but a retry would
        # re-emit and the fuller graph is the one that ran.
        e["critical_path"] = max(e.get("critical_path", 0), num("critical_path"))
        e["initial_width"] = max(e.get("initial_width", 0), num("initial_width"))
        # workers.rs winner_kind(): 0 empty, 1 promote, 2 replay. Keep the
        # heaviest kind seen -- a replay is what actually costs.
        e["kind"] = max(e["kind"], num("winner"))
    return out


# workers.rs winner_kind()
COMMIT_KIND = {0: "empty", 1: "promote", 2: "replay"}


def commit_cell(com, rnd, ready_window=None):
    """The commit this round had to WAIT ON -- the previous round's.

    Commit N applies round N's winner and round N+1 extends on top of it, so
    the cost gating round N is commit N-1, and round 0 has none.

    A commit is also not always a replay, and calling it one hides the single
    biggest cost difference in the round.

      replay   we lost, so their winning block is re-executed on our bank
               before we can extend on top of it -- tens of milliseconds
      promote  we won, so the prefix we already hold is just pointed at -- a
               pointer move, tens of microseconds
      empty    a winnerless round; nothing to apply
    """
    if rnd == 0:
        # Nothing to replay before the first round; it builds on the parent
        # bank. The parent's own timings live in their own strip above the
        # table, not crammed into this cell.
        return ('<span class="dim">&mdash;</span>'
                '<div class="basis">nothing to replay</div>')
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
        '"exec_wall_us","critical_path","initial_width","exec_pool","prefix_len",'
        '"cu","program_cache_us","program_cache_clone_us",'
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
            "crit": num("critical_path"), "iwidth": num("initial_width"),
            "pool": num("exec_pool"), "prefix": num("prefix_len"),
            "cu": num("cu"),
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
            # The DAG shape the simulator measured, per accepted call. These are
            # per CALL -- sixteen orders at a time -- not for the round's order
            # list, so they do not compare against a round-level rebuild.
            "exec_pool": max(c["pool"] for c in calls),
            "crit_max": max(c["crit"] for c in calls),
            "crit_sum": sum(c["crit"] for c in calls),
            "iwidth_p50": _p50([c["iwidth"] for c in calls]),
            "prefix_end": max(c["prefix"] for c in calls),
            "cu_end": max(c["cu"] for c in calls),
            "calls": [{"t": c["t"], "orders": c["orders"], "applied": c["applied"],
                       "crit": c["crit"], "iwidth": c["iwidth"],
                       "prefix": c["prefix"], "cu": c["cu"], "body": c["body"],
                       "exec": c["exec"], "status": c["status"]} for c in calls],
        }
    return out


# ------------------------------------------------------------- conflict DAG
#
# The simulator does not ship its DAG anywhere -- `sim-extend` and `sim-commit`
# carry only its SHAPE (critical_path, initial_width, exec_pool). So the graph
# here is rebuilt, by porting simulation-service/src/replay.rs `build_stream`
# line for line: RAW and WAW edges through `last_writer`, WAR edges through
# `readers_since_write`, then linear runs coalesced into chains.
#
# Two things make the rebuild checkable rather than decorative:
#
#   * the account keys come from the block with the address-lookup tables
#     ALREADY RESOLVED, so the read/write sets are the ones the runtime used,
#     not a guess;
#   * `sim-commit` records the critical_path and initial_width the simulator
#     itself measured while replaying the winner, so the rebuild can be scored
#     against it. On the cleanest round measured -- slot 440267808 round 0, two
#     orders unresolved -- the rebuilt critical path was 55 against a recorded
#     55, and the initial width 32 against 33. Every panel shows that
#     comparison rather than asserting the graph is right.
#
# The residual is foreign bundles. A bundle reaches us as a 32-byte id; its
# member transactions are only knowable if WE received the bundle too, and the
# builder that won a round usually has bundles we never saw. Those orders are
# counted and reported, never silently dropped.

MAX_CHAIN_TXS = 64          # replay.rs


def dag_stream(orders, merge_chains):
    """Port of replay.rs build_stream(). `orders` is an ordered list of
    {"txs": [{"keys": [(pubkey, writable)], "sig": str}], "is_bundle": bool}.

    merge_chains follows the caller the simulator itself uses: the extend path
    passes false (every order is its own chain, because the round-budget
    verdict can refuse an order a merged chain's worker would already have
    carried forward), the winner-replay path passes true."""
    n = len(orders)
    last_writer, readers = {}, {}
    blocked = [0] * n
    dependents = [[] for _ in range(n)]
    for i, order in enumerate(orders):
        deps = set()
        for tx in order["txs"]:
            for key, writable in tx["keys"]:
                writer = last_writer.get(key)
                if writer is not None:
                    deps.add(writer)                    # RAW and WAW
                if writable:
                    deps.update(readers.get(key, ()))   # WAR
        for tx in order["txs"]:
            for key, writable in tx["keys"]:
                if writable:
                    last_writer[key] = i
                    readers.pop(key, None)
                else:
                    readers.setdefault(key, []).append(i)
        blocked[i] = len(deps)
        for dep in deps:
            dependents[dep].append(i)

    # An order touching a duplicated signature never merges: the settle verdict
    # refuses duplicates, and a refused order's writes must not reach a
    # same-chain successor.
    dup = [False] * n
    if merge_chains:
        holder = {}
        for i, order in enumerate(orders):
            for tx in order["txs"]:
                if tx["sig"] in holder:
                    dup[holder[tx["sig"]]] = True
                    dup[i] = True
                else:
                    holder[tx["sig"]] = i

    # A linear run: order i flows into its sole dependent when it is also that
    # dependent's sole blocker, and neither side is a bundle.
    nxt = [None] * n
    if merge_chains:
        for i in range(n):
            if orders[i]["is_bundle"] or dup[i] or len(dependents[i]) != 1:
                continue
            j = dependents[i][0]
            if blocked[j] == 1 and not orders[j]["is_bundle"] and not dup[j]:
                nxt[i] = j

    # Links point forward, so an ascending sweep reaches every head first.
    chain_of = [-1] * n
    chains = []
    for start in range(n):
        if chain_of[start] != -1:
            continue
        cid = len(chains)
        chain_of[start] = cid
        if orders[start]["is_bundle"]:
            chains.append([start])
            continue
        members, ntx, cur = [start], len(orders[start]["txs"]), start
        while nxt[cur] is not None:
            j = nxt[cur]
            if ntx + len(orders[j]["txs"]) > MAX_CHAIN_TXS:
                break
            chain_of[j] = cid
            members.append(j)
            ntx += len(orders[j]["txs"])
            cur = j
        chains.append(members)

    m = len(chains)
    cblocked = [0] * m
    cdeps = [[] for _ in range(m)]
    seen = set()
    for i, targets in enumerate(dependents):
        for t in targets:
            a, b = chain_of[i], chain_of[t]
            if a != b and (a, b) not in seen:
                seen.add((a, b))
                cdeps[a].append(b)
                cblocked[b] += 1
    # Height is the longest chain-path below, computable in reverse because an
    # edge always points at a chain with a larger head index.
    height = [0] * m
    for cid in range(m - 1, -1, -1):
        for nb in cdeps[cid]:
            if height[nb] + 1 > height[cid]:
                height[cid] = height[nb] + 1
    return {"chains": chains, "chain_of": chain_of, "cdeps": cdeps,
            "cblocked": cblocked, "height": height,
            "critical_path": (max(height) + 1) if height else 0,
            "initial_width": sum(1 for b in cblocked if b == 0)}


def dag_schedule(stream, weights, pool):
    """Replay the simulator's dispatch: ready chains pop deepest-remaining-path
    first onto `pool` workers -- replay.rs keeps `ready` as a max-heap on
    (height, id) and hands the top of it to whichever worker is idle.

    The clock is MODEL time in compute units, not wall time. A chain costs the
    sum of its members' CU, so the x axis answers "how long would this DAG take
    on `pool` workers if cost tracked CU", which is the question the shape is
    for. Wall time per extend is measured separately and shown beside it."""
    import heapq

    m = len(stream["chains"])
    cost = [max(1, sum(weights.get(i, 0) for i in members))
            for members in stream["chains"]]
    remaining = list(stream["cblocked"])
    ready = [(-stream["height"][i], i) for i in range(m) if remaining[i] == 0]
    heapq.heapify(ready)
    running = []                       # (finish_time, chain, worker)
    idle = list(range(max(1, pool)))
    now = 0
    out = [None] * m
    while ready or running:
        while ready and idle:
            _, cid = heapq.heappop(ready)
            worker = idle.pop(0)
            out[cid] = {"w": worker, "t0": now, "t1": now + cost[cid]}
            heapq.heappush(running, (now + cost[cid], cid, worker))
        if not running:
            break
        now = running[0][0]
        while running and running[0][0] == now:
            _, cid, worker = heapq.heappop(running)
            idle.append(worker)
            idle.sort()
            for nb in stream["cdeps"][cid]:
                remaining[nb] -= 1
                if remaining[nb] == 0:
                    heapq.heappush(ready, (-stream["height"][nb], nb))
    span = max((s["t1"] for s in out if s), default=0)
    # The critical path is a WALK from the deepest chain downward. Its edges
    # are the consecutive pairs only -- two chains can both sit on the path and
    # still be joined by an edge the path does not take.
    crit, crit_edges = set(), set()
    if m:
        cur = max(range(m), key=lambda i: stream["height"][i])
        while True:
            crit.add(cur)
            nxts = stream["cdeps"][cur]
            if not nxts:
                break
            nxt = max(nxts, key=lambda i: stream["height"][i])
            crit_edges.add((cur, nxt))
            cur = nxt
    return out, span, crit, crit_edges


def bundle_members(slot, ids, stamps):
    """bundle id (hex) -> its member signatures, in submission order.

    The wire carries a bundle as an opaque 32-byte id. The only place its
    contents survive is the `received` event we logged when the bundle reached
    us -- which is exactly why a bundle only WE received can be expanded, and a
    rival's cannot."""
    if not ids or not stamps:
        return {}
    lo, hi = _ch_window(stamps, minutes=6)
    quoted = ",".join("'" + i + "'" for i in sorted(ids) if re.fullmatch(r"[0-9a-f]{64}", i))
    if not quoted:
        return {}
    _, rows = clickhouse(
        "SELECT entity, attrs['sigs'] FROM bifrost_events "
        f"WHERE ts >= '{lo}' AND ts <= '{hi}' AND entity_kind = 'bundle' "
        f"AND entity IN ({quoted}) AND attrs['sigs'] != ''")
    out = {}
    for entity, sigs in rows:
        out.setdefault(entity, [s for s in sigs.split(",") if s])
    return out


def _ch_window(stamps, minutes):
    pad = dt.timedelta(minutes=minutes)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    return lo, hi


def order_outcomes(slot, stamps):
    """entity -> (cu, included) from our own `executed` events.

    This is the only per-order cost we hold. The block's own
    computeUnitsConsumed would need the heavier `full` transaction detail, and
    it would be the WINNER's cost rather than what our simulator spent."""
    if not stamps:
        return {}
    lo, hi = _ch_window(stamps, minutes=3)
    _, rows = clickhouse(
        "SELECT entity, entity_kind, reason, toUInt64(nums['cu']) AS cu "
        f"FROM bifrost_events WHERE ts >= '{lo}' AND ts <= '{hi}' "
        f"AND event = 'executed' AND nums['slot'] = {int(slot)}")
    out = {}
    for entity, kind, reason, cu in rows:
        out[entity] = {"cu": ch_int(cu), "included": reason == "included",
                       "reason": reason or "?"}
    return out


def dag_orders(slot, rnd, source, rounds, stamps):
    """The round's order list, in wire order, resolved to account sets.

    Two different sources, because they answer different questions and are
    stored differently:

      winner  the miniblock that WON the round. Its payload is a list of order
              refs, so the order is exact. This is the one the simulator
              replayed, and therefore the one `sim-commit` measured.
      ours    what we OFFERED. Our own payload embeds whole transactions rather
              than refs, so the list is rebuilt from the row's `signatures`
              array, with bundle members folded back into one order at the
              position of their first transaction."""
    try:
        index = block_index(slot)
    except RuntimeError as err:
        return None, {"err": str(err) + ". The account sets come from the "
                                        "block, so the graph cannot be built "
                                        "without it."}
    if index is None:
        return None, {"err": "SOLANA_RPC is not configured, so the block's "
                             "account sets are unavailable"}
    entry = next((r for r in rounds if r["round"] == rnd), None)
    if entry is None:
        return None, {"err": f"round {rnd} has no miniblock rows"}

    stats = {"votes": 0, "unresolved": 0, "bundles": 0, "opaque": 0,
             "ungrouped": 0}
    refs = []
    if source == "winner":
        win = entry.get("winner")
        if not win:
            return None, {"err": "the relay never echoed a winner for this round"}
        _, rows = clickhouse(
            f"SELECT hex(payload) FROM bifrost_miniblocks WHERE slot = {int(slot)} "
            f"AND index_in_slot = {int(rnd)} AND kind = 'winner' LIMIT 1")
        if not rows:
            return None, {"err": "no winner payload stored"}
        refs = _decode_refs(bytes.fromhex(rows[0][0]))
    else:
        offers = entry.get("offers") or []
        if not offers:
            return None, {"err": "we made no offer in this round"}
        best = max(offers, key=lambda o: o["orders"])
        _, rows = clickhouse(
            "SELECT arrayStringConcat(signatures, ' '), "
            "       arrayStringConcat(bundle_ids, ' ') "
            f"FROM bifrost_miniblocks WHERE slot = {int(slot)} "
            f"AND uuid = '{best['uuid']}' LIMIT 1")
        if not rows:
            return None, {"err": "our offer row disappeared"}
        sigs = [s for s in rows[0][0].split(" ") if s]
        bids = [b for b in rows[0][1].split(" ") if b]
        members = bundle_members(slot, set(bids), stamps)
        owner, emitted = {}, set()
        for bid in bids:
            for sig in members.get(bid, []):
                owner[sig] = bid
        for sig in sigs:
            bid = owner.get(sig)
            if bid is None:
                refs.append(("sig", sig))
            elif bid not in emitted:
                emitted.add(bid)
                refs.append(("bundle", bid))
        stats["bundles"] = len(bids)
        stats["ungrouped"] = sum(1 for b in bids if b not in members)
        refs += [("bundle", b) for b in bids if b not in emitted and b in members]

    by_sig = {v["sig"]: v for v in index.values()}
    if source == "winner":
        wanted = {r.hex() for kind, r in refs if kind == "bundle"}
        members = bundle_members(slot, wanted, stamps)
        stats["bundles"] = len(wanted)

    orders = []
    for kind, ref in refs:
        if kind == "sig":
            tx = by_sig.get(ref)
        elif kind == "tx":
            tx = index.get(ref)
        else:
            bid = ref if isinstance(ref, str) else ref.hex()
            sigs = members.get(bid)
            txs = [by_sig[s] for s in sigs or [] if s in by_sig]
            if not sigs or len(txs) != len(sigs):
                stats["opaque"] += 1
                stats["unresolved"] += 1
                continue
            if all(t["vote"] for t in txs):
                stats["votes"] += 1
                continue
            orders.append({"is_bundle": True, "ref": bid, "label": bid,
                           "txs": [{"keys": t["keys"], "sig": t["sig"]} for t in txs]})
            continue
        if tx is None:
            stats["unresolved"] += 1
            continue
        if tx["vote"]:
            stats["votes"] += 1
            continue
        orders.append({"is_bundle": False, "ref": tx["prefix"], "label": tx["sig"],
                       "txs": [{"keys": tx["keys"], "sig": tx["sig"]}]})
    stats["refs"] = len(refs)
    return orders, stats


def _decode_refs(payload):
    """A winner payload's order refs: wincode WireChosenMiniblock.

    uuid 16B, Option<reward>, Option<execution_cost>, slot u64, index u32,
    is_tail bool -- 47 bytes -- then the ref vector as a u64 count and that
    many [u32 tag][body]: tag 0 is a 16-byte SigPrefix, tag 1 a 32-byte bundle
    id. `orders` follows.

    The count is READ and honoured rather than the buffer being consumed until
    it runs out. The winning builder attaches order bytes only for orders it
    originated, so `orders` is usually empty -- two zero u64 lengths, the "16
    bytes of zero padding" this once looked like -- but when it is NOT empty,
    walking to the end of the buffer parses those bytes as further refs. That
    silently inflated the ref count, which then disagreed with the order_count
    column and got the whole round discarded: measured over one 12-hour range
    it lost 21 of 95 rounds and a third of the winner refs."""
    count = struct.unpack("<Q", payload[47:55])[0]
    off, refs = 55, []
    for _ in range(count):
        tag = struct.unpack("<I", payload[off:off + 4])[0]
        size = {0: 16, 1: 32}.get(tag)
        if size is None:
            raise ValueError(f"unknown order ref tag {tag} at byte {off}")
        if off + 4 + size > len(payload):
            raise ValueError("payload ended inside the ref vector")
        refs.append(("tx" if tag == 0 else "bundle", payload[off + 4:off + 4 + size]))
        off += 4 + size
    return refs


# --------------------------------------------- our own offer, as recorded
#
# A `selected` payload is a wincode WireMiniblock, and it carries the builder's
# OWN dependency graph -- so for our own offers there is nothing to rebuild.
#
#   reward           Option<u64>              1-byte tag, then 8 if Some
#   execution_cost   Option<u64>              same
#   graph            WireMiniBlockGraph
#     slot           u64
#     index_in_slot  u32
#     uuid           16 bytes
#     row_offsets    Vec<u32>                 u64 count, then count * 4
#     col_indices    Vec<u32>                 u64 count, then count * 4
#     nodes          Vec<CsrNode>             u64 count, then count *
#                                             (u32 tag, 16|32 ref, u32 depth)
#   orders           BuilderOriginatedOrders
#     txs            Vec<WireSharableTx>      u64 count, each u64 len then bytes
#     bundles        Vec<WireSharableBundle>  u64 count, each 32-byte id then Vec<tx>
#
# Node i's successors are col_indices[row_offsets[i]..row_offsets[i+1]], and an
# edge i -> s means i must land before s is ready. `depth` is the remaining-path
# height: 1 for a sink, 1 + max(successor depth) otherwise.
#
# WHY THIS REPLACED A REBUILD. The old "ours" graph took its order from the
# row's `signatures` array, which is NOT wire order: miniblock_recording.rs
# builds it by walking `orders.txs` and then every bundle's txs, i.e. the order
# the BYTES were supplied in, with all bundle members appended at the end. A
# conflict DAG is sequence-dependent -- last-writer and readers-since-write both
# are -- so feeding it the wrong sequence produced the wrong edges. Only
# graph.nodes has the real order, and the CSR beside it has the real edges, so
# the rebuild was both unnecessary and wrong.
#
# Validated on every `selected` row of a slot: node count equals the
# `order_count` column, row_offsets is nodes+1 long, uuid/slot/index/reward
# match their columns, tx and bundle counts match theirs, every edge points
# forward, `depth` reproduces as the remaining-path height, and the payload is
# consumed to the last byte with nothing left over.


class _Wire:
    """Little-endian cursor. wincode writes a Vec as a u64 count then items."""

    def __init__(self, buf):
        self.b, self.o = buf, 0

    def u8(self):
        v = self.b[self.o]
        self.o += 1
        return v

    def u32(self):
        v = struct.unpack_from("<I", self.b, self.o)[0]
        self.o += 4
        return v

    def u64(self):
        v = struct.unpack_from("<Q", self.b, self.o)[0]
        self.o += 8
        return v

    def take(self, n):
        v = self.b[self.o:self.o + n]
        if len(v) != n:
            raise ValueError("payload ended early")
        self.o += n
        return v

    def opt_u64(self):
        return self.u64() if self.u8() else None


def decode_selected(payload):
    r = _Wire(payload)
    out = {"reward": r.opt_u64(), "execution_cost": r.opt_u64(),
           "slot": r.u64(), "index_in_slot": r.u32(), "uuid": r.take(16).hex()}
    out["row_offsets"] = [r.u32() for _ in range(r.u64())]
    out["col_indices"] = [r.u32() for _ in range(r.u64())]
    nodes = []
    for _ in range(r.u64()):
        tag = r.u32()
        size = {0: 16, 1: 32}.get(tag)
        if size is None:
            raise ValueError(f"unknown CsrNode tag {tag}")
        ref = r.take(size)
        nodes.append({"kind": "tx" if tag == 0 else "bundle", "ref": ref,
                      "depth": r.u32()})
    out["nodes"] = nodes
    out["txs"] = [r.take(r.u64()) for _ in range(r.u64())]
    bundles = []
    for _ in range(r.u64()):
        bid = r.take(32)
        bundles.append({"id": bid, "txs": [r.take(r.u64()) for _ in range(r.u64())]})
    out["bundles"] = bundles
    out["left"] = len(payload) - r.o
    return out


def _tx_ident(raw):
    """A transaction's full signature and its 16-byte SigPrefix, from the wire
    bytes. The first signature follows a one-byte shortvec count, and every
    transaction we record has fewer than 128 signatures."""
    sig = raw[1:65]
    return b58encode(sig), raw[1:17]


def measured_dag(slot, rnd, data):
    """Our offer's graph exactly as the builder recorded it.

    Returns the same shape dag_schedule() wants, so the dispatch model is
    shared with the rebuilt path: the connector pops ready nodes off a
    BinaryHeap<(depth, index)> -- deepest critical path first -- which is the
    rule dag_schedule already implements."""
    entry = next((r for r in data["rounds"] if r["round"] == rnd), None)
    offers = (entry or {}).get("offers") or []
    if not offers:
        return None, {"err": "we made no offer in this round"}
    best = max(offers, key=lambda o: o["orders"])
    _, rows = clickhouse(
        "SELECT hex(payload), payload_version FROM bifrost_miniblocks "
        f"WHERE slot = {int(slot)} AND uuid = '{best['uuid']}' "
        "AND kind = 'selected' LIMIT 1")
    if not rows or not rows[0][0]:
        return None, {"err": "our offer row has no payload stored"}
    if ch_int(rows[0][1]) != 0:
        # The layout below was established and checked against version 0 only.
        # Guessing at a version that changed would produce a confident graph
        # made of nonsense, which is worse than no graph.
        return None, {"err": f"this offer is payload_version "
                             f"{ch_int(rows[0][1])}; the graph layout is only "
                             "known for version 0"}
    try:
        g = decode_selected(bytes.fromhex(rows[0][0]))
    except Exception as exc:
        return None, {"err": f"could not decode our offer's graph: {exc}"}

    nodes, ro, ci = g["nodes"], g["row_offsets"], g["col_indices"]
    if len(ro) != len(nodes) + 1:
        return None, {"err": f"graph is malformed: {len(ro)} row offsets for "
                             f"{len(nodes)} nodes"}

    # identity, so a node can be labelled and joined to its executed event
    by_prefix, votes = {}, set()
    vote_id = bytes.fromhex(VOTE_ID_HEX)
    for raw in g["txs"]:
        sig, prefix = _tx_ident(raw)
        by_prefix[prefix] = sig
        if vote_id in raw:
            votes.add(prefix)
    bundle_txs = {}
    for b in g["bundles"]:
        members = []
        for raw in b["txs"]:
            sig, prefix = _tx_ident(raw)
            by_prefix[prefix] = sig
            if vote_id in raw:
                votes.add(prefix)
            members.append(prefix)
        bundle_txs[b["id"]] = members

    n_votes = 0
    meta = []
    for nd in nodes:
        if nd["kind"] == "tx":
            is_vote = nd["ref"] in votes
            n_votes += 1 if is_vote else 0
            meta.append({"key": b58encode(nd["ref"]),
                         "label": by_prefix.get(nd["ref"]) or nd["ref"].hex(),
                         "bundle": False, "vote": is_vote, "txs": 1})
        else:
            members = bundle_txs.get(nd["ref"], [])
            all_votes = bool(members) and all(m in votes for m in members)
            n_votes += 1 if all_votes else 0
            meta.append({"key": nd["ref"].hex(), "label": nd["ref"].hex(),
                         "bundle": True, "vote": all_votes,
                         "txs": max(1, len(members))})

    cdeps = [list(ci[ro[i]:ro[i + 1]]) for i in range(len(nodes))]
    cblocked = [0] * len(nodes)
    for succ in cdeps:
        for s in succ:
            if 0 <= s < len(nodes):
                cblocked[s] += 1
    # `depth` is 1 for a sink; height counts edges, so it is one less
    height = [max(0, nd["depth"] - 1) for nd in nodes]
    stream = {"chains": [[i] for i in range(len(nodes))],
              "chain_of": list(range(len(nodes))),
              "cdeps": cdeps, "cblocked": cblocked, "height": height,
              "critical_path": max((nd["depth"] for nd in nodes), default=0),
              "initial_width": sum(1 for b in cblocked if b == 0)}
    stats = {"refs": len(nodes), "votes": n_votes, "unresolved": 0,
             "bundles": len(g["bundles"]), "opaque": 0, "ungrouped": 0,
             "edges": len(ci), "left": g["left"],
             "backward": sum(1 for i in range(len(nodes))
                             for s in cdeps[i] if s <= i)}
    return {"stream": stream, "meta": meta, "orders": len(nodes)}, stats

def dag_payload(slot, rnd, source, data):
    """Everything the browser needs to draw one round's dispatch DAG."""
    rounds = data["rounds"]
    stamps = [i["ts"] for r in rounds
              for i in r["offers"] + ([r["winner"]] if r["winner"] else [])]
    # Our own offer carries the builder's graph, so it is READ, not rebuilt.
    # The winner's payload has only a flat list of order refs -- no CSR -- so
    # that one still has to be reconstructed, which is where the sim-commit
    # comparison earns its place.
    measured = source != "winner"
    if measured:
        got, stats = measured_dag(slot, rnd, data)
        if got is None:
            return {"err": stats["err"]}
        stream, meta = got["stream"], got["meta"]
        merge = False
    else:
        orders, stats = dag_orders(slot, rnd, source, rounds, stamps)
        if orders is None:
            return {"err": stats["err"]}
        if not orders:
            return {"err": "no order in this round could be resolved to its "
                           "account set" + (" -- every ref was a bundle we "
                                            "never received"
                                            if stats.get("opaque") else "")}
        merge = True
        stream = dag_stream(orders, merge_chains=merge)
        meta = [{"key": o["ref"], "label": o.get("label", o["ref"]),
                 "bundle": o["is_bundle"], "vote": False, "txs": len(o["txs"])}
                for o in orders]
    outcomes = order_outcomes(slot, stamps)
    weights, resolved_cu = {}, 0
    for i, m in enumerate(meta):
        hit = outcomes.get(m["key"])
        if hit and hit["cu"]:
            weights[i] = hit["cu"]
            resolved_cu += 1
        else:
            weights[i] = 5000          # a placeholder, flagged in the panel
    ext = (data.get("extends") or {}).get(rnd) or {}
    pool = ext.get("exec_pool") or 8
    placed, span, crit, crit_edges = dag_schedule(stream, weights, pool)

    nodes = []
    for cid, members in enumerate(stream["chains"]):
        slotted = placed[cid] or {"w": 0, "t0": 0, "t1": 0}
        head = meta[members[0]]
        known = [o for o in (outcomes.get(meta[i]["key"]) for i in members) if o]
        state = ("unknown" if not known
                 else "included" if all(o["included"] for o in known)
                 else "excluded")
        nodes.append({
            "i": cid, "w": slotted["w"], "t0": slotted["t0"], "t1": slotted["t1"],
            "b": 1 if head["bundle"] else 0,
            "n": len(members), "s": state, "c": 1 if cid in crit else 0,
            "h": stream["height"][cid], "d": stream["cblocked"][cid],
            "v": 1 if head["vote"] else 0,
            "cu": sum(weights[i] for i in members),
            "r": head["label"],
        })
    edges = [[a, b, 1] if (a, b) in crit_edges else [a, b]
             for a, deps in enumerate(stream["cdeps"]) for b in deps]
    return {
        "nodes": nodes, "edges": edges, "pool": pool, "span": span,
        "chains": len(stream["chains"]), "orders": len(meta),
        "critical_path": stream["critical_path"],
        "initial_width": stream["initial_width"],
        "merged": merge, "measured": measured, "stats": stats,
        "cu_known": resolved_cu,
    }


def dag_html(slot, rnd, source, data):
    rounds = data["rounds"]
    if not rounds:
        return ('<div class="panel"><div class="dtlhead">dispatch DAG</div>'
                '<div class="none">this slot has no miniblock rows</div></div>')
    have = [r["round"] for r in rounds]
    if rnd not in have:
        rnd = have[0]
    base = f"/?win={window_of(slot)}&slot={slot}&tab=dag"
    chips = "".join(
        f'<a class="dchip{" on" if r == rnd else ""}" '
        f'href="{base}&round={r}&src={source}">round {r}</a>' for r in have)
    srcs = "".join(
        f'<a class="dchip{" on" if source == key else ""}" '
        f'href="{base}&round={rnd}&src={key}">{label}</a>'
        for key, label in (("ours", "our offer"), ("winner", "the winner")))

    try:
        payload = dag_payload(slot, rnd, source, data)
    except Exception as exc:
        payload = {"err": f"{type(exc).__name__}: {exc}"}
    head = (f'<div class="panel"><div class="dtlhead">dispatch DAG &mdash; '
            f'round {rnd}</div>'
            f'<div class="dagbar"><span class="dlab">round</span>{chips}'
            f'<span class="dlab">graph of</span>{srcs}</div>')
    if payload.get("err"):
        return head + f'<div class="none">{html.escape(payload["err"])}</div></div>'

    st = payload["stats"]
    if payload.get("measured"):
        prov = ('<div class="dagread">This graph is <b>read, not rebuilt</b>. '
                "Our own <code>selected</code> payload carries the builder's "
                f"dependency graph in CSR form &mdash; {st.get('edges', 0):,} "
                "edges over "
                f"{payload['orders']:,} nodes, with each node's remaining-path "
                "depth &mdash; so nothing here is inferred. Order comes from "
                "<code>graph.nodes</code>, which is the only place the wire "
                "order exists."
                + (f' <span class="warn">{st["backward"]:,} edges point '
                   "backward, which should be impossible in a topological "
                   'order</span>' if st.get("backward") else "")
                + (f' <span class="warn">{st["left"]:,} bytes of the payload '
                   "were not consumed</span>" if st.get("left") else "")
                + "</div>")
    else:
        prov = ('<div class="dagread">This graph is <b>rebuilt</b>. A winning '
                "miniblock's payload carries a flat list of order refs and no "
                "graph, so the edges here are recomputed with the simulator's "
                "own rule from <code>replay.rs</code> and then scored against "
                "what <code>sim-commit</code> measured.</div>")
    recorded = _dag_recorded(rnd, source, data)
    fid = _dag_fidelity(payload, recorded)
    unres = st["unresolved"]
    votes = ""
    if st["votes"]:
        why = ("They are NODES in this graph, not removed: it is the "
               "builder's own recorded graph and dropping a node would "
               "renumber the CSR. They are counted here so the width is not "
               "read as useful parallelism -- a vote touches only its own vote "
               "account, so every one of them starts unblocked and none of "
               "them has an edge. <b>Only orders with a dependency</b> above "
               "hides them."
               if payload.get("measured") else
               "The simulator drops them too on this path: "
               "<code>replayed</code> counts the winner's orders MINUS the "
               "votes billed to <code>votes_cu</code>, which is how the two "
               "figures reconcile."
               if source == "winner" else
               "They are excluded here because a vote touches only its own "
               "vote account, so it adds width to the graph and no depth. "
               "Whether the extend path is handed votes at all is not on "
               "record either way.")
        inout = ("are vote transactions" if payload.get("measured")
                 else "are vote transactions and are not in the graph")
        votes = (f'<div class="dagnote">{st["votes"]:,} of {st["refs"]:,} refs '
                 f"{inout}. {why}</div>")
    warn = ""
    if unres:
        warn = (f'<div class="dagwarn">{unres:,} of {st["refs"]:,} order refs '
                "could not be resolved to an account set"
                + (f' &mdash; {st["opaque"]:,} '
                   + ("is a bundle" if st["opaque"] == 1 else "are bundles")
                   + " we never received, so the member transactions are "
                     "unknown"
                   if st.get("opaque") else "")
                + ". They are absent from the graph, which shortens the "
                  "critical path and understates the width.</div>")
    if st.get("ungrouped"):
        n = st["ungrouped"]
        warn += (f'<div class="dagnote">{n:,} bundle{"" if n == 1 else "s"} in '
                 "our offer could not be expanded, so its transactions appear "
                 "as separate orders rather than one. Every transaction is "
                 "still in the graph and the edges between them are real; what "
                 "is wrong is the grouping, which lets the scheduler interleave "
                 "orders a bundle would have kept together.</div>")
    cu_note = ""
    if payload["cu_known"] < payload["orders"]:
        miss = payload["orders"] - payload["cu_known"]
        cu_note = (f'<div class="dagwarn">{miss:,} order'
                   f'{"" if miss == 1 else "s"} had no <code>executed</code> '
                   "event, so a flat 5,000 CU stands in for the measured cost. "
                   "Their bars are the wrong width; the edges are not affected."
                   "</div>")

    def tile(value, label):
        return f"<td><b>{value}</b><span>{label}</span></td>"

    cells = [tile(f"{payload['orders']:,}", "orders in the graph")]
    if payload.get("measured"):
        # The builder's graph is per order, so a chain count would just repeat
        # the order count; what it does have that a rebuild does not is edges.
        cells.append(tile(f"{payload['stats'].get('edges', 0):,}",
                          "dependency edges"))
    else:
        cells.append(tile(f"{payload['chains']:,}", "chains"
                          + (" (linear runs merged)" if payload["merged"]
                             else " (one per order)")))
    cells += [
        tile(f"{payload['critical_path']:,}", "critical path" + fid["cp"]),
        tile(f"{payload['initial_width']:,}",
             "start with no blocker" + fid["iw"]),
        tile(payload["pool"], "worker lanes"),
        tile(f"{payload['span']/1e6:.2f}M", "CU on the longest lane"),
    ]
    summary = '<table class="dagsum"><tr>' + "".join(cells) + "</tr></table>"

    legend = (
        '<div class="daglegend">'
        '<span><i class="dn pending"></i>pending</span>'
        '<span><i class="dn running"></i>executing</span>'
        '<span><i class="dn included"></i>result: included</span>'
        '<span><i class="dn excluded"></i>not included</span>'
        '<span><i class="dn unknown"></i>no result recorded</span>'
        '<span><i class="dn bundlei"></i>bundle</span>'
        '<span><i class="dn criti"></i>critical path</span>'
        "</div>")

    controls = (
        '<div class="dagctl">'
        '<button type="button" data-act="start" title="back to the start">&#9198;</button>'
        '<button type="button" data-act="play" title="play">&#9654;</button>'
        '<button type="button" data-act="end" title="jump to the end">&#9197;</button>'
        '<input type="range" id="dagscrub" min="0" max="1000" value="1000">'
        '<span class="dagt" id="dagt">t = end</span>'
        + "".join(f'<button type="button" class="spd" data-spd="{s}">{s}&times;</button>'
                  for s in ("0.5", "1", "2", "4"))
        + '<label class="dagdep"><input type="checkbox" id="dagdep">'
          "only orders with a dependency"
          '<span class="daghint" id="daghide"></span></label></div>')

    blob = json.dumps({"nodes": payload["nodes"], "edges": payload["edges"],
                       "pool": payload["pool"], "span": payload["span"]},
                      separators=(",", ":"))
    return (head + prov + votes + warn + cu_note + summary + controls
            + '<div class="dagwrap"><svg id="dagsvg"></svg></div>' + legend
            + _dag_extends(rnd, data)
            + _dag_basis(payload, recorded, source)
            + f'<script id="dagdata" type="application/json">{blob}</script>'
            + f"<script>{DAG_JS}</script></div>")


def _dag_recorded(rnd, source, data):
    """What the simulator itself measured for this round, when it measured it.

    Only the winner-replay path has a recorded comparison: `sim-commit` runs the
    whole winning miniblock through the DAG in one call, so its critical_path
    and initial_width describe exactly the graph drawn here. The extend path
    reports per CALL -- sixteen orders at a time -- so there is no round-level
    number to compare our offer against."""
    if source != "winner":
        return None
    com = (data.get("commits") or {}).get(rnd)
    if not com:
        return None
    # kind 2 is replay -- see workers.rs winner_kind(). A promote (we won the
    # round) and an empty commit never call run_stream, so their critical_path
    # and initial_width are absent rather than zero.
    if com.get("kind") != 2 or not com.get("critical_path"):
        return {"promoted": com.get("kind") == 1}
    return {"cp": com.get("critical_path"), "iw": com.get("initial_width"),
            "replayed": com.get("replayed")}


def _dag_fidelity(payload, recorded):
    if not recorded or recorded.get("cp") in (None, "", 0):
        return {"cp": "", "iw": ""}
    def cmp(got, rec):
        if rec in (None, ""):
            return ""
        rec = int(rec)
        if got == rec:
            return f' &middot; <b class="ok">matches the {rec:,} recorded</b>'
        return f' &middot; <span class="off">{rec:,} recorded</span>'
    return {"cp": cmp(payload["critical_path"], recorded["cp"]),
            "iw": cmp(payload["initial_width"], recorded["iw"])}


def _dag_extends(rnd, data):
    """Each accepted extend's own DAG, as the simulator measured it.

    This is the per-extend view, and it is a TABLE rather than a graph on
    purpose: `sim-extend` records the shape of each call -- its critical path,
    how many orders started unblocked -- but never which orders were in it. The
    batch is capped at sixteen, and nothing in ClickHouse or InfluxDB ties those
    sixteen back to identities, so an honest per-extend graph cannot be drawn.
    What can be shown is the shape, and it is the simulator's own measurement
    rather than a rebuild.

    `prefix` is the running total of orders accepted into the round so far, so
    the gap between consecutive rows is that call's contribution."""
    ext = (data.get("extends") or {}).get(rnd) or {}
    calls = ext.get("calls") or []
    if not calls:
        return ('<div class="dagx"><div class="dagxh">per extend</div>'
                '<div class="none">no sim-extend point landed in this round '
                "&mdash; every extend was refused, or none was attempted"
                "</div></div>")
    rows = []
    prev = 0
    for i, c in enumerate(calls):
        gained = c["prefix"] - prev
        prev = c["prefix"]
        bad = c["status"] != 1
        dropped = c["orders"] - c["applied"]
        rows.append(
            f'<tr{" class=xbad" if bad else ""}>'
            f'<td class="n m">{i}</td>'
            f'<td class="n m">{c["orders"]:,}</td>'
            f'<td class="n m">{c["applied"]:,}'
            + (f' <span class="dropn">-{dropped}</span>' if dropped > 0 else "")
            + "</td>"
            f'<td class="n m">{c["crit"]:,}</td>'
            f'<td class="n m">{c["iwidth"]:,}</td>'
            f'<td class="n m">{c["prefix"]:,}'
            + (f' <span class="dim">+{gained}</span>' if gained else "")
            + "</td>"
            f'<td class="n m">{ms(c["body"])}</td>'
            f'<td class="n m">{ms(c["exec"])}</td>'
            f'<td class="m">{EXT_STATUS.get(c["status"], c["status"])}</td></tr>')
    return ('<div class="dagx"><div class="dagxh">per extend &mdash; '
            f'{len(calls)} accepted call{"" if len(calls) == 1 else "s"}</div>'
            '<table><tr><th class=n>#</th><th class=n>orders</th>'
            "<th class=n>applied</th><th class=n>critical path</th>"
            "<th class=n>start unblocked</th><th class=n>prefix after</th>"
            "<th class=n>body</th><th class=n>exec</th><th>status</th></tr>"
            + "".join(rows) + "</table>"
            '<div class="dagxn">These are the simulator\'s own numbers, one row '
            "per accepted call, each capped at sixteen orders. They are NOT "
            "comparable to the round figures above: that graph is the round's "
            "whole order list at once, this is the same work cut into batches. "
            "Which orders were in which batch is not recorded anywhere, which "
            "is why this is a table and not sixteen little graphs. Refused "
            "extends are missing entirely &mdash; a throttled call never "
            "reaches the worker and emits no point.</div></div>")


def _dag_basis(payload, recorded, source):
    if payload.get("measured"):
        return ('<div class="dagbasis"><b>How this is built.</b> It is not: the '
                "edges, the order and each node\'s depth are read straight out "
                "of our own <code>selected</code> payload, which is a wincode "
                "<code>WireMiniblock</code> carrying the builder\'s graph in "
                "CSR form. The lanes then run the connector\'s own dispatch "
                "rule &mdash; it pops ready nodes off a "
                "<code>BinaryHeap&lt;(depth, index)&gt;</code>, deepest "
                "critical path first &mdash; and the x axis is <b>model time "
                "in compute units</b>, not wall time: a node costs its measured "
                "CU. Wall time per extend is measured, and lives in the rounds "
                "tab. Switch to <b>the winner</b> to see a graph that has to be "
                "reconstructed, and how well that reconstruction scores."
                "</div>")
    merged = ("Linear runs are merged into one chain, capped at 64 "
              "transactions, exactly as the winner-replay path does."
              if payload["merged"] else
              "Every order is its own chain, as on the extend path: the "
              "round-budget verdict can refuse an order that a merged chain's "
              "worker would already have carried forward.")
    rec = ""
    if recorded and recorded.get("promoted"):
        rec = (" Nothing checks these figures on this round: we WON it, so the "
               "commit promoted our own block instead of replaying a rival's, "
               "and the promote path never builds a DAG to measure.")
    elif recorded and recorded.get("cp") not in (None, "", 0):
        rec = (" The recorded figures beside them come from <code>sim-commit</code>, "
               "which is the simulator measuring this same graph as it replayed "
               f"the winner's {int(recorded['replayed']):,} orders.")
    elif source != "winner":
        rec = (" There is nothing to check this against: <code>sim-extend</code> "
               "reports a critical path per CALL, sixteen orders at a time, not "
               "for the round's order list as a whole. Switch to "
               "<b>the winner</b> for a round where the simulator's own number "
               "is on record.")
    return ('<div class="dagbasis"><b>How this is built.</b> Edges are the '
            "conflict edges the simulator uses &mdash; read-after-write and "
            "write-after-write through the last writer of each account, "
            "write-after-read through the readers since that write &mdash; "
            "ported from <code>replay.rs</code>. Account keys come from the "
            "block with lookup tables already resolved, so the read and write "
            f"sets are the ones the runtime used. {merged}{rec} "
            "The x axis is <b>model time in compute units</b>, not wall time: "
            "a chain costs the sum of its orders' measured CU and the lanes "
            "run the simulator's own dispatch order, deepest remaining path "
            "first. Wall time per extend is measured, and lives in the rounds "
            "tab.</div>")


# Drawn in the browser rather than server-side because the interesting part is
# the playback: the same node changes colour as the clock crosses its dispatch
# and its finish, and a scrubbed SVG that re-renders locally beats refetching
# the page for every frame.
DAG_JS = r"""
(function(){
  var el = document.getElementById('dagdata');
  if(!el) return;
  var D = JSON.parse(el.textContent);
  var svg = document.getElementById('dagsvg');
  var NS = 'http://www.w3.org/2000/svg';
  var LANE = 17, PADT = 12, PADL = 46, PADR = 16, PADB = 26;
  var W = 0, H = D.pool * LANE + PADT + PADB;
  var depOnly = false, t = D.span, playing = false, speed = 1, raf = null;

  var deg = {};
  D.edges.forEach(function(e){ deg[e[0]] = 1; deg[e[1]] = 1; });
  function visible(n){ return !depOnly || deg[n.i]; }
  var loners = D.nodes.filter(function(n){ return !deg[n.i]; }).length;
  var hint = document.getElementById('daghide');
  if(hint) hint.textContent = loners
    ? '\u2014 hides ' + loners + ' with no edge either way'
    : '\u2014 every order here has an edge';

  function layout(){
    W = Math.max(svg.clientWidth || 900, 320);
    svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
    svg.setAttribute('height', H);
  }
  function x(v){
    var span = D.span || 1;
    return PADL + (W - PADL - PADR) * (v / span);
  }
  function y(w){ return PADT + w * LANE + LANE / 2; }

  function state(n){
    if(t < n.t0) return 'pending';
    if(t < n.t1) return 'running';
    return n.s;
  }

  function draw(){
    while(svg.firstChild) svg.removeChild(svg.firstChild);
    layout();
    var i, n;
    // lanes
    for(i = 0; i < D.pool; i++){
      var ln = document.createElementNS(NS, 'line');
      ln.setAttribute('x1', PADL); ln.setAttribute('x2', W - PADR);
      ln.setAttribute('y1', y(i)); ln.setAttribute('y2', y(i));
      ln.setAttribute('class', 'dlane');
      svg.appendChild(ln);
      var tx = document.createElementNS(NS, 'text');
      tx.setAttribute('x', PADL - 8); tx.setAttribute('y', y(i) + 3);
      tx.setAttribute('class', 'dlanelbl'); tx.setAttribute('text-anchor', 'end');
      tx.textContent = 'w' + i;
      svg.appendChild(tx);
    }
    // x ticks, in compute units
    var span = D.span || 1;
    for(i = 0; i <= 4; i++){
      var v = span * i / 4;
      var tk = document.createElementNS(NS, 'text');
      tk.setAttribute('x', x(v)); tk.setAttribute('y', H - 8);
      tk.setAttribute('class', 'dtick');
      tk.setAttribute('text-anchor', i === 0 ? 'start' : (i === 4 ? 'end' : 'middle'));
      tk.textContent = (v / 1e6).toFixed(2) + 'M cu';
      svg.appendChild(tk);
    }
    var pos = {};
    D.nodes.forEach(function(nd){ pos[nd.i] = nd; });
    // edges first, so nodes sit on top
    var g = document.createElementNS(NS, 'g');
    g.setAttribute('class', 'dedges');
    D.edges.forEach(function(e){
      var a = pos[e[0]], b = pos[e[1]];
      if(!a || !b || !visible(a) || !visible(b)) return;
      var x1 = x(a.t1), y1 = y(a.w), x2 = x(b.t0), y2 = y(b.w);
      var p = document.createElementNS(NS, 'path');
      var mx = (x1 + x2) / 2;
      p.setAttribute('d', 'M' + x1 + ',' + y1 + ' C' + mx + ',' + y1 + ' '
                     + mx + ',' + y2 + ' ' + x2 + ',' + y2);
      p.setAttribute('class', 'dedge' + (e[2] ? ' dcrit' : ''));
      g.appendChild(p);
    });
    svg.appendChild(g);
    for(i = 0; i < D.nodes.length; i++){
      n = D.nodes[i];
      if(!visible(n)) continue;
      var cx = x(n.t0), cy = y(n.w), s = state(n);
      var shape;
      if(n.b){
        shape = document.createElementNS(NS, 'rect');
        shape.setAttribute('x', cx - 3.5); shape.setAttribute('y', cy - 3.5);
        shape.setAttribute('width', 7); shape.setAttribute('height', 7);
      } else {
        shape = document.createElementNS(NS, 'circle');
        shape.setAttribute('cx', cx); shape.setAttribute('cy', cy);
        shape.setAttribute('r', 3.6);
      }
      shape.setAttribute('class', 'dn ' + s + (n.c ? ' crit' : ''));
      var ttl = document.createElementNS(NS, 'title');
      ttl.textContent = (n.b ? 'bundle ' : 'order ') + n.r
        + (n.n > 1 ? '  (' + n.n + ' orders merged)' : '')
        + '\nlane w' + n.w + '   depth ' + n.h + '   blockers ' + n.d
        + '\n' + n.cu.toLocaleString() + ' cu'
        + '\nresult: ' + n.s + (n.c ? '\non the critical path' : '');
      shape.appendChild(ttl);
      svg.appendChild(shape);
    }
    // the playhead
    if(t < D.span){
      var ph = document.createElementNS(NS, 'line');
      ph.setAttribute('x1', x(t)); ph.setAttribute('x2', x(t));
      ph.setAttribute('y1', PADT - 4); ph.setAttribute('y2', H - PADB + 4);
      ph.setAttribute('class', 'dplay');
      svg.appendChild(ph);
    }
    var lbl = document.getElementById('dagt');
    if(lbl) lbl.textContent = t >= D.span ? 't = end'
      : 't = ' + (t / 1e6).toFixed(2) + 'M cu';
  }

  var scrub = document.getElementById('dagscrub');
  scrub.addEventListener('input', function(){
    playing = false;
    t = D.span * (scrub.value / 1000);
    draw();
  });
  document.getElementById('dagdep').addEventListener('change', function(e){
    depOnly = e.target.checked; draw();
  });
  document.querySelectorAll('.dagctl [data-act]').forEach(function(b){
    b.addEventListener('click', function(){
      var a = b.getAttribute('data-act');
      if(a === 'start'){ playing = false; t = 0; }
      else if(a === 'end'){ playing = false; t = D.span; }
      else { playing = !playing; if(t >= D.span) t = 0; }
      b.textContent = (a === 'play' && playing) ? '⏸' : b.textContent;
      scrub.value = 1000 * t / (D.span || 1);
      draw();
      if(playing) tick();
    });
  });
  document.querySelectorAll('.dagctl .spd').forEach(function(b){
    b.addEventListener('click', function(){
      speed = parseFloat(b.getAttribute('data-spd'));
      document.querySelectorAll('.dagctl .spd').forEach(function(o){
        o.classList.toggle('on', o === b);
      });
    });
  });
  var last = null;
  function tick(){
    if(!playing) return;
    raf = requestAnimationFrame(function(ts){
      if(last === null) last = ts;
      var dt = (ts - last) / 1000; last = ts;
      t += D.span * speed * dt / 6;      // six seconds end to end at 1x
      if(t >= D.span){ t = D.span; playing = false; }
      scrub.value = 1000 * t / (D.span || 1);
      draw();
      if(playing) tick(); else last = null;
    });
  }
  window.addEventListener('resize', draw);
  draw();
})();
"""


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
SHRED_STAGES = [("shred_insert_is_full", "slot complete"),
                ("bank_frozen", "bank frozen")]

# The builder's own milestones for the same slot, from bifrost_events rather
# than InfluxDB. They have exactly ONE writer -- our builder -- so unlike the
# two above there is no leader column to compare against, and no delta.
BUILDER_STAGES = [
    ("warmup", "warmup",
     "builder only &mdash; leader_state entered Warmup, the run-up before the "
     "window"),
    ("sequencing", "sequencing",
     "builder only &mdash; leader_state reached Sequencing for this slot"),
    ("bank_ready", "bank ready",
     "builder only &mdash; the scheduler had a bank and could build on it"),
]

# "first shred received" is DERIVED, not read from a column.
#
# retransmit-first-shred looks like the obvious source, but our simulator host
# never writes it -- it does not run the retransmit path -- so that row was
# permanently blank on the one host we most want to measure.
#
# shred_insert_is_full.total_time_ms is documented as the completion time
# measured from that node's OWN first shred, so
#     first_shred = row time - total_time_ms
# recovers it on every host that completes slots, ours included. Checked
# against the real retransmit stamp where both exist: within 0.2-9.5ms on a
# 4.2.0 host, but ~55ms early on a 4.1.0 one, so the two are NOT the same
# instant and are not labelled as such. Using the derived value for every host
# at least keeps the comparison on one basis.
DERIVED_FIRST_SHRED = "first shred received"


def slot_shreds(slot, stamps):
    """Per-host shred-path timeline for one slot, in two queries.

    Joined on `slot`, never on time: the stores are not always in step -- we
    have seen ClickHouse run 11.5s ahead of three Influx hosts that agreed with
    each other -- so a time join would be quietly wrong.

    Covers the PARENT slot as well as this one. The parent's bank freezing is
    what lets a context install at all, so without it the timeline starts one
    step after the thing that gated it. Returned nested by slot.

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
        + f" WHERE slot >= {int(slot) - 1} AND slot <= {int(slot)} "
        f"AND time >= '{lo}' AND time <= '{hi}' "
        'GROUP BY "host_id"')
    out = {}
    for ser in series:
        host = ser["tags"]["host_id"]
        if host in OFF_CHAIN or (host not in LEADERS and host != SIM):
            continue
        col = {name: i for i, name in enumerate(ser["columns"])}
        for row in ser["values"]:
            def val(name, r=row):
                v = r[col[name]] if name in col else None
                return None if v is None else int(v)

            at = val("slot")
            if at is None:
                continue
            out.setdefault(at, {}).setdefault(ser["name"], {})[host] = {
                "t": _rfc3339_ns(row[0]), "total_time_ms": val("total_time_ms"),
                "num_repaired": val("num_repaired"),
                "num_recovered": val("num_recovered")}
    return out


def slot_readiness(slot, stamps):
    """The builder's readiness milestones for this slot AND its parent.

    Two facts, both per SLOT rather than per round, and both from
    bifrost_events -- which means the ClickHouse clock, so the caller shifts
    them onto InfluxDB's before showing them next to the per-host stamps:

      bank_ready   the scheduler has a bank for this slot and could start
                   building on it. Emitted once per slot of the leader window,
                   carrying window_end and parent_slot, so the window's shape
                   is recoverable from the row itself.
      sequencing   the connector's leader_state reached `Sequencing` for this
                   slot. The state walks Inactive -> Warmup -> Sequencing ->
                   Cooldown(slot) and a row is written per slot per identity,
                   so this is the moment this slot was actually being
                   sequenced rather than merely anticipated. Warmup is kept
                   too: the gap between them is the run-up.

    Unlike slot complete and bank frozen these have exactly one source. They
    are OUR builder's view; no validator writes them, so there is no dogo
    column to put beside them and the panel says so rather than leaving an
    empty cell that reads like missing data."""
    if not stamps:
        return {}
    pad = dt.timedelta(minutes=10)
    lo = (dt.datetime.fromisoformat(min(stamps)[:26]) - pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    hi = (dt.datetime.fromisoformat(max(stamps)[:26]) + pad
          ).strftime("%Y-%m-%d %H:%M:%S")
    want = f"({int(slot) - 1},{int(slot)})"
    try:
        _, rows = clickhouse(
            "SELECT event, toString(ts) AS at, "
            "  toInt64OrZero(toString(nums['slot'])) AS at_slot, "
            "  toInt64OrZero(toString(nums['parent_slot'])) AS parent, "
            "  toInt64OrZero(toString(nums['window_end'])) AS window_end, "
            "  attrs['leader_state'] AS state, attrs['identity'] AS identity "
            f"FROM bifrost_events WHERE ts >= '{lo}' AND ts <= '{hi}' "
            f"  AND instance_id = '{CH_INSTANCE}' "
            f"  AND nums['slot'] IN {want} "
            "  AND (event = 'bank_ready' "
            "       OR (event = 'progress' AND attrs['leader_state'] != 'Inactive')) "
            "ORDER BY ts")
    except Exception:
        return {}
    out = {}
    for event, at, at_slot, parent, window_end, state, identity in rows:
        at_slot = ch_int(at_slot)
        t = _rfc3339_ns(at[:26].replace(" ", "T"))
        bucket = out.setdefault(at_slot, {})
        if event == "bank_ready":
            bucket["bank_ready"] = {"t": t, "parent": ch_int(parent),
                                    "window_end": ch_int(window_end)}
        elif state.startswith("Sequencing"):
            bucket.setdefault("sequencing", {"t": t, "identity": identity})
        elif state.startswith("Warmup"):
            bucket.setdefault("warmup", {"t": t, "identity": identity})
    return out


def with_derived_first_shred(per_slot):
    """Add the derived first-shred stage to a slot's bucket."""
    full = (per_slot or {}).get("shred_insert_is_full") or {}
    out = dict(per_slot or {})
    derived = {}
    for host, e in full.items():
        if e.get("total_time_ms") is None:
            continue
        derived[host] = {"t": e["t"] - e["total_time_ms"] * 1_000_000,
                         "total_time_ms": None, "num_repaired": None,
                         "num_recovered": None}
    if derived:
        out[DERIVED_FIRST_SHRED] = derived
    return out


def shred_html(slot, shreds, parent=None, ready=None, shift=0,
               spread=None, anchors=0):
    """A stage x host grid, with the sim-minus-leader delta called out.

    Shows the PARENT slot as well, and puts it first, because that is the one
    that gates this slot's auction: a context cannot install until the parent's
    bank is frozen. This slot's own completion is downstream of the auction --
    measured on 439391881 the parent froze at +4.5ms and the context installed
    at +22.2ms, while this slot did not complete until +414.8ms, long after the
    last commit at +368.6ms. So the parent row explains when we could start;
    this slot's row only says how our shred reception compared once the block
    was already built."""
    parent = parent if parent is not None else slot - 1
    per_slot = shreds or {}
    # Only the parent. This slot's own completion lands after its auction has
    # already finished -- measured on 439391881, the last commit was at
    # +368.6ms and the slot did not complete until +414.8ms -- so it cannot
    # have gated anything and only added a row to scroll past.
    groups = [(parent, "gates this slot's auction: no context installs until "
                       "the parent's bank is frozen")]
    if not any((per_slot.get(n) or {}) for n, _ in groups):
        return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs '
                'our simulator</div><div class="none">no shred-path rows for this '
                "slot</div></div>")
    # Pick the leader identity that actually reported this slot. They are
    # complementary, so at most one normally has it; ties break on the
    # configured order.
    present = {h for n, _ in groups for v in (per_slot.get(n) or {}).values()
               for h in v}
    leader = next((h for h in LEADERS if h in present), LEADER)
    hosts = [h for h in (leader, SIM) if h]
    head = ("<tr><th>stage</th>"
            + "".join(f"<th class=n>{html.escape(HOST_NAME[h])}</th>" for h in hosts)
            + "<th class=n>sim &minus; leader</th><th>detail</th></tr>")
    body = []
    for which, why in groups:
        bucket = per_slot.get(which) or {}
        if not bucket:
            continue
        body.append(
            f'<tr class="shredgrp"><td colspan="{len(hosts) + 3}">'
            f'parent slot <b>{which}</b>'
            f' <span class="basis">{why}</span></td></tr>')
        staged = []
        for meas, label in SHRED_STAGES:
            seen = {h: e for h, e in (bucket.get(meas) or {}).items()
                    if h in hosts}
            if not seen:
                continue
            cells = []
            for h in hosts:
                e = seen.get(h)
                cells.append(
                    f"<td class='n m'>{dt.datetime.fromtimestamp(e['t']/1e9, dt.UTC).strftime('%H:%M:%S.%f')[:-3]}</td>"
                    if e else "<td class='n m dim'>&mdash;</td>")
            if leader in seen and SIM in seen:
                d = (seen[SIM]["t"] - seen[leader]["t"]) / 1e6
                delta = (f"<td class='n m {'bad' if d > 0 else 'good'}'>"
                         f"{d:+.1f} ms</td>")
            else:
                delta = "<td class='n m dim'>&mdash;</td>"
            note = "; ".join(
                f"{HOST_NAME[h]}: {e['total_time_ms']} ms"
                + (f", {e['num_recovered']:,} recovered"
                   if e.get("num_recovered") else "")
                + (f", {e['num_repaired']:,} repaired"
                   if e.get("num_repaired") else "")
                for h in hosts
                if (e := seen.get(h)) and e.get("total_time_ms") is not None)
            # sort on the leader's stamp when it exists, so the ordering does
            # not flip about with our own host's reception jitter
            key = (seen.get(leader) or seen.get(SIM) or
                   next(iter(seen.values())))["t"]
            staged.append((key,
                           f"<tr><td class=m>{label}</td>{''.join(cells)}{delta}"
                           f"<td class='m dim'>{note}</td></tr>"))

        # The builder's own milestones for the same slot. They sit in the
        # simulator column because that is the host they belong to, and the
        # leader column stays empty because no validator writes them -- the
        # detail cell says so, so an empty cell is never read as a gap.
        for key, label, why in BUILDER_STAGES:
            e = ((ready or {}).get(which) or {}).get(key)
            if not e:
                continue
            at = e["t"] + shift
            cells = []
            for h in hosts:
                cells.append(
                    f"<td class='n m'>{dt.datetime.fromtimestamp(at/1e9, dt.UTC).strftime('%H:%M:%S.%f')[:-3]}</td>"
                    if h == SIM else "<td class='n m dim'>&mdash;</td>")
            staged.append((at,
                           f"<tr class='bstage'><td class=m>{label}</td>"
                           f"{''.join(cells)}<td class='n m dim'>&mdash;</td>"
                           f"<td class='m dim'>{why}</td></tr>"))
        body += [row for _, row in sorted(staged, key=lambda kv: kv[0])]

    if not body:
        return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs '
                'our simulator</div><div class="none">neither the leader nor our '
                "simulator reported the shred path for this slot</div></div>")
    # The last three rows come from ClickHouse and the rest from InfluxDB, so
    # they only sit in one column together once the offset between the two
    # clocks is applied. Say which, because an unstated shift would make the
    # ordering look like a measurement when it is an alignment.
    clocknote = ""
    if any((ready or {}).get(w) for w, _ in groups):
        if anchors:
            clocknote = (
                f'<span class="basis">builder rows shifted {shift / 1e6:+.0f} ms '
                "onto the InfluxDB clock, measured on <code>round_committed</code>, "
                f"which both stores record"
                + (f" (spread {spread:.1f} ms over {anchors} "
                   f"round{'' if anchors == 1 else 's'})" if spread is not None
                   else "") + "</span>")
        else:
            clocknote = (
                '<span class="warn">builder rows are NOT clock-shifted &mdash; no '
                "round of this slot was committed in both stores, so the offset "
                "could not be measured. The two have been seen 899 ms apart, so "
                "do not read the builder rows against the host rows</span>")

    def wrote(host):
        return any(host in per_meas
                   for per_slot_b in (shreds or {}).values()
                   for per_meas in per_slot_b.values())

    missing = ""
    if not wrote(leader):
        # A leader's metric submission can be intermittent while peers report
        # continuously, so an empty leader column is a reporting gap rather
        # than evidence of a slow node. Say so, rather than let it read as a
        # measurement.
        missing = ('<span class="warn">the leader wrote no shred metrics for this slot '
                   "&mdash; its submission is intermittent, so this is a gap in "
                   "reporting rather than a slow node</span>")
    elif not wrote(SIM):
        missing = '<span class="note">our simulator host wrote nothing here</span>'
    return ('<div class="panel"><div class="dtlhead">shred path &mdash; leader vs our '
            'simulator, parent slot'
            + (f'<span class="basis">leader identity {html.escape(leader)}</span>'
               if leader else "")
            + '<span class="note">positive delta = our simulator was later &middot; '
            'joined on slot, not on time &middot; our host does not emit '
            "retransmit-first-shred</span>" + missing + clocknote + "</div>"
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

    with cf.ThreadPoolExecutor(max_workers=6) as pool:
        fut = {"ext": pool.submit(slot_extends, slot, stamps),
               "commit": pool.submit(slot_commits, slot, stamps),
               "relay": pool.submit(slot_relay, slot, stamps),
               "builder": pool.submit(slot_builder_events, slot, stamps),
               "logs": pool.submit(slot_logs, slot, stamps),
               "shred": pool.submit(slot_shreds, slot, stamps),
               "ready": pool.submit(slot_readiness, slot, stamps)}
    try:
        extends, ext_err = fut["ext"].result(), None
    except Exception as exc:
        extends, ext_err = {}, str(exc)[:140]
    try:
        commits = fut["commit"].result()
    except Exception:
        commits = {}
    try:
        builder_ev = fut["builder"].result()
    except Exception:
        builder_ev = {}
    try:
        logs = fut["logs"].result()
    except Exception as exc:
        logs = {"err": str(exc)[:140]}
    try:
        relay_rounds, relay_err = fut["relay"].result(), None
    except Exception as exc:
        relay_rounds, relay_err = {}, str(exc)[:140]
    try:
        shreds, shred_err = fut["shred"].result(), None
    except Exception as exc:
        shreds, shred_err = {}, str(exc)[:140]
    try:
        ready = fut["ready"].result()
    except Exception:
        ready = {}
    data = {"rounds": rounds, "extends": extends, "ext_err": ext_err,
            "commits": commits, "relay": relay_rounds, "builder": builder_ev,
            "logs": logs,
            "relay_err": relay_err,
            "shreds": shreds, "shred_err": shred_err, "ready": ready}

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


def detail_table(title, items, show_won=False, winner_name=None,
                 relay_on=False):
    """`show_won` turns the last column into "won by". A bare YES/- only said
    whether it was us; the point of interest on a lost round is WHO, so the
    winning builder's id goes there when we can name it."""
    if not items:
        return f'<div class="dtl"><div class="dtlhead">{title}</div>' \
               '<div class="none">none</div></div>'
    def split(i):
        """Vote / non-vote for one offer, counted from the payload itself.

        The payload carries whole transactions, so the vote program id appears
        verbatim in the account keys of every vote. That classifies ALL of
        them, including transactions that never landed -- a chain lookup can
        only classify the ones that did, and used to leave the rest as "?".

        A winner's payload holds only refs, not transactions, so there is
        nothing to count the vote program id in and it reads as a dash. The
        comparison tab classifies those refs against the block instead."""
        v = i.get("votes", -1)
        if v is None or v < 0:
            return "&mdash;", "&mdash;"
        return f"{max(0, i['txs'] - v):,}", f"{v:,}"

    head = ("<tr><th>time (UTC)</th><th class=n>orders</th>"
            "<th class=n>non-vote</th><th class=n>vote</th>"
            "<th class=n>bundles</th><th class=n>reward SOL</th>"
            "<th class=n>exec cost</th><th class=n>selected cu</th><th>uuid</th>"
            + ("<th>won by</th>" if show_won else "") + "</tr>")
    body = "".join(
        f"<tr><td class=m>{html.escape(i['ts'][11:23])}</td>"
        f"<td class='n m'>{i['orders']:,}</td>"
        f"<td class='n m'>{split(i)[0]}</td><td class='n m dim'>{split(i)[1]}</td>"
        f"<td class='n m'>{i['bundles']:,}</td><td class='n m'>{sol(i['reward'])}</td>"
        f"<td class='n m'>{i['exec_cost']:,}</td><td class='n m'>{i['sel_cu']:,}</td>"
        f"<td class='m dim'>{html.escape(i['uuid'])}{copy_btn(i['uuid'])}</td>"
        # The relay names the winner whoever it was, ourselves included, so
        # prefer it in BOTH cases. Naming only the opponent meant our own
        # wins fell through to a configured label and read as a generic "us"
        # even when the relay had already named the builder.
        + (f"<td class='m {'goodc' if i['won'] else ''}'>"
           f"{winner_name and html.escape(winner_name) or unnamed(i['won'], relay_on)}"
           "</td>" if show_won else "")
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

# Our own data cannot name a winner: won_by_us=false only says someone else
# took it. The relay's round_chosen does name them, so that is the only source
# used -- no builder names are configured anywhere, and a builder added later
# appears on its own.
# Builder names are never configured. The relay's round_chosen names whoever
# actually won, so a builder we have never seen shows up correctly with no
# change here. Without a relay we can only report ours/not-ours, which is all
# won_by_us actually tells us -- and we say so rather than invent a name.
def unnamed(won, relay_on):
    """We could not name the winner. Say WHY -- an unconfigured relay and a
    round the relay never echoed a winner for are different problems, and
    labelling both the same sent one investigation down the wrong path."""
    why = "no relay configured" if not relay_on else "no winner echo from relay"
    return f'{"ours" if won else "not ours"} <span class="dim">({why})</span>'



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


def timeline_html(slot, extends, commits, shreds, rounds, builder=None):
    """The whole causal chain for a slot, in order.

    It starts on the PARENT slot, because that is what actually gates us: a
    context cannot install until the parent's bank is frozen, so the parent's
    shreds arriving and its bank freezing are the events that decide when we
    can begin sequencing at all. Then our own rounds, then this slot's shreds
    arriving and its bank freezing underneath us -- note the auction normally
    finishes before its own slot is even fully shredded.

    ONE CLOCK ONLY. Every row is InfluxDB, pinned to our own host. Relay and
    ClickHouse timestamps are excluded on purpose -- separate clocks, observed
    11.5s apart, which would silently reorder the sequence."""
    ctx = (commits or {}).get("_context")
    parent = ctx["parent"] if ctx and ctx.get("parent") else slot - 1
    ev = []

    def shred_rows(which, tag, prefix):
        bucket = (shreds or {}).get(which) or {}
        for meas, label, note in (
                ("shred_insert_is_full", "slot complete",
                 "every shred in the blockstore"),
                ("bank_frozen", "bank frozen", "state final, replay done")):
            e = bucket.get(meas, {}).get(SIM)
            if not e:
                continue
            extra = (f'insert took {e["total_time_ms"]:,} ms'
                     + (f', {e["num_recovered"]:,} recovered'
                        if e.get("num_recovered") else "")
                     + (f', {e["num_repaired"]:,} repaired'
                        if e.get("num_repaired") else "")
                     if e.get("total_time_ms") is not None else "")
            ev.append((e["t"], f"{prefix}{label}", note, extra, tag))

    shred_rows(parent, "parent", f"parent {parent} &middot; ")
    if ctx:
        ev.append((ctx["t"], "context installed",
                   "the parent is frozen, so sequencing can begin",
                   f"built on parent {parent}", "start"))
    for idx in sorted(k for k in (extends or {}) if isinstance(k, int)):
        e = extends[idx]
        span = max(0, (e["t_last"] - e["t_first"]) / 1e6)
        busy = 100.0 * e["body_sum"] / max(1, span * 1000)
        tip = html.escape(
            f"{e['n']} extends were ACCEPTED in round {idx}. "
            f"Total extend time {e['body_sum']/1000:.1f} ms is the sum of "
            f"those {e['n']} extend bodies -- it is NOT commit time and not "
            f"one call. Wall {span:.0f} ms is from the first extend starting "
            f"to the last one finishing, so wall minus total "
            f"({span - e['body_sum']/1000:.0f} ms) is time the mutation lane "
            f"sat idle inside this round. p50 {e['body_p50']/1000:.2f} ms and "
            f"max {e['body_max']/1000:.2f} ms are per single extend, not "
            f"totals. Refused extends emit no datapoint at all, so the true "
            f"offered load was higher than {e['n']}.", quote=True)
        ev.append((e["t_first"], f"round {idx} extends",
                   f'{e["n"]} extends &middot; {ms(e["body_sum"])} total extend '
                   f'time &middot; {span:.0f} ms wall ({busy:.0f}% busy)'
                   f'<span class="info" tabindex="0" data-tip="{tip}">i</span>',
                   f'per extend: p50 {ms(e["body_p50"])} &middot; max '
                   f'{ms(e["body_max"])} &middot; {e["applied"]:,} of '
                   f'{e["orders"]:,} orders applied',
                   "ext"))
    for idx in sorted(k for k in (commits or {}) if isinstance(k, int)):
        c = commits[idx]
        kind = COMMIT_KIND.get(c["kind"], "?")
        detail = (f'replay {c["replayed"]:,} orders' if kind == "replay"
                  else "promote, no replay" if kind == "promote"
                  else "winnerless, nothing applied")
        ev.append((c["t"], f"round {idx} commit",
                   f'{kind} &middot; {ms(c["body"])}', detail, kind))
    shred_rows(slot, "chain", "")

    # The builder's own events, shifted onto the InfluxDB clock.
    off, spread, anchors = builder_offset(builder, commits)
    align = ""
    if builder and builder.get("events"):
        if off is None:
            align = ('<span class="warn"> &#9888; builder events omitted: no '
                     "round_committed in both stores to align the clocks</span>")
        else:
            align = ('<span class="note">builder events shifted '
                     f"{off/1e6:+,.0f} ms onto the simulator's clock, measured "
                     f"from {anchors} round_committed pairs (spread "
                     f"{spread:.1f} ms)</span>")
            per_round = {}
            for e in builder["events"]:
                t = e["t"] + off
                if e["event"] == "progress":
                    if e["leader_state"] and e["leader_state"] != "Inactive":
                        ev.append((t, f'progress: {e["leader_state"]}',
                                   "the connector entered this state",
                                   html.escape(e["identity"] or ""), "bld"))
                elif e["event"] == "bank_ready":
                    ev.append((t, "bank_ready (builder)",
                               f'source {html.escape(e["source"] or "?")}'
                               + (" &mdash; real window" if e["source"] == "window"
                                  else " &mdash; synthetic" if e["source"] else ""),
                               f'parent {e["parent_slot"]}', "bld"))
                elif e["event"] == "round_winner":
                    ev.append((t, f'round {e["idx"]} winner announced',
                               "relay told us who took the round",
                               ("ours" if e["won_by_us"] == "true" else
                                html.escape(e["identity"] or "not ours")), "bld"))
                elif e["event"] == "promote_mismatch":
                    ev.append((t, f'round {e["idx"]} promote mismatch',
                               f'chosen {e["chosen_refs"]:,} refs vs expected '
                               f'{e["expected_refs"]:,}',
                               "our prefix was not the winner, so a replay "
                               "follows", "bld"))
                elif e["event"] == "dispatched":
                    per_round[e["idx"]] = per_round.get(e["idx"], 0) + 1
            for idx, n in sorted(per_round.items()):
                first = min(e["t"] for e in builder["events"]
                            if e["event"] == "dispatched" and e["idx"] == idx)
                ev.append((first + off, f"round {idx} dispatched",
                           f"{n:,} miniblock{'' if n == 1 else 's'} sent to the "
                           "relay", "", "bld"))

    # The replay chain: the relay announces a winner, we commit/replay it, then
    # the next round's first extend can run. Those three are already separate
    # rows; what was missing was the gaps between them, which is the part that
    # says how much of a round the replay actually costs us.
    winner_at = {}
    for e in (builder or {}).get("events", []):
        if e["event"] == "round_winner" and off is not None:
            winner_at[e["idx"]] = e["t"] + off
    annotated = []
    for t, label, what, detail, cls in ev:
        m = re.match(r"round (\d+) (commit|extends)$", label)
        if m:
            rnd, kind = int(m.group(1)), m.group(2)
            if kind == "commit" and rnd in winner_at:
                gap = (t - winner_at[rnd]) / 1e6
                detail += (f' &middot; <b>{gap:+,.1f} ms after round {rnd} '
                           f"winner announced</b>")
            elif kind == "extends" and rnd > 0 and (rnd - 1) in commits:
                prev = commits[rnd - 1]
                gap = (t - prev["t"]) / 1e6
                detail += (f' &middot; first extend {gap:+,.1f} ms after the '
                           f"round {rnd - 1} commit landed")
        annotated.append((t, label, what, detail, cls))
    ev = annotated

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
    legend = "".join(
        f'<span class="lg"><i style="background:{colour}"></i>{text}</span>'
        for colour, text in (
            ("#64748b", "parent slot &mdash; the chain arriving before us"),
            ("#c084fc", "builder &mdash; bifrost_events, clock-aligned"),
            ("#2dd4bf", "context install &mdash; we can start sequencing"),
            ("#60a5fa", "our extends"),
            ("#fb923c", "commit: replay &mdash; we lost, their block re-executed"),
            ("#4ade80", "commit: promote &mdash; we won, pointer move"),
            ("#475569", "commit: empty &mdash; winnerless round"),
            ("#22d3ee", "this slot on chain &mdash; complete, frozen")))
    return ('<div class="panel"><div class="dtlhead">slot timeline'
            "<span class='note'>starts on the PARENT slot, because a context "
            "cannot install until the parent's bank is frozen &middot; "
            "InfluxDB only, pinned to our own host, so this is a single clock "
            "and the order is real &middot; relay and ClickHouse timestamps "
            "are excluded unless they can be ALIGNED &middot; extends are "
            "collapsed per round</span>" + align + "</div>"
            f'<div class="legend">{legend}</div>'
            "<table><thead><tr><th class=n>offset</th><th>UTC</th><th>event</th>"
            "<th>what</th><th>detail</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
)


SEV_CLASS = {"warn": "sv-warn", "error": "sv-err", "fatal": "sv-fatal"}


def logs_html(slot, logs):
    """warn / error / fatal for this slot, in order.

    Runs of the same message are collapsed: one ALT failure repeated 171 times
    in a third of a second is one fact, and listing it 171 times buries the
    single line that actually explains the round."""
    if not logs or logs.get("unconfigured"):
        return ('<div class="panel"><div class="dtlhead">logs</div>'
                '<div class="none">ClickStack not configured &mdash; set '
                "OTEL_URL and OTEL_SERVICE</div></div>")
    if logs.get("nowindow"):
        return ('<div class="panel"><div class="dtlhead">logs</div>'
                '<div class="none">this slot has no miniblock rows, so there '
                "is no time window to search logs in &mdash; usually means the "
                "builder was down for it</div></div>")
    if logs.get("err"):
        return ('<div class="panel"><div class="dtlhead">logs</div>'
                f'<div class="err">{html.escape(logs["err"])}</div></div>')
    rows = logs.get("rows") or []
    if not rows:
        return ('<div class="panel"><div class="dtlhead">logs'
                "<span class='note'>warn, error and fatal only &mdash; the "
                "info-level status heartbeats are excluded</span></div>"
                '<div class="none">no warnings or errors for this slot</div>'
                "</div>")

    # collapse consecutive identical messages at the same severity
    runs = []
    for r in rows:
        if runs and runs[-1]["body"] == r["body"] and runs[-1]["sev"] == r["sev"]:
            runs[-1]["n"] += 1
            runs[-1]["last"] = r["t"]
            runs[-1]["tagged"] = runs[-1]["tagged"] or r["tagged"]
        else:
            runs.append({"body": r["body"], "sev": r["sev"], "n": 1,
                         "first": r["t"], "last": r["t"], "idx": r["idx"],
                         "tagged": r["tagged"]})
    t0 = runs[0]["first"]
    counts = {}
    for r in rows:
        counts[r["sev"]] = counts.get(r["sev"], 0) + 1
    summary = " &middot; ".join(
        f'<span class="{SEV_CLASS.get(k, "")}">{v:,} {k}</span>'
        for k, v in sorted(counts.items(), key=lambda kv: -kv[1]))

    body = []
    for r in runs:
        span = (r["last"] - r["first"]) / 1e6
        body.append(
            f'<tr class="{SEV_CLASS.get(r["sev"], "")}">'
            f'<td class="n m">+{(r["first"] - t0) / 1e6:,.1f} ms</td>'
            f'<td class="m dim">{dt.datetime.fromtimestamp(r["first"]/1e9, dt.UTC).strftime("%H:%M:%S.%f")[:-3]}</td>'
            f'<td class="m sevcell">{html.escape(r["sev"])}</td>'
            f'<td class="m">{html.escape(r["body"])}</td>'
            f'<td class="n m">' + (f'&times;{r["n"]:,}' if r["n"] > 1 else "")
            + (f'<div class="basis">over {span:,.0f} ms</div>'
               if r["n"] > 1 and span >= 1 else "") + "</td>"
            f'<td class="m dim">'
            + (f'round {html.escape(r["idx"])}' if r["idx"] else "")
            + ("" if r["tagged"] else '<span class="basis">time-matched, not '
                                      "slot-tagged</span>")
            + "</td></tr>")
    return ('<div class="panel"><div class="dtlhead">logs'
            f'<span class="sevsum">{summary}</span>'
            "<span class='note'>warn, error and fatal only &mdash; info-level "
            "status heartbeats are excluded &middot; lines carrying "
            "LogAttributes['slot'] are matched on the slot exactly; the rest "
            "are matched by time window and marked &middot; repeats of the "
            "same message are collapsed</span></div>"
            "<table><thead><tr><th class=n>offset</th><th>UTC</th>"
            "<th>severity</th><th>message</th><th class=n>count</th>"
            "<th>where</th></tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table></div>")


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
                 relay_err=None, extras=None, shreds=None):
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

    # context install -> this slot's bank frozen: the window every round of
    # this slot had to fit inside. Only meaningful on round 0, where there is
    # no commit to report.
    # What this slot was built ON, and how much room it had. The parent gates
    # everything: no context installs until its bank is frozen.
    ctx = (commits or {}).get("_context")
    parent = (ctx or {}).get("parent") or (slot - 1)
    pbucket = (shreds or {}).get(parent) or {}
    at = lambda meas: ((pbucket.get(meas) or {}).get(SIM) or {}).get("t")
    p_full, p_froze = at("shred_insert_is_full"), at("bank_frozen")
    clock = lambda t: (dt.datetime.fromtimestamp(t / 1e9, dt.UTC)
                       .strftime("%H:%M:%S.%f")[:-3] if t else "&mdash;")
    facts = [("parent slot", f"{parent}"),
             ("parent slot complete", clock(p_full)),
             ("parent bank frozen", clock(p_froze))]
    if ctx:
        facts.append(("context installed", clock(ctx["t"])))
        if p_froze:
            facts.append(("parent froze &rarr; context",
                          ms(int((ctx["t"] - p_froze) / 1000))))
    parent_strip = ('<div class="pfacts">'
                    + "".join(f'<span class="pf"><b>{k}</b>{v}</span>'
                              for k, v in facts)
                    + "</div>")
    ready_window = None

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
            them_name, basis = (rl or {}).get("win_builder"), "winner row"
            their_reward, their_cu = w["reward"], w["exec_cost"]
            their_orders = w["orders"]
        else:
            them_name, basis = None, ""
            their_reward = their_cu = their_orders = None

        # prefer the relay's actual builder id over our generic label
        relay_winner = (rl or {}).get("win_builder")
        winner = (html.escape(relay_winner) if relay_winner
                  else unnamed(we_won, bool(RELAY_URL and RELAY_DS_UID))
                  if w else "&mdash;")
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
            f'<td class="m {"goodc" if we_won else ""}">{winner}'
            # the winning miniblock's own id. Present on our winner row even
            # with no relay configured, and it is the SAME uuid the relay puts
            # on round_winner, so it joins the two stores for one round.
            + (f'<div class="wid m">{html.escape(w["uuid"])}'
               f'{copy_btn(w["uuid"])}</div>' if w and w.get("uuid") else "")
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
            + f'<td class="n m">{commit_cell(com, r["round"], ready_window)}</td>'
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
    return ('<div class="panel">' + parent_strip
            + '<div class="dtlhead">round comparison'
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


def applied_badge(stat):
    """Orders applied vs dropped by this round's extends.

    Both come from sim-extend and are summed over the round's accepted
    extends, so the attribution is exact. `orders` is the batch each extend was
    handed and `applied` is what stuck, so the difference is what the simulator
    refused to apply -- per-order reasons are not in InfluxDB, only the
    per-call status, which is what the dropdown carries.

    Note this cannot see extends that were REFUSED outright: a throttled
    request never reaches the worker and emits no datapoint, so the true
    offered load was higher than `orders`."""
    if not stat:
        return ""
    orders, applied = stat.get("orders", 0), stat.get("applied", 0)
    if not orders:
        return ""
    dropped = max(0, orders - applied)
    tip = html.escape(
        f"{applied:,} of {orders:,} orders were applied across this round's "
        f"{stat['n']} accepted extends; {dropped:,} were not. Refused extends "
        f"emit no datapoint at all, so the real offered load was higher than "
        f"{orders:,}.", quote=True)
    return (f'<span class="apl{" haddrop" if dropped else ""}" '
            f'data-tip="{tip}" tabindex="0">'
            f'<b>{applied:,}</b>/{orders:,} applied'
            + (f' &middot; <span class="dropn">{dropped:,} dropped</span>'
               if dropped else "")
            + "</span>")


def extend_errors(stat):
    """A dropdown of non-SUCCESS extend statuses, only when there are any.

    Sits outside the round row so opening it does not also toggle the round."""
    if not stat:
        return ""
    bad = [(name, n) for name, n in stat.get("statuses", []) if name != "SUCCESS"]
    if not bad:
        return ""
    total = sum(n for _, n in bad)
    items = "".join(
        f'<li><span class="sv-err">{html.escape(name)}</span>'
        f'<span class="dim"> &times;{n:,}</span></li>' for name, n in bad)
    return ('<details class="errdrop"><summary>'
            f'{total:,} extend error{"" if total == 1 else "s"}</summary>'
            f'<ul>{items}</ul>'
            '<div class="basis">status returned by the simulator per extend '
            "call; per-order reasons are not recorded in InfluxDB</div>"
            "</details>")


def replay_badge(rnd, commits):
    """What this round had to replay before it could extend.

    Shows commit N-1, the same attribution the comparison tab uses: commit N
    applies round N's winner and round N+1 builds on it, so the cost gating
    round N is the previous round's commit. Round 0 therefore has none -- it
    builds on the parent bank.

    Only shown when that commit was an actual REPLAY, which is exactly the case
    where we lost the previous round and had to re-execute someone else's
    block. A promote is a pointer move at tens of microseconds and says
    nothing worth a badge."""
    if rnd == 0:
        return ""
    com = (commits or {}).get(rnd - 1)
    if not com or COMMIT_KIND.get(com["kind"]) != "replay":
        return ""
    tip = html.escape(
        f"Before round {rnd} could extend, round {rnd - 1}'s winning block had "
        f"to be re-executed on our bank: {com['replayed']:,} orders in "
        f"{com['body']/1000:.1f} ms. This is the cost of having lost round "
        f"{rnd - 1}; winning it instead would have made this a promote, which "
        f"runs in tens of microseconds. Round 0 has no such cost, it builds on "
        f"the parent bank.", quote=True)
    return (f'<span class="rpl" data-tip="{tip}" tabindex="0">'
            f'replay <b>{ms(com["body"])}</b> &middot; {com["replayed"]:,} orders'
            "</span>")


def reward_badge(rnd, r, w, relay_rounds=None):
    """Our best offer against what actually won the round.

    OURS is the final cumulative offer, not a sum: selected rows are prefixes
    of one another. THEIRS prefers the relay's round_chosen reward, which is
    the accepted block, and falls back to our own winner-echo row.

    A positive margin means we bid more and still lost -- which happens, and is
    the reason this belongs next to the round rather than buried a tab away."""
    offers = r.get("offers") or []
    ours = max((o["reward"] for o in offers), default=None)
    rl = (relay_rounds or {}).get(rnd) or {}
    theirs = rl.get("win_reward") or (w["reward"] if w else None)
    if not ours and not theirs:
        return ""
    if not theirs:
        return (f'<span class="rwd"><b>{big(ours)}</b> ours &middot; '
                "winner unknown</span>")
    we_won = bool(w and w["won"])
    pct = 100.0 * (ours - theirs) / theirs if (ours and theirs) else None
    cls = "rwd" + (" ahead" if pct is not None and pct >= 0 else " behind")
    if we_won:
        # the second number is OUR OWN accepted block, so "vs" would read as
        # though we lost to someone. A gap here means we kept bidding after
        # the relay had already taken an earlier offer.
        cls = "rwd ours"
        tip = html.escape(
            f"Round {rnd}: we won it. Our best offer reached {ours or 0:,} "
            f"lamports and the block the relay accepted was {theirs:,}. A gap "
            f"means we went on offering after the round had been decided, so "
            f"the extra was never in play.", quote=True)
        return (f'<span class="{cls}" data-tip="{tip}" tabindex="0">'
                f'won with <b>{big(theirs)}</b>'
                + (f' &middot; offered up to {big(ours)}'
                   if ours and ours != theirs else "")
                + "</span>")
    tip = html.escape(
        f"Round {rnd}: our best offer was {ours or 0:,} lamports, the winning "
        f"block {theirs:,}. Ours is the final cumulative offer for the round, "
        f"not a sum of the individual selected rows -- those are prefixes of "
        f"one another. A positive margin means we bid MORE and still did not "
        f"win the round.", quote=True)
    return (f'<span class="{cls}" data-tip="{tip}" tabindex="0">'
            f'<b>{big(ours)}</b> vs {big(theirs)} won'
            + (f' <span class="pct">{pct:+.2f}%</span>' if pct is not None else "")
            + "</span>")


def rounds_html(slot, sel_round):
    try:
        data = slot_data(slot)
    except Exception as exc:
        return f'<div class="err">rounds unavailable: {html.escape(str(exc))[:150]}</div>'
    rounds, extends, ext_err = data["rounds"], data["extends"], data["ext_err"]
    relay_rounds = data.get("relay") or {}
    commits = data.get("commits") or {}

    out = []
    if data["shred_err"]:
        out.append('<div class="err" style="margin:0 28px 8px">shred path '
                   f"unavailable: {html.escape(data['shred_err'])}</div>")
    else:
        shift, spread, anchors = builder_offset(data.get("builder"),
                                                data.get("commits"))
        out.append(shred_html(
            slot, data["shreds"],
            ((data.get("commits") or {}).get("_context") or {}).get("parent"),
            data.get("ready"), shift or 0, spread, anchors))
    if ext_err:
        out.append('<div class="err" style="margin:0 28px 8px">sim-extend '
                   f"unavailable: {html.escape(ext_err)}</div>")
    for r in rounds:
        w = r["winner"]
        stat = extends.get(r["round"])
        open_ = r["round"] == sel_round
        # Every round's detail is rendered up front and toggled in the browser.
        # It used to be a link, so opening a round was a navigation: a full
        # round trip and a re-render of the strip for data the page already
        # had. The slot fetch is one call either way, so there was nothing to
        # gain by making the user wait for it again.
        rnd = r["round"]
        summary = (
            f'<div class="rnd{" open" if open_ else ""}" data-round="{rnd}" '
            f'role="button" tabindex="0" aria-expanded="{str(open_).lower()}">'
            f'<span class="rid">round {rnd}</span>'
            f'<span class="cnt">{len(r["offers"])} offer'
            f'{"" if len(r["offers"]) == 1 else "s"}</span>'
            + (f'<span class="won">won</span>' if w and w["won"]
               else '<span class="lost">not ours</span>' if w
               else '<span class="nowin">no winner echo</span>')
            + (f'<span class="last">is_last</span>' if w and w["is_last"] else "")
            + (f'<span class="ext">{stat["n"]} ext &middot; '
               f'{ms(stat["body_sum"])} total extend time</span>' if stat
               else '<span class="noext">0 ext</span>' if not ext_err else "")
            + reward_badge(rnd, r, w, relay_rounds)
            + replay_badge(rnd, commits)
            + applied_badge(stat)
            + f'<span class="chev">{"&minus;" if open_ else "+"}</span></div>')
        errdrop = extend_errors(stat)
        detail = ('<div class="detail" data-round="{}"{}>'.format(
                      rnd, "" if open_ else " hidden")
                  + extend_table(stat)
                  + pcache_table(stat)
                  + detail_table(f"our offers &mdash; {len(r['offers'])}",
                                 r["offers"])
                  + detail_table(
                      "winner miniblock", [w] if w else [], show_won=True,
                      winner_name=(relay_rounds.get(rnd) or {}).get(
                          "win_builder") or None,
                      relay_on=bool(RELAY_URL and RELAY_DS_UID))
                  + "</div>")
        out.append(f'<div class="rndwrap">{summary}{errdrop}{detail}</div>')
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
form.jump{margin-left:auto}
form.jump input{background:#0e151d;border:1px solid #22303f;border-radius:6px;
  color:#cfe0f0;padding:5px 11px;width:150px;
  font:12px ui-monospace,Menlo,monospace}
form.jump input:focus{outline:none;border-color:#5eead4}
form.jump input::placeholder{color:#4d5c70}
a.navlink{margin-left:14px;color:#5eead4;font-size:12px;text-decoration:none;
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
tr:hover .cp,td:hover .cp{opacity:1}
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
.chip.runstart{border-left:3px solid #fbbf24}
.runmark{font-size:8.5px;color:#fbbf24;text-transform:uppercase;
  letter-spacing:.06em;margin-top:3px}
.runbar{padding:10px 28px 0}
.runjumps{display:flex;gap:6px;overflow-x:auto;padding:7px 0 2px;
  scrollbar-width:thin}
a.runjump{flex:0 0 auto;display:flex;flex-direction:column;gap:1px;
  padding:4px 9px;border:1px solid #22303f;border-radius:6px;color:#9fb2c8;
  font:10.5px/1.3 ui-monospace,Menlo,monospace;text-decoration:none}
a.runjump:hover{border-color:#5eead4;color:#5eead4}
a.runjump.cold{border-color:#78350f;color:#fbbf24}
.rj{color:#5b6b80;font-size:9px}
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
.chip .tag .allc{color:#5eead4;font-weight:600}
.chip .tag .okc{color:#5eead4}
.chip .tag .lostc{color:#5b6b80}
.chip .tag .dimc{color:#46566b}
.dimc{color:#5b6b80}
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
td.bar{width:18%;padding:0 9px}
td.bar span{display:block;width:9px;height:9px;border-radius:50%;
  box-shadow:0 0 0 2px #0b1016}
/* One hue per actor, applied THREE ways so a row is identifiable without
   reading it: the dot, a left rule, a tinted band, and the event name.
   Keep these in step with the legend in timeline_html -- they were allowed to
   drift once and the legend then described colours the table never used. */
tr.tl-parent  td.bar span{background:#64748b}
tr.tl-bld     td.bar span{background:#c084fc}
tr.tl-start   td.bar span{background:#2dd4bf}
tr.tl-ext     td.bar span{background:#60a5fa}
tr.tl-replay  td.bar span{background:#fb923c}
tr.tl-promote td.bar span{background:#4ade80}
tr.tl-empty   td.bar span{background:#475569}
tr.tl-chain   td.bar span{background:#22d3ee}
tr.tl-parent  td:first-child{border-left:3px solid #64748b}
tr.tl-bld     td:first-child{border-left:3px solid #c084fc}
tr.tl-start   td:first-child{border-left:3px solid #2dd4bf}
tr.tl-ext     td:first-child{border-left:3px solid #60a5fa}
tr.tl-replay  td:first-child{border-left:3px solid #fb923c}
tr.tl-promote td:first-child{border-left:3px solid #4ade80}
tr.tl-empty   td:first-child{border-left:3px solid #475569}
tr.tl-chain   td:first-child{border-left:3px solid #22d3ee}
tr.tl-parent  td{background:#0f1419}
tr.tl-bld     td{background:#1b1230}
tr.tl-start   td{background:#07231f}
tr.tl-ext     td{background:#0d1a2c}
tr.tl-replay  td{background:#2a1a0c}
tr.tl-promote td{background:#0b2416}
tr.tl-empty   td{background:#111820}
tr.tl-chain   td{background:#07202a}
tr.tl-parent  td:nth-child(3) b{color:#94a3b8}
tr.tl-bld     td:nth-child(3) b{color:#d8b4fe}
tr.tl-start   td:nth-child(3) b{color:#5eead4}
tr.tl-ext     td:nth-child(3) b{color:#93c5fd}
tr.tl-replay  td:nth-child(3) b{color:#fdba74}
tr.tl-promote td:nth-child(3) b{color:#86efac}
tr.tl-empty   td:nth-child(3) b{color:#94a3b8}
tr.tl-chain   td:nth-child(3) b{color:#67e8f9}
.panel tbody tr.tl-parent td,.panel tbody tr.tl-bld td,
.panel tbody tr.tl-start td,.panel tbody tr.tl-ext td,
.panel tbody tr.tl-replay td,.panel tbody tr.tl-promote td,
.panel tbody tr.tl-empty td,.panel tbody tr.tl-chain td{
  border-bottom:1px solid #0b1016;padding:6px 9px}
.sevsum{margin-left:10px;font-size:11px;text-transform:none;letter-spacing:0}
.sv-warn .sevcell,.sevsum .sv-warn{color:#fbbf24;font-weight:600}
.sv-err .sevcell,.sevsum .sv-err{color:#f87171;font-weight:600}
.sv-fatal .sevcell,.sevsum .sv-fatal{color:#fca5a5;font-weight:700}
tr.sv-warn td{background:#2a1f06}
tr.sv-warn td:first-child{border-left:3px solid #fbbf24}
tr.sv-err td{background:#2a0f0f}
tr.sv-err td:first-child{border-left:3px solid #f87171}
tr.sv-fatal td{background:#3b0d0d}
tr.sv-fatal td:first-child{border-left:3px solid #ef4444}
.legend{display:flex;gap:14px;flex-wrap:wrap;padding:8px 0 2px}
.lg{display:flex;gap:6px;align-items:center;color:#8fa6bf;font-size:10.5px}
.lg i{width:9px;height:9px;border-radius:50%;display:inline-block;font-style:normal}
.basis{color:#5b6b80;font-size:9.5px;margin-top:2px;text-transform:none}
.pfacts{display:flex;gap:18px;flex-wrap:wrap;padding:2px 0 12px;
  border-bottom:1px solid #1e2937;margin-bottom:11px}
.pf{display:flex;flex-direction:column;gap:1px;font-size:11.5px;
  font-family:ui-monospace,Menlo,monospace;color:#cfe0f0}
.pf b{color:#61748b;font-size:9px;text-transform:uppercase;letter-spacing:.06em;
  font-weight:500;font-family:ui-sans-serif,sans-serif}
.wid{color:#7d90a6;font-size:9.5px;margin-top:3px;letter-spacing:-.02em}
.rejnote{color:#fbbf24;font-size:9.5px;margin-top:3px}
.goodc{color:#5eead4}
.slotbar{padding:20px 28px 4px;color:#9fb2c8;font-size:13px}
.slotbar b{color:#5eead4;font-family:ui-monospace,Menlo,monospace}
.rndwrap{margin:0 28px 8px}
.rnd{display:flex;cursor:pointer;gap:14px;align-items:center;background:#111823;
  border:1px solid #1e2937;border-radius:9px;padding:10px 15px;text-decoration:none}

.rnd:hover{border-color:#2f4256}
.rnd.open{border-color:#14b8a6;background:#0f1a22;border-bottom-left-radius:0;
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
.rwd{font-size:11px;border:1px solid #22303f;border-radius:5px;padding:1px 8px;
  color:#9fb2c8;font-family:ui-monospace,Menlo,monospace;cursor:help}
.rwd b{color:#cfe0f0;font-weight:600}
.rwd.ahead{border-color:#14b8a655;background:#0f766e22}
.rwd.ahead .pct{color:#5eead4}
.rwd.behind{border-color:#78350f55;background:#78350f18}
.rwd.behind .pct{color:#fbbf24}
.apl{font-size:11px;border:1px solid #22303f;border-radius:5px;padding:1px 8px;
  color:#9fb2c8;font-family:ui-monospace,Menlo,monospace;cursor:help}
.apl b{color:#cfe0f0;font-weight:600}
.apl.haddrop{border-color:#78350f55;background:#78350f18}
.dropn{color:#fbbf24}
details.errdrop{margin:0 28px 8px;background:#2a0f0f;border:1px solid #7f1d1d;
  border-radius:0 0 9px 9px;border-top:none;padding:6px 15px;font-size:11.5px}
details.errdrop summary{cursor:pointer;color:#f87171;font-weight:600;
  list-style:none}
details.errdrop summary::-webkit-details-marker{display:none}
details.errdrop summary::before{content:"\25B8 ";color:#f87171}
details.errdrop[open] summary::before{content:"\25BE "}
details.errdrop ul{margin:7px 0 0;padding-left:18px;color:#cfe0f0;
  font-family:ui-monospace,Menlo,monospace;font-size:11px}
details.errdrop li{margin:2px 0}
.rpl{font-size:11px;border:1px solid #78350f55;background:#78350f18;
  border-radius:5px;padding:1px 8px;color:#fbbf24;cursor:help;
  font-family:ui-monospace,Menlo,monospace}
.rpl b{color:#fdba74;font-weight:600}
.rwd.ours{border-color:#14b8a655;background:#0f766e22;color:#5eead4}
.rwd.ours b{color:#5eead4}
.votes{color:#93c5fd;font-size:11px;border:1px solid #1d4ed855;border-radius:5px;
  padding:1px 7px}
.chev{margin-left:auto;color:#5b6b80;font-family:ui-monospace,Menlo,monospace}
.dtlhead .note{text-transform:none;letter-spacing:0;color:#4d5c70;font-size:10px;
  margin-left:10px}
.warnpill{margin-left:8px;color:#fbbf24;font-size:9.5px;letter-spacing:.04em;
  border:1px solid #78350f;background:#78350f22;border-radius:4px;padding:1px 6px}
.detail .bad{color:#fbbf24}

tr.bstage td{background:#0d1620}
tr.bstage td:first-child{color:#8fa3ba}
#view.loading{opacity:.45;transition:opacity .12s ease-in 80ms}
/* /analysis */
.aform{display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end;
  padding:14px 28px;border-bottom:1px solid #1e2937}
.aform label{display:flex;flex-direction:column;gap:4px;color:#61748b;
  font-size:10.5px;text-transform:uppercase;letter-spacing:.06em}
.aform input,.aform select{background:#0c141b;border:1px solid #22303f;
  border-radius:5px;color:#cfe0f0;padding:5px 8px;font-size:12px;
  font-family:ui-monospace,Menlo,monospace;min-width:170px}
.aform input.thin{min-width:64px}
.aform input:focus,.aform select:focus{outline:none;border-color:#2c6b86}
.aform button{background:#0f766e;border:1px solid #14b8a6;color:#eafffb;
  border-radius:5px;padding:6px 18px;font-size:12px;cursor:pointer}
.aform button:hover{background:#14b8a6}
.awrap{padding:0 28px 40px}
.ahead{display:flex;flex-wrap:wrap;gap:16px;align-items:baseline;
  padding:14px 0 4px;font-size:12px;color:#cfe0f0}
.ahead code{font-family:ui-monospace,Menlo,monospace;font-size:11.5px}
.asec{margin:22px 0 0;border-top:1px solid #1e2937;padding-top:14px}
.asec h2{font-size:13px;font-weight:600;color:#cfe0f0;margin:0 0 10px}
.asum{color:#8fa3ba;font-size:12px;line-height:1.7;margin:0 0 10px}
.asum b{color:#dbe7f3}
.asubh{color:#61748b;font-size:10.5px;text-transform:uppercase;
  letter-spacing:.06em;margin:16px 0 6px}
.atab{width:100%;border-collapse:collapse;font-size:11.5px;margin-bottom:4px}
.atab th{color:#61748b;font-weight:500;text-align:left;padding:4px 9px;
  border-bottom:1px solid #1e2937;white-space:nowrap}
.atab td{padding:3px 9px;border-bottom:1px solid #141d27;color:#a9bed4}
.atab th.n,.atab td.n{text-align:right}
.atab .m{font-family:ui-monospace,Menlo,monospace}
.atab .dim{color:#4d5c70}
.atab .abad{color:#f87171}
.atab .awarn{color:#fbbf24}
.amax{white-space:nowrap}
.atwhere{margin-left:10px;padding-left:10px;border-left:1px solid #22303f}
.alink{color:#7fd8f0;text-decoration:none;border-bottom:1px dotted #2c6b86}
.alink:hover{color:#a9e8f7;border-bottom-style:solid}
.anote{color:#61748b;font-size:11px;line-height:1.6;margin:8px 0 0;
  max-width:1100px}
.anote b{color:#8fa3ba}
.anote code{font-family:ui-monospace,Menlo,monospace;color:#8fa3ba}
.anote .warn{color:#fbbf24}
details.aslot{border:1px solid #1a2430;border-radius:5px;margin:5px 0;
  background:#0c141b}
details.aslot>summary{cursor:pointer;padding:7px 11px;display:flex;
  flex-wrap:wrap;gap:12px;align-items:baseline;font-size:12px;color:#a9bed4}
details.aslot>summary:hover{background:#111c26}
details.aslot[open]>summary{border-bottom:1px solid #1a2430}
.aslotn{font-family:ui-monospace,Menlo,monospace;color:#cfe0f0;font-weight:600}
details.around{border-top:1px solid #141d27;margin:0 0 0 16px}
details.around>summary{cursor:pointer;padding:5px 11px;display:flex;
  flex-wrap:wrap;gap:11px;align-items:baseline;font-size:11.5px;color:#8fa3ba}
details.around>summary:hover{background:#111c26}
.arnd{font-family:ui-monospace,Menlo,monospace;color:#a9bed4}
.amiss{color:#e3c07a;font-family:ui-monospace,Menlo,monospace}
.agoto{margin-left:auto}
.tailpill{border:1px solid #2c6b86;border-radius:9px;padding:0 7px;
  font-size:9.5px;color:#7fd8f0;text-transform:uppercase;letter-spacing:.05em}
.alist{padding:7px 11px 10px 27px;border-top:1px solid #101820}
.alist b{display:block;color:#8fa3ba;font-size:10.5px;font-weight:500;
  text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px}
.alist code{display:inline-block;font-family:ui-monospace,Menlo,monospace;
  font-size:10.5px;color:#7e93aa;background:#0a1118;border:1px solid #16212c;
  border-radius:3px;padding:1px 5px;margin:0 4px 4px 0;word-break:break-all}
.alist code.bid{color:#8fa3ba;border-color:#1e3245}
.alist .dim{color:#4d5c70;font-size:10.5px}
/* reward chart */
.chartlegend{display:flex;flex-wrap:wrap;gap:18px;margin:0 0 8px;color:#8fa3ba;
  font-size:11px}
.chartlegend span{display:flex;align-items:center;gap:6px}
.chartlegend i{width:11px;height:3px;border-radius:2px;display:inline-block}
.chartlegend i.nooffer{width:1px;height:9px;border-radius:0;background:#4d5c70}
.chartwrap{background:#0c141b;border:1px solid #1a2430;border-radius:5px;
  padding:4px 0}
#rwchart{width:100%;display:block}
.cgrid{stroke:#16212c;stroke-width:1}
.cgridv{stroke:#111b24;stroke-width:1}
.caxis{stroke:#22303f;stroke-width:1}
.ctick{fill:#5b6b80;font-size:9.5px;font-family:ui-monospace,Menlo,monospace}
.cband{fill:#1e2a36;opacity:.55}
.cline{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.cnone{stroke:#4d5c70;stroke-width:1}
.clabel{font-size:10.5px;font-weight:600}
.chair{stroke:#7fd8f0;stroke-width:1;opacity:.7}
.ctip{position:fixed;z-index:60;background:#0f1a24;border:1px solid #2b3a4b;
  border-radius:5px;padding:7px 10px;font-size:11px;color:#cfe0f0;
  pointer-events:none;box-shadow:0 6px 20px #0008;line-height:1.6}
.ctip b{display:block;font-family:ui-monospace,Menlo,monospace;margin-bottom:3px}
.ctip span{display:flex;align-items:center;gap:6px;
  font-family:ui-monospace,Menlo,monospace}
.ctip i{width:9px;height:3px;border-radius:2px;display:inline-block}
.ctip .g{color:#8fa3ba;margin-top:2px}
/* dispatch DAG */
.dagbar{display:flex;align-items:center;flex-wrap:wrap;gap:5px;
  padding:8px 0 10px;border-bottom:1px solid #141d27;margin-bottom:10px}
.dlab{color:#61748b;font-size:10.5px;text-transform:uppercase;
  letter-spacing:.06em;margin:0 4px 0 10px}
.dlab:first-child{margin-left:0}
.dchip{display:inline-block;padding:2px 9px;border:1px solid #223142;
  border-radius:11px;color:#8fa3ba;font-size:11px;text-decoration:none}
.dchip:hover{border-color:#2f4459;color:#cfe0f0}
.dchip.on{background:#16303f;border-color:#2c6b86;color:#7fd8f0}
.dagsum{width:100%;border-collapse:collapse;margin:2px 0 12px}
.dagsum td{border:none;border-left:2px solid #1c2836;padding:1px 0 1px 10px;
  vertical-align:top}
.dagsum td:first-child{border-left:none;padding-left:0}
.dagsum b{display:block;font-size:16px;color:#dbe7f3;font-weight:600;
  font-family:ui-monospace,Menlo,monospace}
.dagsum span{display:block;color:#61748b;font-size:10.5px;margin-top:1px}
.dagsum .ok{display:inline;font-size:10.5px;color:#5eead4;font-family:inherit}
.dagsum .off{color:#fbbf24}
.dagread{background:#0d1a16;border:1px solid #1b3b32;border-radius:4px;
  color:#8fc4b4;font-size:11.5px;padding:6px 10px;margin:0 0 9px;line-height:1.6}
.dagread b{color:#bfe6d8}
.dagread code{font-family:ui-monospace,Menlo,monospace;color:#a9d8c6}
.dagread .warn{color:#fbbf24}
.dagnote{background:#0e1721;border:1px solid #1d2b3a;border-radius:4px;
  color:#8fa3ba;font-size:11.5px;padding:6px 10px;margin:0 0 9px}
.dagnote code{font-family:ui-monospace,Menlo,monospace;color:#a9bed4}
.dagwarn{background:#22190c;border:1px solid #4a3714;border-radius:4px;
  color:#e3c07a;font-size:11.5px;padding:6px 10px;margin:0 0 9px}
.dagwarn code{font-family:ui-monospace,Menlo,monospace;color:#f0d49a}
.dagctl{display:flex;align-items:center;gap:7px;margin:0 0 6px}
.dagctl button{background:#131e29;border:1px solid #223142;color:#9db2c8;
  border-radius:4px;font-size:11px;padding:2px 8px;cursor:pointer;
  line-height:1.5}
.dagctl button:hover{border-color:#2f4459;color:#dbe7f3}
.dagctl .spd{padding:2px 6px;font-size:10px}
.dagctl .spd.on{background:#16303f;border-color:#2c6b86;color:#7fd8f0}
.dagctl input[type=range]{flex:1;accent-color:#2c6b86;height:3px}
.dagt{color:#8fa3ba;font-size:11px;font-family:ui-monospace,Menlo,monospace;
  min-width:96px;text-align:right}
.daghint{color:#4d5c70;margin-left:5px}
.dagdep{color:#61748b;font-size:10.5px;display:flex;align-items:center;gap:4px;
  cursor:pointer;white-space:nowrap}
.dagwrap{background:#080e14;border:1px solid #16212c;border-radius:4px;
  overflow-x:auto;padding:2px 0}
#dagsvg{width:100%;display:block}
.dlane{stroke:#141f2a;stroke-width:1}
.dlanelbl{fill:#4d5c70;font-size:9px;font-family:ui-monospace,Menlo,monospace}
.dtick{fill:#4d5c70;font-size:9px;font-family:ui-monospace,Menlo,monospace}
.dedge{fill:none;stroke:#31445a;stroke-width:.5;opacity:.5}
.dedge.dcrit{stroke:#b6822a;opacity:.85;stroke-width:.9}
.dplay{stroke:#7fd8f0;stroke-width:1;opacity:.8}
svg .dn{stroke-width:1}
svg .dn.pending{fill:#1d2c3b;stroke:#2b4055}
svg .dn.running{fill:#7fd8f0;stroke:#a9e8f7}
svg .dn.included{fill:#8fa3ba;stroke:#b3c4d6}
svg .dn.excluded{fill:#6b4a2c;stroke:#8d6338}
svg .dn.unknown{fill:#243244;stroke:#35485e}
svg rect.dn{fill:#3b82c4;stroke:#5ea3e0}
svg rect.dn.pending{fill:#1e3245;stroke:#2d4a66}
svg .dn.crit{stroke:#facc15;stroke-width:1.6}
.daglegend{display:flex;flex-wrap:wrap;gap:14px;padding:8px 2px 0;
  color:#61748b;font-size:10.5px}
.daglegend span{display:flex;align-items:center;gap:5px}
.daglegend i{width:8px;height:8px;border-radius:50%;display:inline-block;
  border:1px solid}
.daglegend i.pending{background:#1d2c3b;border-color:#2b4055}
.daglegend i.running{background:#7fd8f0;border-color:#a9e8f7}
.daglegend i.included{background:#8fa3ba;border-color:#b3c4d6}
.daglegend i.excluded{background:#6b4a2c;border-color:#8d6338}
.daglegend i.unknown{background:#243244;border-color:#35485e}
.daglegend i.bundlei{background:#3b82c4;border-color:#5ea3e0;border-radius:1px}
.daglegend i.criti{background:transparent;border-color:#facc15;border-width:2px}
.dagx{margin:14px 0 0;padding-top:11px;border-top:1px solid #141d27}
.dagxh{color:#61748b;font-size:10.5px;text-transform:uppercase;
  letter-spacing:.06em;margin-bottom:6px}
.dagx table{width:100%;border-collapse:collapse;font-size:11.5px}
.dagx th{color:#61748b;font-weight:500;text-align:left;padding:3px 9px;
  border-bottom:1px solid #1e2937}
.dagx td{padding:3px 9px;border-bottom:1px solid #141d27;color:#a9bed4}
.dagx th.n,.dagx td.n{text-align:right}
.dagx tr.xbad td{background:#2a1a0d;color:#e3c07a}
.dagx .dim{color:#4d5c70}
.dagxn{color:#61748b;font-size:11px;line-height:1.6;margin-top:8px}
.dagbasis{color:#61748b;font-size:11px;line-height:1.6;margin:12px 0 0;
  padding-top:9px;border-top:1px solid #141d27}
.dagbasis b{color:#8fa3ba;font-weight:600}
.dagbasis code{font-family:ui-monospace,Menlo,monospace;color:#8fa3ba}
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
tr.shredgrp td{background:#111a24;color:#9fb2c8;font-size:11px;padding:7px 9px;
  border-top:1px solid #1e2937}
tr.shredgrp b{color:#5eead4;font-family:ui-monospace,Menlo,monospace}
tr.shredgrp .basis{display:inline;margin-left:8px}
.dtlhead .basis{display:inline-block;margin-left:10px;margin-top:0}
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
  var nodes = [];
  // Re-scanned rather than captured once: swapping the panel replaces these
  // nodes, and a captured list would keep ticking elements no longer on screen.
  function rescan(){ nodes = [].slice.call(document.querySelectorAll('.age[data-t]')); }
  window.__simbenchAges = function(){ rescan(); tick(); };
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
    if (!nodes.length) rescan();
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
  rescan();
  tick();
  setInterval(function(){ rescan(); tick(); }, 1000);
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

// Round rows expand in place. Everything is already in the DOM, so opening a
// round is a class change rather than a navigation; the URL is kept in step
// with replaceState so a link still deep-links, without costing a reload.
(function(){
  function setOpen(row, open){
    var d = document.querySelector('.detail[data-round="' + row.dataset.round + '"]');
    row.classList.toggle('open', open);
    row.setAttribute('aria-expanded', open ? 'true' : 'false');
    var chev = row.querySelector('.chev');
    if (chev) chev.innerHTML = open ? '&minus;' : '+';
    if (d) d.hidden = !open;
  }
  function toggle(row){
    var open = !row.classList.contains('open');
    document.querySelectorAll('.rnd.open').forEach(function(o){
      if (o !== row) setOpen(o, false);
    });
    setOpen(row, open);
    try {
      var u = new URL(window.location.href);
      if (open) u.searchParams.set('round', row.dataset.round);
      else u.searchParams.delete('round');
      history.replaceState(null, '', u);
    } catch (e) { /* deep-linking is a nicety, not a requirement */ }
  }
  document.addEventListener('click', function(ev){
    if (ev.target.closest('.cp') || ev.target.closest('.info')) return;
    var row = ev.target.closest('.rnd');
    if (row) toggle(row);
  });
  document.addEventListener('keydown', function(ev){
    if (ev.key !== 'Enter' && ev.key !== ' ') return;
    var row = ev.target.closest && ev.target.closest('.rnd');
    if (row) { ev.preventDefault(); toggle(row); }
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


# Within one leader window the header, the 30-day strip and the window bar are
# identical for all four slots and all five tabs -- only the panel below them
# changes. So a click there re-fetches just that panel and swaps it in, and the
# other nineteen combinations are pulled in the background afterwards. Once the
# window is warm, moving between its slots and tabs costs nothing.
#
# The links stay real hrefs and the handler still serves the whole page, so
# with JavaScript off, or if a fetch fails, navigation simply falls back to
# what it always did.
NAV_JS = r"""
(function(){
  var view = document.getElementById('view');
  if (!view || !window.fetch || !window.history || !history.pushState) return;

  var cache = new Map();          // url -> html of #view
  var inflight = new Map();       // url -> promise, so a click never duplicates
                                  // a prefetch already in the air
  var current = location.pathname + location.search;
  cache.set(key(current), view.innerHTML);

  function key(u){
    // `round` only opens a row, which is done in the DOM, so two URLs that
    // differ by it share one rendering.
    try {
      var url = new URL(u, location.origin);
      url.searchParams.delete('round');
      return url.pathname + '?' + url.searchParams.toString();
    } catch (e) { return u; }
  }
  function sameWindow(u){
    try {
      var a = new URL(u, location.origin), b = new URL(location.href);
      return a.pathname === b.pathname &&
             a.searchParams.get('win') === b.searchParams.get('win') &&
             a.searchParams.get('win') !== null;
    } catch (e) { return false; }
  }
  function fetchView(u){
    var k = key(u);
    if (cache.has(k)) return Promise.resolve(cache.get(k));
    if (inflight.has(k)) return inflight.get(k);
    var sep = u.indexOf('?') === -1 ? '?' : '&';
    var p = fetch(u + sep + 'partial=1', {headers: {'X-Partial': '1'}})
      .then(function(r){
        if (!r.ok) throw new Error('http ' + r.status);
        return r.text();
      })
      .then(function(html){ cache.set(k, html); inflight.delete(k); return html; })
      .catch(function(err){ inflight.delete(k); throw err; });
    inflight.set(k, p);
    return p;
  }

  // innerHTML never runs a <script>, and the DAG panel ships one. Re-create
  // them so the panel wires itself up exactly as it does on a full load.
  function runScripts(root){
    var list = [].slice.call(root.querySelectorAll('script'));
    for (var i = 0; i < list.length; i++){
      var old = list[i];
      if (old.type && old.type !== 'text/javascript') continue;   // JSON payloads
      var s = document.createElement('script');
      if (old.src) s.src = old.src; else s.textContent = old.textContent;
      old.parentNode.replaceChild(s, old);
    }
  }

  function markSelection(u){
    var slot = new URL(u, location.origin).searchParams.get('slot');
    document.querySelectorAll('.winbar ~ .strip .chip.sm[data-slot]')
      .forEach(function(c){
        c.classList.toggle('on', c.dataset.slot === slot);
      });
  }

  function show(u, push){
    return fetchView(u).then(function(html){
      view.innerHTML = html;
      runScripts(view);
      markSelection(u);
      if (window.__simbenchAges) window.__simbenchAges();
      var slot = new URL(u, location.origin).searchParams.get('slot');
      document.title = 'simbench' + (slot ? ' · slot ' + slot : '');
      if (push) history.pushState({v: 1}, '', u);
      current = u;
      view.classList.remove('loading');
    }).catch(function(){
      // one failure and we stop pretending: hand the click back to the browser
      window.location.href = u;
    });
  }

  document.addEventListener('click', function(ev){
    if (ev.defaultPrevented || ev.button || ev.metaKey || ev.ctrlKey ||
        ev.shiftKey || ev.altKey) return;
    if (ev.target.closest('.cp')) return;               // copy buttons
    var a = ev.target.closest('a');
    if (!a || a.target || a.hasAttribute('download')) return;
    var href = a.getAttribute('href');
    if (!href || href.charAt(0) !== '/') return;
    var isSlot = !a.closest('.tabs') && !!a.closest('.winbar ~ .strip');
    if (!isSlot && !a.closest('.tabs')) return;
    if (!sameWindow(href)) return;
    // A slot chip is rendered without a tab, so following it verbatim would
    // drop whoever is on the dag tab back to rounds every time they step to
    // the next slot. Carry the current tab across instead: the point of this
    // is moving freely in both directions.
    if (isSlot) {
      try {
        var t = new URL(href, location.origin), now = new URL(location.href);
        // The hrefs were rendered when some other slot was selected, and the
        // selected chip's own href deliberately drops `slot`. After a swap
        // that stale href would navigate to "no slot selected", so the chip's
        // identity comes from data-slot, which is always right.
        if (a.dataset.slot) t.searchParams.set('slot', a.dataset.slot);
        if (!t.searchParams.get('tab') && now.searchParams.get('tab')) {
          t.searchParams.set('tab', now.searchParams.get('tab'));
          if (now.searchParams.get('src'))
            t.searchParams.set('src', now.searchParams.get('src'));
        }
        href = t.pathname + '?' + t.searchParams.toString();
      } catch (e) { /* fall through with the literal href */ }
    }
    ev.preventDefault();
    if (key(href) === key(current)) return;
    if (!cache.has(key(href))) view.classList.add('loading');
    show(href, true);
  });

  window.addEventListener('popstate', function(){
    show(location.pathname + location.search, false);
  });

  // Warm the rest of the window: every tab of every slot in it. Two at a time
  // and only once the page is idle, so the view the user is actually looking
  // at never waits behind a prefetch.
  var TABS = ['', 'compare', 'timeline', 'dag', 'logs'];
  function prefetchAll(){
    var here = new URL(location.href);
    var win = here.searchParams.get('win');
    if (!win) return;
    var slots = [].slice.call(
      document.querySelectorAll('.winbar ~ .strip .chip.sm[data-slot]'))
      .map(function(c){ return c.dataset.slot; });
    var queue = [];
    slots.forEach(function(s){
      TABS.forEach(function(t){
        queue.push('/?win=' + win + '&slot=' + s + (t ? '&tab=' + t : ''));
      });
    });
    queue = queue.filter(function(u){ return !cache.has(key(u)); });
    var active = 0;
    function pump(){
      while (active < 2 && queue.length){
        active++;
        fetchView(queue.shift())
          .catch(function(){})
          .then(function(){ active--; pump(); });
      }
    }
    pump();
  }
  if ('requestIdleCallback' in window) requestIdleCallback(prefetchAll, {timeout: 2500});
  else setTimeout(prefetchAll, 1200);
})();
"""


def rounds_tag(entry):
    """"won" is per ROUND, so say how many.

    A slot runs several auction rounds and each is won or lost on its own, so
    "we produced this slot" collapsed a slot where we took one round of eight
    into the same label as one where we took all eight -- and measured over
    seven days both really happen. The denominator is the rounds the relay
    echoed a winner for, which is what there was to win."""
    won, total = entry.get("won_rounds", 0), entry.get("rounds", 0)
    if not total:
        # we competed, but the relay never echoed a winner for any round
        return '<span class="dimc">no rounds echoed</span>'
    if not won:
        return '<span class="lostc">0/%d rounds</span>' % total
    cls = "allc" if won == total else "okc"
    return '<span class="%s">%d/%d rounds</span>' % (cls, won, total)


def strip_html(windows, sel_win, sel_slot):
    """One chip per leader window, newest at the left."""
    # The strip is newest-first, so a chip whose run differs from the chip to
    # its LEFT is where that run began. Marking the boundary makes deploys
    # navigable: 21 runs across ~700 windows is otherwise invisible scrolling.
    seen_runs = []
    for i, w in enumerate(windows):
        newer = windows[i - 1]["run_id"] if i else None
        w["run_starts_here"] = bool(w["run_id"]) and w["run_id"] != newer
        if w["run_starts_here"]:
            seen_runs.append(w)

    def chip(w):
        on = w["win"] == sel_win
        href = "/" if on else f"/?win={w['win']}"
        cls = ("chip" + (" on" if on else "") + (" won" if w["won"] else "")
               + (" runstart" if w.get("run_starts_here") else ""))
        return (f'<a class="{cls}" href="{href}" '
                f'title="{html.escape(w["ts"][:19])} UTC &mdash; leader '
                f'{html.escape(w["leader"])} &mdash; '
                f'{w["won_rounds"]}/{w["rounds"]} rounds won across '
                f'{w["won"]}/{len(w["slots"])} slots &mdash; run '
                f'{html.escape(w["run_id"] or "?")}'
                + (f' started {html.escape((w.get("run_started") or "")[:19])} '
                   f'UTC, {w.get("run_slots", 0)} slots'
                   if w.get("run_started") else "")
                + (f', this window {int(w["run_age"] // 60)}m in'
                   if w.get("run_age") is not None else "")
                + '">'
                f'<div class="slot">{w["win"]}<span class="n">'
                f'&ndash;{str(w["win"] + WINDOW - 1)[-2:]}</span>{copy_btn(w["win"])}</div>'
                f'<div class="ageline">{age_html(w["ts"])}</div>'
                f'<div class="meta">{html.escape(short_id(w["leader"]))}'
                f'{" &#9888;" if w["leader_split"] else ""}</div>'
                + (f'<div class="runmark" id="run-{html.escape(short_run(w["run_id"]))}">'
                   f'run starts here</div>' if w.get("run_starts_here") else "")
                + f'<div class="runline">run {html.escape(short_run(w["run_id"]))}'
                + (' <span class="warn">&#9888;</span>' if w["run_split"] else "")
                + (f'<span class="coldpill" tabindex="0" data-tip="{cold_why(w)}"'
                   f">cold</span>" if w.get("run_cold") else "")
                + "</div>"
                + f'<div class="tag">{rounds_tag(w)}</div></a>')

    chips = "".join(chip(w) for w in windows)
    slots = sum(len(w["slots"]) for w in windows)
    won = sum(w["won"] for w in windows)
    # Rounds first, slots second: a bare "27 won" was slots, which reads
    # like 27 whole blocks when most of those slots were one round of eight.
    won_rounds = sum(w.get("won_rounds", 0) for w in windows)
    rounds = sum(w.get("rounds", 0) for w in windows)
    leaders = len({w["leader"] for w in windows if w["leader"]})
    return (f'<div class="striphead"><span class="cap">leader windows</span>'
            f'<span class="sub">{len(windows)} windows &middot; {slots} slots contested '
            f'&middot; <b class="okc">{won_rounds:,}/{rounds:,} rounds won</b>'
            f'<span class="dimc"> across {won} slots</span> &middot; {leaders} leaders '
            f'&middot; last 30d on <code>{CH_BUILDER}</code></span>'
            f'<span class="hint">newest at the left &mdash; scroll right for older</span>'
            f'</div>{runbar(seen_runs)}<div class="strip">{chips}</div>'
            + window_html(windows, sel_win, sel_slot))


def runbar(run_starts):
    """One jump target per builder run, newest first.

    run_id changes on every restart, so these are the deploy boundaries. The
    strip is long -- hundreds of windows -- and a run change is exactly the
    thing you want to land on when a metric shifts."""
    if len(run_starts) < 2:
        return ""
    items = "".join(
        f'<a class="runjump{" cold" if w.get("run_cold") else ""}" '
        f'href="#run-{html.escape(short_run(w["run_id"]))}" '
        f'title="{html.escape(w["run_id"])} &mdash; first window {w["win"]}, '
        f'{w.get("run_slots", 0)} slots">'
        f'{html.escape(short_run(w["run_id"]))}'
        f'<span class="rj">{ago(w["ts"])}</span></a>'
        for w in run_starts)
    return ('<div class="runbar"><span class="cap">builder runs</span>'
            f'<span class="sub">{len(run_starts)} in the strip &middot; newest '
            "first &middot; jump to where each began</span>"
            f'<div class="runjumps">{items}</div></div>')


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
        return (f'<a class="{cls}" href="{href}" data-slot="{s["slot"]}">'
                f'<div class="slot">{s["slot"]}{copy_btn(s["slot"])}</div>'
                f'<div class="ageline">+{s["slot"] - sel_win} &middot; '
                f'{age_html(s["ts"])}</div>'
                + f'<div class="tag">{rounds_tag(s)} '
                f'&middot; {s["offers"]} offers</div></a>')

    return (f'<div class="winbar"><span class="cap hascp">window {sel_win}'
            f'{copy_btn(sel_win)}</span>'
            f'<span class="sub">slots {sel_win}&ndash;{sel_win + WINDOW - 1} &middot; '
            f'{len(win["slots"])} contested &middot; '
            f'<b class="okc">{win["won_rounds"]}/{win["rounds"]} rounds won</b>'
            f'<span class="dimc"> across {win["won"]}/{len(win["slots"])} slots</span>'
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

def page(sel_win=None, sel_slot=None, sel_round=None, tab="rounds",
         src="ours", partial=False):
    # A partial serves only the panel, so the 30-day strip -- the expensive
    # half of this page -- is not built at all.
    strip, windows = "", []
    if partial:
        if sel_slot is not None and sel_win is None:
            sel_win = window_of(sel_slot)
    else:
        try:
            windows = produced_windows()
        except Exception as exc:
            strip = (f'<div class="err">produced-window list unavailable: '
                     f"{html.escape(str(exc))[:160]}</div>")
            windows = []
        else:
            if not windows:
                strip = ('<div class="err">no produced slots in the last 30 '
                         "days</div>")
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
                               ("timeline", "timeline"), ("dag", "dag"),
                               ("logs", "logs")))
        if tab == "dag":
            try:
                d = slot_data(sel_slot)
                pick = sel_round if sel_round is not None else (
                    d["rounds"][0]["round"] if d["rounds"] else 0)
                inner = dag_html(sel_slot, pick, src, d)
            except Exception as exc:
                inner = ('<div class="err">dispatch DAG unavailable: '
                         f"{html.escape(str(exc))[:160]}</div>")
            blurb = ("the conflict graph the simulator executes, and how it "
                     "spreads across the worker lanes.")
        elif tab == "logs":
            try:
                d = slot_data(sel_slot)
                inner = logs_html(sel_slot, d.get("logs"))
            except Exception as exc:
                inner = ('<div class="err">logs unavailable: '
                         f"{html.escape(str(exc))[:160]}</div>")
            blurb = "warnings and errors the builder logged during this slot."
        elif tab == "timeline":
            try:
                d = slot_data(sel_slot)
                inner = timeline_html(sel_slot, d["extends"], d["commits"],
                                      d.get("shreds"), d["rounds"],
                                      d.get("builder"))
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
                                     d.get("relay_err"), compare_extras(sel_slot),
                                     d.get("shreds"))
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
    # Inside a window the header, the strip and the window bar are the same
    # for every slot and every tab, so only this much travels on a click.
    if partial:
        return body
    title = f" &middot; slot {sel_slot}" if sel_slot else \
            f" &middot; window {sel_win}" if sel_win else ""
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">block-builder slot explorer</div>
  <form class="jump" method="get" action="/">
    <input name="slot" inputmode="numeric" pattern="[0-9]*" placeholder="go to slot"
           aria-label="go to slot" />
  </form>
  <a class="navlink" href="/analysis">range analysis</a>
  <a class="navlink" href="/reference">metrics reference &mdash; what is bad?</a>
</header>
{health_html()}
{strip}
<div id="view">{body}</div>
<script>{TICK_JS.replace("__SERVER_NOW__", f"{dt.datetime.now(dt.UTC).timestamp():.3f}")}</script>
<script>{NAV_JS}</script>
</body></html>"""


# ============================================================== /analysis
#
# A port of bcj_report.py, which was a standalone script wanting its own
# credentials. The four reports are unchanged in substance; what differs is
# that the range is resolved to concrete timestamps up front and both stores
# are bounded by it, rather than the script's habit of bounding InfluxDB by
# slot alone and letting it walk the retention.

ANALYSIS_IDENTITY = os.environ.get("ANALYSIS_IDENTITY", "")
ANALYSIS_MAX_DAYS = 7
PCTS = (50, 90, 95, 99)
# One round's missing-order list can run to hundreds of signatures and a day's
# worth to five figures, which is megabytes of HTML nobody reads. Each round
# renders this many and says how many it withheld.
SIG_CAP = 100

_analysis_cache = {}
_analysis_lock = threading.Lock()


def pct(values, p):
    """Linear interpolation between order statistics, as numpy does it."""
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    k = (len(ordered) - 1) * p / 100.0
    lo = int(k)
    if lo >= len(ordered) - 1:
        return float(ordered[-1])
    return ordered[lo] + (ordered[lo + 1] - ordered[lo]) * (k - lo)


def resolve_range(start, end):
    """-24h / -30m / -7d, or an absolute date or datetime, to real timestamps.

    Resolved here rather than handed to ClickHouse as `now() - INTERVAL ...`
    because the same window has to bound InfluxDB, and the two stores must be
    asked about the same wall-clock span for the cross-store joins below to
    mean anything."""
    # Quantised to the minute. An open-ended range means "up to now", and an
    # unrounded now() moves between one request and the next, so the cache key
    # never repeated and every reload paid the full ten seconds again.
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, second=0, microsecond=0)
    rel = re.fullmatch(r"-(\d+)([hdm])", (start or "").strip())
    if rel:
        n, unit = int(rel.group(1)), rel.group(2)
        lo = now - dt.timedelta(**{{"h": "hours", "d": "days",
                                    "m": "minutes"}[unit]: n})
    else:
        text = (start or "").strip()
        if len(text) == 10:
            text += " 00:00:00"
        try:
            lo = dt.datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"could not read a start time from {start!r}")
    if (end or "").strip():
        text = end.strip()
        if len(text) == 10:
            text += " 00:00:00"
        try:
            hi = dt.datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"could not read an end time from {end!r}")
    else:
        hi = now
    if hi <= lo:
        raise ValueError("the end of the range is not after its start")
    if hi - lo > dt.timedelta(days=ANALYSIS_MAX_DAYS):
        raise ValueError(f"range is {(hi - lo).days} days; the cap is "
                         f"{ANALYSIS_MAX_DAYS}. Every query here is bounded by "
                         "it, so a wide range is a slow page, not a rich one.")
    return lo, hi


def _ch_ts(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def _ix_ts(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def analysis_connectors(lo, hi):
    _, rows = clickhouse(
        "SELECT connector_identity, uniqExact(slot) AS slots "
        f"FROM bifrost_miniblocks WHERE ts >= '{_ch_ts(lo)}' AND ts <= '{_ch_ts(hi)}' "
        f"AND instance_id = '{CH_INSTANCE}' AND connector_identity != '' "
        "GROUP BY connector_identity ORDER BY slots DESC")
    return [(r[0], ch_int(r[1])) for r in rows]


def analysis_slots(lo, hi, identity):
    _, rows = clickhouse(
        f"SELECT DISTINCT slot FROM bifrost_miniblocks "
        f"WHERE ts >= '{_ch_ts(lo)}' AND ts <= '{_ch_ts(hi)}' "
        f"AND connector_identity = '{identity}' AND instance_id = '{CH_INSTANCE}' "
        "ORDER BY slot")
    return [int(r[0]) for r in rows]


def analysis_missing(slots):
    """Winner refs that we never offered in that round, at any rung.

    The union over the whole ladder, not just the best rung: the question is
    what we never had at all, and a later rung can drop an order an earlier one
    carried."""
    slot_list = ",".join(str(s) for s in slots)
    winners, bad = {}, []
    _, rows = clickhouse(
        "SELECT slot, index_in_slot, is_last, order_count, hex(payload) "
        f"FROM bifrost_miniblocks WHERE slot IN ({slot_list}) AND kind = 'winner' "
        f"AND instance_id = '{CH_INSTANCE}'")
    for slot, idx, is_last, order_count, hexpay in rows:
        try:
            refs = _decode_refs(bytes.fromhex(hexpay))
        except Exception as exc:
            bad.append(f"slot {slot} round {idx}: {exc}")
            continue
        # order_count is written as chosen.order_refs.len(), so it is an
        # independent check on the decode rather than a restatement of it.
        if len(refs) != ch_int(order_count):
            bad.append(f"slot {slot} round {idx}: decoded {len(refs)} refs, "
                       f"the column says {order_count}")
            continue
        winners[(int(slot), int(idx))] = {
            "txs": {b58encode(r) for k, r in refs if k == "tx"},
            "bundles": {r.hex() for k, r in refs if k == "bundle"},
            "is_tail": is_last in TRUEISH, "refs": len(refs)}

    ours_tx, ours_bundle = {}, {}
    _, rows = clickhouse(
        "SELECT slot, index_in_slot, arrayStringConcat(sig_prefixes, ','), "
        "       arrayStringConcat(bundle_ids, ',') "
        f"FROM bifrost_miniblocks WHERE slot IN ({slot_list}) AND kind = 'selected' "
        f"AND instance_id = '{CH_INSTANCE}'")
    for slot, idx, pfx, bids in rows:
        key = (int(slot), int(idx))
        ours_tx.setdefault(key, set()).update(p for p in pfx.split(",") if p)
        ours_bundle.setdefault(key, set()).update(b for b in bids.split(",") if b)

    missing, every_sig = {}, set()
    for key, w in winners.items():
        gap_tx = sorted(w["txs"] - ours_tx.get(key, set()))
        gap_bundle = sorted(w["bundles"] - ours_bundle.get(key, set()))
        if gap_tx or gap_bundle:
            missing[key] = {"txs": gap_tx, "bundles": gap_bundle,
                            "is_tail": w["is_tail"], "refs": w["refs"]}
            every_sig |= set(gap_tx)

    votes = _vote_sigs(missing)

    per_slot, totals = {}, {"tx": 0, "bundle": 0, "vote": 0, "refs": 0,
                            "rounds": 0, "winner_rounds": len(winners)}
    for (slot, idx), gap in sorted(missing.items()):
        non_vote = [s for s in gap["txs"] if s not in votes]
        gap["non_vote"] = non_vote
        gap["votes"] = len(gap["txs"]) - len(non_vote)
        totals["tx"] += len(non_vote)
        totals["bundle"] += len(gap["bundles"])
        totals["vote"] += gap["votes"]
        if non_vote or gap["bundles"]:
            totals["rounds"] += 1
            bucket = per_slot.setdefault(slot, {"rounds": [], "tx": 0,
                                                "bundle": 0, "vote": 0, "refs": 0})
            bucket["rounds"].append({"round": idx, "non_vote": non_vote,
                                     "bundles": gap["bundles"],
                                     "votes": gap["votes"], "refs": gap["refs"],
                                     "is_tail": gap["is_tail"]})
            bucket["tx"] += len(non_vote)
            bucket["bundle"] += len(gap["bundles"])
            bucket["vote"] += gap["votes"]
            bucket["refs"] += gap["refs"]
    totals["refs"] = sum(w["refs"] for w in winners.values())
    return {"per_slot": per_slot, "totals": totals, "bad": bad}


def _vote_sigs(missing):
    """Which of the missing winner refs are votes.

    A winner tx ref carries no bytes, so a vote is only identifiable by having
    reached our own ingest with route=vote. That is an IN list against
    bifrost_events, and the cost is entirely in how much of the table it has to
    look at: asked once for a whole day it scans every receive in the range,
    which is millions of rows. Asked per leader window, bounded to that
    window's own couple of seconds, the same answer costs about a second --
    measured 12s down to ~2s over 8 windows. Windows are independent, so they
    go out together."""
    import concurrent.futures as cf

    by_window = {}
    for (slot, _), gap in missing.items():
        by_window.setdefault(slot // WINDOW, set()).update(gap["txs"])
    if not by_window:
        return set()

    _, bounds = clickhouse(
        "SELECT intDiv(slot, %d) AS win, toString(min(ts)), toString(max(ts)) "
        "FROM bifrost_miniblocks WHERE slot IN (%s) GROUP BY win"
        % (WINDOW, ",".join(str(s) for s, _ in missing)))
    span = {int(w): (lo, hi) for w, lo, hi in bounds}

    def one(win):
        sigs = [x for x in sorted(by_window[win])
                if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{1,44}", x)]
        if not sigs or win not in span:
            return set()
        lo, hi = span[win]
        found = set()
        # Still chunked: ClickHouse rejects a query much past a megabyte, and
        # a busy window can carry several thousand refs.
        for i in range(0, len(sigs), 4000):
            chunk = ",".join("'" + x + "'" for x in sigs[i:i + 4000])
            _, rows = clickhouse(
                "SELECT DISTINCT entity FROM bifrost_events "
                f"WHERE ts >= '{lo[:19]}' - INTERVAL 120 SECOND "
                f"AND ts <= '{hi[:19]}' + INTERVAL 30 SECOND "
                "AND event = 'received' AND attrs['route'] = 'vote' "
                f"AND entity_kind = 'tx' AND entity IN ({chunk})")
            found |= {r[0] for r in rows}
        return found

    out = set()
    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        for found in pool.map(one, sorted(by_window)):
            out |= found
    return out


def _commit_rows(lo, hi, slots):
    """sim-commit for the range, kept to the slots we care about."""
    cols, rows = influx(
        f'SELECT * FROM "sim-commit" WHERE time >= \'{_ix_ts(lo)}\' '
        f"AND time <= '{_ix_ts(hi)}' "
        + "".join(f"AND \"host_id\" != '{h}' " for h in OFF_CHAIN)
        + (f"AND \"host_id\" = '{SIM}'" if SIM else ""))
    at = {name: i for i, name in enumerate(cols)}
    keep = set(slots)
    out = []
    for r in rows:
        try:
            slot = int(r[at["slot"]])
        except (TypeError, ValueError, KeyError):
            continue
        if slot in keep:
            out.append(r)
    return at, out


REPLAY_FIELDS = [
    ("body_us", "total commit"),
    ("queue_us", "wait to start"),
    ("sanitize_us", ""),
    ("check_us", ""),
    ("exec_wall_us", "wall in the pool"),
    ("execute_us", "summed lane work"),
    ("load_us", "account loads"),
    ("program_cache_us", ""),
    ("account_cache_clone_us", ""),
]
REPLAY_SHAPE = [
    ("replayed", "orders"),
    ("critical_path", "longest chain"),
    ("initial_width", "start unblocked"),
    ("exec_pool", "lanes used"),
]


def analysis_replay(commits):
    at, rows = commits
    if not rows:
        return {"n": 0}

    def series(name):
        """Values with the (slot, round) they came from, so a maximum can be
        pointed at rather than merely printed."""
        i = at.get(name)
        if i is None:
            return []
        out = []
        for r in rows:
            v = r[i]
            if v is None:
                continue
            try:
                out.append((float(v), int(r[at["slot"]]), int(r[at["index"]])))
            except (TypeError, ValueError):
                continue
        return out

    def summarise(fields, scale):
        table = []
        for name, note in fields:
            vals = series(name)
            if not vals:
                table.append({"name": name, "note": note, "n": 0})
                continue
            nums = [v / scale for v, _, _ in vals]
            top = max(vals, key=lambda x: x[0])
            table.append({"name": name, "note": note, "n": len(nums),
                          "mean": sum(nums) / len(nums),
                          "pcts": [pct(nums, p) for p in PCTS],
                          "max": top[0] / scale,
                          "at": (top[1], top[2])})
        return table

    promoted = sum(1 for r in rows
                   if at.get("promoted_len") is not None
                   and r[at["promoted_len"]] is not None
                   and float(r[at["promoted_len"]]) >= 0)
    wall = [v for v, _, _ in series("exec_wall_us")]
    work = [v for v, _, _ in series("execute_us")]
    replayed = [v for v, _, _ in series("replayed")]
    body = [v for v, _, _ in series("body_us")]
    return {
        "n": len(rows), "promoted": promoted, "replays": len(rows) - promoted,
        "timing": summarise(REPLAY_FIELDS, 1000.0),
        "shape": summarise(REPLAY_SHAPE, 1.0),
        "parallelism": (sum(work) / sum(wall)) if wall and sum(wall) else None,
        "per_order": (sum(body) / sum(replayed)) if replayed and sum(replayed) else None,
    }


def analysis_gap(slots, threshold):
    slot_list = ",".join(str(s) for s in slots)
    _, rows = clickhouse(
        "SELECT slot, index_in_slot, kind, max(reward), max(order_count), "
        "       max(is_last) "
        f"FROM bifrost_miniblocks WHERE slot IN ({slot_list}) "
        f"AND instance_id = '{CH_INSTANCE}' GROUP BY slot, index_in_slot, kind")
    best, win = {}, {}
    for slot, idx, kind, reward, orders, is_last in rows:
        key = (int(slot), int(idx))
        (best if kind == "selected" else win)[key] = (
            ch_int(reward), ch_int(orders), is_last in TRUEISH)

    flagged, gaps, series = [], [], []
    for key in sorted(win):
        w_reward, w_orders, is_tail = win[key]
        o_reward, o_orders, _ = best.get(key, (0, 0, False))
        if w_reward <= 0:
            continue
        gap = 100.0 * (w_reward - o_reward) / w_reward
        gaps.append(gap)
        series.append([key[0], key[1], o_reward, w_reward])
        if gap >= threshold:
            flagged.append({"slot": key[0], "round": key[1], "tail": is_tail,
                            "ours": o_reward, "theirs": w_reward,
                            "our_orders": o_orders, "their_refs": w_orders,
                            "gap": gap})
    return {"flagged": flagged, "rounds": len(gaps), "series": series,
            "slots": len({f["slot"] for f in flagged}),
            "slots_total": len({s for s, _ in win}),
            "mean": (sum(gaps) / len(gaps)) if gaps else None,
            "pcts": [pct(gaps, p) for p in PCTS] if gaps else None,
            "threshold": threshold}


def analysis_skew(lo, hi, slots, commits):
    """ClickHouse-minus-InfluxDB skew, bounded per leader window.

    round_committed is logged when the builder receives the commit response
    and sim-commit is written when the commit body finishes, so for one round

        observed(ch) - observed(ix) = return_latency + skew,  latency >= 0

    and the minimum over a window's rounds bounds the skew from above, tightly,
    because some round in a window always returns fast. Per window and not once
    for the range: the two hosts drift tens of milliseconds apart over a day and
    get pulled back by NTP, so a single figure for a range would be a fiction."""
    slot_list = ",".join(str(s) for s in slots)
    _, rows = clickhouse(
        "SELECT toUnixTimestamp64Nano(ts), nums['slot'], nums['index'] "
        f"FROM bifrost_events WHERE ts >= '{_ch_ts(lo)}' AND ts <= '{_ch_ts(hi)}' "
        f"AND event = 'round_committed' AND instance_id = '{CH_INSTANCE}' "
        f"AND nums['slot'] IN ({slot_list})")
    ch_ts = {(int(s), int(i)): int(t) for t, s, i in rows}
    at, crows = commits
    per_window = {}
    for r in crows:
        key = (int(r[at["slot"]]), int(r[at["index"]]))
        if key in ch_ts:
            delta = (ch_ts[key] - _rfc3339_ns(r[0])) / 1e6
            per_window.setdefault(key[0] // WINDOW, []).append(delta)
    return {w: (min(d), len(d)) for w, d in per_window.items()}


def analysis_install(lo, hi, slots, identity, commits):
    slot_list = ",".join(str(s) for s in slots)
    _, rows = clickhouse(
        "SELECT toUnixTimestamp64Nano(ts), nums['slot'] FROM bifrost_events "
        f"WHERE ts >= '{_ch_ts(lo)}' AND ts <= '{_ch_ts(hi)}' AND event = 'progress' "
        f"AND instance_id = '{CH_INSTANCE}' AND attrs['identity'] = '{identity}' "
        f"AND attrs['leader_state'] = 'Sequencing' AND nums['slot'] IN ({slot_list})")
    seq = {}
    for t, slot in rows:
        slot = int(slot)
        seq[slot] = min(int(t), seq.get(slot, 1 << 62))

    cols, vals = influx(
        f'SELECT "slot","event" FROM "sim-context" WHERE time >= \'{_ix_ts(lo)}\' '
        f"AND time <= '{_ix_ts(hi)}' "
        + "".join(f"AND \"host_id\" != '{h}' " for h in OFF_CHAIN)
        + (f"AND \"host_id\" = '{SIM}'" if SIM else ""))
    at = {n: i for i, n in enumerate(cols)}
    install = {}
    for v in vals:
        if at.get("event") is None or v[at["event"]] != "install":
            continue
        try:
            slot = int(v[at["slot"]])
        except (TypeError, ValueError):
            continue
        install[slot] = min(_rfc3339_ns(v[0]), install.get(slot, 1 << 62))

    skew = analysis_skew(lo, hi, slots, commits)
    raw, corrected, per_slot = [], [], []
    for slot in sorted(seq):
        if slot not in install:
            per_slot.append({"slot": slot, "note": "no install row"})
            continue
        d = (install[slot] - seq[slot]) / 1e6
        raw.append(d)
        s_ms, pairs = skew.get(slot // WINDOW, (None, 0))
        if s_ms is None:
            per_slot.append({"slot": slot, "raw": d,
                             "note": "no commit pair in this window"})
            continue
        corrected.append(d + s_ms)
        per_slot.append({"slot": slot, "raw": d, "skew": s_ms,
                         "corrected": d + s_ms, "pairs": pairs})
    def block(vals):
        if not vals:
            return None
        return {"n": len(vals), "mean": sum(vals) / len(vals),
                "pcts": [pct(vals, p) for p in PCTS], "max": max(vals)}
    return {"per_slot": per_slot, "raw": block(raw), "corrected": block(corrected)}


def analysis_run(start, end, threshold, identity):
    """All four reports for one range, cached: the page is several seconds of
    querying and people re-read it while changing one field."""
    lo, hi = resolve_range(start, end)
    # Resolve the connector BEFORE the cache is consulted: the key has to be
    # the identity actually used, or a request that left it blank stores under
    # one key and looks up under another, and never hits.
    connectors = analysis_connectors(lo, hi)
    picked = (identity or ANALYSIS_IDENTITY
              or (connectors[0][0] if connectors else ""))
    # The threshold is deliberately NOT part of the key. Only report 3 depends
    # on it and that one costs half a second, while the rest of the run costs
    # ten; keying on it would re-query everything to move a slider.
    key = (_ch_ts(lo), _ch_ts(hi), picked)
    stamp = time.monotonic()
    with _analysis_lock:
        hit = _analysis_cache.get(key)
    out = hit[1] if hit and stamp - hit[0] < 900 else None
    if out is None:
        slots = analysis_slots(lo, hi, picked) if picked else []
        out = {"lo": lo, "hi": hi, "identity": picked, "slots": slots,
               "connectors": connectors,
               "windows": sorted({s // WINDOW for s in slots}),
               "cached_at": dt.datetime.now(dt.UTC).replace(microsecond=0)}
        if slots:
            out["missing"] = analysis_missing(slots)
            # One sim-commit fetch feeds both the replay percentiles and the
            # per-window clock skew; they used to ask for it separately.
            commits = _commit_rows(lo, hi, slots)
            out["replay"] = analysis_replay(commits)
            out["install"] = analysis_install(lo, hi, slots, picked, commits)
        with _analysis_lock:
            if len(_analysis_cache) > 6:
                _analysis_cache.clear()
            _analysis_cache[key] = (stamp, out)
    out = dict(out, threshold=threshold, connectors=connectors)
    if out["slots"]:
        out["gap"] = analysis_gap(out["slots"], threshold)
    return out


def _slot_link(slot, rnd=None, tab=None):
    href = f"/?win={window_of(slot)}&slot={slot}"
    if tab:
        href += f"&tab={tab}"
    if rnd is not None:
        href += f"&round={rnd}"
    label = f"{slot}" + (f" r{rnd}" if rnd is not None else "")
    return f'<a class="alink" href="{href}">{label}</a>'


def _pct_row(entry, unit):
    if not entry.get("n"):
        return ("<tr><td class=m>" + html.escape(entry["name"])
                + '</td><td class="n dim">0</td>'
                + '<td class="n dim">&mdash;</td>' * (len(PCTS) + 2) + "</tr>")
    fmt = (lambda v: f"{v:,.2f}") if unit == "ms" else (lambda v: f"{v:,.1f}")
    note = (f' <span class="dim">{html.escape(entry["note"])}</span>'
            if entry["note"] else "")
    slot, rnd = entry["at"]
    return ("<tr><td class=m>" + html.escape(entry["name"]) + note + "</td>"
            f'<td class="n m">{entry["n"]:,}</td>'
            f'<td class="n m">{fmt(entry["mean"])}</td>'
            + "".join(f'<td class="n m">{fmt(v)}</td>' for v in entry["pcts"])
            + f'<td class="n m amax">{fmt(entry["max"])}'
            f'<span class="atwhere">{_slot_link(slot, rnd)}</span></td></tr>')


def _pct_table(entries, unit, head):
    return ('<table class="atab"><tr><th>field</th><th class=n>n</th>'
            f"<th class=n>mean {head}</th>"
            + "".join(f'<th class=n>p{p}</th>' for p in PCTS)
            + '<th class=n>max &rarr; where</th></tr>'
            + "".join(_pct_row(e, unit) for e in entries) + "</table>")


def analysis_missing_html(rep, qs=""):
    per_slot, tot = rep["per_slot"], rep["totals"]
    if not per_slot:
        return ('<div class="none">no round had a winner ref we never '
                "offered</div>")
    body = []
    for slot in sorted(per_slot):
        b = per_slot[slot]
        rounds = []
        for r in b["rounds"]:
            # The lists themselves are fetched when the round is opened. A day
            # of these is tens of thousands of signatures, which inlined came
            # to a four megabyte page nobody reads to the end of.
            items = [f'<div class="alist lazy" data-slot="{slot}" '
                     f'data-round="{r["round"]}">loading&hellip;</div>']
            rounds.append(
                '<details class="around"><summary>'
                f'<span class="arnd">round {r["round"]}</span>'
                + ('<span class="tailpill">tail</span>' if r["is_tail"] else "")
                + f'<span class="amiss">{len(r["non_vote"]):,} tx</span>'
                + f'<span class="amiss">{len(r["bundles"]):,} bundles</span>'
                + f'<span class="dim">of {r["refs"]:,} winner refs</span>'
                + (f'<span class="dim">{r["votes"]:,} votes skipped</span>'
                   if r["votes"] else "")
                + f'<span class="agoto">{_slot_link(slot, r["round"])}</span>'
                + "</summary>" + "".join(items) + "</details>")
        body.append(
            '<details class="aslot"><summary>'
            f'<span class="aslotn">{slot}</span>'
            f'<span class="dim">{len(b["rounds"])} round'
            f'{"" if len(b["rounds"]) == 1 else "s"}</span>'
            f'<span class="amiss">{b["tx"]:,} non-vote tx</span>'
            f'<span class="amiss">{b["bundle"]:,} bundles</span>'
            f'<span class="dim">of {b["refs"]:,} winner refs in those rounds</span>'
            "</summary>" + "".join(rounds) + "</details>")
    share = (100.0 * (tot["tx"] + tot["bundle"]) / tot["refs"]) if tot["refs"] else 0
    return (f'<div class="amiss-root" data-qs="{html.escape(qs)}"></div>'
            f'<div class="asum"><b>{tot["tx"]:,}</b> non-vote signatures and '
            f'<b>{tot["bundle"]:,}</b> bundle ids across '
            f'<b>{tot["rounds"]:,}</b> rounds &mdash; {share:.1f}% of all '
            f'{tot["refs"]:,} winner refs over {tot["winner_rounds"]:,} rounds. '
            f'{tot["vote"]:,} vote signatures excluded.</div>'
            + "".join(body)
            + '<div class="anote">Signatures inside a winner bundle are '
              "invisible here: a bundle ref is a bare 32-byte id and the "
              "winning builder attaches no member bytes, so a bundle counts as "
              "one missing thing however many transactions it held. Votes are "
              "identified by having reached our own ingest with "
              "<code>route=vote</code> &mdash; a winner tx ref carries no bytes, "
              "so one we never saw cannot be classified and is counted as "
              "non-vote."
            + (f' <span class="warn">{len(rep["bad"])} winner payloads failed '
               "to decode and are absent: "
               + html.escape("; ".join(rep["bad"][:3])) + "</span>"
               if rep["bad"] else "") + "</div>")


def analysis_replay_html(rep):
    if not rep.get("n"):
        return '<div class="none">no sim-commit rows in this range</div>'
    extra = []
    if rep["parallelism"]:
        extra.append("parallelism <b>"
                     f"{rep['parallelism']:.2f}&times;</b> "
                     '<span class="dim">sum(execute_us) / sum(exec_wall_us)</span>')
    if rep["per_order"]:
        extra.append(f"per order <b>{rep['per_order']:.1f} &micro;s</b> "
                     '<span class="dim">sum(body_us) / sum(replayed)</span>')
    return (f'<div class="asum"><b>{rep["n"]:,}</b> commits &mdash; '
            f'{rep["promoted"]:,} promoted, {rep["replays"]:,} full replays. '
            + " &middot; ".join(extra) + "</div>"
            + _pct_table(rep["timing"], "ms", "ms")
            + '<div class="asubh">shape of the replayed batch '
              "&mdash; counts, not milliseconds</div>"
            + _pct_table(rep["shape"], "count", "count")
            + '<div class="anote">Every <b>max</b> links to the slot and round '
              "it came from. A promoted commit is one where our own block won "
              "and was adopted rather than replayed, so it does no execution "
              "work and drags the low percentiles down &mdash; the split is "
              "given above.</div>")


def analysis_gap_html(rep):
    if not rep["flagged"]:
        return (f'<div class="none">no round was {rep["threshold"]:g}% or more '
                f'below the winner, out of {rep["rounds"]:,} rounds</div>')
    rows = "".join(
        "<tr>"
        f'<td class=m>{_slot_link(f["slot"], f["round"])}</td>'
        + ('<td><span class="tailpill">tail</span></td>' if f["tail"]
           else "<td></td>")
        + f'<td class="n m">{f["ours"]:,}</td>'
        f'<td class="n m">{f["theirs"]:,}</td>'
        f'<td class="n m">{f["our_orders"]:,}</td>'
        f'<td class="n m">{f["their_refs"]:,}</td>'
        f'<td class="n m {"abad" if f["gap"] >= 50 else "awarn"}">'
        f'{f["gap"]:.2f}%</td></tr>'
        for f in rep["flagged"])
    share = 100.0 * len(rep["flagged"]) / rep["rounds"] if rep["rounds"] else 0
    pcts = ("  ".join(f"p{p} {v:.2f}%" for p, v in zip(PCTS, rep["pcts"]))
            if rep["pcts"] else "")
    return (f'<div class="asum"><b>{len(rep["flagged"]):,}</b> of '
            f'{rep["rounds"]:,} rounds ({share:.1f}%) across '
            f'<b>{rep["slots"]:,}</b> of {rep["slots_total"]:,} slots were at '
            f'least {rep["threshold"]:g}% below the winner. Gap over every '
            f'round: mean {rep["mean"]:.2f}% &middot; {pcts}</div>'
            '<table class="atab"><tr><th>slot &middot; round</th><th></th>'
            "<th class=n>our best reward</th><th class=n>winner</th>"
            "<th class=n>our orders</th><th class=n>winner refs</th>"
            "<th class=n>gap</th></tr>" + rows + "</table>"
            '<div class="anote">A gap of 100% is a round we did not offer into '
            "at all, not a round we lost narrowly. A <b>negative</b> gap would "
            "mean our offer carried the higher reward and still lost, which is "
            "worth chasing separately.</div>")


def analysis_install_html(rep):
    if not rep["per_slot"]:
        return '<div class="none">no Sequencing/install pair in this range</div>'
    rows = "".join(
        "<tr>"
        f'<td class=m>{_slot_link(e["slot"], tab="timeline")}</td>'
        + (f'<td class="n m">{e["raw"]:.2f}</td>' if "raw" in e
           else '<td class="n dim">&mdash;</td>')
        + (f'<td class="n m">{e["skew"]:+.2f}</td>' if "skew" in e
           else '<td class="n dim">&mdash;</td>')
        + (f'<td class="n m">{e["corrected"]:.2f}</td>' if "corrected" in e
           else '<td class="n dim">&mdash;</td>')
        + f'<td class="dim">{html.escape(e.get("note") or str(e.get("pairs", "")) + " pairs")}</td>'
        "</tr>" for e in rep["per_slot"])
    summary = []
    for label, block in (("raw", rep["raw"]), ("corrected", rep["corrected"])):
        if not block:
            continue
        summary.append(
            f'<tr><td class=m>{label}</td><td class="n m">{block["n"]:,}</td>'
            f'<td class="n m">{block["mean"]:.2f}</td>'
            + "".join(f'<td class="n m">{v:.2f}</td>' for v in block["pcts"])
            + f'<td class="n m">{block["max"]:.2f}</td></tr>')
    return ('<table class="atab"><tr><th>series</th><th class=n>n</th>'
            "<th class=n>mean ms</th>"
            + "".join(f"<th class=n>p{p}</th>" for p in PCTS)
            + "<th class=n>max</th></tr>" + "".join(summary) + "</table>"
            '<div class="anote">Sequencing is a builder event in ClickHouse and '
            "the install is a simulator row in InfluxDB, so every pair is "
            "cross-clock. The skew column is that leader window's own bound, "
            "taken as the minimum of "
            "<code>round_committed</code> minus <code>sim-commit</code> over "
            "the window's rounds &mdash; that difference is the return latency "
            "plus the skew, and some round always returns fast, so the minimum "
            "bounds the skew tightly from above. <b>corrected = raw + skew.</b> "
            "It is estimated per window and not once for the range because the "
            "two hosts drift tens of milliseconds apart over a day and get "
            "pulled back by NTP. A negative corrected value means the simulator "
            "had the context installed before the builder entered Sequencing "
            "for that slot.</div>"
            '<details class="aslot"><summary><span class="aslotn">per slot</span>'
            f'<span class="dim">{len(rep["per_slot"])} slots</span></summary>'
            '<table class="atab"><tr><th>slot</th><th class=n>raw ms</th>'
            "<th class=n>skew ms</th><th class=n>corrected ms</th><th>basis</th>"
            "</tr>" + rows + "</table></details>")


# A round's missing-order list is fetched when the round is opened, against the
# already-cached run, so the page ships the summary and nothing else.
ANALYSIS_JS = r"""
(function(){
  var root = document.querySelector('.amiss-root');
  if(!root) return;
  var qs = root.dataset.qs || '';
  // `toggle` does not bubble, hence the capture phase.
  document.addEventListener('toggle', function(ev){
    var d = ev.target;
    if(!d.open || !d.classList || !d.classList.contains('around')) return;
    var box = d.querySelector('.alist.lazy');
    if(!box || box.dataset.done) return;
    box.dataset.done = '1';
    fetch('/analysis/orders?' + qs + '&slot=' + box.dataset.slot
          + '&round=' + box.dataset.round)
      .then(function(r){ if(!r.ok) throw 0; return r.text(); })
      .then(function(h){ box.innerHTML = h; })
      .catch(function(){
        box.textContent = 'could not load this round';
        box.dataset.done = '';
      });
  }, true);
})();
"""


def analysis_orders_html(start, end, threshold, identity, slot, rnd):
    """One round's missing refs, for the lazy expansion.

    Reads the cached run rather than recomputing: the page has just built it,
    and rebuilding for every expansion would re-query ClickHouse for each
    click."""
    rep = analysis_run(start, end, threshold, identity)
    bucket = ((rep.get("missing") or {}).get("per_slot") or {}).get(slot)
    entry = next((r for r in (bucket or {}).get("rounds", [])
                  if r["round"] == rnd), None)
    if entry is None:
        return '<span class="dim">this round is no longer in the result</span>'
    out = []
    for label, vals, cls in (("non-vote signatures", entry["non_vote"], "sig"),
                             ("bundle ids", entry["bundles"], "bid")):
        if not vals:
            continue
        shown = vals[:SIG_CAP]
        more = len(vals) - len(shown)
        out.append(f"<b>{len(vals):,} {label}</b>"
                   + "".join(f'<code class="{cls}">{html.escape(v)}</code>'
                             for v in shown)
                   + (f'<span class="dim">&hellip; and {more:,} more, withheld '
                      f"so one round cannot flood the page</span>"
                      if more else ""))
    return "".join(out) or '<span class="dim">nothing missing here</span>'


# Two series over an ordered sequence, so: multi-line, categorical colour.
# Slots 1 and 2 of the validated dark categorical order -- blue then orange --
# checked against this page's own surface rather than assumed:
#   validate_palette.js "#3987e5,#d95926" --mode dark --surface #0c141b --pairs all
#   lightness band PASS, chroma PASS, CVD dE 26.8 (protan) PASS,
#   normal-vision dE 31.8 PASS, contrast >= 3:1 PASS
OURS_HUE = "#3987e5"
WINNER_HUE = "#d95926"


def analysis_chart_html(rep):
    """Best offer against the winning bid, one point per miniblock.

    A log y axis, because the rewards run from tens of thousands of lamports to
    tens of millions and a linear axis would press nine rounds in ten flat
    against the floor.

    A round we did not offer into is a GAP in our line, not a zero plotted on
    the floor: nothing was bid, and drawing that as a very small bid would
    invite reading it as a near miss. Those rounds get a tick along the bottom
    instead, and the count is stated."""
    pts = rep.get("series") or []
    if len(pts) < 2:
        return '<div class="none">not enough rounds in this range to plot</div>'
    live = [v for _, _, ours, theirs in pts for v in (ours, theirs) if v > 0]
    if not live:
        return '<div class="none">no round in this range carried a reward</div>'
    silent = sum(1 for _, _, ours, _ in pts if ours <= 0)
    blob = json.dumps({"pts": pts, "lo": min(live), "hi": max(live),
                       "ours": OURS_HUE, "win": WINNER_HUE},
                      separators=(",", ":"))
    rows = "".join(
        f'<tr><td class=m>{_slot_link(s, r)}</td>'
        f'<td class="n m">{o:,}</td><td class="n m">{t:,}</td>'
        f'<td class="n m">{100.0 * (t - o) / t:.2f}%</td></tr>'
        for s, r, o, t in pts)
    return (
        '<div class="chartlegend">'
        f'<span><i style="background:{OURS_HUE}"></i>our best offer</span>'
        f'<span><i style="background:{WINNER_HUE}"></i>the winning bid</span>'
        + (f'<span><i class="nooffer"></i>no offer from us '
           f"({silent:,} of {len(pts):,})</span>" if silent else "")
        + "</div>"
        '<div class="chartwrap"><svg id="rwchart" role="img" '
        'aria-label="best offer against the winning bid, per miniblock">'
        "</svg></div>"
        f'<script id="rwdata" type="application/json">{blob}</script>'
        f"<script>{CHART_JS}</script>"
        '<div class="anote">One point per miniblock, in slot then round order. '
        "The y axis is <b>logarithmic</b>: rewards here span four orders of "
        "magnitude and a linear axis would flatten almost every round against "
        "the floor. A round we never offered into breaks our line rather than "
        "plotting a zero &mdash; nothing was bid, and a dot on the floor would "
        "read as a near miss."
        + (f" There are {silent:,} of those." if silent else "")
        + " Only rounds the relay echoed a winner for are plotted.</div>"
        '<details class="aslot"><summary><span class="aslotn">the same data as '
        f'a table</span><span class="dim">{len(pts):,} miniblocks</span>'
        "</summary>"
        '<table class="atab"><tr><th>slot &middot; round</th>'
        "<th class=n>our best</th><th class=n>winner</th><th class=n>gap</th>"
        "</tr>" + rows + "</table></details>")


CHART_JS = r"""
(function(){
  var el = document.getElementById('rwdata');
  if(!el) return;
  var D = JSON.parse(el.textContent);
  var svg = document.getElementById('rwchart');
  var NS = 'http://www.w3.org/2000/svg';
  var PADL = 62, PADR = 74, PADT = 14, PADB = 34, H = 300;
  var tip = null;

  // one decade below the smallest real bid, so the floor is never a data point
  var lo = Math.pow(10, Math.floor(Math.log10(D.lo)) ),
      hi = Math.pow(10, Math.ceil(Math.log10(D.hi)));
  var l0 = Math.log10(lo), l1 = Math.log10(hi);

  function mk(n, attrs, parent){
    var e = document.createElementNS(NS, n);
    for(var k in attrs) e.setAttribute(k, attrs[k]);
    (parent || svg).appendChild(e);
    return e;
  }
  function x(i){ return PADL + (W - PADL - PADR) * (D.pts.length < 2 ? 0 : i / (D.pts.length - 1)); }
  function y(v){ return PADT + (H - PADT - PADB) * (1 - (Math.log10(v) - l0) / (l1 - l0)); }
  function sol(v){
    if(v >= 1e9) return (v / 1e9).toFixed(2) + ' SOL';
    if(v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
    if(v >= 1e3) return (v / 1e3).toFixed(0) + 'k';
    return String(v);
  }

  var W = 900;
  function draw(){
    while(svg.firstChild) svg.removeChild(svg.firstChild);
    W = Math.max(svg.clientWidth || 900, 360);
    svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
    svg.setAttribute('height', H);

    // y gridlines, one per decade
    for(var d = Math.ceil(l0); d <= l1; d++){
      var v = Math.pow(10, d), yy = y(v);
      mk('line', {x1: PADL, x2: W - PADR, y1: yy, y2: yy, class: 'cgrid'});
      mk('text', {x: PADL - 8, y: yy + 3, class: 'ctick', 'text-anchor': 'end'})
        .textContent = sol(v);
    }
    // x ticks at slot boundaries, thinned to fit
    var bounds = [], last = null;
    D.pts.forEach(function(p, i){ if(p[0] !== last){ bounds.push([i, p[0]]); last = p[0]; } });
    var step = Math.ceil(bounds.length / Math.max(2, Math.floor(W / 130)));
    bounds.forEach(function(b, n){
      if(n % step) return;
      mk('line', {x1: x(b[0]), x2: x(b[0]), y1: PADT, y2: H - PADB, class: 'cgridv'});
      mk('text', {x: x(b[0]), y: H - PADB + 14, class: 'ctick', 'text-anchor': 'middle'})
        .textContent = b[1];
    });

    // the gap between the two, so the story reads without tracing two lines
    var band = [], back = [];
    D.pts.forEach(function(p, i){
      if(p[2] > 0 && p[3] > 0){ band.push(x(i) + ',' + y(p[3])); back.unshift(x(i) + ',' + y(p[2])); }
    });
    if(band.length > 1)
      mk('polygon', {points: band.concat(back).join(' '), class: 'cband'});

    // a run of consecutive rounds we bid in; a break means we bid nothing
    // a point is [slot, round, ours, winner]; winner drawn first so ours sits
    // on top where the two coincide
    [[3, D.win, 'cwin'], [2, D.ours, 'cours']].forEach(function(s){
      var run = [];
      function flush(){
        if(run.length > 1) mk('polyline', {points: run.join(' '), class: 'cline ' + s[2], stroke: s[1]});
        else if(run.length === 1) mk('circle', {cx: +run[0].split(',')[0], cy: +run[0].split(',')[1], r: 1.6, fill: s[1]});
        run = [];
      }
      D.pts.forEach(function(p, i){
        var v = p[s[0]];
        if(v > 0) run.push(x(i) + ',' + y(v)); else flush();
      });
      flush();
    });

    // rounds with no offer of ours, marked on the axis rather than the floor
    D.pts.forEach(function(p, i){
      if(p[2] > 0) return;
      mk('line', {x1: x(i), x2: x(i), y1: H - PADB, y2: H - PADB + 5, class: 'cnone'});
    });

    // direct labels, so identity is never colour alone
    var lastPt = D.pts[D.pts.length - 1];
    if(lastPt[3] > 0) mk('text', {x: W - PADR + 6, y: y(lastPt[3]) + 3, class: 'clabel', fill: D.win}).textContent = 'winner';
    var lastOurs = null;
    for(var i = D.pts.length - 1; i >= 0; i--) if(D.pts[i][2] > 0){ lastOurs = D.pts[i]; break; }
    if(lastOurs) mk('text', {x: W - PADR + 6, y: y(lastOurs[2]) + 3, class: 'clabel', fill: D.ours}).textContent = 'ours';

    mk('line', {x1: PADL, x2: PADL, y1: PADT, y2: H - PADB, class: 'caxis'});
    mk('line', {x1: PADL, x2: W - PADR, y1: H - PADB, y2: H - PADB, class: 'caxis'});
    hair = mk('line', {x1: 0, x2: 0, y1: PADT, y2: H - PADB, class: 'chair', opacity: 0});
  }

  var hair = null;
  function nearest(px){
    var i = Math.round((px - PADL) / Math.max(1, (W - PADL - PADR)) * (D.pts.length - 1));
    return Math.max(0, Math.min(D.pts.length - 1, i));
  }
  svg.addEventListener('mousemove', function(ev){
    var r = svg.getBoundingClientRect();
    var px = (ev.clientX - r.left) * (W / r.width);
    if(px < PADL || px > W - PADR) return;
    var i = nearest(px), p = D.pts[i];
    if(hair){ hair.setAttribute('x1', x(i)); hair.setAttribute('x2', x(i)); hair.setAttribute('opacity', 1); }
    if(!tip){ tip = document.createElement('div'); tip.className = 'ctip'; document.body.appendChild(tip); }
    var gap = p[3] > 0 ? (100 * (p[3] - p[2]) / p[3]).toFixed(2) + '%' : '-';
    tip.innerHTML = '<b>' + p[0] + ' round ' + p[1] + '</b>'
      + '<span><i style="background:' + D.ours + '"></i>ours '
      + (p[2] > 0 ? p[2].toLocaleString() : 'no offer') + '</span>'
      + '<span><i style="background:' + D.win + '"></i>winner ' + p[3].toLocaleString() + '</span>'
      + '<span class="g">gap ' + gap + '</span>';
    tip.style.left = Math.min(ev.clientX + 14, window.innerWidth - tip.offsetWidth - 10) + 'px';
    tip.style.top = (ev.clientY + 16) + 'px';
  });
  svg.addEventListener('mouseleave', function(){
    if(hair) hair.setAttribute('opacity', 0);
    if(tip){ tip.remove(); tip = null; }
  });
  window.addEventListener('resize', draw);
  draw();
})();
"""

def analysis_page(start, end, threshold, identity):
    try:
        rep = analysis_run(start, end, threshold, identity)
    except ValueError as exc:
        rep = {"err": str(exc)}
    except Exception as exc:
        rep = {"err": f"{type(exc).__name__}: {exc}"}

    picked = rep.get("identity", identity)
    options = "".join(
        f'<option value="{html.escape(i)}"'
        + (" selected" if i == picked else "")
        + f">{html.escape(short_id(i))} &middot; {n} slots</option>"
        for i, n in rep.get("connectors", []))
    if picked and picked not in {i for i, _ in rep.get("connectors", [])}:
        options = (f'<option value="{html.escape(picked)}" selected>'
                   f"{html.escape(short_id(picked))}</option>") + options
    form = (
        '<form class="aform" method="get" action="/analysis">'
        '<label>from<input name="start" value="' + html.escape(start) + '" '
        'placeholder="-24h or 2026-08-21"></label>'
        '<label>to<input name="end" value="' + html.escape(end or "") + '" '
        'placeholder="now"></label>'
        '<label>reward gap<input name="threshold" class="thin" value="'
        + html.escape(f"{threshold:g}") + '">%</label>'
        '<label>connector<select name="identity">' + options + "</select></label>"
        '<button type="submit">run</button></form>')

    if rep.get("err"):
        body = f'<div class="err">{html.escape(rep["err"])}</div>'
    elif not rep.get("slots"):
        body = (f'<div class="empty"><b>No slots for that connector in that '
                "range.</b><br>Pick another connector or widen the range.</div>")
    else:
        qs = urllib.parse.urlencode(
            {"start": start, "end": end or "", "threshold": f"{threshold:g}",
             "identity": rep["identity"]})
        head = (f'<div class="ahead">'
                f'<span class="hascp"><code>{html.escape(rep["identity"])}</code>'
                f'{copy_btn(rep["identity"])}</span>'
                f'<span class="dim">{len(rep["slots"]):,} slots across '
                f'{len(rep["windows"]):,} leader windows &middot; '
                f'{rep["slots"][0]}&ndash;{rep["slots"][-1]} &middot; '
                f'{_ch_ts(rep["lo"])} to {_ch_ts(rep["hi"])} UTC</span></div>')
        body = head + "".join(
            f'<section class="asec"><h2>{n}. {title}</h2>{inner}</section>'
            for n, title, inner in (
                (1, "Winner landed it, we never offered it",
                 analysis_missing_html(rep["missing"], qs)),
                (2, "Commit-time replay in the simulator",
                 analysis_replay_html(rep["replay"])),
                (3, f"Rounds at least {threshold:g}% below the winner",
                 analysis_gap_html(rep["gap"])),
                (4, "Leader Sequencing &rarr; simulator context install",
                 analysis_install_html(rep["install"])),
                (5, "Reward per miniblock &mdash; our best offer against the "
                    "winning bid",
                 analysis_chart_html(rep["gap"]))))
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; analysis</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">range analysis</div>
  <a class="navlink" href="/">back to the slot explorer</a>
  <a class="navlink" href="/reference">metrics reference</a>
</header>
{form}
<main class="awrap">{body}</main>
<script>{ANALYSIS_JS}</script>
<script>{TICK_JS.replace("__SERVER_NOW__", f"{dt.datetime.now(dt.UTC).timestamp():.3f}")}</script>
</body></html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/analysis/orders":
            qs = urllib.parse.parse_qs(parsed.query)
            one = lambda k, d: (qs.get(k, [d])[0] or d)
            try:
                out = analysis_orders_html(
                    one("start", "-24h"), one("end", ""),
                    float(one("threshold", "5")), one("identity", ""),
                    int(one("slot", "0")), int(one("round", "0")))
            except Exception as exc:
                out = f'<span class="dim">{html.escape(str(exc)[:160])}</span>'
            body = out.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/analysis":
            qs = urllib.parse.parse_qs(parsed.query)
            one = lambda k, d: (qs.get(k, [d])[0] or d)
            try:
                thr = float(one("threshold", "5"))
            except ValueError:
                thr = 5.0
            try:
                body = analysis_page(one("start", "-24h"), one("end", ""),
                                     thr, one("identity", "")).encode()
            except Exception as exc:
                body = f"<pre>{html.escape(str(exc))}</pre>".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
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
            src = qs.get("src", ["ours"])[0]
            body = page(as_int("win"), as_int("slot"), as_int("round"),
                        tab if tab in ("compare", "timeline", "dag", "logs")
                        else "rounds",
                        src if src in ("ours", "winner") else "ours",
                        partial=qs.get("partial", ["0"])[0] == "1").encode()
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
