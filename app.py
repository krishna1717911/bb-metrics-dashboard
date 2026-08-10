#!/usr/bin/env python3
"""
simbench - the sim-extend measurement, viewed through its slot column.

InfluxDB only. Standard library only: no pip installs, no CDN, no build step.

    python3 app.py                 # http://127.0.0.1:8899
    python3 app.py --port 9000
    INFLUX_PASS=... python3 app.py

A slot only exists here if the builder registered it in a leader window
(state.rs:257) AND the connector announced its parent (state.rs:175) AND that
parent froze locally (state.rs:279). No context, no datapoint - so every slot
below is one this simulator actually worked.
"""

import argparse
import base64
import html
import json
import math
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- influx client

def _require(name, hint):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"{name} is not set. {hint}\n"
            "Copy .env.example to .env and fill it in, then:  set -a; . ./.env; set +a"
        )
    return value


# Comma-separated; tried in order, so list a MagicDNS name and its IP fallback.
INFLUX_HOSTS = [h.strip() for h in
                _require("INFLUX_HOSTS", "comma-separated, e.g. INFLUX_HOSTS=host-a,host-b").split(",")
                if h.strip()]
INFLUX_PORT = int(os.environ.get("INFLUX_PORT", "8086"))
INFLUX_DB = os.environ.get("INFLUX_DB", "solana")
INFLUX_USER = os.environ.get("INFLUX_USER", "metrics")  # read-only user
INFLUX_PASS = _require("INFLUX_PASS", "read-only InfluxDB password.")
MEASUREMENT = "sim-extend"

_auth = base64.b64encode(f"{INFLUX_USER}:{INFLUX_PASS}".encode()).decode()
_good_host = None


def influx(query):
    """Run InfluxQL, returning (columns, rows). Sticks to the first host that answers."""
    global _good_host
    hosts = ([_good_host] if _good_host else []) + [
        h for h in INFLUX_HOSTS if h != _good_host
    ]
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
            series = result.get("series")
            if not series:
                return [], []
            return series[0]["columns"], series[0]["values"]
        except (urllib.error.URLError, socket.timeout, OSError) as err:
            last = err
            continue
    raise RuntimeError(f"no influx host reachable ({last})")


# --------------------------------------------------- clickhouse (drill-down only)
# The main page is Influx-only. These two tables are consulted for the per-slot
# detail view and one summary card, where "what did the builder actually offer,
# and who won" is exactly the question.
CH_URL = os.environ.get("CH_URL", "")  # blank disables the drill-down panels
CH_USER = os.environ.get("CH_USER", "")
CH_PASS = os.environ.get("CH_PASS", "")
CH_DB = os.environ.get("CH_DB", "block_builder")


TRUEISH = {"1", "true", "True"}


def ch_bool(v):
    """won_by_us is Bool, which TabSeparated renders as true/false - not 1/0."""
    return str(v).strip() in TRUEISH


def ch_int(v, default=0):
    """ClickHouse TabSeparated writes NULL as the two characters \\N."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def clickhouse(sql):
    """-> (columns, rows). Raises if unconfigured; callers degrade the panel."""
    if not CH_URL or not CH_PASS:
        raise RuntimeError("CH_URL / CH_PASS not set - ClickHouse panels disabled")
    qs = urllib.parse.urlencode(
        {"user": CH_USER, "password": CH_PASS, "database": CH_DB,
         "query": sql + " FORMAT TabSeparatedWithNames"}
    )
    with urllib.request.urlopen(CH_URL + "?" + qs, timeout=120) as resp:
        text = resp.read().decode()
    lines = [ln for ln in text.split("\n") if ln]
    if not lines:
        return [], []
    return lines[0].split("\t"), [ln.split("\t") for ln in lines[1:]]


_host_cache = {"host": None, "at": 0.0}


def active_host():
    """host_id is the validator identity (validator/.../run/execute.rs:160), stamped on
    every point. TELEMETRY_MAP.md: always bind it - several nodes write the same
    measurement names. Pick whichever wrote most recently."""
    import time as _time

    now = _time.time()
    if _host_cache["host"] and now - _host_cache["at"] < 300:
        return _host_cache["host"]
    _, rows = influx(f'SHOW TAG VALUES FROM "{MEASUREMENT}" WITH KEY = "host_id"')
    best, best_ts = None, ""
    for r in rows:
        h = r[1]
        _, last = influx(
            f'SELECT "slot" FROM "{MEASUREMENT}" WHERE "host_id" = \'{h}\' '
            "ORDER BY time DESC LIMIT 1"
        )
        if last and str(last[0][0]) > best_ts:
            best, best_ts = h, str(last[0][0])
    _host_cache.update(host=best, at=now)
    return best


# ---------------------------------------------------------------------- fetch

RANGES = ["1h", "6h", "24h", "7d", "30d"]
SLOT_PRESETS = [10, 25, 50, 100, 250, 500, 1000]
SLOT_LOOKBACK = os.environ.get("SLOT_LOOKBACK", "30d")

# measurement -> (fields, extra WHERE). age_us is a 500ms gauge that reads 0 while
# the lane is idle, so it is only meaningful on busy samples.
SOURCES = {
    "sim-extend": (["slot", "body_us", "queue_us", "layer_count", "max_layer_width",
                    "exec_wall_us", "execute_us", "load_us", "exec_pool",
                    "account_cache_clone_us", "account_cache_entries_cloned",
                    "program_cache_us", "program_cache_clone_us",
                    "program_cache_entries", "program_cache_loaded"], None),
    "sim-commit": (["slot", "body_us", "layer_count", "max_layer_width",
                    "exec_wall_us", "execute_us", "load_us",
                    "account_cache_clone_us", "account_cache_entries_cloned",
                    "program_cache_us"], None),
    "sim-mutation-lane": (["slot", "age_us"], "busy = 1"),
}


def fetch_one(measurement, fields, extra, rng, host):
    where = f'time > now() - {rng}'
    if host:
        where += f" AND \"host_id\" = '{host}'"
    if extra:
        where += f" AND {extra}"
    cols, rows = influx(
        f'SELECT {", ".join(chr(34) + f + chr(34) for f in fields)} '
        f'FROM "{measurement}" WHERE {where}'
    )
    if not rows:
        return []
    ix = {c: i for i, c in enumerate(cols)}
    out = []
    for r in rows:
        slot = r[ix["slot"]] if "slot" in ix else None
        if not slot:
            continue
        point = {"time": r[0], "slot": int(slot)}
        for f in fields:
            if f != "slot":
                point[f] = r[ix[f]] if (f in ix and r[ix[f]] is not None) else 0
        out.append(point)
    return out


def fetch_all(rng, host):
    data, errs = {}, []
    for meas, (fields, extra) in SOURCES.items():
        try:
            data[meas] = fetch_one(meas, fields, extra, rng, host)
        except Exception as exc:
            data[meas] = []
            errs.append(f"{meas}: {html.escape(str(exc))}")
    return data, errs


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    lo = int(math.floor(k))
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def stats(vals):
    if not vals:
        return None
    s = sorted(vals)
    return {"n": len(s), "mean": sum(s) / len(s), "p50": percentile(s, 50),
            "p90": percentile(s, 90), "p95": percentile(s, 95),
            "p99": percentile(s, 99), "max": s[-1]}


def slot_runs(slots):
    """Consecutive slot numbers, collapsed. Solana leases leadership in blocks of
    NUM_CONSECUTIVE_LEADER_SLOTS = 4, so the run-length histogram says whether we
    see whole windows or fragments of them."""
    runs, cur = [], None
    for s in sorted(slots):
        if cur is not None and s == cur[1] + 1:
            cur = (cur[0], s)
        else:
            if cur:
                runs.append(cur)
            cur = (s, s)
    if cur:
        runs.append(cur)
    return runs


# ------------------------------------------------------------------- rendering

W, H, PAD = 660, 200, 34
WARN_US, CRIT_US = 25_000, 47_000
BANDS = {
    "queue_us": (1_000, 5_000),
    # A worker count has no "too high"; never colour it.
    "exec_pool": (1 << 30, 1 << 30),
    # Overlays run a few thousand accounts; flag an order of magnitude past that.
    "acct_entries": (20_000, 100_000),
    "depth": (16, 48), "depth_commit": (16, 48),
    "width": (8, 24), "width_commit": (8, 24),
    "pc_entries": (1_000, 5_000), "pc_loaded": (1_000, 5_000),
}
COUNTS = {"depth", "depth_commit", "width", "width_commit",
          "pc_entries", "pc_loaded", "exec_pool", "acct_entries"}


def bands_for(key):
    return BANDS.get(key, (WARN_US, CRIT_US))


def band(v, key=None):
    warn, crit = bands_for(key)
    return "crit" if v > crit else ("hot" if v > warn else "")


def fmt_us(v):
    if v is None:
        return "-"
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f} s"
    if v >= 1_000:
        return f"{v/1_000:.1f} ms"
    return f"{v:.0f} us"


def fmt_val(v, key=None):
    if key in COUNTS:
        return f"{v:,.0f}" if float(v).is_integer() else f"{v:,.1f}"
    return fmt_us(v)


def histogram_svg(vals, key=None):
    if not vals:
        return '<div class="empty">no data</div>'
    if key in COUNTS:
        hi = max(vals) or 1
        nb = min(40, max(6, int(hi)))
        buckets = [0] * nb
        for v in vals:
            buckets[min(nb - 1, int(max(0, v - 1) / max(hi, 1) * nb))] += 1

        def tick_at(f):
            return fmt_val(1 + hi * f, key)
    else:
        lo = max(1.0, min(vals))
        hi = max(max(vals), lo * 10)
        lo_e, hi_e = math.log10(lo), math.log10(hi)
        span = (hi_e - lo_e) or 1.0
        nb = 36
        buckets = [0] * nb
        for v in vals:
            idx = int((math.log10(max(v, 1.0)) - lo_e) / span * nb)
            buckets[max(0, min(nb - 1, idx))] += 1

        def tick_at(f):
            return fmt_val(10 ** (lo_e + span * f), key)

    peak = max(buckets) or 1
    bw = (W - 2 * PAD) / nb
    bars = "".join(
        f'<rect x="{PAD + i*bw:.1f}" y="{H - PAD - (H-2*PAD)*(c/peak):.1f}" '
        f'width="{max(1.0, bw-1.5):.1f}" height="{(H-2*PAD)*(c/peak):.1f}" rx="1" '
        f'fill="url(#g1)"><title>{c} points</title></rect>'
        for i, c in enumerate(buckets) if c
    )
    ticks = "".join(
        f'<text x="{PAD + (W-2*PAD)*f:.1f}" y="{H-10}" class="tick" '
        f'text-anchor="middle">{tick_at(f)}</text>'
        for f in (0, 0.25, 0.5, 0.75, 1.0)
    )
    return (
        f'<svg viewBox="0 0 {W} {H}" class="chart" preserveAspectRatio="none">'
        f'<defs><linearGradient id="g1" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="#5eead4"/><stop offset="100%" stop-color="#0f766e"/>'
        f'</linearGradient></defs>'
        f'<line x1="{PAD}" y1="{H-PAD}" x2="{W-PAD}" y2="{H-PAD}" class="axis"/>'
        + bars + ticks + "</svg>"
    )


def slot_series_svg(points, field, key=None):
    """Value against slot number - the slot column as the x axis."""
    if not points:
        return '<div class="empty">no data</div>'
    pts = sorted(points, key=lambda p: p["slot"])
    lo_s, hi_s = pts[0]["slot"], pts[-1]["slot"]
    span = max(1, hi_s - lo_s)
    hi_v = max(p[field] for p in pts) or 1
    step = max(1, len(pts) // 2500)
    dots = "".join(
        f'<circle cx="{PAD + (W-2*PAD)*((p["slot"]-lo_s)/span):.1f}" '
        f'cy="{H - PAD - (H-2*PAD)*(p[field]/hi_v):.1f}" r="1.6" fill="#5eead4" '
        f'fill-opacity=".5"><title>slot {p["slot"]}: {fmt_val(p[field], key)}</title></circle>'
        for p in pts[::step]
    )
    return (
        f'<svg viewBox="0 0 {W} {H}" class="chart" preserveAspectRatio="none">'
        f'<line x1="{PAD}" y1="{H-PAD}" x2="{W-PAD}" y2="{H-PAD}" class="axis"/>'
        f'{dots}'
        f'<text x="{PAD}" y="{H-10}" class="tick">{lo_s}</text>'
        f'<text x="{W-PAD}" y="{H-10}" class="tick" text-anchor="end">{hi_s}</text>'
        f'<text x="{PAD}" y="16" class="tick">peak {fmt_val(hi_v, key)}</text>'
        "</svg>"
    )


CSS = """
*{box-sizing:border-box}
body{margin:0;background:#0b1016;color:#dbe4ee;
  font:14px/1.55 ui-sans-serif,-apple-system,"SF Pro Text",Segoe UI,sans-serif}
header{padding:22px 28px 16px;border-bottom:1px solid #1e2937;
  display:flex;flex-wrap:wrap;gap:16px;align-items:baseline}
h1{margin:0;font-size:17px;font-weight:650;letter-spacing:-.01em}
h1 span{color:#5eead4}
.sub{color:#6b7f96;font-size:12.5px}
main{padding:20px 28px 60px;max-width:none}
.ctl{margin-left:auto;display:flex;flex-direction:column;gap:6px;align-items:flex-end}
.row{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.cap{color:#61748b;font-size:10.5px;text-transform:uppercase;letter-spacing:.07em;
  min-width:78px;text-align:right}
form.inline{display:flex;gap:6px;align-items:center}
input[type=number]{background:#0c1219;border:1px solid #263243;color:#dbe4ee;width:74px;
  padding:5px 8px;border-radius:7px;font-size:12.5px;font-family:ui-monospace,Menlo,monospace}
a.rng,button{background:#151d27;border:1px solid #263243;color:#9fb2c8;
  padding:5px 11px;border-radius:7px;text-decoration:none;font-size:12.5px;cursor:pointer}
a.rng.on{background:#0f766e;border-color:#14b8a6;color:#eafffb;font-weight:600}
.scope{background:#0d1620;border:1px solid #1e2937;border-radius:9px;padding:10px 15px;
  margin-bottom:16px;font-size:12.5px;color:#9fb2c8}
.scope b{color:#5eead4}
.err{background:#1e0d0d;border-color:#7f1d1d;color:#fca5a5}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:18px;align-items:start}
.grid.one{grid-template-columns:1fr}
.charts{display:grid;grid-template-columns:1fr 1fr;gap:18px;align-items:start}
.pane{min-width:0}
/* Two cards per row halves the card width, so drop each card's charts to a
   single column before they get too narrow to read. */
@media (max-width:1500px){.charts{grid-template-columns:1fr}}
@media (max-width:1100px){.grid{grid-template-columns:1fr}
  .charts{grid-template-columns:1fr 1fr}}
@media (max-width:820px){.charts{grid-template-columns:1fr}}
.card{background:#111823;border:1px solid #1e2937;border-radius:12px;padding:17px 19px;
  min-width:0;overflow:hidden}
.card h2{margin:0 0 3px;font-size:14.5px;font-weight:620;
  font-family:ui-monospace,Menlo,monospace;color:#5eead4}
.blurb{color:#7d90a6;font-size:12px;margin-bottom:13px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(72px,1fr));
  gap:7px;margin-bottom:15px}
.kpi{background:#0c1219;border:1px solid #1c2634;border-radius:8px;padding:8px 6px;
  text-align:center}
.kpi .k{color:#61748b;font-size:10px;text-transform:uppercase;letter-spacing:.06em}
.kpi .v{font-family:ui-monospace,Menlo,monospace;font-size:13.5px;margin-top:3px;color:#e6eef8}
.kpi .ksub{color:#6b7f96;font-size:9.5px;margin-top:2px;font-family:ui-monospace,Menlo,monospace}
.kpi.hot .v{color:#fbbf24}
.kpi.crit .v{color:#f87171}
.lbl{color:#61748b;font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 5px}
.chart{width:100%;height:190px;display:block}
.axis{stroke:#243040;stroke-width:1}
.tick{fill:#5b6b80;font-size:9.5px;font-family:ui-monospace,Menlo,monospace}
.empty{color:#4d5c70;padding:34px;text-align:center;font-size:12.5px}
table.tail{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:6px}
table.tail th{color:#61748b;font-weight:500;text-align:left;padding:4px 8px;
  border-bottom:1px solid #1e2937;font-size:10.5px;text-transform:uppercase;letter-spacing:.05em}
table.tail td{padding:4px 8px;border-bottom:1px solid #151d27}
table.tail tr.hot td.num{color:#fbbf24}
table.tail tr.crit td.num{color:#f87171}
.mono{font-family:ui-monospace,Menlo,monospace}
a.slotlink{color:#5eead4;text-decoration:none;border-bottom:1px dotted #14b8a6}
a.slotlink:hover{color:#eafffb;border-bottom-color:#5eead4}
table.wide{width:100%;border-collapse:collapse;font-size:11.5px;margin-bottom:10px;
  display:block;overflow-x:auto;white-space:nowrap}
table.wide th{color:#61748b;font-weight:500;text-align:left;padding:4px 9px;
  border-bottom:1px solid #1e2937;font-size:10px;text-transform:uppercase;letter-spacing:.05em}
table.wide td{padding:4px 9px;border-bottom:1px solid #151d27;
  font-family:ui-monospace,Menlo,monospace}
.prov{background:#12203a;border:1px solid #1d4ed8;border-radius:9px;padding:12px 16px;
  margin-bottom:18px;font-size:13px;color:#bfdbfe}
.prov b{color:#93c5fd}
.prov .chip{display:inline-block;padding:2px 9px;border-radius:6px;font-weight:650;
  font-family:ui-monospace,Menlo,monospace;margin:0 4px}
.chip.ok{background:#0f766e;color:#eafffb}
.chip.hot{background:#78350f;color:#fbbf24}
.chip.crit{background:#7f1d1d;color:#fca5a5}
.back{color:#7d90a6;text-decoration:none;font-size:12.5px}
.back:hover{color:#5eead4}
.num{text-align:right}
.dim{color:#5b6b80}
footer{color:#4d5c70;font-size:11.5px;padding:0 28px 40px;max-width:none}
code{font-family:ui-monospace,Menlo,monospace;color:#8fa6bf}
"""


def kpi(name, value, cls="", sub=None):
    extra = f'<div class="ksub">{sub}</div>' if sub else ""
    return (f'<div class="kpi {cls}"><div class="k">{name}</div>'
            f'<div class="v">{value}</div>{extra}</div>')


def slot_link(slot, key=None, val=None, stat=None, rng=None):
    q = {"slot": slot}
    if key:
        q.update(**{"from": key, "val": f"{val:.0f}", "stat": stat or "",
                    "band": band(val, key) or "ok"})
    if rng:
        q["rng"] = rng
    return f'<a class="slotlink" href="/slot?{urllib.parse.urlencode(q)}">{slot}</a>'


def tail_block(points, field, key, p99, rng=None):
    trips = [(p[field], p["slot"], p["time"]) for p in points if p[field] >= p99]
    if not trips:
        return ""
    per_slot = {}
    for val, sl, ts in trips:
        prev = per_slot.get(sl)
        per_slot[sl] = (max(val, prev[0]) if prev else val,
                        prev[1] if prev else ts,
                        (prev[2] + 1) if prev else 1)
    ordered = sorted(per_slot.items(), key=lambda kv: kv[1][0], reverse=True)[:25]
    rows = "".join(
        f'<tr class="{band(v, key)}">'
        f'<td class="mono">{slot_link(sl, key, v, "tail", rng)}</td>'
        f'<td class="mono num">{fmt_val(v, key)}</td><td class="num">{n}</td>'
        f'<td class="mono dim">{str(ts)[:19].replace("T"," ")}Z</td></tr>'
        for sl, (v, ts, n) in ordered
    )
    return (f'<div class="lbl">p99 tail &mdash; {len(trips)} point(s) &ge; '
            f'{fmt_val(p99, key)}, across {len(per_slot)} slot(s)</div>'
            '<table class="tail"><thead><tr><th>slot</th><th class="num">peak</th>'
            f'<th class="num">pts</th><th>when (UTC)</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>')


def card(points, field, key, title, blurb, rng=None):
    vals = [p[field] for p in points]
    st = stats(vals)
    head = f'<h2>{html.escape(title)}</h2><div class="blurb">{blurb}</div>'
    if not st:
        return f'<div class="card">{head}<div class="empty">no points in range</div></div>'
    p95_cls = band(st["p95"], key)
    p99_cls, max_cls = band(st["p99"], key), band(st["max"], key)
    peak_sub = None
    if max_cls:
        peak = max(points, key=lambda p: p[field])
        peak_sub = f'slot {slot_link(peak["slot"], key, st["max"], "max", rng)}'
    tail = tail_block(points, field, key, st["p99"], rng) if (max_cls or p99_cls) else ""
    return f"""<div class="card">{head}
      <div class="kpis">
        {kpi("points", f'{st["n"]:,}')}
        {kpi("mean", fmt_val(st["mean"], key))}
        {kpi("p50", fmt_val(st["p50"], key))}
        {kpi("p90", fmt_val(st["p90"], key))}
        {kpi("p95", fmt_val(st["p95"], key), p95_cls)}
        {kpi("p99", fmt_val(st["p99"], key), p99_cls)}
        {kpi("max", fmt_val(st["max"], key), max_cls, peak_sub)}
      </div>
      {tail}
      <div class="charts">
        <div class="pane"><div class="lbl">distribution</div>
          {histogram_svg(vals, key)}</div>
        <div class="pane"><div class="lbl">by slot number</div>
          {slot_series_svg(points, field, key)}</div>
      </div>
    </div>"""


def table(cols, rows, cls="wide", limit=200):
    if not rows:
        return '<div class="empty">no rows</div>'
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join(
        "<tr>" + "".join(
            f'<td>{html.escape("" if str(c) == chr(92) + "N" else str(c))[:64]}</td>'
            for c in r) + "</tr>"
        for r in rows[:limit]
    )
    more = (f'<div class="dim" style="font-size:11.5px">+{len(rows)-limit} more rows</div>'
            if len(rows) > limit else "")
    return f'<table class="{cls}"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>{more}'


CARDS = [
    ("sim-extend", "body_us", "body_us",
     "body_us (extend)",
     "Wall clock inside <code>state.extend</code> (<code>workers.rs:283-285</code>). "
     "The round's critical path - the mutation lane is blocked this long."),
    ("sim-commit", "body_us", "body_us_commit",
     "body_us (commit)",
     "Wall clock inside <code>state.commit_round</code> (<code>workers.rs:331-333</code>). "
     "A promote is a pointer move and costs microseconds; a replay re-executes a whole "
     "winning block."),
    ("sim-extend", "exec_wall_us", "exec_wall", "exec_wall_us (extend)",
     "Wall clock of <code>run_stream</code> alone (<code>replay.rs</code>, "
     "<code>stats.exec_wall_us</code>) - the execution phase without sanitize, the slot "
     "lock or checkpointing. Same axis as <code>body_us</code>, so "
     "<code>exec_wall_us / body_us</code> is the share of the call that was actually "
     "executing, and everything else is the unmeasured remainder."),
    ("sim-commit", "exec_wall_us", "exec_wall_commit", "exec_wall_us (commit)",
     "The same execution-phase wall clock on the commit side. A promote runs no stream "
     "and records 0."),
    ("sim-extend", "execute_us", "execute_us", "execute_us (extend)",
     "BPF execution summed across replay workers - <b>CPU time, not wall clock</b>, so it "
     "routinely exceeds <code>exec_wall_us</code>. The ratio "
     "<code>execute_us / exec_wall_us</code> is the effective parallelism actually "
     "achieved, against <code>exec_pool</code> = 8 available."),
    ("sim-extend", "load_us", "load_us", "load_us (extend)",
     "Account loading, also a CPU sum across workers. Since the override map is now "
     "consulted lazily in <code>do_load</code> rather than copied per loader, this no "
     "longer scales with overlay size."),
    ("sim-extend", "exec_pool", "exec_pool", "exec_pool (extend)",
     "Replay workers available to the batch. Constant at 8 when a stream ran; <b>0 means "
     "<code>run_stream</code> was never called</b> - a refusal or a promote. That makes it "
     "the cleanest \"did this batch execute\" predicate, cleaner than "
     "<code>layer_count &gt; 0</code>."),
    ("sim-mutation-lane", "age_us", "age_us",
     "age_us",
     "Age of the in-flight mutation, sampled every 500ms by the watchdog thread "
     "(<code>metrics.rs:109-114</code>), busy samples only. Not per-call: it is the only "
     "signal that can see a job which never returns, since a hung call emits no point."),
    ("sim-extend", "queue_us", "queue_us",
     "queue_us",
     "Wait in the bounded(1) mutation channel before the worker took the job "
     "(<code>workers.rs:192-280</code>). Lane-busy requests emit no point at all."),
    ("sim-extend", "layer_count", "depth",
     "layer_count \u2014 critical path (extend)",
     "Wire name <code>layer_count</code>, but since the DAG-stream migration it carries "
     "<code>stream.critical_path</code> = <code>max(height) + 1</code>: the <b>longest "
     "dependency chain</b> in the batch (<code>replay.rs</code> build_stream). That is the "
     "batch's serial floor - no amount of workers executes it faster. Zero means the batch "
     "executed nothing: a refusal or a promote."),
    ("sim-extend", "max_layer_width", "width",
     "max_layer_width \u2014 initial width (extend)",
     "Wire name <code>max_layer_width</code>, but it now carries "
     "<code>stream.initial_width</code>: orders with <b>in-degree zero</b>, i.e. how many "
     "could dispatch immediately. It is <b>not</b> the widest wave - orders unblock as "
     "verdicts land, so peak concurrency can exceed this. Compare against "
     "<code>exec_pool</code> = 8: below that, the pool starts partly idle."),
    ("sim-commit", "layer_count", "depth_commit",
     "layer_count \u2014 critical path (commit)",
     "Same measure on the commit side. A promote executes nothing and records 0; a replay "
     "carries a whole winning block, so its chains run far deeper than an extend's."),
    ("sim-commit", "max_layer_width", "width_commit",
     "max_layer_width \u2014 initial width (commit)",
     "Independent orders at the head of a commit replay. Wide here means the winner's "
     "block had a lot of mutually disjoint orders to start on."),
    ("sim-extend", "account_cache_clone_us", "acct_clone", "account_cache_clone_us (extend)",
     "Time spent copying the reactor's private overlay into the immutable snapshot the "
     "workers read (<code>replay.rs</code>, <code>working.clone()</code>). Taken "
     "<b>once per wave and only when the overlay changed</b> - the DAG-stream rewrite "
     "replaced the old per-layer <code>Arc::make_mut</code>, and this is the field that "
     "made the cost visible."),
    ("sim-extend", "account_cache_entries_cloned", "acct_entries",
     "account_cache_entries_cloned (extend)",
     "How many accounts those snapshots carried, summed over the batch's waves. Divide by "
     "the wave count for the overlay size; against <code>overlay_len</code> it says how "
     "many times the batch re-copied the same state."),
    ("sim-commit", "account_cache_clone_us", "acct_clone_commit",
     "account_cache_clone_us (commit)",
     "The same per-wave overlay snapshot on the commit-replay side, where batches are "
     "far wider and the overlay is larger."),
    ("sim-extend", "program_cache_us", "pc_us",
     "program_cache_us (extend)",
     "Time in <code>replenish_program_cache_for_simulation</code> "
     "(<code>transaction_processor.rs:585</code>): take the global cache read lock, "
     "extract hits, compile the misses. A fresh <code>ProgramCacheForTxBatch</code> is "
     "built per order (<code>replay.rs:127</code>), so this recurs every order."),
    ("sim-extend", "program_cache_clone_us", "pc_clone_us",
     "program_cache_clone_us",
     "Cost of forking the shared <code>ProgramCacheSnapshot</code> "
     "(<code>replay.rs:441-450</code>). The fork is copy-on-write and happens at most "
     "once per batch, only if some order modified a program - so this is zero on the "
     "overwhelming majority of batches."),
    ("sim-commit", "program_cache_us", "pc_us_commit",
     "program_cache_us (commit)",
     "The same program-cache path on the commit-replay side."),
]


def page(rng, mode, nslots):
    errs, host, data = [], None, {}
    try:
        host = active_host()
    except Exception as exc:
        errs.append(html.escape(str(exc)))
    data, ferrs = fetch_all(SLOT_LOOKBACK if mode == "slots" else rng, host)
    errs += ferrs

    scope_note = f"time window <b>last {rng}</b>"
    if mode == "slots":
        # The slot window is defined by sim-extend, then applied to every source.
        base = sorted({p["slot"] for p in data.get("sim-extend", [])}, reverse=True)
        keep = set(base[:nslots])
        if keep:
            data = {m: [p for p in pts if p["slot"] in keep] for m, pts in data.items()}
            scope_note = (f"last <b>{len(keep)}</b> slots with an extend &mdash; "
                          f"<code>{min(keep)}</code> &rarr; <code>{max(keep)}</code>"
                          f" (resolved over a {SLOT_LOOKBACK} lookback)")

    rngs = "".join(
        f'<a class="rng {"on" if mode=="time" and r==rng else ""}" '
        f'href="/?mode=time&range={r}&n={nslots}">{r}</a>' for r in RANGES
    )
    slot_btns = "".join(
        f'<a class="rng {"on" if mode=="slots" and n==nslots else ""}" '
        f'href="/?mode=slots&range={rng}&n={n}">{n}</a>' for n in SLOT_PRESETS
    )
    cards = [card(data.get(m, []), f, k, t, b, rng) for m, f, k, t, b in CARDS]
    err_html = f'<div class="scope err">{"; ".join(errs)}</div>' if errs else ""
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench - sim-extend by slot</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">{MEASUREMENT} &middot; host_id {html.escape((host or "?")[:12])}&hellip;
    &middot; influx {INFLUX_DB}@{_good_host or INFLUX_HOSTS[0]}</div>
  <div class="ctl">
    <div class="row"><span class="cap">by time</span>{rngs}</div>
    <div class="row"><span class="cap">last N slots</span>{slot_btns}
      <form method="get" class="inline">
        <input type="hidden" name="mode" value="slots">
        <input type="hidden" name="range" value="{rng}">
        <input type="number" name="n" min="1" max="20000" value="{nslots}">
        <button type="submit">go</button>
      </form>
    </div>
  </div>
</header>
<main>{err_html}<div class="scope">{scope_note}</div>
<div class="grid">{"".join(cards)}</div></main>
<footer>Every point is one <b>accepted</b> Extend; lane-busy requests are refused
before the worker and emit nothing. Refusals and promotes record all-zero stage
stats (<code>state.rs:52</code>). Amber &gt; 25 ms
(<code>STALL_WARN_US</code>, metrics.rs:20) &middot; red &gt; 47 ms (one round);
<code>queue_us</code> amber &gt; 1 ms, depth &gt; 16, width &gt; 8 (the pool size).
</footer></body></html>"""


def influx_rows(measurement, slot, host, fields="*"):
    where = f'"slot" = {slot}'
    if host:
        where += f" AND \"host_id\" = '{host}'"
    cols, rows = influx(f'SELECT {fields} FROM "{measurement}" WHERE {where} ORDER BY time')
    drop = {"host_id"}
    keep = [i for i, c in enumerate(cols) if c not in drop]
    trim = lambda v: (str(v)[11:23] if v and str(v)[:2] == "20" else v)
    return ([cols[i] for i in keep],
            [[trim(r[i]) if i == 0 else (r[i] if r[i] is not None else "") for i in keep]
             for r in rows])


def slot_page(slot, prov, rng):
    panels, errs = [], []
    host = None
    try:
        host = active_host()
    except Exception as exc:
        errs.append(f"influx host: {html.escape(str(exc))}")

    # --- why you are here -------------------------------------------------
    if prov.get("from"):
        b = prov.get("band") or "ok"
        try:
            shown = fmt_val(float(prov["val"]), prov["from"])
        except (TypeError, ValueError):
            shown = prov.get("val", "?")
        label = {"crit": "RED", "hot": "AMBER", "ok": "green"}.get(b, b)
        prov_html = (
            f'<div class="prov">You arrived from <b>{html.escape(prov["from"])}</b>'
            f' &mdash; the <b>{html.escape(prov.get("stat") or "value")}</b> for this slot was'
            f'<span class="chip {b}">{shown}</span>'
            f'({label}; thresholds for this metric are '
            f'{fmt_val(bands_for(prov["from"])[0], prov["from"])} amber / '
            f'{fmt_val(bands_for(prov["from"])[1], prov["from"])} red).</div>')
    else:
        prov_html = ""

    # --- sim-extend -------------------------------------------------------
    try:
        c, r = influx_rows("sim-extend", slot, host)
        panels.append(("sim-extend &mdash; every accepted Extend on this slot",
                       "One row per accepted Extend (<code>workers.rs:288</code>). "
                       "<code>layer_count = 0</code> means the batch executed nothing.",
                       table(c, r)))
    except Exception as exc:
        errs.append(f"sim-extend: {html.escape(str(exc))}")

    # --- sim-commit -------------------------------------------------------
    try:
        c, r = influx_rows("sim-commit", slot, host)
        panels.append(("sim-commit &mdash; how each round closed",
                       "<code>winner</code>: 0 empty / 1 promote (our prefix won) / 2 replay. "
                       "<code>promoted_len</code> is -1 unless an applied promote. "
                       "<code>refusal</code> 5 = PREFIX_LEN_MISMATCH, which forces a replay "
                       "of a round we did win (<code>state.rs:669</code>).",
                       table(c, r)))
    except Exception as exc:
        errs.append(f"sim-commit: {html.escape(str(exc))}")

    # --- bifrost_miniblocks ----------------------------------------------
    try:
        c, r = clickhouse(
            "SELECT ts, kind, index_in_slot, is_last, won_by_us, reward, order_count, "
            "transaction_count, bundle_count, execution_cost, selected_cu, "
            "local_builder_id, connector_identity, uuid "
            f"FROM bifrost_miniblocks WHERE slot = {slot} ORDER BY ts")
        panels.append(("bifrost_miniblocks &mdash; offers shipped and the winner echo",
                       "<code>kind=selected</code> is an offer we sent; "
                       "<code>kind=winner</code> is the relay's echo of what won. "
                       "<code>won_by_us</code> is the authoritative ownership flag.",
                       table(c, r)))
    except Exception as exc:
        errs.append(f"miniblocks: {html.escape(str(exc))}")

    # --- bifrost_events ---------------------------------------------------
    try:
        c, r = clickhouse(
            "SELECT stage, event, reason, count() AS rows, uniqExact(entity) AS entities, "
            "min(ts) AS first_ts, max(ts) AS last_ts FROM bifrost_events "
            f"WHERE nums['slot'] = {slot} GROUP BY stage, event, reason "
            "ORDER BY rows DESC LIMIT 60")
        panels.append(("bifrost_events &mdash; what the builder did on this slot",
                       "Grouped by stage/event/reason. <code>check_dropped</code> reasons are "
                       "simulator error text; <code>validator:executed</code> is the "
                       "connector's per-transaction result.",
                       table(c, r)))
    except Exception as exc:
        errs.append(f"events: {html.escape(str(exc))}")

    body = "".join(
        f'<div class="card"><h2>{t}</h2><div class="blurb">{b}</div>{content}</div>'
        for t, b, content in panels)
    err_html = f'<div class="scope err">{"; ".join(errs)}</div>' if errs else ""
    back = f'/?mode=time&range={rng}' if rng else '/'
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>slot {slot} - simbench</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span> &middot; slot {slot}</h1>
  <div class="sub">everything three stores know about this slot</div>
  <div class="ctl"><a class="back" href="{back}">&larr; back to the overview</a></div>
</header>
<main>{prov_html}{err_html}<div class="grid one">{body}</div></main>
<footer>Sources: InfluxDB <code>sim-extend</code> / <code>sim-commit</code> (host_id
{html.escape((host or "?")[:12])}&hellip;), ClickHouse
<code>block_builder.bifrost_miniblocks</code> and <code>bifrost_events</code>.</footer>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/slot":
            try:
                slot = int(qs.get("slot", ["0"])[0])
            except ValueError:
                slot = 0
            prov = {k: qs.get(k, [""])[0] for k in ("from", "val", "stat", "band")}
            try:
                body = slot_page(slot, prov, qs.get("rng", [""])[0]).encode()
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
        rng = qs.get("range", ["24h"])[0]
        if rng not in RANGES:
            rng = "24h"
        mode = qs.get("mode", ["time"])[0]
        if mode not in ("time", "slots"):
            mode = "time"
        try:
            nslots = max(1, min(20000, int(qs.get("n", ["100"])[0])))
        except ValueError:
            nslots = 100
        try:
            body = page(rng, mode, nslots).encode()
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
