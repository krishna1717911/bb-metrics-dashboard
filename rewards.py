#!/usr/bin/env python3
"""GBX vs Agave Harmonic per-slot revenue -- the /rewards page.

Kept out of app.py deliberately: a self-contained feature over a static
rewards_daily.json, where app.py is already 8k lines several people edit.
app.py needs only the route and the nav link.

The data is per-day 0.5 mSOL histograms carrying both a slot count and an exact
SOL sum per bin. Histograms are what make an arbitrary date range exact: bin
counts and sums add across days, percentiles do not. Every curve, percentile
and trimmed mean here is computed from summed bins at request time, so ?from=
and ?to= can name any sub-range without a re-pull.

Three line panels, in the order the argument runs:
  1. the ECDF -- what the two distributions look like
  2. the quantile ratio harmonic(p)/gbx(p) -- WHERE they differ
  3. the cumulative share of the mean gap -- how much of the total gap each
     part of the distribution accounts for
then the Sankeys, which follow the money rather than compare it.

Regenerating the data is an offline step (Dune + reports.firedancer.io + kobe);
nothing here queries anything at request time.
"""

import html
import json
import os

REWARDS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "rewards_daily.json")

BIN = 0.0005          # SOL per histogram bucket
TRIM_LO, TRIM_HI = 0.01, 0.99
C_GBX = "#3987e5"
C_HARM = "#d95926"
C_FEE = "#199e70"
C_JITO = "#c98500"
C_STAKE = "#9085e9"
C_OTHER = "#d55181"
C_RATIO = "#c98500"
C_GAP = "#3987e5"


def _pool(days, dates, side):
    """Sum a cohort's histograms and totals over a date range.

    Exact: both bin counts and per-bin SOL sums add. This is the whole reason
    the source is histograms rather than per-day percentiles.
    """
    h, out = {}, dict(slots=0, fee=0.0, jito_gross=0.0, jito_net=0.0,
                      other=0.0, like=0.0, with_jito=0, with_other=0,
                      comm_num=0.0, vals=0)
    for d in dates:
        e = days[d][side]
        for b, c, sm in zip(e["bins"], e["counts"], e["sums"]):
            r = h.setdefault(b, [0, 0.0])
            r[0] += c
            r[1] += sm
        for k in ("slots", "with_jito", "with_other"):
            out[k] += e[k]
        for k in ("fee", "jito_gross", "jito_net", "other", "like"):
            out[k] += e[k]
        out["comm_num"] += e["comm_bps"] * e["slots"]
        out["vals"] = max(out["vals"], e.get("validators", 11))
    out["hist"] = h
    out["comm_bps"] = out["comm_num"] / out["slots"] if out["slots"] else 0
    return out


def _pct(h, total, q):
    """Exact nearest-rank percentile from a summed histogram, in SOL."""
    target, c = q * total, 0
    for b in sorted(h):
        c += h[b][0]
        if c >= target:
            return b * BIN
    return 0.0


def _pct_smooth(h, total, q):
    """Percentile interpolated INSIDE the containing bin.

    _pct returns the bin's left edge, so every quantile lands on a 0.5 mSOL
    lattice -- across p1..p99 that is only ~76 distinct values. A ratio of two
    such numbers can only sit on a lattice too, which is the sawtooth on the
    quantile-ratio panel. It is not a display-precision problem: printing more
    decimals just prints the steps more exactly.

    Interpolating on rank within the bin makes the quantile a continuous
    function of q, which is also strictly closer to the underlying value than
    the left edge is.
    """
    target, c = q * total, 0
    for b in sorted(h):
        n = h[b][0]
        if n and c + n >= target:
            return (b + (target - c) / n) * BIN
        c += n
    return max(h) * BIN if h else 0.0


def _quantiles(h, total, n=99):
    """q[1..n] in SOL -- the grid both comparison panels are computed on."""
    return [_pct_smooth(h, total, i / (n + 1)) for i in range(1, n + 1)]


def _trimmed_mean(h, total, lo=TRIM_LO, hi=TRIM_HI):
    """Mean with the tails outside [lo, hi] dropped.

    Uses each bin's exact SOL sum rather than its midpoint, so only the two
    boundary bins are apportioned (at half weight) and everything between them
    is exact. Returns (mean SOL, slots kept).
    """
    def bin_at(q):
        t, c = q * total, 0
        for b in sorted(h):
            c += h[b][0]
            if c >= t:
                return b
        return max(h) if h else 0
    b_lo, b_hi = bin_at(lo), bin_at(hi)
    cnt, tot = 0.0, 0.0
    for b in sorted(h):
        if b < b_lo or b > b_hi:
            continue
        k, sm = h[b]
        w = 0.5 if (b == b_lo or b == b_hi) else 1.0
        cnt += k * w
        tot += sm * w
    return (tot / cnt / 1e9 if cnt else 0.0), cnt


def _curve(h, total, xmax, n=200):
    cum, run = {}, 0
    for b in sorted(h):
        run += h[b][0]
        cum[b] = run
    bs, out, j, r = sorted(cum), [], 0, 0
    for i in range(n + 1):
        bi = int((xmax * i / n) / BIN)
        while j < len(bs) and bs[j] <= bi:
            r = cum[bs[j]]
            j += 1
        out.append((xmax * i / n, r / total if total else 0))
    return out


def _sankey_flows(P):
    """Per-slot flows for one cohort. Everything divided by that cohort's own
    slot count, so a 77k-slot cohort and a 1.5M-slot one are on one scale."""
    n = max(P["slots"], 1)
    fee = P["fee"] / 1e9 / n
    jg = P["jito_gross"] / 1e9 / n
    jn = P["jito_net"] / 1e9 / n
    oth = P["other"] / 1e9 / n
    keep = jn * P["comm_bps"] / 10000.0
    return dict(fee=fee, jg=jg, jn=jn, oth=oth, jito_cut=jg - jn,
                keep=keep, stake=jn - keep, gross=fee + jg + oth)


def _pool_cu(days, dates, side):
    """Sum absolute SOL per (band, bucket) over a range, then normalise.

    Stored as SOL rather than shares precisely so this is a plain sum -- shares
    could not be pooled across days any more than percentiles could.
    """
    grid = [[0.0] * 10 for _ in range(100)]
    for d in dates:
        cu = days[d][side].get("cu")
        if not cu:
            continue
        for p in range(100):
            for b in range(10):
                grid[p][b] += cu[p][b]
    out, totals = [], []
    for p in range(100):
        t = sum(grid[p])
        totals.append(t)
        out.append([(v / t * 100 if t else 0.0) for v in grid[p]])
    return out, totals




REWARDS_CSS = """
.rw-wrap{margin:16px 28px 34px}
.rw-hero{display:flex;gap:10px;flex-wrap:wrap;margin:0 0 18px}
.rw-tile{background:#0e151d;border:1px solid #1e2937;border-radius:10px;
  padding:12px 16px;min-width:148px}
.rw-tile .k{color:#6b7f96;font-size:10px;text-transform:uppercase;
  letter-spacing:.07em}
.rw-tile .v{color:#dbe4ee;font-size:20px;font-weight:650;margin-top:5px;
  font-variant-numeric:tabular-nums}
.rw-tile .d{color:#6b7f96;font-size:11px;margin-top:3px}
.rw-tile .up{color:#5eead4}
.rw-tile .dn{color:#fca5a5}
.rw-grid{display:grid;gap:16px;margin-bottom:16px;
  grid-template-columns:repeat(auto-fit,minmax(440px,1fr))}
.rw-grid>.rw-box{margin-bottom:0}
.rw-box{background:#0e151d;border:1px solid #1e2937;border-radius:11px;
  padding:13px 15px 9px;overflow-x:auto;margin-bottom:16px}
.rw-box.wide{grid-column:1/-1}
.rw-box h2{margin:0 0 6px;font-size:13px;font-weight:650;color:#dbe4ee}
.rw-box .cs{color:#6b7f96;font-size:10.5px;margin:-3px 0 8px;line-height:1.45}
details.rw-more{margin:10px 0 0}
details.rw-more>summary{cursor:pointer;color:#6b7f96;font-size:10.5px;
  list-style:none;padding:3px 0;user-select:none}
details.rw-more>summary::-webkit-details-marker{display:none}
details.rw-more>summary:before{content:"▸ ";color:#5eead4}
details.rw-more[open]>summary:before{content:"▾ "}
details.rw-more>summary:hover{color:#5eead4}
details.rw-more .body{color:#6b7f96;font-size:11px;line-height:1.6;
  padding:8px 0 2px;border-top:1px solid #1e2937;margin-top:4px}
.rw-strip{margin:0 28px 14px;color:#6b7f96;font-size:11px;line-height:1.6}
.rw-strip b{color:#5eead4;font-weight:600}
.rw-legend{display:flex;gap:16px;flex-wrap:wrap;margin:8px 0 2px;
  padding-top:11px;border-top:1px solid #1e2937}
.rw-lg{display:flex;align-items:center;gap:7px;color:#9fb2c8;font-size:11.5px}
.rw-sw{width:15px;height:3px;border-radius:2px;flex:none}
.rw-stats{display:flex;gap:0;flex-wrap:wrap;margin-top:12px;
  border:1px solid #1e2937;border-radius:9px;overflow:hidden}
.rw-stats div{flex:1;min-width:170px;padding:12px 15px;
  border-right:1px solid #1e2937}
.rw-stats div:last-child{border-right:none}
.rw-stats .sv{color:#dbe4ee;font-size:19px;font-weight:650;
  font-variant-numeric:tabular-nums}
.rw-stats .sk{color:#8fa6bf;font-size:11px;margin-top:4px}
.rw-stats .ss{color:#6b7f96;font-size:10px;margin-top:2px}
.rw-tbl{width:100%;border-collapse:collapse;margin-top:8px;font-size:12px}
.rw-tbl th{text-align:right;color:#6b7f96;font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.06em;padding:7px 9px;
  border-bottom:1px solid #22303f;white-space:nowrap}
.rw-tbl th:first-child{text-align:left}
.rw-tbl td{text-align:right;padding:6px 9px;border-bottom:1px solid #141c26;
  font-variant-numeric:tabular-nums;color:#c3d3e6}
.rw-tbl td:first-child{text-align:left;color:#dbe4ee}
.rw-tbl tr.win td{background:#0f766e18}
.rw-tbl tr.out td{opacity:.42}
.rw-key{display:grid;grid-template-columns:repeat(auto-fit,minmax(275px,1fr));
  gap:4px 18px;margin-top:10px;padding-top:10px;border-top:1px solid #1e2937}
.rw-key div{color:#6b7f96;font-size:11px;line-height:1.55}
.rw-key b{color:#9fb2c8;font-weight:600}
.rw-note{color:#6b7f96;font-size:11px;margin-top:9px;line-height:1.65}
.rw-ctl{display:flex;gap:16px;flex-wrap:wrap;align-items:center;margin:-2px 0 8px}
.rw-ctl .g{display:flex;gap:4px;align-items:center}
.rw-ctl .lb{color:#6b7f96;font-size:10px;text-transform:uppercase;
  letter-spacing:.06em;margin-right:2px}
.rw-ctl a{color:#8fa6bf;font-size:10.5px;text-decoration:none;padding:3px 9px;
  border:1px solid #22303f;border-radius:5px}
.rw-ctl a:hover{border-color:#5eead4;color:#5eead4}
.rw-ctl a.on{background:#12314d;border-color:#3987e5;color:#7cc0ff;font-weight:650}
.rw-zoom{display:flex;gap:6px;align-items:center;color:#4d5c70;font-size:10px;
  margin:4px 0 0}
.rw-zoom b{color:#6b7f96;font-weight:600}
.rw-zoom button{background:#0e151d;border:1px solid #22303f;border-radius:5px;
  color:#8fa6bf;font:10px ui-monospace,Menlo,monospace;padding:2px 8px;cursor:pointer}
.rw-zoom button:hover{border-color:#5eead4;color:#5eead4}
.rw-ok{color:#5eead4}
form.rw-range{display:flex;gap:9px;align-items:flex-end;flex-wrap:wrap;
  margin:0 0 14px}
form.rw-range label{display:flex;flex-direction:column;gap:4px;
  color:#6b7f96;font-size:10px;text-transform:uppercase;letter-spacing:.06em}
form.rw-range select,form.rw-range button{background:#0e151d;
  border:1px solid #22303f;border-radius:6px;color:#cfe0f0;padding:5px 10px;
  font:12px ui-monospace,Menlo,monospace}
form.rw-range button{color:#5eead4;border-color:#14b8a655;cursor:pointer}
form.rw-range button:hover{background:#0f766e22;border-color:#5eead4}
form.rw-range .q{display:flex;gap:5px;align-items:center;margin-left:6px}
form.rw-range .q a{color:#8fa6bf;font-size:10.5px;text-decoration:none;
  padding:4px 8px;border:1px solid #22303f;border-radius:5px}
form.rw-range .q a:hover{border-color:#5eead4;color:#5eead4}
#rw-tip{position:fixed;display:none;z-index:80;background:#0d151d;
  border:1px solid #2f4256;border-radius:8px;padding:9px 11px;
  box-shadow:0 10px 30px #000a;font-size:11.5px;color:#c3d3e6;
  pointer-events:none;min-width:210px}
#rw-tip .tx{color:#dbe4ee;font-weight:650;margin-bottom:6px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr{display:flex;align-items:center;gap:7px;margin-top:3px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr span:last-child{margin-left:auto;color:#dbe4ee}
"""

CU_LABELS = ["0-10k", "10-50k", "50-100k", "100-150k", "150-200k",
             "200-250k", "250-300k", "300-500k", "500k-1m", "1m+"]

# Viridis. Perceptually uniform and monotone in lightness, so height and colour
# agree instead of competing -- a jet/turbo ramp reverses lightness mid-scale
# and invents banding that is not in the data.
VIRIDIS = ["#440154", "#472d7b", "#3b528b", "#2c728e", "#21918c",
           "#27ad81", "#5ec962", "#aadc32", "#fde725"]


def _lerp_hex(a, b, t):
    ar, ag, ab = int(a[1:3], 16), int(a[3:5], 16), int(a[5:7], 16)
    br, bg, bb = int(b[1:3], 16), int(b[3:5], 16), int(b[5:7], 16)
    return "#%02x%02x%02x" % (round(ar + (br - ar) * t),
                              round(ag + (bg - ag) * t),
                              round(ab + (bb - ab) * t))


def _viridis(f):
    f = max(0.0, min(1.0, f))
    x = f * (len(VIRIDIS) - 1)
    i = min(int(x), len(VIRIDIS) - 2)
    return _lerp_hex(VIRIDIS[i], VIRIDIS[i + 1], x - i)




# ---------------------------------------------------------------- Plotly specs
# Figures are built as plain dicts and handed to Plotly in the browser. Nothing
# is drawn here: zoom, pan, box-select, hover, legend toggling, 3-D orbit, PNG
# export and autoscale all come from the library rather than from hand-rolled
# SVG and event handlers.

PLOTLY_CDN = "https://cdn.plot.ly/plotly-3.0.1.min.js"

# dark ground matching the dashboard's panels
AXIS = dict(gridcolor="#1a2431", zerolinecolor="#243242", linecolor="#243242",
            tickfont=dict(color="#6b7f96", size=11),
            titlefont=dict(color="#8fa6bf", size=12))
LAYOUT = dict(
    paper_bgcolor="#0e151d", plot_bgcolor="#0b131c",
    font=dict(color="#c3d3e6", family="ui-sans-serif,-apple-system,Segoe UI,sans-serif"),
    margin=dict(l=64, r=24, t=16, b=52),
    hoverlabel=dict(bgcolor="#0d151d", bordercolor="#2f4256",
                    font=dict(color="#c3d3e6", size=12)),
    legend=dict(orientation="h", y=-0.18, x=0, font=dict(color="#9fb2c8", size=11),
                bgcolor="rgba(0,0,0,0)"),
    dragmode="zoom",
)
CONFIG = dict(displaylogo=False, responsive=True,
              modeBarButtonsToRemove=["lasso2d", "select2d"],
              toImageButtonOptions=dict(format="png", scale=2))


def _fig_ecdf(G, Hm, xmax):
    """Both cohorts' ECDFs. Hover is unified on x so the two are read together."""
    def tr(P, name, colour):
        pts = _curve(P["hist"], P["slots"], xmax)
        xs = [x for x, _ in pts]
        ys = [f * 100 for _, f in pts]
        return dict(type="scatter", mode="lines", name=f'{name} (n={P["slots"]:,})',
                    x=xs, y=ys, line=dict(color=colour, width=2.4),
                    hovertemplate="%{x:.6f} SOL")
    traces = [tr(Hm, "Agave Harmonic", C_HARM), tr(G, "GBX", C_GBX)]
    for P, colour, nm in ((Hm, C_HARM, "Harmonic"), (G, C_GBX, "GBX")):
        qs = [0.50, 0.90]
        traces.append(dict(
            type="scatter", mode="markers", showlegend=False,
            x=[_pct(P["hist"], P["slots"], q) for q in qs], y=[q * 100 for q in qs],
            marker=dict(color=colour, size=10, line=dict(color="#0e151d", width=2)),
            text=[f"{nm} p{int(q*100)}" for q in qs],
            hovertemplate="%{text}: %{x:.6f} SOL<extra></extra>"))
    lay = dict(LAYOUT, height=430,
               xaxis=dict(AXIS, title="per-slot revenue (SOL) — fee + Jito tip net of 6%",
                          range=[0, xmax]),
               yaxis=dict(AXIS, title="cumulative probability", ticksuffix="%",
                          range=[0, 100], hoverformat=".1f"),
               hovermode="y unified")
    return dict(data=traces, layout=lay, config=CONFIG)


def _fig_ratio(qg, qh):
    rat = [(h / g if g > 0 else 1.0) for g, h in zip(qg, qh)]
    ps = list(range(1, 100))
    traces = [
        dict(type="scatter", mode="lines", name="harmonic / gbx", x=ps, y=rat,
             line=dict(color=C_RATIO, width=2.4),
             hovertemplate="%{y:.6f}×<extra></extra>"),
        dict(type="scatter", mode="lines", name="parity", x=[1, 99], y=[1, 1],
             line=dict(color="#8fa6bf", width=1.4, dash="solid"), hoverinfo="skip"),
    ]
    lay = dict(LAYOUT, height=330,
               xaxis=dict(AXIS, title="percentile", tickprefix="p",
                          hoverformat="d"),
               yaxis=dict(AXIS, title="harmonic ÷ gbx", ticksuffix="×",
                          hoverformat=".6f"),
               hovermode="x unified", showlegend=False)
    return dict(data=traces, layout=lay, config=CONFIG)


def _fig_gap(qg, qh):
    d = [h - g for g, h in zip(qg, qh)]
    tot = sum(d) or 1.0
    run, cum = 0.0, []
    for v in d:
        run += v
        cum.append(run / tot * 100)
    ps = list(range(1, 100))
    traces = [
        dict(type="scatter", mode="lines", name="even split", x=[1, 99], y=[0, 100],
             line=dict(color="#6b7f96", width=1.2, dash="dash"), hoverinfo="skip"),
        dict(type="scatter", mode="lines", name="cumulative share", x=ps, y=cum,
             line=dict(color=C_GAP, width=2.4), fill="tozeroy",
             fillcolor="rgba(57,135,229,0.10)",
             hovertemplate="p1–p%{x} hold %{y:.2f}% of the gap<extra></extra>"),
    ]
    lay = dict(LAYOUT, height=330,
               xaxis=dict(AXIS, title="percentile", tickprefix="p",
                          hoverformat="d"),
               yaxis=dict(AXIS, title="share of the mean gap", ticksuffix="%",
                          hoverformat=".2f"),
               hovermode="x unified", showlegend=False)
    return dict(data=traces, layout=lay, config=CONFIG)


def _fig_cu3d(days, dates, side, render, hscale, inspect):
    """The compute-unit field as a real 3-D scene -- Plotly supplies the orbit."""
    grid, _ = _pool_cu(days, dates, side)
    zmax = max((v for row in grid for v in row), default=1.0) or 1.0
    z = [[((v / zmax) ** 0.5 * zmax if hscale == "sqrt" else v) for v in row]
         for row in grid]
    xs, ys = CU_LABELS, list(range(1, 101))
    hover = ("percentile p%{y}<br>%{x} CU<br>"
             "%{customdata:.2f}% of the percentile's priority fee<extra></extra>")
    cd = grid
    if render == "points":
        fx, fy, fz, fc = [], [], [], []
        for p in range(100):
            for b in range(10):
                fx.append(xs[b]); fy.append(p + 1); fz.append(z[p][b]); fc.append(grid[p][b])
        traces = [dict(type="scatter3d", mode="markers", name="share",
                       x=fx, y=fy, z=fz, customdata=fc,
                       marker=dict(size=3.4, color=fc, colorscale="Viridis",
                                   cmin=0, cmax=zmax, opacity=0.88,
                                   colorbar=dict(title="%", thickness=12, len=0.7,
                                                 tickfont=dict(color="#6b7f96", size=10))),
                       hovertemplate=hover)]
    elif render == "ribbons":
        traces = []
        for b in range(10):
            traces.append(dict(
                type="scatter3d", mode="lines", name=xs[b],
                x=[xs[b]] * 100, y=ys, z=[z[p][b] for p in range(100)],
                customdata=[grid[p][b] for p in range(100)],
                line=dict(width=5, color=[grid[p][b] for p in range(100)],
                          colorscale="Viridis", cmin=0, cmax=zmax),
                hovertemplate=hover))
    else:
        traces = [dict(type="surface", name="share", x=xs, y=ys, z=z,
                       customdata=cd, colorscale="Viridis", cmin=0, cmax=zmax,
                       opacity=0.95, hovertemplate=hover,
                       colorbar=dict(title="%", thickness=12, len=0.7,
                                     tickfont=dict(color="#6b7f96", size=10)),
                       contours=dict(z=dict(show=True, usecolormap=True,
                                            project=dict(z=True))))]
    # the inspected slice, drawn over the scene
    ip = max(1, min(100, inspect)) - 1
    traces.append(dict(type="scatter3d", mode="lines+markers",
                       name=f"p{ip+1}", x=xs, y=[ip + 1] * 10,
                       z=[z[ip][b] for b in range(10)], customdata=grid[ip],
                       line=dict(color="#ffffff", width=6),
                       marker=dict(color="#ffffff", size=4),
                       hovertemplate=hover))
    ax3 = dict(backgroundcolor="#0b131c", gridcolor="#1a2431", zerolinecolor="#243242",
               showbackground=True, tickfont=dict(color="#6b7f96", size=10),
               titlefont=dict(color="#8fa6bf", size=11))
    lay = dict(LAYOUT, height=620, margin=dict(l=8, r=8, t=8, b=8),
               scene=dict(
                   xaxis=dict(ax3, title="compute units"),
                   yaxis=dict(ax3, title="reward percentile"),
                   zaxis=dict(ax3, title="share of percentile (%)"),
                   camera=dict(eye=dict(x=1.7, y=-1.5, z=0.9)),
                   aspectratio=dict(x=1, y=1.7, z=0.7)),
               showlegend=False)
    return dict(data=traces, layout=lay, config=CONFIG)


def _fig_sankey(P, title, colour):
    """Native Sankey. Per slot, so the two cohorts are comparable."""
    F = _sankey_flows(P)
    lbl = ["fees", "Jito tips (gross)", "Titan/Bifrost", "distributable",
           "Jito 6% commission", "validator keeps", "stakers"]
    src = [0, 1, 1, 2, 3, 3]
    tgt = [3, 3, 4, 3, 5, 6]
    val = [F["fee"], F["jn"], F["jito_cut"], F["oth"], F["fee"] + F["keep"] + F["oth"],
           F["stake"]]
    link_c = ["rgba(25,158,112,0.38)", "rgba(201,133,0,0.38)", "rgba(201,133,0,0.6)",
              "rgba(213,81,129,0.38)", "rgba(57,135,229,0.30)", "rgba(144,133,233,0.5)"]
    node_c = [C_FEE, C_JITO, C_OTHER, colour, C_JITO, colour, C_STAKE]
    tr = dict(type="sankey", orientation="h", valueformat=".6f", valuesuffix=" SOL",
              node=dict(pad=16, thickness=14, label=lbl, color=node_c,
                        line=dict(color="#0e151d", width=1)),
              link=dict(source=src, target=tgt, value=val, color=link_c))
    lay = dict(LAYOUT, height=300, margin=dict(l=10, r=10, t=28, b=10),
               title=dict(text=f"{title} — SOL per slot, n={P['slots']:,}",
                          font=dict(color="#8fa6bf", size=12), x=0.01),
               showlegend=False)
    return dict(data=[tr], layout=lay, config=CONFIG)


COL_KEY = [
    ("day", "the report's own slot range for that date, not a UTC calendar day"),
    ("GBX slots", "blocks the 11 GBX validators produced that day"),
    ("H vals", "how many Agave Harmonic validators ran that day (47&ndash;62; "
               "membership is resolved per day)"),
    ("H slots", "blocks the Agave Harmonic cohort produced that day"),
    ("GBX mean / H mean", "trimmed mean SOL per slot, p1&ndash;p99, of fee + "
                          "Jito tip net of 6%"),
    ("GBX vs H", "GBX trimmed mean over Harmonic's, as a percent"),
    ("GBX comm / H comm", "block-weighted validator MEV commission &mdash; the "
                          "share of each tip the operator keeps, stakers get the rest"),
    ("GBX other", "Titan/Bifrost tips per slot, on routes Harmonic never "
                  "receives; excluded from every comparison here"),
]


def rewards_page(CSS, purl, d_from=None, d_to=None, opts=None):
    with open(REWARDS_JSON) as fh:
        D = json.load(fh)
    meta, days = D["meta"], D["days"]
    all_dates = sorted(days)
    if d_from not in days:
        d_from = all_dates[0]
    if d_to not in days:
        d_to = all_dates[-1]
    if d_from > d_to:
        d_from, d_to = d_to, d_from
    dates = [d for d in all_dates if d_from <= d <= d_to]

    o = opts or {}
    cu_side = o.get("cu") if o.get("cu") in ("gbx", "harm") else "gbx"
    render = o.get("render") if o.get("render") in ("surface", "ribbons", "points") else "surface"
    hscale = o.get("hscale") if o.get("hscale") in ("linear", "sqrt") else "linear"
    try:
        inspect = max(1, min(100, int(o.get("insp") or 99)))
    except ValueError:
        inspect = 99

    G, Hm = _pool(days, dates, "gbx"), _pool(days, dates, "harm")
    gt, g_kept = _trimmed_mean(G["hist"], G["slots"])
    ht, h_kept = _trimmed_mean(Hm["hist"], Hm["slots"])
    gmed = _pct(G["hist"], G["slots"], .50)
    hmed = _pct(Hm["hist"], Hm["slots"], .50)
    d_mean = (gt / ht - 1) * 100 if ht else 0
    d_med = (gmed / hmed - 1) * 100 if hmed else 0
    qg = _quantiles(G["hist"], G["slots"])
    qh = _quantiles(Hm["hist"], Hm["slots"])
    xmax = max(_pct(G["hist"], G["slots"], .90),
               _pct(Hm["hist"], Hm["slots"], .90)) * 1.6 or 0.1

    figs = {
        "ecdf": _fig_ecdf(G, Hm, xmax),
        "ratio": _fig_ratio(qg, qh),
        "gap": _fig_gap(qg, qh),
        "cu3d": _fig_cu3d(days, dates, cu_side, render, hscale, inspect),
        "skg": _fig_sankey(G, "GBX", C_GBX),
        "skh": _fig_sankey(Hm, "Agave Harmonic", C_HARM),
    }

    rat = [(h / g if g > 0 else 1.0) for g, h in zip(qg, qh)]
    dgap = [h - g for g, h in zip(qg, qh)]
    r90 = sum(dgap[:90]) / sum(dgap) if sum(dgap) else 0
    wins = sum(1 for d in dates
               if days[d]["gbx"]["like"] / max(days[d]["gbx"]["slots"], 1)
               > days[d]["harm"]["like"] / max(days[d]["harm"]["slots"], 1))

    tiles = [
        ("GBX trimmed mean", f"{gt:.6f}", f"{G['slots']:,} slots, p1&ndash;p99", ""),
        ("Agave Harmonic", f"{ht:.6f}",
         f"{Hm['slots']:,} slots, {Hm['vals']} vals", ""),
        ("GBX vs Harmonic", f"{d_mean:+.1f}%", "on the trimmed mean",
         "up" if d_mean > 0 else "dn"),
        ("median gap", f"{d_med:+.1f}%", f"{gmed:.4f} vs {hmed:.4f} SOL",
         "up" if d_med > 0 else "dn"),
        ("GBX tip share", f"{G['jito_net']/G['like']*100:.1f}%" if G["like"] else "&mdash;",
         f"Harmonic {Hm['jito_net']/Hm['like']*100:.1f}%" if Hm["like"] else "", ""),
        ("MEV commission", f"{G['comm_bps']/100:.1f}%",
         f"Harmonic {Hm['comm_bps']/100:.1f}% &mdash; they keep more", ""),
    ]
    hero = "".join(f'<div class="rw-tile"><div class="k">{k}</div>'
                   f'<div class="v {c}">{v}</div><div class="d">{d}</div></div>'
                   for k, v, d, c in tiles)
    stats = (
        f'<div class="rw-stats">'
        f'<div><div class="sv">{gt:.4f} SOL</div>'
        f'<div class="sk">GBX mean, p1&ndash;p99</div>'
        f'<div class="ss">tail values outside p1&ndash;p99 excluded</div></div>'
        f'<div><div class="sv">{ht:.4f} SOL</div>'
        f'<div class="sk">Harmonic mean, p1&ndash;p99</div>'
        f'<div class="ss">tail values outside p1&ndash;p99 excluded</div></div>'
        f'<div><div class="sv">{d_mean:+.1f}%</div>'
        f'<div class="sk">GBX mean relative to Harmonic</div></div>'
        f'<div><div class="sv">{d_med:+.1f}%</div>'
        f'<div class="sk">GBX median relative to Harmonic</div></div></div>')

    opts_html = lambda sel: "".join(
        f'<option value="{d}"{" selected" if d == sel else ""}>{d}</option>'
        for d in all_dates)
    dep_val = purl("/rewards").partition("dep=")[2]
    base = purl("/rewards")
    quick = "".join(
        f'<a href="{base}&from={all_dates[max(0,len(all_dates)-n)]}'
        f'&to={all_dates[-1]}">{lab}</a>'
        for n, lab in ((7, "last 7d"), (14, "last 14d"), (len(all_dates), "all")))
    rng = (f'<form class="rw-range" method="get" action="/rewards">'
           f'<input type="hidden" name="dep" value="{html.escape(dep_val)}">'
           f'<label>from<select name="from">{opts_html(d_from)}</select></label>'
           f'<label>to<select name="to">{opts_html(d_to)}</select></label>'
           f'<button type="submit">apply</button>'
           f'<span class="q">{quick}</span></form>')

    def ctl(name, cur, choices):
        out = []
        for val, lab in choices:
            cv = dict(cu=cu_side, render=render, hscale=hscale)
            cv[name] = val
            href = (f'{base}&from={d_from}&to={d_to}&cu={cv["cu"]}'
                    f'&render={cv["render"]}&hscale={cv["hscale"]}&insp={inspect}')
            out.append(f'<a class="{"on" if val == cur else ""}" href="{href}">{lab}</a>')
        return "".join(out)
    cu_ctl = (
        f'<div class="rw-ctl">'
        f'<span class="g"><span class="lb">cohort</span>'
        f'{ctl("cu", cu_side, [("gbx","GBX"),("harm","Agave Harmonic")])}</span>'
        f'<span class="g"><span class="lb">render</span>'
        f'{ctl("render", render, [("surface","Surface"),("ribbons","Ribbons"),("points","Points")])}</span>'
        f'<span class="g"><span class="lb">height</span>'
        f'{ctl("hscale", hscale, [("linear","Linear"),("sqrt","Square root")])}</span>'
        f'</div>')

    cu_grid, _ = _pool_cu(days, dates, cu_side)
    insp_row = "".join(f'<td>{cu_grid[inspect-1][b]:.1f}%</td>' for b in range(10))
    cu_slots = sum(days[d][cu_side]["slots"] for d in dates)
    cu_side_label = "GBX" if cu_side == "gbx" else "Agave Harmonic"

    rows = []
    for d in all_dates:
        g, h = days[d]["gbx"], days[d]["harm"]
        gp, hp = _pool(days, [d], "gbx"), _pool(days, [d], "harm")
        a, _ = _trimmed_mean(gp["hist"], gp["slots"])
        b, _ = _trimmed_mean(hp["hist"], hp["slots"])
        dd = (a / b - 1) * 100 if b else 0
        inr = d_from <= d <= d_to
        cls = ("win" if dd > 0 else "") + ("" if inr else " out")
        rows.append(
            f'<tr class="{cls.strip()}"><td>{d}</td><td>{g["slots"]:,}</td>'
            f'<td>{h.get("validators","")}</td><td>{h["slots"]:,}</td>'
            f'<td>{a:.6f}</td><td>{b:.6f}</td>'
            f'<td class="{"greenc" if dd > 0 else "redc"}">{dd:+.1f}%</td>'
            f'<td>{g["comm_bps"]/100:.1f}%</td><td>{h["comm_bps"]/100:.1f}%</td>'
            f'<td>{g["other"]/max(g["slots"],1)/1e9:.6f}</td></tr>')
    key = "".join(f'<div><b>{k}</b> &mdash; {v}</div>' for k, v in COL_KEY)
    cav = "".join(f"<li>{html.escape(c)}</li>" for c in meta["caveats"])
    figs_json = json.dumps(figs, separators=(",", ":"))

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; GBX vs Agave Harmonic</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="{PLOTLY_CDN}" charset="utf-8"></script>
<style>{CSS}{REWARDS_CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">GBX vs Agave Harmonic &mdash; per-slot revenue,
    {d_from} to {d_to}</div>
  <a class="navlink" href="{purl("/")}">&larr; back to slots</a>
  <a class="navlink" href="{purl("/reference")}">metrics reference</a>
</header>

<div class="rw-strip">
  <b>Verified against reports.firedancer.io on all {len(all_dates)} days.</b>
  Reward = fee + Jito tip net of 6%; means trimmed to p1&ndash;p99;
  GBX's Titan/Bifrost tips excluded from every comparison.
  <details class="rw-more" style="display:inline-block;margin-left:6px">
    <summary>method, caveats and sources</summary>
    <div class="body">
      <b>Verification.</b> {html.escape(meta['verification'])}<br>
      <b>Basis.</b> {html.escape(meta['basis'])}<br>
      <b>Like-for-like.</b> {html.escape(meta['like_note'])}<br>
      <b>Means.</b> {html.escape(meta.get('trimmed_note',''))}<br>
      <b>Money flow.</b> {html.escape(meta['sankey_note'])}<br>
      <b>Compute mix.</b> {html.escape(meta.get('cu_note',''))}<br>
      <b>Before quoting a number.</b>
      <ul style="margin:4px 0 0 18px;padding:0">{cav}</ul>
      <b>Sources.</b> histograms <code>{html.escape(meta['sources']['hist'])}</code>;
      commission <code>{html.escape(meta['sources']['commission'])}</code>;
      compute mix <code>{html.escape(meta['sources'].get('cu',''))}</code>;
      reference <code>{html.escape(meta['sources']['report'])}</code>.
    </div>
  </details>
</div>

<div class="rw-wrap">
  {rng}
  <div class="rw-hero">{hero}</div>

  <div class="rw-box wide">
    <h2>Rewards distribution &mdash; {d_from} to {d_to}</h2>
    <div class="cs">Share of slots earning at or below <i>x</i>. Drag to zoom a
      region, double-click to reset.</div>
    <div id="fig-ecdf"></div>
    {stats}
  </div>

  <div class="rw-grid">
  <div class="rw-box">
    <h2>Quantile ratio &mdash; harmonic / gbx</h2>
    <div class="cs">Above 1.0 Harmonic leads. Rising = the gap widens with
      block value.</div>
    <div id="fig-ratio"></div>
  </div>
  <div class="rw-box">
    <h2>Cumulative share of the mean gap ({ht-gt:.6f} SOL)</h2>
    <div class="cs">Below the diagonal = concentrated in the tail.
      p1&ndash;p90 hold {r90*100:.0f}%.</div>
    <div id="fig-gap"></div>
  </div>
  </div>

  <div class="rw-box wide">
    <h2>Where compute goes, percentile by percentile</h2>
    <div class="cs">Each reward percentile's priority fee split across ten
      compute-unit bins; height is that bin's share, so every slice sums to
      100%. Drag to orbit, scroll to zoom. Aggregated over every slot in the
      band, not the one slot at the cut.</div>
    {cu_ctl}
    <div id="fig-cu3d"></div>
    <table class="rw-tbl" style="margin-top:4px">
      <thead><tr><th>p{inspect} &mdash; the highlighted slice</th>
        {"".join(f"<th>{l}</th>" for l in CU_LABELS)}</tr></thead>
      <tbody><tr><td>share of priority fee</td>{insp_row}</tr></tbody>
    </table>
    <div class="rw-note">{cu_side_label}, {len(dates)} day{"s" if len(dates)!=1 else ""},
      {cu_slots:,} slots &mdash; about {cu_slots//100:,} per percentile band.</div>
  </div>

  <div class="rw-grid">
  <div class="rw-box"><div id="fig-skg"></div></div>
  <div class="rw-box"><div id="fig-skh"></div></div>
  </div>
  <div class="rw-note" style="margin:-8px 0 16px">Both Sankeys are per slot on
    the same footing. Harmonic's operators run {Hm['comm_bps']/100:.1f}% MEV
    commission against GBX's {G['comm_bps']/100:.1f}%, so more of each tip stays
    with the validator rather than the stakers.</div>

  <table class="rw-tbl">
    <thead><tr><th>day</th><th>GBX slots</th><th>H vals</th><th>H slots</th>
      <th>GBX mean</th><th>H mean</th><th>GBX vs H</th>
      <th>GBX comm</th><th>H comm</th><th>GBX other</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <details class="rw-more">
    <summary>what each column means</summary>
    <div class="body"><div class="rw-key" style="border:none;margin:0;padding:0">
      {key}</div>
      Shaded rows are days GBX led; faded rows fall outside the selected range.
    </div>
  </details>
</div>

<script>
var FIGS = {figs_json};
(function(){{
  function draw(){{
    if (!window.Plotly) {{ return setTimeout(draw, 60); }}
    Object.keys(FIGS).forEach(function(k){{
      var el = document.getElementById('fig-' + k);
      if (el) Plotly.newPlot(el, FIGS[k].data, FIGS[k].layout, FIGS[k].config);
    }});
  }}
  draw();
}})();
</script>
</body></html>"""
