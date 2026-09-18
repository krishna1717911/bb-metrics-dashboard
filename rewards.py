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


def _quantiles(h, total, n=99):
    """q[1..n] in SOL -- the grid both comparison panels are computed on."""
    return [_pct(h, total, i / (n + 1)) for i in range(1, n + 1)]


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


def _axes(svg, L, T, pw, ph, xlab, ylab, xt, yt, W, H):
    for val, y in yt:
        svg.append(f'<line x1="{L}" y1="{y:.1f}" x2="{L+pw}" y2="{y:.1f}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{L-9}" y="{y+3.5:.1f}" text-anchor="end" '
                   f'fill="#6b7f96" font-size="10.5">{val}</text>')
    for val, x in xt:
        svg.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T+ph}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{x:.1f}" y="{T+ph+18}" text-anchor="middle" '
                   f'fill="#6b7f96" font-size="10.5">{val}</text>')
    svg.append(f'<text x="{L+pw/2:.0f}" y="{H-8}" text-anchor="middle" '
               f'fill="#8fa6bf" font-size="11.5">{xlab}</text>')
    svg.append(f'<text x="14" y="{T+ph/2:.0f}" fill="#8fa6bf" font-size="11.5" '
               f'transform="rotate(-90 14 {T+ph/2:.0f})" '
               f'text-anchor="middle">{ylab}</text>')


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


def _sankey(P, side, title, col, scale_gross, W=900, H=430):
    """Where a cohort's gross inflow ends up, PER SLOT, over the range.

    Three columns: source stream -> intermediate -> destination. Ribbon height
    is proportional to SOL per slot, and `scale_gross` is shared by both
    cohorts' diagrams, so ribbons are comparable BETWEEN them and not just
    within one. Without that the two would each fill the panel and look equal.
    """
    F = _sankey_flows(P)
    fee, jg, jn, oth = F["fee"], F["jg"], F["jn"], F["oth"]
    jito_cut, keep, stake, gross = F["jito_cut"], F["keep"], F["stake"], F["gross"]
    if gross <= 0 or scale_gross <= 0:
        return "", []
    T, B = 42, 24
    ph = H - T - B
    scale = ph / scale_gross
    x0, x2, x3 = 16, W * 0.42, W - 118
    sv = []

    def node(x, y, h, c, lab, val, anchor="start"):
        # label sits ABOVE the bar: at 620 wide there is no room for three
        # columns of side-set text without collisions.
        h = max(h, 1.2)
        sv.append(f'<rect x="{x:.0f}" y="{y:.1f}" width="9" height="{h:.1f}" '
                  f'fill="{c}" rx="2"/>')
        sv.append(f'<text x="{x:.0f}" y="{y-4:.1f}" fill="#c3d3e6" '
                  f'font-size="9.5" text-anchor="start">{lab} '
                  f'<tspan fill="#6b7f96">{val:.5f}</tspan></text>')

    def link(xa, ya, xb, yb, h, c, op=0.34):
        h = max(h, 1.2)
        mx = (xa + xb) / 2
        sv.append(f'<path d="M{xa+9:.0f},{ya:.1f} C{mx:.0f},{ya:.1f} '
                  f'{mx:.0f},{yb:.1f} {xb:.0f},{yb:.1f} L{xb:.0f},{yb+h:.1f} '
                  f'C{mx:.0f},{yb+h:.1f} {mx:.0f},{ya+h:.1f} '
                  f'{xa+9:.0f},{ya+h:.1f} Z" fill="{c}" opacity="{op}"/>')

    # column 1: sources
    y = T
    y_fee, h_fee = y, fee * scale
    node(x0, y_fee, h_fee, C_FEE, "fees", fee); y += h_fee + 16
    y_jg, h_jg = y, jg * scale
    node(x0, y_jg, h_jg, C_JITO, "Jito gross", jg); y += h_jg + 16
    y_o, h_o = y, oth * scale
    if oth > 0:
        node(x0, y_o, h_o, C_OTHER, "Titan/Bifrost", oth)

    # column 2: after Jito's cut
    y = T
    y_dist, h_dist = y, (fee + jn + oth) * scale
    node(x2, y_dist, h_dist, col, "distributable", fee + jn + oth)
    y_cut = y_dist + h_dist + 18
    node(x2, y_cut, jito_cut * scale, C_JITO, "Jito 6%", jito_cut)

    link(x0, y_fee, x2, y_dist, h_fee, C_FEE)
    link(x0, y_jg, x2, y_dist + h_fee, jn * scale, C_JITO)
    link(x0, y_jg + jn * scale, x2, y_cut, jito_cut * scale, C_JITO, 0.55)
    if oth > 0:
        link(x0, y_o, x2, y_dist + h_fee + jn * scale, h_o, C_OTHER)

    # column 3: validator vs stakers
    y = T
    y_keep, h_keep = y, (fee + keep + oth) * scale
    node(x3, y_keep, h_keep, col, "validator", fee + keep + oth)
    y_st = y_keep + h_keep + 18
    node(x3, y_st, stake * scale, C_STAKE, "stakers", stake)
    link(x2, y_dist, x3, y_keep, h_fee, C_FEE)
    link(x2, y_dist + h_fee, x3, y_keep + h_fee, keep * scale, C_JITO)
    link(x2, y_dist + h_fee + keep * scale, x3, y_st, stake * scale, C_STAKE, 0.5)
    if oth > 0:
        link(x2, y_dist + h_fee + jn * scale, x3,
             y_keep + h_fee + keep * scale, h_o, C_OTHER)

    sv.append(f'<text x="{x0}" y="14" fill="#8fa6bf" font-size="11" '
              f'font-weight="600">{title} '
              f'<tspan fill="#6b7f96">&mdash; SOL/slot, n={P["slots"]:,}</tspan>'
              f'</text>')
    facts = [("gross inflow / slot", gross), ("fees / slot", fee),
             ("Jito tips gross / slot", jg), ("Jito 6% cut / slot", jito_cut),
             ("to stakers / slot", stake),
             ("validator keeps / slot", fee + keep + oth)]
    return (f'<svg viewBox="0 0 {W} {H}" '
            f'style="width:100%;display:block" role="img" '
            f'aria-label="{title} revenue flow: gross inflow to Jito '
            f'commission, stakers and the validator. Values in the table '
            f'below.">{"".join(sv)}</svg>'), facts


def _panel_ratio(qg, qh, W=900, H=430):
    """harmonic(p) / gbx(p) at each percentile.

    A ratio rather than two lines because the question is where the cohorts
    differ, not what either earns. 1.0 is parity; the shaded band marks the
    percentiles where GBX is ahead.
    """
    L, R, T, B = 66, 60, 22, 44
    pw, ph = W - L - R, H - T - B
    rat = [(h / g if g > 0 else 1.0) for g, h in zip(qg, qh)]
    lo = min(0.9, min(rat) * 0.98)
    hi = max(1.1, max(rat) * 1.02)
    sx = lambda i: L + pw * i / (len(rat) - 1)
    sy = lambda v: T + ph * (1 - (v - lo) / (hi - lo))
    sv = []
    ticks = [(f"p{p}", sx(p - 1)) for p in (1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99)]
    ysteps = [lo + (hi - lo) * i / 4 for i in range(5)]
    _axes(sv, L, T, pw, ph, "percentile", "",
          ticks, [(f"{v:.2f}×", sy(v)) for v in ysteps], W, H)
    # parity line
    if lo <= 1.0 <= hi:
        sv.append(f'<line x1="{L}" y1="{sy(1):.1f}" x2="{L+pw}" y2="{sy(1):.1f}" '
                  f'stroke="#8fa6bf" stroke-width="1.4"/>')
    # shade where GBX leads
    under = [i for i, v in enumerate(rat) if v < 1.0]
    if under and lo <= 1.0 <= hi:
        runs, cur = [], [under[0]]
        for i in under[1:]:
            (cur.append(i) if i == cur[-1] + 1 else (runs.append(cur), cur := [i]))
        runs.append(cur)
        for rn in runs:
            pts = " ".join(f"{sx(i):.1f},{sy(rat[i]):.1f}" for i in rn)
            sv.append(f'<polygon points="{sx(rn[0]):.1f},{sy(1):.1f} {pts} '
                      f'{sx(rn[-1]):.1f},{sy(1):.1f}" fill="{C_GBX}" '
                      f'opacity="0.18"/>')
        e = runs[0][-1]
        sv.append(f'<text x="{sx(runs[0][0])+4:.1f}" y="{sy(1)+16:.1f}" '
                  f'fill="{C_GBX}" font-size="10" font-weight="600">GBX ahead, '
                  f'p{runs[0][0]+1}&ndash;p{e+1}</text>')
    pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(rat))
    sv.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
              f'stroke-width="4.4" stroke-linejoin="round"/>')
    sv.append(f'<polyline points="{pts}" fill="none" stroke="{C_RATIO}" '
              f'stroke-width="2.2" stroke-linejoin="round"/>')
    sv.append(f'<text x="{sx(len(rat)-1):.1f}" y="{sy(rat[-1])-9:.1f}" '
              f'text-anchor="end" fill="{C_RATIO}" font-size="10.5" '
              f'font-weight="600">{rat[-1]:.2f}× at p99</text>')
    return (f'<svg viewBox="0 0 {W} {H}" data-zoom="1" '
            f'style="width:100%;display:block;cursor:grab" role="img" '
            f'aria-label="Ratio of Agave Harmonic to GBX revenue at each '
            f'percentile. Values in the table below.">{"".join(sv)}</svg>'), rat


def _panel_gap(qg, qh, W=900, H=430):
    """Cumulative share of the total mean gap, by percentile.

    Each percentile contributes (harm(p) - gbx(p)) / n to the difference in
    trimmed means. Plotting the running share against the diagonal shows
    whether the gap is spread evenly or concentrated: a curve below the
    diagonal means the tail is doing the work.
    """
    L, R, T, B = 66, 60, 22, 44
    pw, ph = W - L - R, H - T - B
    d = [h - g for g, h in zip(qg, qh)]
    tot = sum(d)
    sv = []
    sx = lambda i: L + pw * i / (len(d) - 1)
    sy = lambda f: T + ph * (1 - f)
    _axes(sv, L, T, pw, ph, "percentile", "",
          [(f"p{p}", sx(p - 1)) for p in (1, 10, 25, 50, 75, 90, 99)],
          [(f"{int(f*100)}%", sy(f)) for f in (0, .25, .5, .75, 1)], W, H)
    sv.append(f'<line x1="{L}" y1="{sy(0):.1f}" x2="{L+pw}" y2="{sy(1):.1f}" '
              f'stroke="#6b7f96" stroke-width="1" stroke-dasharray="4 4"/>')
    run, pts = 0.0, []
    for i, v in enumerate(d):
        run += v
        pts.append(f"{sx(i):.1f},{sy(run/tot if tot else 0):.1f}")
    sv.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="#0e151d" '
              f'stroke-width="4.4" stroke-linejoin="round"/>')
    sv.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{C_GAP}" '
              f'stroke-width="2.2" stroke-linejoin="round"/>')
    # how much of the gap sits above p90
    r90 = sum(d[:90]) / tot if tot else 0
    sv.append(f'<text x="{L+pw-6:.0f}" y="{sy(.52):.1f}" text-anchor="end" '
              f'fill="#8fa6bf" font-size="9.5" font-style="italic">below the '
              f'diagonal &rarr; tail-concentrated</text>')
    return (f'<svg viewBox="0 0 {W} {H}" data-zoom="1" '
            f'style="width:100%;display:block;cursor:grab" role="img" '
            f'aria-label="Cumulative share of the total mean gap by '
            f'percentile.">{"".join(sv)}</svg>'), tot, r90


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


def _panel_cu3d(days, dates, side, render, hscale, inspect, W=1180, H=680):
    """Container + data for the orbiting compute-unit field.

    The projection is done in the browser (see the CU3D module), not here:
    dragging re-projects every frame, and keeping a second copy of the maths
    server-side would be two implementations drifting apart. Python's job is to
    pool the bands over the selected range and hand over the grid.
    """
    grid, totals = _pool_cu(days, dates, side)
    zmax = max((v for row in grid for v in row), default=1.0) or 1.0
    ramp = [[int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)] for c in VIRIDIS]
    data = {
        "grid": [[round(v, 4) for v in row] for row in grid],
        "zmax": round(zmax, 4),
        "labels": CU_LABELS,
        "ramp": ramp,
        "sqrt": hscale == "sqrt",
        "render": render,
        "insp": max(1, min(100, inspect)),
        "az0": 0.62, "el0": 0.60,
    }
    key = []
    kx, ky = W - 74, 58
    for i in range(48):
        key.append(f'<rect x="{kx}" y="{ky + (47-i)*3.4:.1f}" width="11" height="3.6" '
                   f'fill="{_viridis(i/47)}"/>')
    key.append(f'<text x="{kx+16}" y="{ky+6}" fill="#6b7f96" font-size="9">'
               f'{zmax:.0f}%</text>')
    key.append(f'<text x="{kx+16}" y="{ky+47*3.4+6:.0f}" fill="#6b7f96" '
               f'font-size="9">0%</text>')
    svg = (f'<svg viewBox="0 0 {W} {H}" style="width:100%;display:block;cursor:grab" '
           f'role="img" aria-label="Compute-unit bucket against reward percentile, '
           f'height is that bucket\'s share of the percentile\'s priority fee. '
           f'Orbit with the mouse. Values in the table below.">'
           f'<g id="cu3d"></g>{"".join(key)}'
           f'<text x="{W/2:.0f}" y="{H-16}" fill="#8fa6bf" font-size="11" '
           f'text-anchor="middle">compute units &rarr; &middot; reward percentile '
           f'&rarr; &middot; height = share of that percentile</text></svg>'
           f'<noscript><div class="rw-note">This panel is drawn in the browser so '
           f'it can be orbited; the numbers are in the table below.</div></noscript>')
    return svg, grid, totals, data



COL_KEY = [
    ("day", "the report's own slot range for that date, not a UTC calendar day"),
    ("GBX slots", "blocks the 11 GBX validators produced that day"),
    ("H vals", "how many Agave Harmonic validators ran that day (50&ndash;61; "
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
    render = o.get("render") if o.get("render") in ("surface", "ribbons", "points") else "points"
    hscale = o.get("hscale") if o.get("hscale") in ("linear", "sqrt") else "linear"
    cam = o.get("cam") if o.get("cam") in ("iso", "along", "over") else "iso"
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

    # ---------------- panel 1: ECDF
    W2, H2 = 1500, 470
    L2, R2, T2, B2 = 78, 150, 18, 48
    pw2, ph2 = W2 - L2 - R2, H2 - T2 - B2
    xmax = max(_pct(G["hist"], G["slots"], .90),
               _pct(Hm["hist"], Hm["slots"], .90)) * 1.6 or 0.1
    sx2 = lambda v: L2 + pw2 * min(v / xmax, 1.0)
    sy2 = lambda f: T2 + ph2 * (1 - f)
    p2 = []
    step = xmax / 6
    _axes(p2, L2, T2, pw2, ph2,
          "per-slot revenue (SOL) &mdash; fee + Jito tip net of 6%",
          "cumulative probability",
          [(f"{step*i:.3f}", sx2(step * i)) for i in range(7)],
          [(f"{i*10}%", sy2(i / 10)) for i in range(11)], W2, H2)
    for P, c in ((Hm, C_HARM), (G, C_GBX)):
        pts = " ".join(f"{sx2(x):.1f},{sy2(f):.1f}"
                       for x, f in _curve(P["hist"], P["slots"], xmax))
        p2.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
                  f'stroke-width="4.8" stroke-linejoin="round"/>')
        p2.append(f'<polyline points="{pts}" fill="none" stroke="{c}" '
                  f'stroke-width="2.3" stroke-linejoin="round"/>')
    for P, c in ((Hm, C_HARM), (G, C_GBX)):
        for q in (.50, .90):
            v = _pct(P["hist"], P["slots"], q)
            if v <= xmax:
                dy = -11 if c == C_GBX else 15
                p2.append(f'<circle cx="{sx2(v):.1f}" cy="{sy2(q):.1f}" r="4.4" '
                          f'fill="{c}" stroke="#0e151d" stroke-width="2"/>')
                p2.append(f'<text x="{sx2(v):.1f}" y="{sy2(q)+dy:.1f}" '
                          f'text-anchor="middle" fill="{c}" font-size="9.5" '
                          f'font-weight="600">{v:.4f}</text>')
    p2.append(f'<line id="rw-cross" x1="{L2}" y1="0" x2="{L2+pw2}" y2="0" '
              f'stroke="#5eead4" stroke-width="1" stroke-dasharray="3 3" '
              f'opacity="0"/>')
    p2.append(f'<circle id="rw-dg" r="5" fill="none" stroke="{C_GBX}" '
              f'stroke-width="2" opacity="0"/>')
    p2.append(f'<circle id="rw-dh" r="5" fill="none" stroke="{C_HARM}" '
              f'stroke-width="2" opacity="0"/>')
    p2.append(f'<rect id="rw-hit" x="{L2}" y="{T2}" width="{pw2}" '
              f'height="{ph2}" fill="transparent"/>')

    ratio_svg, rat = _panel_ratio(qg, qh)
    gap_svg, gap_tot, r90 = _panel_gap(qg, qh)

    cu_svg, cu_grid, cu_tot, cu_data = _panel_cu3d(
        days, dates, cu_side, render, hscale, inspect)

    sg_max = max(_sankey_flows(G)["gross"], _sankey_flows(Hm)["gross"])
    sk_g, f_g = _sankey(G, "gbx", "GBX", C_GBX, sg_max)
    sk_h, f_h = _sankey(Hm, "harm", "Agave Harmonic", C_HARM, sg_max)

    # ---------------- chrome
    tiles = [
        ("GBX trimmed mean", f"{gt:.6f}", f"{G['slots']:,} slots, p1&ndash;p99", ""),
        ("Agave Harmonic", f"{ht:.6f}",
         f"{Hm['slots']:,} slots, {Hm['vals']} vals", ""),
        ("GBX vs Harmonic", f"{d_mean:+.1f}%", "on the trimmed mean",
         "up" if d_mean > 0 else "dn"),
        ("median gap", f"{d_med:+.1f}%",
         f"{gmed:.4f} vs {hmed:.4f} SOL", "up" if d_med > 0 else "dn"),
        ("GBX tip share", f"{G['jito_net']/G['like']*100:.1f}%"
         if G["like"] else "&mdash;",
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

    opts = lambda sel: "".join(
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
           f'<label>from<select name="from">{opts(d_from)}</select></label>'
           f'<label>to<select name="to">{opts(d_to)}</select></label>'
           f'<button type="submit">apply</button>'
           f'<span class="q">{quick}</span></form>')

    lg = (f'<div class="rw-lg"><span class="rw-sw" style="background:{C_GBX}">'
          f'</span>GBX n={G["slots"]:,}</div>'
          f'<div class="rw-lg"><span class="rw-sw" style="background:{C_HARM}">'
          f'</span>Agave Harmonic n={Hm["slots"]:,}</div>')
    sk_lg = "".join(
        f'<div class="rw-lg"><span class="rw-sw" style="background:{c}"></span>'
        f'{n}</div>' for c, n in ((C_FEE, "fees &mdash; kept in full"),
                                  (C_JITO, "Jito tips &amp; its 6% cut"),
                                  (C_STAKE, "staker share"),
                                  (C_OTHER, "Titan/Bifrost (GBX only)")))

    rows = []
    for d in all_dates:
        g, h = days[d]["gbx"], days[d]["harm"]
        gp = _pool(days, [d], "gbx")
        hp = _pool(days, [d], "harm")
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


    # one link per option; the panel is server-rendered so state lives in the URL
    def ctl(name, cur, opts_):
        qs = lambda k, v: (f'{base}&from={d_from}&to={d_to}&cu={cu_side}'
                           f'&render={render}&hscale={hscale}&cam={cam}&insp={inspect}'
                           ).replace(f'&{k}={{"cu":cu_side,"render":render,"hscale":hscale,'
                                     f'"cam":cam}}[k]', f'&{k}={v}')
        out = []
        for val, lab in opts_:
            cur_vals = dict(cu=cu_side, render=render, hscale=hscale, cam=cam)
            cur_vals[name] = val
            href = (f'{base}&from={d_from}&to={d_to}&cu={cur_vals["cu"]}'
                    f'&render={cur_vals["render"]}&hscale={cur_vals["hscale"]}'
                    f'&cam={cur_vals["cam"]}&insp={inspect}')
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
        f'<span class="g"><span class="lb">camera</span>'
        f'<a data-cam="0.62,0.60" class="on">Isometric</a>'
        f'<a data-cam="1.57,0.32">Along percentile</a>'
        f'<a data-cam="0.00,1.50">Overhead</a>'
        f'<a data-cam="0.00,0.10">Front</a></span>'
        f'</div>')
    insp_row = "".join(
        f'<td>{cu_grid[inspect-1][b]:.1f}%</td>' for b in range(10))
    cu_slots = sum(days[d][cu_side]["slots"] for d in dates)
    cu_side_label = 'GBX' if cu_side == 'gbx' else 'Agave Harmonic'

    key = "".join(f'<div><b>{k}</b> &mdash; {v}</div>' for k, v in COL_KEY)
    js = json.dumps({"gbx": qg, "harm": qh, "xmax": xmax})
    cu_data_json = json.dumps(cu_data, separators=(',', ':'))
    cav = "".join(f"<li>{html.escape(c)}</li>" for c in meta["caveats"])
    facts = "".join(
        f'<tr><td>{n}</td><td>{a:.6f}</td><td>{b:.6f}</td></tr>'
        for (n, a), (_, b) in zip(f_g, f_h))

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; GBX vs Agave Harmonic</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
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
      <b>Before quoting a number.</b>
      <ul style="margin:4px 0 0 18px;padding:0">{cav}</ul>
      <b>Sources.</b> histograms <code>{html.escape(meta['sources']['hist'])}</code>;
      commission <code>{html.escape(meta['sources']['commission'])}</code>;
      reference <code>{html.escape(meta['sources']['report'])}</code>.
    </div>
  </details>
</div>

<div class="rw-wrap">
  {rng}
  <div class="rw-hero">{hero}</div>

  <div class="rw-box wide">
    <h2>Rewards distribution &mdash; {d_from} to {d_to}</h2>
    <div class="cs">Share of slots earning at or below <i>x</i>. Hover reads
      horizontally: pick a percentile, compare SOL.</div>
    <svg viewBox="0 0 {W2} {H2}" data-zoom="1"
         style="width:100%;display:block;cursor:crosshair" role="img"
         aria-label="Empirical CDF of per-slot revenue pooled over {d_from} to
         {d_to}.">{''.join(p2)}</svg>
    <div class="rw-legend">{lg}</div>
    <div class="rw-zoom"><b>scroll</b> zoom &middot; <b>drag</b> pan &middot; <b>double-click</b> reset<button type="button">reset</button></div>
    {stats}
  </div>

  <div class="rw-grid">
  <div class="rw-box">
    <h2>Quantile ratio &mdash; harmonic / gbx</h2>
    <div class="cs">Above 1.0 Harmonic leads. Rising = the gap widens with
      block value.</div>
    {ratio_svg}
    <div class="rw-zoom"><b>scroll</b> zoom &middot; <b>drag</b> pan &middot; <b>double-click</b> reset<button type="button">reset</button></div>
  </div>

  <div class="rw-box">
    <h2>Cumulative share of the mean gap ({ht-gt:.6f} SOL)</h2>
    <div class="cs">Below the diagonal = concentrated in the tail.
      p1&ndash;p90 hold {r90*100:.0f}%.</div>
    {gap_svg}
    <div class="rw-zoom"><b>scroll</b> zoom &middot; <b>drag</b> pan &middot; <b>double-click</b> reset<button type="button">reset</button></div>
  </div>
  </div>

  <div class="rw-box wide">
    <h2>Where compute goes, percentile by percentile</h2>
    <div class="cs">Each reward percentile's priority fee split across ten
      compute-unit bins. Height is that bin's share of the percentile, so every
      slice sums to 100%. Aggregated over every slot in the band, not the one
      slot at the cut.</div>
    {cu_ctl}
    {cu_svg}
    <div class="rw-zoom"><b>drag</b> orbit &middot; <b>shift+drag</b> pan &middot;
      <b>scroll</b> zoom &middot; <b>double-click</b> reset</div>
    <table class="rw-tbl" style="margin-top:4px">
      <thead><tr><th>p{inspect} &mdash; the highlighted slice</th>
        {"".join(f"<th>{l}</th>" for l in CU_LABELS)}</tr></thead>
      <tbody><tr><td>share of priority fee</td>{insp_row}</tr></tbody>
    </table>
    <div class="rw-note">{cu_side_label}, {len(dates)} day{"s" if len(dates)!=1 else ""},
      {cu_slots:,} slots &mdash; about {cu_slots//100:,} per percentile band.
      {html.escape(meta.get('cu_note',''))}</div>
  </div>

  <div class="rw-grid">
  <div class="rw-box">
    <h2>Where the money goes &mdash; GBX</h2>
    <div class="cs">Per slot. Both panels share one scale.</div>
    {sk_g}
    <div class="rw-legend">{sk_lg}</div>
  </div>

  <div class="rw-box">
    <h2>Where the money goes &mdash; Agave Harmonic</h2>
    <div class="cs">MEV commission {Hm['comm_bps']/100:.1f}% vs GBX's
      {G['comm_bps']/100:.1f}% &mdash; more of each tip stays with the
      validator.</div>
    {sk_h}
    <table class="rw-tbl" style="max-width:100%;margin-top:14px">
      <thead><tr><th>SOL per slot</th><th>GBX</th><th>Harmonic</th></tr></thead>
      <tbody>{facts}</tbody>
    </table>
  </div>
  </div>

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

<div id="rw-tip"></div>
<script>
var CU3D={cu_data_json};
// ---- the compute-unit field, projected in the browser so it can be orbited.
// The projection lives here rather than in Python because dragging has to
// re-project every frame; keeping a second copy server-side would be two
// implementations of the same maths drifting apart.
(function(){{
  var host = document.getElementById('cu3d');
  if (!host) return;
  var D = CU3D;                       // {{grid:[100][10], zmax, labels, sqrt, render}}
  var W = 1180, H = 680;
  var st = {{az: D.az0, el: D.el0, zoom: 1, ox: 0, oy: 0}};

  function project(bx, by, bz){{
    // bx,by,bz all in [0,1]. Centre, spin about the vertical, then tilt.
    var x = bx - 0.5, y = by - 0.5, z = bz;
    var ca = Math.cos(st.az), sa = Math.sin(st.az);
    var rx = x * ca - y * sa, ry = x * sa + y * ca;
    var ce = Math.cos(st.el), se = Math.sin(st.el);
    return [rx, ry * se - z * ce];
  }}
  function hgt(v){{ return D.sqrt ? Math.sqrt(v / D.zmax) : v / D.zmax; }}

  // fit: project the unit cube's corners so the frame never clips as it spins
  function fit(){{
    var xs = [], ys = [];
    for (var i = 0; i < 8; i++){{
      var p = project(i & 1, (i >> 1) & 1, (i >> 2) & 1);
      xs.push(p[0]); ys.push(p[1]);
    }}
    var x0 = Math.min.apply(null, xs), x1 = Math.max.apply(null, xs);
    var y0 = Math.min.apply(null, ys), y1 = Math.max.apply(null, ys);
    var pad = 96;
    var sx = (W - pad * 2) / (x1 - x0), sy = (H - pad * 2) / (y1 - y0);
    var s = Math.min(sx, sy) * st.zoom;
    return {{s: s, cx: W / 2 - (x0 + x1) / 2 * s + st.ox,
                  cy: H / 2 - (y0 + y1) / 2 * s + st.oy}};
  }}
  var F;
  function P(b, p, v){{
    var q = project(b / 9, p / 99, hgt(v));
    return [F.cx + q[0] * F.s, F.cy + q[1] * F.s];
  }}
  function S(b, p, v){{ var q = P(b, p, v); return q[0].toFixed(1) + ',' + q[1].toFixed(1); }}

  function viridis(f){{
    var C = D.ramp;
    f = Math.max(0, Math.min(1, f));
    var x = f * (C.length - 1), i = Math.min(Math.floor(x), C.length - 2), t = x - i;
    function ch(a, b){{ return Math.round(a + (b - a) * t); }}
    var a = C[i], b = C[i + 1];
    return 'rgb(' + ch(a[0],b[0]) + ',' + ch(a[1],b[1]) + ',' + ch(a[2],b[2]) + ')';
  }}

  function draw(){{
    F = fit();
    var g = D.grid, out = [];
    // floor
    out.push('<polygon points="' + S(0,0,0) + ' ' + S(9,0,0) + ' ' + S(9,99,0) +
             ' ' + S(0,99,0) + '" fill="#0b131c" fill-opacity="0.72" ' +
             'stroke="#243242" stroke-width="1"/>');
    // the two walls furthest from the camera, so they sit behind the data
    var back = Math.cos(st.az) >= 0 ? 99 : 0, side = Math.sin(st.az) >= 0 ? 0 : 9;
    out.push('<polygon points="' + S(0,back,0) + ' ' + S(9,back,0) + ' ' +
             S(9,back,1) + ' ' + S(0,back,1) + '" fill="#0a1017" ' +
             'fill-opacity="0.55" stroke="#1e2b39" stroke-width="1"/>');
    out.push('<polygon points="' + S(side,0,0) + ' ' + S(side,99,0) + ' ' +
             S(side,99,1) + ' ' + S(side,0,1) + '" fill="#0a1017" ' +
             'fill-opacity="0.4" stroke="#1e2b39" stroke-width="1"/>');
    for (var k = 1; k <= 4; k++){{
      var hv = k / 4;
      out.push('<line x1="' + P(0,back,0)[0].toFixed(1) + '" y1="' +
               (P(0,back,0)[1] - (P(0,back,0)[1]-P(0,back,D.zmax)[1])*hv).toFixed(1) +
               '" x2="' + P(9,back,0)[0].toFixed(1) + '" y2="' +
               (P(9,back,0)[1] - (P(9,back,0)[1]-P(9,back,D.zmax)[1])*hv).toFixed(1) +
               '" stroke="#1a2431" stroke-width="0.8"/>');
    }}
    for (var b = 0; b < 10; b++)
      out.push('<line x1="'+P(b,0,0)[0].toFixed(1)+'" y1="'+P(b,0,0)[1].toFixed(1)+
               '" x2="'+P(b,99,0)[0].toFixed(1)+'" y2="'+P(b,99,0)[1].toFixed(1)+
               '" stroke="#1a2431" stroke-width="0.7"/>');
    for (var p = 0; p < 100; p += 10)
      out.push('<line x1="'+P(0,p,0)[0].toFixed(1)+'" y1="'+P(0,p,0)[1].toFixed(1)+
               '" x2="'+P(9,p,0)[0].toFixed(1)+'" y2="'+P(9,p,0)[1].toFixed(1)+
               '" stroke="#1a2431" stroke-width="0.7"/>');

    // painter's order, recomputed each frame because rotating changes what is behind
    var ord = [];
    for (var i = 0; i < 100; i++) ord.push(i);
    ord.sort(function(a,b){{ return P(0,a,0)[1] - P(0,b,0)[1]; }});

    if (D.render === 'surface'){{
      for (var oi = 0; oi < ord.length; oi++){{
        var pp = ord[oi]; if (pp >= 99) continue; var qq = pp + 1;
        for (var bb = 0; bb < 9; bb++){{
          var m = (g[pp][bb]+g[pp][bb+1]+g[qq][bb]+g[qq][bb+1])/4, c = viridis(hgt(m));
          out.push('<polygon points="'+S(bb,pp,g[pp][bb])+' '+S(bb+1,pp,g[pp][bb+1])+
                   ' '+S(bb+1,qq,g[qq][bb+1])+' '+S(bb,qq,g[qq][bb])+'" fill="'+c+
                   '" fill-opacity="0.92" stroke="'+c+'" stroke-width="0.5"/>');
        }}
      }}
    }} else if (D.render === 'ribbons'){{
      for (var bb2 = 9; bb2 >= 0; bb2--){{
        var top = [], base = [], mv = 0;
        for (var p2 = 0; p2 < 100; p2++){{ top.push(S(bb2,p2,g[p2][bb2])); mv += g[p2][bb2]; }}
        for (var p3 = 99; p3 >= 0; p3--) base.push(S(bb2,p3,0));
        var c2 = viridis(hgt(mv/100));
        out.push('<polygon points="'+top.join(' ')+' '+base.join(' ')+'" fill="'+c2+
                 '" fill-opacity="0.22"/>');
        out.push('<polyline points="'+top.join(' ')+'" fill="none" stroke="'+c2+
                 '" stroke-width="2" stroke-linejoin="round"/>');
      }}
    }} else {{
      for (var oi2 = 0; oi2 < ord.length; oi2++){{
        var p4 = ord[oi2];
        for (var b4 = 0; b4 < 10; b4++){{
          var v = g[p4][b4], f = hgt(v), a = P(b4,p4,v), fl = P(b4,p4,0);
          out.push('<line x1="'+a[0].toFixed(1)+'" y1="'+a[1].toFixed(1)+'" x2="'+
                   fl[0].toFixed(1)+'" y2="'+fl[1].toFixed(1)+'" stroke="'+viridis(f)+
                   '" stroke-width="0.6" opacity="0.26"/>');
          out.push('<circle cx="'+a[0].toFixed(1)+'" cy="'+a[1].toFixed(1)+'" r="'+
                   (1.5+5*Math.pow(f,0.6)).toFixed(2)+'" fill="'+viridis(f)+
                   '" fill-opacity="0.9" stroke="#0b131c" stroke-width="0.5"/>');
        }}
      }}
    }}
    // inspected slice
    var ip = D.insp - 1, sl = [];
    for (var b5 = 0; b5 < 10; b5++) sl.push(S(b5,ip,g[ip][b5]));
    out.push('<polyline points="'+sl.join(' ')+'" fill="none" stroke="#fff" '+
             'stroke-width="2.4" stroke-linejoin="round" opacity="0.95"/>');
    for (var b6 = 0; b6 < 10; b6++){{
      var a6 = P(b6,ip,g[ip][b6]);
      out.push('<circle cx="'+a6[0].toFixed(1)+'" cy="'+a6[1].toFixed(1)+
               '" r="3.4" fill="#fff" stroke="#0b131c" stroke-width="1"/>');
    }}
    // axis labels, placed just outside the floor so they follow the spin
    for (var b7 = 0; b7 < 10; b7++){{
      var t7 = P(b7, -6, 0);
      out.push('<text x="'+t7[0].toFixed(1)+'" y="'+t7[1].toFixed(1)+
               '" fill="#6b7f96" font-size="10" text-anchor="middle">'+D.labels[b7]+'</text>');
    }}
    for (var p8 = 9; p8 < 100; p8 += 15){{
      var t8 = P(10.6, p8, 0);
      out.push('<text x="'+t8[0].toFixed(1)+'" y="'+t8[1].toFixed(1)+
               '" fill="#6b7f96" font-size="10" text-anchor="middle">p'+(p8+1)+'</text>');
    }}
    host.innerHTML = out.join('');
  }}

  var frame = null;
  function redraw(){{ if (!frame) frame = requestAnimationFrame(function(){{ frame=null; draw(); }}); }}

  var svg = host.ownerSVGElement, drag = null;
  svg.addEventListener('mousedown', function(ev){{
    drag = {{x: ev.clientX, y: ev.clientY, az: st.az, el: st.el,
            ox: st.ox, oy: st.oy, shift: ev.shiftKey}};
    svg.style.cursor = 'grabbing'; ev.preventDefault();
  }});
  window.addEventListener('mousemove', function(ev){{
    if (!drag) return;
    var dx = ev.clientX - drag.x, dy = ev.clientY - drag.y;
    if (drag.shift){{ st.ox = drag.ox + dx; st.oy = drag.oy + dy; }}
    else {{
      st.az = drag.az + dx * 0.010;                       // spin
      st.el = Math.max(0.05, Math.min(1.5, drag.el - dy * 0.006));  // tilt, clamped
    }}
    redraw();
  }});
  window.addEventListener('mouseup', function(){{
    if (drag){{ drag = null; svg.style.cursor = 'grab'; }}
  }});
  svg.addEventListener('wheel', function(ev){{
    ev.preventDefault();
    st.zoom = Math.max(0.45, Math.min(6, st.zoom * (ev.deltaY < 0 ? 1.12 : 1/1.12)));
    redraw();
  }}, {{passive:false}});
  svg.addEventListener('dblclick', function(){{
    st.az = D.az0; st.el = D.el0; st.zoom = 1; st.ox = 0; st.oy = 0; redraw();
  }});
  document.querySelectorAll('[data-cam]').forEach(function(btn){{
    btn.addEventListener('click', function(){{
      var v = btn.dataset.cam.split(',');
      st.az = parseFloat(v[0]); st.el = parseFloat(v[1]);
      st.zoom = 1; st.ox = 0; st.oy = 0; redraw();
      document.querySelectorAll('[data-cam]').forEach(function(o){{ o.classList.remove('on'); }});
      btn.classList.add('on');
    }});
  }});
  draw();
}})();


// Pan/zoom for every chart marked data-zoom. Works on the viewBox rather than a
// CSS transform so strokes and text keep their weight as you zoom in, and so
// the hover maths below can stay in user units.
(function(){{
  document.querySelectorAll('svg[data-zoom]').forEach(function(svg){{
    var vb = svg.viewBox.baseVal;
    var home = {{x: vb.x, y: vb.y, w: vb.width, h: vb.height}};
    function set(x, y, w, h){{
      var minW = home.w / 40, maxW = home.w * 1.6;
      w = Math.max(minW, Math.min(maxW, w));
      h = w * home.h / home.w;
      x = Math.max(home.x - home.w, Math.min(home.x + home.w * 1.6 - w, x));
      y = Math.max(home.y - home.h, Math.min(home.y + home.h * 1.6 - h, y));
      svg.setAttribute('viewBox', x + ' ' + y + ' ' + w + ' ' + h);
    }}
    function pos(ev){{
      var r = svg.getBoundingClientRect(), v = svg.viewBox.baseVal;
      return {{x: v.x + (ev.clientX - r.left) / r.width * v.width,
               y: v.y + (ev.clientY - r.top) / r.height * v.height}};
    }}
    svg.addEventListener('wheel', function(ev){{
      ev.preventDefault();
      var v = svg.viewBox.baseVal, p = pos(ev);
      var k = ev.deltaY < 0 ? 0.84 : 1 / 0.84;          // zoom about the cursor
      set(p.x - (p.x - v.x) * k, p.y - (p.y - v.y) * k, v.width * k, v.height * k);
    }}, {{passive: false}});
    var drag = null;
    svg.addEventListener('mousedown', function(ev){{
      if (ev.button !== 0) return;
      drag = {{sx: ev.clientX, sy: ev.clientY,
               vx: svg.viewBox.baseVal.x, vy: svg.viewBox.baseVal.y}};
      svg.style.cursor = 'grabbing';
    }});
    window.addEventListener('mousemove', function(ev){{
      if (!drag) return;
      var r = svg.getBoundingClientRect(), v = svg.viewBox.baseVal;
      set(drag.vx - (ev.clientX - drag.sx) / r.width * v.width,
          drag.vy - (ev.clientY - drag.sy) / r.height * v.height, v.width, v.height);
    }});
    window.addEventListener('mouseup', function(){{
      if (drag) {{ drag = null; svg.style.cursor = svg.dataset.grab || 'grab'; }}
    }});
    svg.addEventListener('dblclick', function(){{
      svg.setAttribute('viewBox', home.x+' '+home.y+' '+home.w+' '+home.h);
    }});
    var box = svg.closest('.rw-box');
    if (box) {{
      var bar = box.querySelector('.rw-zoom button');
      if (bar) bar.addEventListener('click', function(){{
        svg.setAttribute('viewBox', home.x+' '+home.y+' '+home.w+' '+home.h);
      }});
    }}
  }});
}})();

// ECDF hover. Reads the live viewBox so it stays correct after a zoom or pan.
(function(){{
  var D={js}, L={L2}, PW={pw2}, T={T2}, PH={ph2};
  var boxes=document.querySelectorAll('.rw-box svg'), svg=boxes[0],
      hit=document.getElementById('rw-hit'),
      cross=document.getElementById('rw-cross'),
      dg=document.getElementById('rw-dg'), dh=document.getElementById('rw-dh'),
      tip=document.getElementById('rw-tip');
  if(!svg||!hit) return;
  function px(v){{ return L+PW*Math.min(v/D.xmax,1); }}
  hit.addEventListener('mousemove',function(ev){{
    var r=svg.getBoundingClientRect(), v=svg.viewBox.baseVal;
    if(!r.height) return;
    // screen -> user units through the CURRENT viewBox, not the authored one
    var uy = v.y + (ev.clientY - r.top) / r.height * v.height;
    var f=Math.min(1,Math.max(0,1-(uy-T)/PH));
    var q=Math.min(99,Math.max(1,Math.round(f*100)));
    var y=T+PH*(1-q/100), g=D.gbx[q-1], h=D.harm[q-1];
    cross.setAttribute('y1',y); cross.setAttribute('y2',y);
    cross.setAttribute('opacity','1');
    dg.setAttribute('cx',px(g)); dg.setAttribute('cy',y);
    dg.setAttribute('opacity', g<=D.xmax?'1':'0');
    dh.setAttribute('cx',px(h)); dh.setAttribute('cy',y);
    dh.setAttribute('opacity', h<=D.xmax?'1':'0');
    var pct=h>0?((g/h-1)*100):0, sign=pct>=0?'+':'';
    var hh='<div class="tx">p'+q+'</div>';
    hh+='<div class="tr"><span class="rw-sw" style="background:{C_GBX}"></span>'
      +'<span>GBX</span><span>'+g.toFixed(5)+' SOL</span></div>';
    hh+='<div class="tr"><span class="rw-sw" style="background:{C_HARM}"></span>'
      +'<span>Agave Harmonic</span><span>'+h.toFixed(5)+' SOL</span></div>';
    hh+='<div class="tr" style="border-top:1px solid #22303f;margin-top:6px;'
      +'padding-top:5px"><span>GBX vs H</span><span style="color:'
      +(pct>=0?'#5eead4':'#fca5a5')+'">'+sign+pct.toFixed(1)+'%</span></div>';
    tip.innerHTML=hh; tip.style.display='block';
    var tw=tip.offsetWidth, th=tip.offsetHeight;
    tip.style.left=Math.min(window.innerWidth-tw-12,ev.clientX+16)+'px';
    tip.style.top=Math.max(8,ev.clientY-th/2)+'px';
  }});
  hit.addEventListener('mouseleave',function(){{
    cross.setAttribute('opacity','0'); tip.style.display='none';
    dg.setAttribute('opacity','0'); dh.setAttribute('opacity','0');
  }});
}})();
</script>
</body></html>"""
