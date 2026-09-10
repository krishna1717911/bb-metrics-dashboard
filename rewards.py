#!/usr/bin/env python3
"""GBX vs Agave Harmonic per-slot revenue -- the /rewards page.

Kept out of app.py deliberately: a self-contained feature over a static
rewards_daily.json, where app.py is already 8k lines several people edit.
app.py needs only the route and the nav link.

The data is per-day 0.5 mSOL HISTOGRAMS, not per-day percentiles. That is what
makes an arbitrary date range exact: bin counts sum across days, percentiles do
not. Every curve and every percentile here is computed from summed bins at
request time, so ?from=/?to= can name any sub-range without a re-pull.

Regenerating the data is an offline step (Dune + reports.firedancer.io + kobe);
nothing here queries anything at request time.
"""

import html
import json
import os

REWARDS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "rewards_daily.json")

BIN = 0.0005          # SOL per histogram bucket
C_GBX = "#3987e5"
C_HARM = "#d95926"
C_FEE = "#199e70"     # sankey: kept-in-full stream
C_JITO = "#c98500"    # sankey: jito's own cut
C_STAKE = "#9085e9"   # sankey: staker share
C_OTHER = "#d55181"   # sankey: titan/bifrost

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
.rw-box{background:#0e151d;border:1px solid #1e2937;border-radius:11px;
  padding:16px 18px 10px;overflow-x:auto;margin-bottom:16px}
.rw-box h2{margin:0 0 2px;font-size:14px;font-weight:650;color:#dbe4ee}
.rw-box .cs{color:#6b7f96;font-size:11.5px;margin-bottom:10px;line-height:1.6}
.rw-legend{display:flex;gap:16px;flex-wrap:wrap;margin:8px 0 2px;
  padding-top:11px;border-top:1px solid #1e2937}
.rw-lg{display:flex;align-items:center;gap:7px;color:#9fb2c8;font-size:11.5px}
.rw-sw{width:15px;height:3px;border-radius:2px;flex:none}
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
.rw-note{color:#6b7f96;font-size:11px;margin-top:9px;line-height:1.65}
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
  pointer-events:none;min-width:200px}
#rw-tip .tx{color:#dbe4ee;font-weight:650;margin-bottom:6px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr{display:flex;align-items:center;gap:7px;margin-top:3px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr span:last-child{margin-left:auto;color:#dbe4ee}
"""


def _pool(days, dates, side):
    """Sum a cohort's histograms and component totals over a date range.

    Exact: bin counts add. This is the whole reason the source is histograms
    rather than the per-day percentiles -- those cannot be pooled.
    """
    h, out = {}, dict(slots=0, fee=0.0, jito_gross=0.0, jito_net=0.0,
                      other=0.0, like=0.0, with_jito=0, with_other=0,
                      comm_num=0.0, vals=0)
    for d in dates:
        e = days[d][side]
        for b, c in zip(e["bins"], e["counts"]):
            h[b] = h.get(b, 0) + c
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
        c += h[b]
        if c >= target:
            return b * BIN
    return 0.0


def _curve(h, total, xmax, n=200):
    """(x, cumulative-share) sampled evenly across [0, xmax]."""
    cum, run = {}, 0
    for b in sorted(h):
        run += h[b]
        cum[b] = run
    bs, out, j, r = sorted(cum), [], 0, 0
    for i in range(n + 1):
        x = xmax * i / n
        bi = int(x / BIN)
        while j < len(bs) and bs[j] <= bi:
            r = cum[bs[j]]
            j += 1
        out.append((x, r / total if total else 0))
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


def _sankey(P, side, title, col, scale_gross):
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
    W, H = 1010, 250
    T, B, LW = 26, 26, 150
    ph = H - T - B
    scale = ph / scale_gross
    x0, x1, x2, x3 = 24, 24 + LW, 24 + LW * 2 + 210, 24 + LW * 3 + 300
    sv = []

    def node(x, y, h, c, lab, val, anchor="start"):
        h = max(h, 1.2)
        sv.append(f'<rect x="{x:.0f}" y="{y:.1f}" width="11" height="{h:.1f}" '
                  f'fill="{c}" rx="2"/>')
        tx = x + 16 if anchor == "start" else x - 6
        sv.append(f'<text x="{tx:.0f}" y="{y+h/2+3.5:.1f}" fill="#c3d3e6" '
                  f'font-size="10.5" text-anchor="{anchor}">{lab} '
                  f'<tspan fill="#6b7f96">{val:.6f}</tspan></text>')

    def link(xa, ya, xb, yb, h, c, op=0.34):
        h = max(h, 1.2)
        mx = (xa + xb) / 2
        sv.append(f'<path d="M{xa+11:.0f},{ya:.1f} C{mx:.0f},{ya:.1f} '
                  f'{mx:.0f},{yb:.1f} {xb:.0f},{yb:.1f} L{xb:.0f},{yb+h:.1f} '
                  f'C{mx:.0f},{yb+h:.1f} {mx:.0f},{ya+h:.1f} '
                  f'{xa+11:.0f},{ya+h:.1f} Z" fill="{c}" opacity="{op}"/>')

    # column 1: sources
    y = T
    y_fee, h_fee = y, fee * scale
    node(x0, y_fee, h_fee, C_FEE, "fees", fee); y += h_fee + 6
    y_jg, h_jg = y, jg * scale
    node(x0, y_jg, h_jg, C_JITO, "Jito tips (gross)", jg); y += h_jg + 6
    y_o, h_o = y, oth * scale
    if oth > 0:
        node(x0, y_o, h_o, C_OTHER, "Titan/Bifrost", oth)

    # column 2: after Jito's cut
    y = T
    y_dist, h_dist = y, (fee + jn + oth) * scale
    node(x2, y_dist, h_dist, col, "distributable", fee + jn + oth)
    y_cut = y_dist + h_dist + 10
    node(x2, y_cut, jito_cut * scale, C_JITO, "Jito 6% commission", jito_cut)

    link(x0, y_fee, x2, y_dist, h_fee, C_FEE)
    link(x0, y_jg, x2, y_dist + h_fee, jn * scale, C_JITO)
    link(x0, y_jg + jn * scale, x2, y_cut, jito_cut * scale, C_JITO, 0.55)
    if oth > 0:
        link(x0, y_o, x2, y_dist + h_fee + jn * scale, h_o, C_OTHER)

    # column 3: validator vs stakers
    y = T
    y_keep, h_keep = y, (fee + keep + oth) * scale
    node(x3, y_keep, h_keep, col, "validator keeps", fee + keep + oth, "end")
    y_st = y_keep + h_keep + 10
    node(x3, y_st, stake * scale, C_STAKE, "stakers", stake, "end")
    link(x2, y_dist, x3, y_keep, h_fee, C_FEE)
    link(x2, y_dist + h_fee, x3, y_keep + h_fee, keep * scale, C_JITO)
    link(x2, y_dist + h_fee + keep * scale, x3, y_st, stake * scale, C_STAKE, 0.5)
    if oth > 0:
        link(x2, y_dist + h_fee + jn * scale, x3,
             y_keep + h_fee + keep * scale, h_o, C_OTHER)

    sv.append(f'<text x="{x0}" y="{T-10}" fill="#8fa6bf" font-size="11" '
              f'font-weight="600">{title} &mdash; SOL per slot '
              f'<tspan fill="#6b7f96">({P["slots"]:,} slots)</tspan></text>')
    facts = [("gross inflow / slot", gross), ("fees / slot", fee),
             ("Jito tips gross / slot", jg), ("Jito 6% cut / slot", jito_cut),
             ("to stakers / slot", stake),
             ("validator keeps / slot", fee + keep + oth)]
    return (f'<svg viewBox="0 0 {W} {H}" width="100%" '
            f'style="max-width:{W}px;display:block" role="img" '
            f'aria-label="{title} revenue flow: gross inflow to Jito '
            f'commission, stakers and the validator. Values in the table '
            f'below.">{"".join(sv)}</svg>'), facts


def rewards_page(CSS, purl, d_from=None, d_to=None):
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

    G, Hm = _pool(days, dates, "gbx"), _pool(days, dates, "harm")
    gm = G["like"] / G["slots"] / 1e9 if G["slots"] else 0
    hm = Hm["like"] / Hm["slots"] / 1e9 if Hm["slots"] else 0
    delta = (gm / hm - 1) * 100 if hm else 0
    wins = sum(1 for d in dates
               if days[d]["gbx"]["like"] / max(days[d]["gbx"]["slots"], 1)
               > days[d]["harm"]["like"] / max(days[d]["harm"]["slots"], 1))

    # ---- panel 1: daily means across the whole window, range shaded
    W, H = 1010, 300
    L, R, T, B = 66, 122, 16, 46
    pw, ph = W - L - R, H - T - B
    mean_of = lambda d, s: days[d][s]["like"] / max(days[d][s]["slots"], 1) / 1e9
    ymax = max(max(mean_of(d, s) for s in ("gbx", "harm"))
               for d in all_dates) * 1.08
    sx = lambda i: L + pw * i / max(len(all_dates) - 1, 1)
    sy = lambda v: T + ph * (1 - v / ymax)
    p1 = []
    i0 = all_dates.index(dates[0]); i1 = all_dates.index(dates[-1])
    p1.append(f'<rect x="{sx(i0):.1f}" y="{T}" width="{max(sx(i1)-sx(i0),2):.1f}" '
              f'height="{ph}" fill="#5eead4" opacity="0.07"/>')
    _axes(p1, L, T, pw, ph, "day", "mean SOL / slot",
          [(all_dates[i][5:], sx(i)) for i in range(0, len(all_dates), 4)],
          [(f"{ymax*i/4:.3f}", sy(ymax * i / 4)) for i in range(5)], W, H)
    for s, c, nm in (("harm", C_HARM, "Agave Harmonic"), ("gbx", C_GBX, "GBX")):
        pts = " ".join(f"{sx(i):.1f},{sy(mean_of(d, s)):.1f}"
                       for i, d in enumerate(all_dates))
        p1.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
                  f'stroke-width="4.6" stroke-linejoin="round"/>')
        p1.append(f'<polyline points="{pts}" fill="none" stroke="{c}" '
                  f'stroke-width="2.2" stroke-linejoin="round"/>')
        p1.append(f'<text x="{L+pw+9}" y="{sy(mean_of(all_dates[-1], s))+4:.1f}" '
                  f'fill="{c}" font-size="11" font-weight="600">{nm}</text>')

    # ---- panel 2: pooled ECDF for the range
    W2, H2 = 1010, 400
    L2, R2, T2, B2 = 66, 132, 16, 46
    pw2, ph2 = W2 - L2 - R2, H2 - T2 - B2
    xmax = max(_pct(G["hist"], G["slots"], .90),
               _pct(Hm["hist"], Hm["slots"], .90)) * 1.6 or 0.1
    sx2 = lambda v: L2 + pw2 * min(v / xmax, 1.0)
    sy2 = lambda f: T2 + ph2 * (1 - f)
    p2 = []
    step = xmax / 6
    _axes(p2, L2, T2, pw2, ph2,
          "per-slot revenue (SOL) &mdash; fee + Jito tip net of 6%",
          "cumulative share of slots",
          [(f"{step*i:.3f}", sx2(step * i)) for i in range(7)],
          [(f"{i*10}%", sy2(i / 10)) for i in range(11)], W2, H2)
    for P, c in ((Hm, C_HARM), (G, C_GBX)):
        pts = " ".join(f"{sx2(x):.1f},{sy2(f):.1f}"
                       for x, f in _curve(P["hist"], P["slots"], xmax))
        p2.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
                  f'stroke-width="4.8" stroke-linejoin="round"/>')
        p2.append(f'<polyline points="{pts}" fill="none" stroke="{c}" '
                  f'stroke-width="2.3" stroke-linejoin="round"/>')
        for q in (.50, .90):
            v = _pct(P["hist"], P["slots"], q)
            if v <= xmax:
                dy = -11 if c == C_GBX else 15
                p2.append(f'<circle cx="{sx2(v):.1f}" cy="{sy2(q):.1f}" r="4.4" '
                          f'fill="{c}" stroke="#0e151d" stroke-width="2"/>')
                p2.append(f'<text x="{sx2(v):.1f}" y="{sy2(q)+dy:.1f}" '
                          f'text-anchor="middle" fill="{c}" font-size="9.5" '
                          f'font-weight="600">{v:.4f}</text>')
    # horizontal: the mouse picks a PERCENTILE, and the tooltip reads each
    # cohort's SOL at that level -- the horizontal gap is the revenue gap.
    p2.append(f'<line id="rw-cross" x1="{L2}" y1="0" x2="{L2+pw2}" y2="0" '
              f'stroke="#5eead4" stroke-width="1" stroke-dasharray="3 3" '
              f'opacity="0"/>')
    p2.append(f'<circle id="rw-dg" r="5" fill="none" stroke="{C_GBX}" '
              f'stroke-width="2" opacity="0"/>')
    p2.append(f'<circle id="rw-dh" r="5" fill="none" stroke="{C_HARM}" '
              f'stroke-width="2" opacity="0"/>')
    p2.append(f'<rect id="rw-hit" x="{L2}" y="{T2}" width="{pw2}" '
              f'height="{ph2}" fill="transparent"/>')

    # one scale for both diagrams: the larger per-slot gross sets the height
    sg_max = max(_sankey_flows(G)["gross"], _sankey_flows(Hm)["gross"])
    sk_g, f_g = _sankey(G, "gbx", "GBX", C_GBX, sg_max)
    sk_h, f_h = _sankey(Hm, "harm", "Agave Harmonic", C_HARM, sg_max)

    tiles = [
        ("GBX mean / slot", f"{gm:.6f}", f"{G['slots']:,} slots", ""),
        ("Agave Harmonic", f"{hm:.6f}",
         f"{Hm['slots']:,} slots, {Hm['vals']} vals", ""),
        ("GBX vs Harmonic", f"{delta:+.1f}%",
         f"{len(dates)} day{'s' if len(dates) != 1 else ''} selected",
         "up" if delta > 0 else "dn"),
        ("days GBX ahead", f"{wins} / {len(dates)}", "on mean per slot", ""),
        ("GBX tip share", f"{G['jito_net']/G['like']*100:.1f}%"
         if G["like"] else "&mdash;",
         f"Harmonic {Hm['jito_net']/Hm['like']*100:.1f}%"
         if Hm["like"] else "", ""),
        ("MEV commission", f"{G['comm_bps']/100:.1f}%",
         f"Harmonic {Hm['comm_bps']/100:.1f}% &mdash; they keep more", ""),
    ]
    hero = "".join(f'<div class="rw-tile"><div class="k">{k}</div>'
                   f'<div class="v {c}">{v}</div><div class="d">{d}</div></div>'
                   for k, v, d, c in tiles)

    # carry the current deployment through the form; purl() renders
    # "<path>?dep=<name>", so the name is whatever follows "dep=".
    dep_val = purl("/rewards").partition("dep=")[2]

    opts = lambda sel: "".join(
        f'<option value="{d}"{" selected" if d == sel else ""}>{d}</option>'
        for d in all_dates)
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
          f'</span>GBX</div>'
          f'<div class="rw-lg"><span class="rw-sw" style="background:{C_HARM}">'
          f'</span>Agave Harmonic</div>')
    sk_lg = "".join(
        f'<div class="rw-lg"><span class="rw-sw" style="background:{c}"></span>'
        f'{n}</div>' for c, n in ((C_FEE, "fees &mdash; kept in full"),
                                  (C_JITO, "Jito tips &amp; its 6% cut"),
                                  (C_STAKE, "staker share"),
                                  (C_OTHER, "Titan/Bifrost (GBX only)")))

    rows = []
    for d in all_dates:
        g, h = days[d]["gbx"], days[d]["harm"]
        a = g["like"] / max(g["slots"], 1) / 1e9
        b = h["like"] / max(h["slots"], 1) / 1e9
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

    # exact p1..p100 in SOL, so the tooltip inverts the curve without
    # re-reading the sampled polyline
    js = json.dumps({
        "gbx": [_pct(G["hist"], G["slots"], q / 100) for q in range(1, 101)],
        "harm": [_pct(Hm["hist"], Hm["slots"], q / 100) for q in range(1, 101)],
        "xmax": xmax})
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

<div class="callout">
  <b class="rw-ok">Verified against reports.firedancer.io on all
  {len(all_dates)} days.</b> {html.escape(meta['verification'])}<br><br>
  <b>Basis.</b> {html.escape(meta['basis'])}<br>
  <b>Like-for-like.</b> {html.escape(meta['like_note'])}
</div>

<div class="rw-wrap">
  {rng}
  <div class="rw-hero">{hero}</div>

  <div class="rw-box">
    <h2>Mean revenue per slot, by day</h2>
    <div class="cs">Whole window, with the selected range shaded. Each point is
      that day's block-weighted mean of fee + Jito tip net of 6%.</div>
    <svg viewBox="0 0 {W} {H}" width="100%" style="max-width:{W}px;display:block"
         role="img" aria-label="Daily mean revenue per slot, GBX versus Agave
         Harmonic. Values in the table below.">{''.join(p1)}</svg>
  </div>

  <div class="rw-box">
    <h2>Revenue distribution &mdash; {d_from} to {d_to}</h2>
    <div class="cs">Exact empirical CDF pooled over the selected range: at
      revenue <i>x</i>, the height is the share of that cohort's slots earning
      at or below <i>x</i>. Lower and further right is better. Markers show p50
      and p90 with their SOL values. Pooling is exact because the source is
      0.5 mSOL histograms &mdash; bin counts sum across days where percentiles
      could not. <b>Hover reads horizontally</b>: pick a percentile and compare
      each cohort's SOL at that level, which is the gap that matters.</div>
    <svg viewBox="0 0 {W2} {H2}" width="100%"
         style="max-width:{W2}px;display:block" role="img"
         aria-label="Empirical CDF of per-slot revenue pooled over
         {d_from} to {d_to}, GBX versus Agave Harmonic.">{''.join(p2)}</svg>
    <div class="rw-legend">{lg}</div>
    <div class="rw-note">x-axis is framed on p90 &times; 1.6; the tail runs far
      past it. p96&ndash;p100 rest on few slots when the range is short.</div>
  </div>

  <div class="rw-box">
    <h2>Where the money goes</h2>
    <div class="cs">{html.escape(meta['sankey_note'])}</div>
    {sk_g}{sk_h}
    <div class="rw-legend">{sk_lg}</div>
    <table class="rw-tbl" style="max-width:520px">
      <thead><tr><th>SOL per slot</th><th>GBX</th>
        <th>Agave Harmonic</th></tr></thead>
      <tbody>{facts}</tbody>
    </table>
    <div class="rw-note">Every flow is <b>per slot</b>, and both diagrams share
      one scale, so ribbons are comparable between the two cohorts as well as
      within each. Without that, GBX's {G['slots']:,} slots and Harmonic's
      {Hm['slots']:,} would each fill the panel and look equal.
      Harmonic's staker ribbon is the wider one relative to its tips: its
      operators run {Hm['comm_bps']/100:.1f}% MEV commission against GBX's
      {G['comm_bps']/100:.1f}%, so they keep more of each tip.</div>
  </div>

  <table class="rw-tbl">
    <thead><tr><th>day</th><th>GBX slots</th><th>H vals</th><th>H slots</th>
      <th>GBX mean</th><th>H mean</th><th>GBX vs H</th>
      <th>GBX comm</th><th>H comm</th><th>GBX other</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="rw-note">All SOL per slot. Shaded rows are days GBX led; faded
    rows fall outside the selected range. <code>GBX other</code> is
    Titan/Bifrost revenue excluded from every comparison on this page.</div>

  <div class="rw-note" style="margin-top:22px">
    <b style="color:#8fa6bf">Read this before quoting a number</b>
    <ul style="margin:6px 0 0 18px;padding:0">{cav}</ul>
    <div style="margin-top:10px"><b>Sources.</b>
      histograms <code>{html.escape(meta['sources']['hist'])}</code>;
      commission <code>{html.escape(meta['sources']['commission'])}</code>;
      reference <code>{html.escape(meta['sources']['report'])}</code>.
    </div>
  </div>
</div>

<div id="rw-tip"></div>
<script>
(function(){{
  var D={js}, L={L2}, PW={pw2}, T={T2}, PH={ph2}, VB={W2};
  var boxes=document.querySelectorAll('.rw-box svg'), svg=boxes[1],
      hit=document.getElementById('rw-hit'),
      cross=document.getElementById('rw-cross'),
      dg=document.getElementById('rw-dg'), dh=document.getElementById('rw-dh'),
      tip=document.getElementById('rw-tip');
  if(!svg||!hit) return;
  function px(v){{ return L+PW*Math.min(v/D.xmax,1); }}
  hit.addEventListener('mousemove',function(ev){{
    var r=svg.getBoundingClientRect(), k=VB/r.height;
    // the mouse picks a percentile off the y axis
    var f=Math.min(1,Math.max(0,1-((ev.clientY-r.top)*k-T)/PH));
    var q=Math.min(100,Math.max(1,Math.round(f*100)));
    var y=T+PH*(1-q/100);
    var g=D.gbx[q-1], h=D.harm[q-1];
    cross.setAttribute('y1',y); cross.setAttribute('y2',y);
    cross.setAttribute('opacity','1');
    dg.setAttribute('cx',px(g)); dg.setAttribute('cy',y);
    dg.setAttribute('opacity', g<=D.xmax?'1':'0');
    dh.setAttribute('cx',px(h)); dh.setAttribute('cy',y);
    dh.setAttribute('opacity', h<=D.xmax?'1':'0');
    var pct=h>0?((g/h-1)*100):0;
    var sign=pct>=0?'+':'';
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
