#!/usr/bin/env python3
"""GBX vs Agave Harmonic per-slot revenue -- the /rewards page.

Kept out of app.py deliberately: a self-contained feature over a static
rewards_daily.json, where app.py is already 8k lines several people edit.
app.py needs only the route and the nav link.

Regenerating the data is an offline step (Dune + reports.firedancer.io); nothing
here queries anything at request time.

Why there is no all-window ECDF: the source is per-day exact percentiles, and
percentiles cannot be pooled across days -- each day's curve is its own sample.
So the page shows a daily series of selected percentiles plus one day's full
curve, never a 30-day aggregate curve.
"""

import html
import json
import os

REWARDS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "rewards_daily.json")

# Two cohorts, two hues, assigned in fixed palette order and validated as a
# set against this dashboard's surface (#0b1016).
C_GBX = "#3987e5"
C_HARM = "#d95926"

REWARDS_CSS = """
.rw-wrap{margin:16px 28px 34px}
.rw-hero{display:flex;gap:10px;flex-wrap:wrap;margin:0 0 18px}
.rw-tile{background:#0e151d;border:1px solid #1e2937;border-radius:10px;
  padding:12px 16px;min-width:150px}
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
.rw-note{color:#6b7f96;font-size:11px;margin-top:9px;line-height:1.65}
.rw-ok{color:#5eead4}
.rw-days{display:flex;gap:4px;flex-wrap:wrap;margin:0 0 12px}
.rw-days a{color:#8fa6bf;font-size:10.5px;text-decoration:none;padding:3px 7px;
  border:1px solid #22303f;border-radius:5px;font-variant-numeric:tabular-nums}
.rw-days a:hover{border-color:#5eead4;color:#5eead4}
.rw-days a.sel{background:#12314d;border-color:#3987e5;color:#7cc0ff;
  font-weight:650}
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


def _axes(svg, L, T, pw, ph, xlab, ylab, xticks, yticks, W, H):
    """Recessive grid and axis labels shared by both panels."""
    for val, y in yticks:
        svg.append(f'<line x1="{L}" y1="{y:.1f}" x2="{L+pw}" y2="{y:.1f}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{L-9}" y="{y+3.5:.1f}" text-anchor="end" '
                   f'fill="#6b7f96" font-size="10.5">{val}</text>')
    for val, x in xticks:
        svg.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T+ph}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{x:.1f}" y="{T+ph+18}" text-anchor="middle" '
                   f'fill="#6b7f96" font-size="10.5">{val}</text>')
    svg.append(f'<text x="{L+pw/2:.0f}" y="{H-8}" text-anchor="middle" '
               f'fill="#8fa6bf" font-size="11.5">{xlab}</text>')
    svg.append(f'<text x="14" y="{T+ph/2:.0f}" fill="#8fa6bf" font-size="11.5" '
               f'transform="rotate(-90 14 {T+ph/2:.0f})" '
               f'text-anchor="middle">{ylab}</text>')


def rewards_page(CSS, purl, sel_day=None):
    with open(REWARDS_JSON) as fh:
        D = json.load(fh)
    meta, days = D["meta"], D["days"]
    dates = sorted(days)
    if sel_day not in days:
        sel_day = dates[-1]

    def bw(side, key):
        s = sum(days[d][side]["slots"] for d in dates)
        return sum(days[d][side][key] * days[d][side]["slots"] for d in dates) / s

    g_like, h_like = bw("gbx", "mean_like"), bw("harm", "mean_like")
    g_fee, h_fee = bw("gbx", "mean_fee"), bw("harm", "mean_fee")
    g_jito, h_jito = bw("gbx", "mean_jito"), bw("harm", "mean_jito")
    g_other = bw("gbx", "mean_other")
    g_slots = sum(days[d]["gbx"]["slots"] for d in dates)
    h_slots = sum(days[d]["harm"]["slots"] for d in dates)
    delta = (g_like / h_like - 1) * 100
    wins = sum(1 for d in dates
               if days[d]["gbx"]["mean_like"] > days[d]["harm"]["mean_like"])

    # ---------------- panel 1: daily mean, both cohorts
    W, H = 1010, 330
    L, R, T, B = 66, 120, 16, 46
    pw, ph = W - L - R, H - T - B
    ymax = max(max(days[d][s]["mean_like"] for s in ("gbx", "harm"))
               for d in dates) * 1.08
    sx = lambda i: L + (pw * i / max(len(dates) - 1, 1))
    sy = lambda v: T + ph * (1 - v / ymax)
    p1 = []
    yt = [(f"{ymax*i/4:.3f}", sy(ymax * i / 4)) for i in range(5)]
    xt = [(dates[i][5:], sx(i)) for i in range(0, len(dates), 4)]
    _axes(p1, L, T, pw, ph, "day", "mean SOL / block", xt, yt, W, H)
    for side, col, name in (("harm", C_HARM, "Agave Harmonic"),
                            ("gbx", C_GBX, "GBX")):
        pts = " ".join(f"{sx(i):.1f},{sy(days[d][side]['mean_like']):.1f}"
                       for i, d in enumerate(dates))
        p1.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
                  f'stroke-width="4.6" stroke-linejoin="round"/>')
        p1.append(f'<polyline points="{pts}" fill="none" stroke="{col}" '
                  f'stroke-width="2.2" stroke-linejoin="round"/>')
        p1.append(f'<text x="{L+pw+9}" y="{sy(days[dates[-1]][side]["mean_like"])+4:.1f}" '
                  f'fill="{col}" font-size="11" font-weight="600">{name}</text>')
    for i, d in enumerate(dates):
        if d == sel_day:
            p1.append(f'<line x1="{sx(i):.1f}" y1="{T}" x2="{sx(i):.1f}" '
                      f'y2="{T+ph}" stroke="#5eead4" stroke-width="1" '
                      f'stroke-dasharray="3 3" opacity="0.8"/>')

    # ---------------- panel 2: the selected day's curve
    W2, H2 = 1010, 400
    L2, R2, T2, B2 = 66, 132, 16, 46
    pw2, ph2 = W2 - L2 - R2, H2 - T2 - B2
    gp = days[sel_day]["gbx"]["pctl"]
    hp = days[sel_day]["harm"]["pctl"]
    xmax = max(gp[89], hp[89]) * 1.6           # framed on p90, tail clipped
    sx2 = lambda v: L2 + pw2 * min(v / xmax, 1.0)
    sy2 = lambda f: T2 + ph2 * (1 - f)
    p2 = []
    yt2 = [(f"{i*10}%", sy2(i / 10)) for i in range(11)]
    step = xmax / 6
    xt2 = [(f"{step*i:.3f}", sx2(step * i)) for i in range(7)]
    _axes(p2, L2, T2, pw2, ph2,
          "per-slot revenue (SOL) &mdash; fee + Jito tip net of 6%",
          "cumulative share of slots", xt2, yt2, W2, H2)
    for arr, col, name in ((hp, C_HARM, "Agave Harmonic"), (gp, C_GBX, "GBX")):
        pts = " ".join(f"{sx2(v):.1f},{sy2((i+1)/100):.1f}"
                       for i, v in enumerate(arr) if v <= xmax)
        p2.append(f'<polyline points="{pts}" fill="none" stroke="#0e151d" '
                  f'stroke-width="4.8" stroke-linejoin="round"/>')
        p2.append(f'<polyline points="{pts}" fill="none" stroke="{col}" '
                  f'stroke-width="2.3" stroke-linejoin="round"/>')
    for arr, col in ((hp, C_HARM), (gp, C_GBX)):
        for q in (50, 90):
            v = arr[q - 1]
            if v <= xmax:
                p2.append(f'<circle cx="{sx2(v):.1f}" cy="{sy2(q/100):.1f}" '
                          f'r="4.4" fill="{col}" stroke="#0e151d" '
                          f'stroke-width="2"/>')
    p2.append(f'<line id="rw-cross" x1="0" y1="{T2}" x2="0" y2="{T2+ph2}" '
              f'stroke="#5eead4" stroke-width="1" stroke-dasharray="3 3" '
              f'opacity="0"/>')
    p2.append(f'<rect id="rw-hit" x="{L2}" y="{T2}" width="{pw2}" '
              f'height="{ph2}" fill="transparent"/>')

    # ---------------- hero
    tiles = [
        ("GBX mean / block", f"{g_like:.6f}", f"{g_slots:,} slots", ""),
        ("Agave Harmonic", f"{h_like:.6f}", f"{h_slots:,} slots", ""),
        ("GBX vs Harmonic", f"{delta:+.1f}%",
         f"block-weighted, {len(dates)} days", "up" if delta > 0 else "dn"),
        ("days GBX ahead", f"{wins} / {len(dates)}", "on mean per block", ""),
        ("GBX tip share", f"{g_jito/g_like*100:.1f}%",
         f"Harmonic {h_jito/h_like*100:.1f}%", ""),
        ("excluded from both", f"{g_other:.6f}",
         "GBX Titan/Bifrost, SOL/block", ""),
    ]
    hero = "".join(
        f'<div class="rw-tile"><div class="k">{k}</div>'
        f'<div class="v {c}">{v}</div><div class="d">{d}</div></div>'
        for k, v, d, c in tiles)

    picker = "".join(
        f'<a class="{"sel" if d == sel_day else ""}" '
        f'href="{purl("/rewards")}&day={d}">{d[5:]}</a>' for d in dates)

    lg = (f'<div class="rw-lg"><span class="rw-sw" style="background:{C_GBX}">'
          f'</span>GBX ({days[sel_day]["gbx"]["slots"]:,} slots)</div>'
          f'<div class="rw-lg"><span class="rw-sw" style="background:{C_HARM}">'
          f'</span>Agave Harmonic ({days[sel_day]["harm"]["validators"]} '
          f'validators, {days[sel_day]["harm"]["slots"]:,} slots)</div>')

    rows = []
    for d in dates:
        g, h = days[d]["gbx"], days[d]["harm"]
        dd = (g["mean_like"] / h["mean_like"] - 1) * 100
        rows.append(
            f'<tr class="{"win" if dd > 0 else ""}">'
            f'<td>{d}</td><td>{g["slots"]:,}</td><td>{h["validators"]}</td>'
            f'<td>{h["slots"]:,}</td>'
            f'<td>{g["mean_like"]:.6f}</td><td>{h["mean_like"]:.6f}</td>'
            f'<td class="{"greenc" if dd > 0 else "redc"}">{dd:+.1f}%</td>'
            f'<td>{g["pctl"][49]:.5f}</td><td>{h["pctl"][49]:.5f}</td>'
            f'<td>{g["pctl"][89]:.5f}</td><td>{h["pctl"][89]:.5f}</td>'
            f'<td>{g["mean_other"]:.6f}</td></tr>')

    js = json.dumps({"gbx": gp, "harm": hp, "xmax": xmax})
    cav = "".join(f"<li>{html.escape(c)}</li>" for c in meta["caveats"])

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; GBX vs Agave Harmonic</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}{REWARDS_CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">GBX vs Agave Harmonic &mdash; per-slot revenue,
    {meta['window']}</div>
  <a class="navlink" href="{purl("/")}">&larr; back to slots</a>
  <a class="navlink" href="{purl("/reference")}">metrics reference</a>
</header>

<div class="callout">
  <b class="rw-ok">Verified against reports.firedancer.io on all
  {len(dates)} days.</b> {html.escape(meta['verification'])}<br><br>
  <b>Basis.</b> {html.escape(meta['basis'])}<br>
  <b>Like-for-like.</b> {html.escape(meta['like_note'])}
</div>

<div class="rw-wrap">
  <div class="rw-hero">{hero}</div>

  <div class="rw-box">
    <h2>Mean revenue per slot, by day</h2>
    <div class="cs">Block-weighted mean of fee + Jito tip net of 6%, one point
      per day per cohort. The dashed line marks the day charted below.</div>
    <svg viewBox="0 0 {W} {H}" width="100%" style="max-width:{W}px;display:block"
         role="img" aria-label="Daily mean revenue per slot, GBX versus Agave
         Harmonic. Full values in the table below.">{''.join(p1)}</svg>
  </div>

  <div class="rw-box">
    <h2>Revenue distribution &mdash; {sel_day}</h2>
    <div class="cs">Exact empirical CDF for that day: at revenue <i>x</i>, the
      height is the share of that cohort's slots earning at or below <i>x</i>.
      Lower and further right is better. Markers show p50 and p90.
      <b>One day at a time on purpose</b> &mdash; percentiles cannot be pooled
      across days, so there is no 30-day curve.</div>
    <div class="rw-days">{picker}</div>
    <svg viewBox="0 0 {W2} {H2}" width="100%"
         style="max-width:{W2}px;display:block" role="img"
         aria-label="Empirical CDF of per-slot revenue for {sel_day}, GBX
         versus Agave Harmonic.">{''.join(p2)}</svg>
    <div class="rw-legend">{lg}</div>
    <div class="rw-note">x-axis is framed on p90 &times; 1.6; the top tail runs
      far past it (p100 reached 86.9 SOL on one day). p96&ndash;p100 rest on
      single-digit slot counts per day &mdash; p5&ndash;p95 is the usable
      band.</div>
  </div>

  <table class="rw-tbl">
    <thead><tr><th>day</th><th>GBX slots</th><th>H vals</th><th>H slots</th>
      <th>GBX mean</th><th>H mean</th><th>GBX vs H</th>
      <th>GBX p50</th><th>H p50</th><th>GBX p90</th><th>H p90</th>
      <th>GBX other</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="rw-note">All SOL per slot. Shaded rows are days GBX led.
    <code>GBX other</code> is Titan/Bifrost revenue excluded from every
    comparison on this page.</div>

  <div class="rw-note" style="margin-top:22px">
    <b style="color:#8fa6bf">Read this before quoting a number</b>
    <ul style="margin:6px 0 0 18px;padding:0">{cav}</ul>
    <div style="margin-top:10px">
      <b>Sources.</b> GBX <code>{html.escape(meta['sources']['gbx'])}</code>;
      Harmonic <code>{html.escape(meta['sources']['harmonic'])}</code>;
      reference <code>{html.escape(meta['sources']['report'])}</code>.
      Harmonic membership is taken per day from each day's own report &mdash;
      35 of 75 validators come and go over the window.
    </div>
  </div>
</div>

<div id="rw-tip"></div>
<script>
(function(){{
  var D={js}, L={L2}, T={T2}, PW={pw2}, PH={ph2}, VB={W2};
  var svgs=document.querySelectorAll('.rw-box svg'),
      svg=svgs[svgs.length-1],
      hit=document.getElementById('rw-hit'),
      cross=document.getElementById('rw-cross'),
      tip=document.getElementById('rw-tip');
  if(!svg||!hit) return;
  function at(arr,x){{
    for(var i=0;i<arr.length;i++){{ if(arr[i]>x) return i; }}
    return 100;
  }}
  hit.addEventListener('mousemove',function(ev){{
    var r=svg.getBoundingClientRect(), k=VB/r.width;
    var ux=(ev.clientX-r.left)*k;
    var frac=Math.min(1,Math.max(0,(ux-L)/PW)), x=frac*D.xmax;
    cross.setAttribute('x1',L+PW*frac); cross.setAttribute('x2',L+PW*frac);
    cross.setAttribute('opacity','1');
    var h='<div class="tx">'+x.toFixed(5)+' SOL</div>';
    h+='<div class="tr"><span class="rw-sw" style="background:{C_GBX}"></span>'
      +'<span>GBX</span><span>'+at(D.gbx,x)+'%</span></div>';
    h+='<div class="tr"><span class="rw-sw" style="background:{C_HARM}"></span>'
      +'<span>Agave Harmonic</span><span>'+at(D.harm,x)+'%</span></div>';
    tip.innerHTML=h; tip.style.display='block';
    var tw=tip.offsetWidth, th=tip.offsetHeight;
    tip.style.left=Math.min(window.innerWidth-tw-12,ev.clientX+16)+'px';
    tip.style.top=Math.max(8,ev.clientY-th/2)+'px';
  }});
  hit.addEventListener('mouseleave',function(){{
    cross.setAttribute('opacity','0'); tip.style.display='none';
  }});
}})();
</script>
</body></html>"""
