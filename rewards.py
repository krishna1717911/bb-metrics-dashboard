#!/usr/bin/env python3
"""Per-block reward distribution by scheduler group -- the /rewards page.

Kept out of app.py deliberately: this is a self-contained feature reading a
static rewards_aug2026.json, and app.py is already 8k lines that several
people edit. app.py only needs the route and the nav link.

Regenerating the data is a separate offline step (Dune + svt.one); nothing
here queries anything at request time.
"""

import html
import json
import os

# --------------------------------------------------------------- rewards ECDF
# Per-block reward distribution by scheduler/client, August 2026. Static: the
# numbers come from rewards_aug2026.json, generated once from Dune (exact
# per-validator histograms of fee+tips) and cross-checked against svt.one.
# Nothing here queries anything at request time.

REWARDS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "rewards_aug2026.json")

# Featured series carry hue; everything else is recessive. Four hues max --
# a categorical palette is assigned in fixed order and never cycled, so the
# 15 groups here cannot each get their own colour. The rest stay grey and
# their numbers live in the table below the chart.
# Palette slots assigned in fixed order, never cycled. Validated as a 7-slot
# set against this dashboard's own surface (#0b1016): all six checks pass.
REW_FEATURED = [
    ("GBX (Astralane)",                    "#3987e5", 2.6),
    ("Agave Jito",                         "#d95926", 2.0),
    ("Frankendancer Harmonic Performance", "#199e70", 2.0),
    ("Frankendancer Harmonic Balanced",    "#c98500", 2.0),
    ("Firedancer Harmonic Balanced",       "#d55181", 2.0),
    ("Agave JitoBAM",                      "#008300", 2.0),
    ("Firedancer BAM",                     "#9085e9", 2.0),
]
REW_BASELINE = "All validators"

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
.rw-chartbox{background:#0e151d;border:1px solid #1e2937;border-radius:11px;
  padding:16px 18px 10px;overflow-x:auto}
.rw-chartbox h2{margin:0 0 2px;font-size:14px;font-weight:650;color:#dbe4ee}
.rw-chartbox .cs{color:#6b7f96;font-size:11.5px;margin-bottom:10px}
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
.rw-tbl tr.me td{background:#12314d33}
.rw-tbl tr.me td:first-child{color:#7cc0ff;font-weight:650}
.rw-tbl tr.base td{color:#8fa6bf;font-style:italic}
.rw-dot{display:inline-block;width:8px;height:8px;border-radius:50%;
  margin-right:7px;vertical-align:middle}
.rw-note{color:#6b7f96;font-size:11px;margin-top:9px;line-height:1.6}
#rw-tip{position:fixed;display:none;z-index:80;background:#0d151d;
  border:1px solid #2f4256;border-radius:8px;padding:9px 11px;
  box-shadow:0 10px 30px #000a;font-size:11.5px;color:#c3d3e6;
  pointer-events:none;min-width:186px}
#rw-tip .tx{color:#dbe4ee;font-weight:650;margin-bottom:6px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr{display:flex;align-items:center;gap:7px;margin-top:3px;
  font-variant-numeric:tabular-nums}
#rw-tip .tr span:last-child{margin-left:auto;color:#dbe4ee}
"""


def rewards_page(CSS, purl):
    """Per-block reward ECDF by client. Static file, no live queries."""
    with open(REWARDS_JSON) as fh:
        D = json.load(fh)
    meta, groups, per_val = D["meta"], D["groups"], D["gbx_validators"]
    XMAX, NPTS = meta["xmax"], meta["npts"]

    # ---- geometry
    W, H = 1010, 470
    L, R, T, B = 62, 176, 18, 46
    pw, ph = W - L - R, H - T - B
    sx = lambda v: L + pw * (v / XMAX)
    sy = lambda f: T + ph * (1.0 - f)

    def poly(ecdf):
        return " ".join(f"{sx(XMAX*i/NPTS):.1f},{sy(v):.1f}"
                        for i, v in enumerate(ecdf))

    featured = {k for k, _, _ in REW_FEATURED}
    # rows kept out of the table: the window-matched control is a diagnostic,
    # and the Harmonic roll-ups double-count the per-family rows below them.
    HIDDEN = {"Agave Jito (window-matched)",
              "Harmonic Performance", "Harmonic Balanced", "Harmonic FIFO"}
    # >4 series: the legend carries identity on its own. Direct labels would
    # cascade far from their curves (every curve sits at 95-99% at x-max) and
    # point at the wrong line, which is worse than no label.
    DIRECT_LABELS = len([k for k, _, _ in REW_FEATURED if k in groups]) <= 4
    svg = []

    # grid + axes, recessive
    for i in range(11):
        y = sy(i / 10.0)
        svg.append(f'<line x1="{L}" y1="{y:.1f}" x2="{L+pw}" y2="{y:.1f}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{L-9}" y="{y+3.5:.1f}" text-anchor="end" '
                   f'fill="#6b7f96" font-size="10.5">{i*10}%</text>')
    xt = 0.0
    while xt <= XMAX + 1e-9:
        x = sx(xt)
        svg.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T+ph}" '
                   f'stroke="#1a2431" stroke-width="1"/>')
        svg.append(f'<text x="{x:.1f}" y="{T+ph+18}" text-anchor="middle" '
                   f'fill="#6b7f96" font-size="10.5">{xt:.2f}</text>')
        xt += 0.02
    svg.append(f'<text x="{L+pw/2:.0f}" y="{H-8}" text-anchor="middle" '
               f'fill="#8fa6bf" font-size="11.5">total reward per block '
               f'(SOL) &mdash; fee + Jito tips (svt.one basis)</text>')
    svg.append(f'<text x="14" y="{T+ph/2:.0f}" fill="#8fa6bf" font-size="11.5" '
               f'transform="rotate(-90 14 {T+ph/2:.0f})" '
               f'text-anchor="middle">cumulative share of blocks</text>')

    # layer 1: non-featured clients, recessive
    for k, v in sorted(groups.items(), key=lambda kv: -kv[1]["mean"]):
        if k in featured or k == REW_BASELINE or k in HIDDEN:
            continue   # HIDDEN roll-ups are drawn via REW_FEATURED, not here
        svg.append(f'<polyline points="{poly(v["ecdf"])}" fill="none" '
                   f'stroke="#2b3a4c" stroke-width="1"/>')
    # layer 2: all-validator baseline, dashed neutral (not a hue -- it is a
    # reference line, not a series competing for identity)
    if REW_BASELINE in groups:
        svg.append(f'<polyline points="{poly(groups[REW_BASELINE]["ecdf"])}" '
                   f'fill="none" stroke="#93a7bd" stroke-width="1.8" '
                   f'stroke-dasharray="5 4"/>')
    # layer 3: featured, on top, with a surface ring so overlaps stay legible
    for k, col, wd in REW_FEATURED:
        if k not in groups:
            continue
        p = poly(groups[k]["ecdf"])
        svg.append(f'<polyline points="{p}" fill="none" stroke="#0e151d" '
                   f'stroke-width="{wd+2.4:.1f}" stroke-linejoin="round"/>')
        svg.append(f'<polyline points="{p}" fill="none" stroke="{col}" '
                   f'stroke-width="{wd}" stroke-linejoin="round"/>')

    # p50 / p75 / p90 markers on GBX only -- selective, not every point
    gbx = groups.get("GBX (Astralane)")
    if gbx:
        for q, lab in (("p50", "P50"), ("p75", "P75"), ("p90", "P90")):
            xv = gbx[q]
            if xv is None or xv > XMAX:
                continue
            f = {"p50": .50, "p75": .75, "p90": .90}[q]
            svg.append(f'<circle cx="{sx(xv):.1f}" cy="{sy(f):.1f}" r="4.6" '
                       f'fill="#3987e5" stroke="#0e151d" stroke-width="2"/>')
            svg.append(f'<text x="{sx(xv):.1f}" y="{sy(f)-11:.1f}" '
                       f'text-anchor="middle" fill="#7cc0ff" font-size="9.5" '
                       f'font-weight="600">{lab} {xv:.4f}</text>')

    # direct labels at the right edge (<=4 featured series, so all get one)
    lab_y = []
    for k, col, _ in (REW_FEATURED if DIRECT_LABELS else []):
        if k not in groups:
            continue
        y = sy(groups[k]["ecdf"][-1])
        while any(abs(y - o) < 13 for o in lab_y):
            y += 13
        lab_y.append(y)
        svg.append(f'<text x="{L+pw+9}" y="{y+3.5:.1f}" fill="{col}" '
                   f'font-size="11" font-weight="600">{html.escape(k)}</text>')
    if DIRECT_LABELS and REW_BASELINE in groups:
        y = sy(groups[REW_BASELINE]["ecdf"][-1])
        while any(abs(y - o) < 13 for o in lab_y):
            y += 13
        svg.append(f'<text x="{L+pw+9}" y="{y+3.5:.1f}" fill="#93a7bd" '
                   f'font-size="11" font-style="italic">all validators</text>')

    svg.append(f'<line id="rw-cross" x1="0" y1="{T}" x2="0" y2="{T+ph}" '
               f'stroke="#5eead4" stroke-width="1" stroke-dasharray="3 3" '
               f'opacity="0"/>')
    svg.append(f'<rect id="rw-hit" x="{L}" y="{T}" width="{pw}" height="{ph}" '
               f'fill="transparent"/>')

    # ---- hero tiles
    def pctdiff(a, b):
        return (a / b - 1) * 100 if (a and b) else 0.0
    cohort = groups.get("Agave Jito", {})
    allv = groups.get(REW_BASELINE, {})
    rank = sorted(((v["mean"], k) for k, v in groups.items()
                   if k != REW_BASELINE and k not in HIDDEN), reverse=True)
    my_rank = next((i + 1 for i, (_, k) in enumerate(rank)
                    if k == "GBX (Astralane)"), None)
    d_coh = pctdiff(gbx["mean"], cohort.get("mean"))
    d_wm = pctdiff(gbx["mean"], groups.get("Agave Jito (window-matched)", {}).get("mean"))
    d_all = pctdiff(gbx["mean"], allv.get("mean"))
    tiles = [
        ("mean reward / block", f'{gbx["mean"]:.4f}', "SOL, fee + tips", ""),
        ("vs Agave Jito cohort", f'{d_coh:+.1f}%',
         "same client, our validators removed", "up" if d_coh > 0 else "dn"),
        ("vs all validators", f'{d_all:+.1f}%',
         f'{allv.get("blocks",0):,} blocks', "up" if d_all > 0 else "dn"),
        ("rank by mean", f'{my_rank} / {len(rank)}', "scheduler groups", ""),
        ("tips as share of reward", f'{gbx["tip_share"]*100:.1f}%',
         f'cohort {cohort.get("tip_share",0)*100:.1f}% &mdash; post-commission,'
         f' understated', ""),
        ("blocks measured", f'{gbx["blocks"]:,}',
         f'{gbx["validators"]} validators, post-connection only', ""),
    ]
    hero = "".join(
        f'<div class="rw-tile"><div class="k">{k}</div>'
        f'<div class="v {cls}">{v}</div><div class="d">{d}</div></div>'
        for k, v, d, cls in tiles)

    # ---- legend
    lg = "".join(
        f'<div class="rw-lg"><span class="rw-sw" style="background:{c}"></span>'
        f'{html.escape(k)}</div>'
        for k, c, _ in REW_FEATURED if k in groups)
    lg += ('<div class="rw-lg"><span class="rw-sw" style="background:'
           'repeating-linear-gradient(90deg,#93a7bd 0 5px,transparent 5px 9px)">'
           '</span>all validators (baseline)</div>'
           '<div class="rw-lg"><span class="rw-sw" style="background:#2b3a4c">'
           '</span>other scheduler groups</div>')

    # ---- full table: every group, so identity is never colour-alone
    colour_of = {k: c for k, c, _ in REW_FEATURED}
    rows = []
    for k, v in sorted(groups.items(), key=lambda kv: -kv[1]["mean"]):
        if k == REW_BASELINE or k in HIDDEN:
            continue
        dot = (f'<span class="rw-dot" style="background:{colour_of[k]}"></span>'
               if k in colour_of else
               '<span class="rw-dot" style="background:#2b3a4c"></span>')
        rel = pctdiff(v["mean"], allv.get("mean"))
        rows.append(
            f'<tr class="{"me" if k=="GBX (Astralane)" else ""}">'
            f'<td>{dot}{html.escape(k)}</td>'
            f'<td>{v["validators"]:,}</td><td>{v["blocks"]:,}</td>'
            f'<td>{v["p10"]:.4f}</td><td>{v["p50"]:.4f}</td>'
            f'<td>{v["p75"]:.4f}</td><td>{v["p90"]:.4f}</td>'
            f'<td>{v["p99"]:.4f}</td><td><b>{v["mean"]:.4f}</b></td>'
            f'<td>{v["tip_share"]*100:.1f}%</td>' if v["tip_share"] is not None else '<td>&mdash;</td>'
            f'<td class="{"greenc" if rel>0 else "redc"}">{rel:+.1f}%</td></tr>')
    if allv:
        rows.append(
            f'<tr class="base"><td>all validators (baseline)</td>'
            f'<td>{allv["validators"]:,}</td><td>{allv["blocks"]:,}</td>'
            f'<td>{allv["p10"]:.4f}</td><td>{allv["p50"]:.4f}</td>'
            f'<td>{allv["p75"]:.4f}</td><td>{allv["p90"]:.4f}</td>'
            f'<td>{allv["p99"]:.4f}</td><td><b>{allv["mean"]:.4f}</b></td>'
            f'<td>{allv["tip_share"]*100:.1f}%</td><td>&mdash;</td></tr>')

    # ---- GBX per-validator, with the detrended before/after
    vrows = []
    for lab, v in sorted(per_val.items(), key=lambda kv: -kv[1]["post_n"]):
        lift = v["detrended_lift"]
        lc = "greenc" if (lift or 0) > 0 else "redc"
        ls = f'{lift*100:+.1f}%' if lift is not None else "&mdash;"
        pr = f'{v["pre_ratio"]:.3f}' if v["pre_ratio"] else "&mdash;"
        po = f'{v["post_ratio"]:.3f}' if v["post_ratio"] else "&mdash;"
        cflag = ' class="redc"' if v.get("mev_commission", 0) == 0 else ''
        vrows.append(
            f'<tr><td>{html.escape(lab)}</td>'
            f'<td style="font-size:11px">{html.escape(v.get("connected",""))}</td>'
            f'<td{cflag}>{v.get("mev_commission",0)/100:.0f}%</td>'
            f'<td{cflag}>{v.get("tip_per_slot",0):.6f}</td>'
            f'<td>{v["pre_n"]:,}</td><td>{pr}</td>'
            f'<td>{v["post_n"]:,}</td><td>{po}</td>'
            f'<td class="{lc}">{ls}</td></tr>')

    # data for the hover layer
    js_series = [{"k": k, "c": c, "e": groups[k]["ecdf"]}
                 for k, c, _ in REW_FEATURED if k in groups]
    if REW_BASELINE in groups:
        js_series.append({"k": "all validators", "c": "#93a7bd",
                          "e": groups[REW_BASELINE]["ecdf"]})

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>simbench &middot; reward distribution by scheduler</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}{REWARDS_CSS}</style></head><body>
<header>
  <h1>sim<span>bench</span></h1>
  <div class="sub">reward distribution by scheduler &mdash; per-block ECDF,
    August 2026</div>
  <a class="navlink" href="{purl("/")}">&larr; back to slots</a>
  <a class="navlink" href="{purl("/reference")}">metrics reference</a>
</header>

<div class="callout">
  <b>Tips here are svt.one <code>jitoReward</code> spread evenly over the
  epoch's leader slots &mdash; a post-commission figure.</b>
  It understates real tip capture wherever MEV commission is below 100%:
  network tip share reads {allv.get("tip_share",0)*100:.1f}% on this basis against 18.4% measured
  from on-chain tip-account inflows. Three GBX validators (BCJ, Cogent51,
  ssx2rHZN) run 0% MEV commission and so contribute <b>zero</b> tips here
  despite capturing ~180 SOL in August. Because the same method is applied to
  {allv.get("validators",0)} validators, the <i>relative</i> ranking holds up far better than
  the absolute level &mdash; but do not quote these SOL figures as revenue.
  Tips are also an epoch constant, so the curves show fee shape only.
</div>

<div class="rw-wrap">
  <div class="rw-hero">{hero}</div>

  <div class="rw-chartbox">
    <h2>Per-block reward distribution</h2>
    <div class="cs">Each curve is the exact empirical CDF over every block the
      group produced in August 2026 (GBX: post-connection only): at reward <i>x</i>, the height is the share
      of that group's blocks earning at or below <i>x</i>. Lower and further
      right is better. Markers show GBX's own P50/P75/P90.</div>
    <svg viewBox="0 0 {W} {H}" width="100%" style="max-width:{W}px;display:block"
         role="img" aria-label="Empirical CDF of per-block reward by scheduler
         group, August 2026. Full values in the table below.">
      {''.join(svg)}
    </svg>
    <div class="rw-legend">{lg}</div>
    <div class="rw-note">
      x-axis is trimmed at {XMAX:.2f} SOL, where the curves stand at
      95&ndash;99% &mdash; the remaining 1&ndash;5% is a long right tail that
      would flatten everything else. P99 values are in the table.
      Bin width {meta['bin_sol']*1000:.1f} mSOL; fee percentiles are exact
      cumulative counts, not a t-digest estimate. Tips are a per-validator,
      per-epoch constant, so each validator-epoch's fee curve is shifted by a
      fixed amount &mdash; the curve shape is the fee distribution, not the
      tip distribution.
    </div>
  </div>

  <table class="rw-tbl">
    <thead><tr><th>scheduler group</th><th>vals</th><th>blocks</th>
      <th>p10</th><th>p50</th><th>p75</th><th>p90</th><th>p99</th>
      <th>mean</th><th>tip share</th><th>vs all</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="rw-note">
    All values SOL per block, fee + Jito tips.
    {meta['unmapped_blocks']:,} blocks belong to validators absent from the
    client report and are counted in the baseline only, never in a client row.
  </div>

  <h2 style="margin:30px 0 2px;font-size:14px;color:#dbe4ee">GBX validators
    &mdash; before / after connection</h2>
  <div class="rw-note" style="margin:0 0 10px">
    <b>0% MEV commission is flagged red &mdash; svt.one reports no tips for
    those validators at all, so their totals are fee-only.</b>
    The pre/post columns are fee-only and <b>detrended</b>: each block is divided by the network median
    fee in its own hour, so 1.000 means a typical block. The raw comparison is
    invalid &mdash; August's network median fee swung from 0.0137 to 0.0304 SOL
    (2.2&times;), which alone makes early-August connections look good and late
    ones look bad. Do not pool these: differing pre/post sample sizes make the
    pooled figure a Simpson's-paradox artifact.
  </div>
  <table class="rw-tbl">
    <thead><tr><th>validator</th><th>connected</th><th>mev comm</th>
      <th>tip/slot</th><th>pre n</th><th>pre ratio</th><th>post n</th>
      <th>post ratio</th><th>detrended lift</th></tr></thead>
    <tbody>{''.join(vrows)}</tbody>
  </table>

  <div class="rw-note" style="margin-top:22px;line-height:1.75">
    <b style="color:#8fa6bf">Provenance and caveats</b><br>
    <b>Window</b> {meta['window']} (epochs {meta['epochs']}),
    {meta['slots_total']:,} blocks network-wide.
    <b>Rewards</b> Dune <code>solana.rewards</code>, <code>reward_type='Fee'</code>
    &mdash; exact per-validator histograms, {meta['dune_credits_spent']} credits.
    <b>Tips</b> {html.escape(meta['tip_source'])}.<br>
    <b>Tip caveat</b> {html.escape(meta['tip_caveat'])}<br>
    <b>Connection dates</b> {html.escape(meta['gbx_note'])}<br>
    <b>Window control</b> {html.escape(meta['window_matched_note'])}<br>
    <b>Harmonic modes</b> {html.escape(meta.get('harmonic_modes',''))}<br>
    <b>Roll-ups</b> {html.escape(meta.get('rollup_note',''))}<br>
    <b>Known weakness</b> client labels come from
    <code>fd validator report.csv</code>, whose block counts imply a ~22-day
    window, applied here to all 31 days of August. A validator that changed
    scheduler mid-month is mislabelled. Regenerating that report over exactly
    August would close the last gap.
  </div>
</div>

<div id="rw-tip"></div>
<script>
(function(){{
  var S={json.dumps(js_series)}, XMAX={XMAX}, N={NPTS},
      L={L}, T={T}, PW={pw}, PH={ph};
  var svg=document.querySelector('.rw-chartbox svg'),
      hit=document.getElementById('rw-hit'),
      cross=document.getElementById('rw-cross'),
      tip=document.getElementById('rw-tip');
  if(!svg||!hit) return;
  function move(ev){{
    var r=svg.getBoundingClientRect(), k=1010/r.width;
    var ux=(ev.clientX-r.left)*k;
    var f=Math.min(1,Math.max(0,(ux-L)/PW)), i=Math.round(f*N);
    var x=XMAX*i/N;
    cross.setAttribute('x1',L+PW*i/N); cross.setAttribute('x2',L+PW*i/N);
    cross.setAttribute('opacity','1');
    var h='<div class="tx">'+x.toFixed(4)+' SOL</div>';
    S.forEach(function(s){{
      h+='<div class="tr"><span class="rw-sw" style="background:'+s.c+
         '"></span><span>'+s.k+'</span><span>'+
         (s.e[i]*100).toFixed(1)+'%</span></div>';
    }});
    tip.innerHTML=h; tip.style.display='block';
    var tw=tip.offsetWidth, th=tip.offsetHeight;
    tip.style.left=Math.min(window.innerWidth-tw-12,ev.clientX+16)+'px';
    tip.style.top=Math.max(8,ev.clientY-th/2)+'px';
  }}
  hit.addEventListener('mousemove',move);
  hit.addEventListener('mouseleave',function(){{
    cross.setAttribute('opacity','0'); tip.style.display='none';
  }});
}})();
</script>
</body></html>"""
