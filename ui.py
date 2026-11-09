"""The page furniture the BI dashboard (astralane-quant/python/bi_dashboard)
uses, rebuilt for this server-rendered site: a sidebar grouped by section
with favourite pages, rows of KPI tiles that say how far a number moved and
whether that is better or worse, info boxes, expanders, and a grid for the
Plotly charts.

Only markup and CSS live here. Pages pass in what to show; nothing in this
module queries anything.
"""

import html

import theme
import json


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def delta(cur, base, better=None, unit="", pct=True, digits=1, small=1.0):
    """The st.metric delta: the change from `base` to `cur`, coloured by
    whether it is good news. `better` is "up", "down" or None (neither
    direction is good or bad, e.g. traffic volume). A change under `small`
    percent reads as flat, so noise is not painted red or green."""
    c, b = _num(cur), _num(base)
    if c is None or b is None:
        return '<span class="udelta na">no baseline</span>'
    diff = c - b
    rel = (100.0 * diff / abs(b)) if b else None
    flat = (rel is not None and abs(rel) < small) or (b == 0 and c == 0)
    arrow = "&#9650;" if diff > 0 else ("&#9660;" if diff < 0 else "&#9679;")
    if flat or better is None or diff == 0:
        cls = "flat"
    elif (diff > 0) == (better == "up"):
        cls = "good"
    else:
        cls = "bad"
    if pct and rel is not None:
        txt = f"{rel:+.{digits}f}%"
    else:
        txt = f"{diff:+,.{digits}f}{unit}"
    return f'<span class="udelta {cls}">{arrow} {txt}</span>'


def kpi(label, value, sub="", d="", help_=""):
    """One tile: label, the big number, the delta and a line of context."""
    t = f' title="{html.escape(help_, quote=True)}"' if help_ else ""
    return (f'<div class="kpi"{t}><div class="kl">{label}</div>'
            f'<div class="kv">{value}</div>'
            + (f'<div class="kdl">{d}</div>' if d else "")
            + (f'<div class="ks">{sub}</div>' if sub else "") + "</div>")


def kpi_row(tiles):
    return f'<div class="kpirow">{"".join(tiles)}</div>'


def info(text, kind="info"):
    """st.info / st.warning: a coloured box with an explanation in it."""
    return f'<div class="ubox ubox-{kind}">{text}</div>'


def expander(title, inner, open_=False):
    return (f'<details class="exp"{" open" if open_ else ""}>'
            f"<summary>{title}</summary><div class=\"expb\">{inner}</div>"
            "</details>")


def section(title, sub=""):
    return (f'<h2 class="usec">{title}'
            + (f'<span class="usub">{sub}</span>' if sub else "") + "</h2>")


def chart_grid(figs, cls="cgrid"):
    """Plotly figures as JSON blobs in a responsive grid; CHART_JS draws
    every `.ufig` on the page once Plotly has loaded."""
    out = []
    for f in figs:
        blob = json.dumps(f, separators=(",", ":")).replace("</", "<\\/")
        out.append('<div class="ucard"><div class="utool">'
                   '<button type="button" class="ucsv" title="download this '
                   "chart's data as CSV\">&#11015; CSV</button></div>"
                   '<div class="uplot"></div>'
                   f'<script type="application/json" class="ufig">{blob}'
                   "</script></div>")
    return f'<div class="{cls}">{"".join(out)}</div>'


# Draws every chart_grid figure. Plotly is loaded once, after the page.
CHART_JS = r"""
(function(){
  var figs = document.querySelectorAll('.ufig');
  if (!figs.length) return;
  function draw(){
    figs.forEach(function(s){
      var f = JSON.parse(s.textContent),
          el = s.parentNode.querySelector('.uplot');
      bbTheme.plot(el, f.data, f.layout, {displaylogo:false, responsive:true,
        modeBarButtonsToRemove:['lasso2d','select2d','autoScale2d']});
    });
  }
  // st.download_button: every chart's points, one row per (series, x)
  document.addEventListener('click', function(ev){
    var b = ev.target.closest && ev.target.closest('.ucsv');
    if (!b) return;
    var f = JSON.parse(b.closest('.ucard').querySelector('.ufig').textContent);
    var q = function(v){ v = v == null ? '' : String(v);
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
    var rows = ['series,x,y'];
    f.data.forEach(function(t){
      (t.x || []).forEach(function(x, i){
        rows.push([q(t.name), q(x), q((t.y || [])[i])].join(','));
      });
    });
    var name = ((f.layout.title || {}).text || 'chart')
      .replace(/[^a-z0-9]+/gi, '_').replace(/^_|_$/g, '').toLowerCase();
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([rows.join('\n') + '\n'],
                                          {type: 'text/csv'}));
    a.download = name + '.csv';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function(){ URL.revokeObjectURL(a.href); }, 1000);
  });
  if (window.Plotly) return draw();
  var sc = document.createElement('script');
  sc.src = '__PLOTLY_CDN__'; sc.charset = 'utf-8'; sc.onload = draw;
  document.head.appendChild(sc);
})();
"""


# Sidebar favourites live in this browser only (localStorage), like the BI
# dashboard's cookie: a convenience, never state anything depends on.
SIDEBAR_JS = r"""
(function(){
  var KEY = 'simbench_favourites_v1', nav = document.querySelector('.snav');
  if (!nav) return;
  function read(){
    try { var v = JSON.parse(localStorage.getItem(KEY) || '[]');
          return Array.isArray(v) ? v : []; } catch (e) { return []; }
  }
  function save(v){ try { localStorage.setItem(KEY, JSON.stringify(v)); } catch (e) {} }
  function render(){
    var fav = read(), box = nav.querySelector('.sfav');
    nav.querySelectorAll('.sstar').forEach(function(b){
      var on = fav.indexOf(b.dataset.id) >= 0;
      b.textContent = on ? '★' : '☆';
      b.classList.toggle('on', on);
      b.title = on ? 'remove from favourites' : 'add to favourites';
    });
    var links = fav.map(function(id){
      var a = nav.querySelector('.slink[data-id="' + id + '"]');
      return a ? '<a class="slink" href="' + a.getAttribute('href') + '">'
                 + a.textContent + '</a>' : '';
    }).join('');
    box.innerHTML = links || '<div class="snone">star a page to pin it here</div>';
  }
  nav.addEventListener('click', function(ev){
    var b = ev.target.closest('.sstar');
    if (!b) return;
    ev.preventDefault();
    var fav = read(), i = fav.indexOf(b.dataset.id);
    if (i >= 0) fav.splice(i, 1); else fav.push(b.dataset.id);
    save(fav); render();
  });
  var t = document.querySelector('.stoggle');
  if (t) t.addEventListener('click', function(){
    document.body.classList.toggle('snav-open');
  });
  render();
})();
"""


CSS = theme.CSS + """
/* ---- the shell: sidebar + main, as the BI dashboard lays it out ---- */
.app{display:grid;grid-template-columns:232px minmax(0,1fr);min-height:100vh}
.snav{position:sticky;top:0;height:100vh;overflow-y:auto;border-right:1px solid var(--line);
  background:var(--bg2);padding:16px 10px 24px;font-size:13px}
.snav .sbrand{display:block;padding:2px 10px 14px;font-weight:650;font-size:16px;
  color:var(--text);text-decoration:none}
.snav .sbrand span{color:var(--primary)}
.snav .sgrp{margin:14px 0 4px;padding:0 10px;font-size:10px;text-transform:uppercase;
  letter-spacing:.06em;color:var(--text3);font-weight:650}
.snav .srow{position:relative}
.snav .slink{display:block;padding:6px 30px 6px 10px;border-radius:7px;color:var(--text);
  text-decoration:none;line-height:1.35}
.snav .slink:hover{background:var(--hover);color:var(--text)}
.snav .slink.on{background:var(--hover);color:var(--text);font-weight:600;
  box-shadow:inset 3px 0 0 var(--primary)}
.snav .sstar{position:absolute;right:4px;top:3px;width:24px;height:24px;border:0;
  background:transparent;color:var(--text3);cursor:pointer;font-size:14px;line-height:1}
.snav .sstar.on{color:var(--warn)}
.snav .sstar:hover{color:var(--warn)}
.snav .sfav .slink{padding-right:10px}
.snav .snone{padding:4px 10px;color:var(--text3);font-size:11.5px}
.smain{min-width:0}
@media (min-width:901px){.smain>header h1{display:none}}
.stoggle{display:none;background:var(--bg);border:1px solid var(--line2);color:var(--accent);
  border-radius:6px;padding:3px 9px;font-size:13px;cursor:pointer}
@media (max-width:900px){
  .app{grid-template-columns:minmax(0,1fr)}
  .snav{display:none;position:fixed;z-index:50;width:250px;left:0;top:0;
    box-shadow:0 0 0 100vmax var(--shadow)}
  body.snav-open .snav{display:block}
  .stoggle{display:inline-block}
}

/* ---- st.metric-style KPI tiles ---- */
.kpirow{display:grid;gap:10px;margin:10px 28px 4px;
  grid-template-columns:repeat(auto-fill,minmax(180px,1fr))}
.kpi{border:1px solid var(--line);border-radius:10px;background:var(--bg);padding:11px 14px 10px}
.kpi .kl{font-size:11.5px;color:var(--text2);line-height:1.35}
.kpi .kv{font-size:22px;font-weight:650;color:var(--text);margin-top:3px;
  font-variant-numeric:tabular-nums;letter-spacing:-.01em}
.kpi .kdl{margin-top:2px;font-size:12px}
.kpi .ks{margin-top:3px;font-size:11px;color:var(--text3);line-height:1.4}
.udelta{font-weight:600;font-variant-numeric:tabular-nums}
.udelta.good{color:var(--good)}
.udelta.bad{color:var(--bad)}
.udelta.flat{color:var(--text2)}
.udelta.na{color:var(--text3);font-weight:400}

/* ---- st.info / st.warning ---- */
.ubox{margin:12px 28px;padding:11px 15px;border-radius:9px;font-size:12.5px;line-height:1.6}
.ubox-info{background:var(--accent-bg);border:1px solid var(--accent-bg);color:var(--accent)}
.ubox-warn{background:var(--warn-bg);border:1px solid var(--orange-line);color:var(--orange)}
.ubox-err{background:var(--bad-bg);border:1px solid var(--bad-line);color:var(--bad)}
.ubox b{color:var(--text)}

/* ---- st.expander ---- */
.exp{margin:10px 28px;border:1px solid var(--line);border-radius:9px;background:var(--bg)}
.exp>summary{cursor:pointer;padding:9px 14px;color:var(--accent);font-size:13px}
.exp>summary:hover{color:var(--accent)}
.exp .expb{padding:2px 14px 12px;overflow-x:auto}

/* ---- section headers and the chart grid ---- */
.usec{margin:26px 28px 2px;font-size:15px;font-weight:650;color:var(--text);
  display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
.usec .usub{font-size:12px;font-weight:400;color:var(--text3)}
.cgrid{display:grid;gap:12px;margin:10px 28px;
  grid-template-columns:repeat(auto-fill,minmax(440px,1fr))}
@media (max-width:600px){.cgrid{grid-template-columns:minmax(0,1fr)}}
.ucard{position:relative;border:1px solid var(--line);border-radius:10px;background:var(--bg);
  padding:6px 6px 2px}
.utool{position:absolute;right:10px;bottom:8px;z-index:5}
.ucsv{background:var(--bg);border:1px solid var(--line2);color:var(--text2);border-radius:5px;
  font-size:10.5px;padding:1px 7px;cursor:pointer}
.ucsv:hover{color:var(--accent);border-color:var(--accent)}
.uplot{height:250px}

/* ---- a plain data table ---- */
.utab{border-collapse:collapse;width:100%;font-size:12px}
.utab th{text-align:left;color:var(--text3);font-size:10.5px;text-transform:uppercase;
  padding:0 12px 6px 0;border-bottom:1px solid var(--line);font-weight:600}
.utab td{padding:5px 12px 5px 0;border-bottom:1px solid var(--line);color:var(--text);
  font-variant-numeric:tabular-nums}
.utab td.n,.utab th.n{text-align:right}
.utab td.m{font:11.5px ui-monospace,Menlo,monospace}
"""


def sidebar(pages, sections, here, href):
    """The BI dashboard's navigation: favourites first, then one group per
    section, the current page highlighted. `pages` is app.PAGES."""
    out = [f"<script>{theme.HEAD_JS}</script>",
           '<nav class="snav" aria-label="pages">',
           f'<a class="sbrand" href="{html.escape(href("/"))}">'
           "sim<span>bench</span></a>",
           '<button type="button" class="sthemebtn" title="Toggle the theme '
           'for this browser only"></button>'
           "<script>bbTheme.label(document.currentScript.previousElementSibling)"
           "</script>",
           '<div class="sgrp">&#9733; favourites</div><div class="sfav"></div>']
    for sid, label, _ in sections:
        out.append(f'<div class="sgrp">{html.escape(label)}</div>')
        for p in pages:
            if p["section"] != sid:
                continue
            on = " on" if p["id"] == here else ""
            out.append(
                '<div class="srow">'
                f'<a class="slink{on}" data-id="{html.escape(p["id"])}" '
                f'href="{html.escape(href(p["path"]))}">'
                f'{html.escape(p["title"])}</a>'
                f'<button type="button" class="sstar" data-id="{html.escape(p["id"])}" '
                'aria-label="favourite">&#9734;</button></div>')
    out.append("</nav>")
    return "".join(out)
