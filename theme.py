"""Colours, light and dark. Since 2026-11-20 the palette is val-gbx-comp's
(warm neutrals, blue + Gravity orange); the token names below were laid
down for the BI dashboard's Streamlit theme and kept, so no page changed.

Every colour on every page is a CSS custom property defined here; no page
carries a literal colour. The values are Streamlit 1.52's own (read off its
frontend bundle, `static/js/index.*.js`: `colors`, `requiredThemeColors`, the
dark `genericColors`, `computeDerivedColors` and the chart palettes), so the
two dashboards render with the same palette:

  page / sidebar      bgColor / secondaryBg (white, #f0f2f6 | #0e1117, #262730)
  panels              bgMix = halfway between the two
  text                bodyText, and fadedText60 / fadedText40 for the quieter
  borders             fadedText05 / fadedText10 (bodyText at 10 / 20 %)
  good bad warn ...   {green,red,yellow,orange,blue,violet}TextColor, the
                      matching *Color for fills and *BackgroundColor for tints
  links / accent      blueTextColor (Streamlit's `link`)
  primary             #ff4b4b (focus, active nav, sliders)
  charts c0..c9       chartCategoricalColors (light and dark orders differ)

Light or dark follows the operating system, as Streamlit's "Use system
setting" does, unless this browser picked one with the sidebar toggle
(localStorage, like the BI dashboard's per-browser theme toggle).

Plotly cannot read CSS variables, so figures keep `var(--x)` strings and
`bbTheme.plot()` resolves them against the live theme before drawing, and
redraws every figure when the theme changes.
"""

# The palette of val-gbx-comp (the validator-vs-Gravity site on the same
# box): warm neutrals, one blue for focus and links, Gravity's orange as the
# second accent. `surface` is the card colour, a step off the page.
LIGHT = {
    "bg": "#f6f6f4", "bg1": "#f1f1ee", "bg2": "#eeeeea", "bg3": "#e4e4df",
    "surface": "#ffffff",
    # lines stay translucent (text at 8 / 13 / 24 %): opaque values here
    # made every Plotly label render washed out
    "line": "rgba(20,20,19,.08)", "line2": "rgba(20,20,19,.13)",
    "line3": "rgba(20,20,19,.24)",
    "text": "#141413", "text2": "#52514e", "text3": "#85847e",
    "on-color": "#ffffff", "primary": "#2a78d6",
    "hover": "rgba(20,20,19,.05)", "shadow": "rgba(0,0,0,.12)",
    "good": "#1e7a3c", "good-main": "#2f9e55",
    "good-bg": "rgba(47,158,85,.10)", "good-line": "rgba(47,158,85,.35)",
    "bad": "#c0392b", "bad-main": "#e0533f",
    "bad-bg": "rgba(224,83,63,.09)", "bad-line": "rgba(224,83,63,.35)",
    "warn": "#8a6508", "warn-main": "#e0a91b",
    "warn-bg": "rgba(224,169,27,.12)", "warn-line": "rgba(224,169,27,.5)",
    "orange": "#c4501d", "orange-main": "#eb6834",
    "orange-bg": "rgba(235,104,52,.10)", "orange-line": "rgba(235,104,52,.4)",
    "accent": "#1f63b8", "accent-main": "#2a78d6",
    "accent-bg": "rgba(42,120,214,.09)", "accent-line": "rgba(42,120,214,.38)",
    "violet": "#5b3f8c", "violet-main": "#7c4fd6",
    "violet-bg": "rgba(124,79,214,.1)", "violet-line": "rgba(124,79,214,.4)",
    # chart series: blue first, Gravity orange in the slot Titan's lines use
    "c0": "#2a78d6", "c1": "#8cbcf0", "c2": "#d6453d", "c3": "#f0a8a2",
    "c4": "#1f9a85", "c5": "#86d9b0", "c6": "#eb6834", "c7": "#f2c46b",
    "c8": "#6d4fc0", "c9": "#cfcdc6",
    "c0-a": "rgba(42,120,214,.3)", "c2-a": "rgba(214,69,61,.35)",
    "c4-a": "rgba(31,154,133,.35)", "c6-a": "rgba(235,104,52,.35)",
    "c6-b": "rgba(235,104,52,.6)", "c8-a": "rgba(109,79,192,.45)",
}

DARK = {
    "bg": "#141413", "bg1": "#181817", "bg2": "#272725", "bg3": "#30302d",
    "surface": "#1d1d1b",
    "line": "rgba(243,242,238,.09)", "line2": "rgba(243,242,238,.14)",
    "line3": "rgba(243,242,238,.26)",
    "text": "#f3f2ee", "text2": "#c3c2b7", "text3": "#8d8c84",
    "on-color": "#141413", "primary": "#3987e5",
    "hover": "rgba(243,242,238,.06)", "shadow": "rgba(0,0,0,.55)",
    "good": "#6fd08f", "good-main": "#2f9e55",
    "good-bg": "rgba(111,208,143,.14)", "good-line": "rgba(111,208,143,.4)",
    "bad": "#ff8a7a", "bad-main": "#e0533f",
    "bad-bg": "rgba(255,138,122,.14)", "bad-line": "rgba(255,138,122,.4)",
    "warn": "#f2d27a", "warn-main": "#e0a91b",
    "warn-bg": "rgba(242,210,122,.14)", "warn-line": "rgba(242,210,122,.4)",
    "orange": "#f29a6e", "orange-main": "#d95926",
    "orange-bg": "rgba(217,89,38,.16)", "orange-line": "rgba(217,89,38,.45)",
    "accent": "#6aa8f0", "accent-main": "#3987e5",
    "accent-bg": "rgba(57,135,229,.15)", "accent-line": "rgba(57,135,229,.45)",
    "violet": "#b79cf5", "violet-main": "#8b63e6",
    "violet-bg": "rgba(139,99,230,.18)", "violet-line": "rgba(139,99,230,.45)",
    "c0": "#5b9df0", "c1": "#2a6bc0", "c2": "#f08a80", "c3": "#c0443a",
    "c4": "#5fd0b4", "c5": "#1f9a85", "c6": "#f08a5d", "c7": "#f2c46b",
    "c8": "#9b7fe6", "c9": "#cfcdc6",
    "c0-a": "rgba(91,157,240,.3)", "c2-a": "rgba(240,138,128,.35)",
    "c4-a": "rgba(95,208,180,.35)", "c6-a": "rgba(240,138,93,.35)",
    "c6-b": "rgba(240,138,93,.6)", "c8-a": "rgba(155,127,230,.45)",
}

assert LIGHT.keys() == DARK.keys()


def _vars(d):
    return ";".join(f"--{k}:{v}" for k, v in d.items())


CSS = (
    f":root{{color-scheme:light;{_vars(LIGHT)}}}"
    "@media (prefers-color-scheme:dark){:root:not([data-theme=light])"
    f"{{color-scheme:dark;{_vars(DARK)}}}}}"
    f":root[data-theme=dark]{{color-scheme:dark;{_vars(DARK)}}}"
    "html,body{background:var(--bg);color:var(--text)}"
    "::selection{background:var(--accent-bg)}"
    ".sthemebtn{display:block;width:calc(100% - 20px);margin:0 10px 6px;"
    "padding:6px 10px;border:1px solid var(--line2);border-radius:8px;"
    "background:var(--bg);color:var(--text);font-size:12.5px;cursor:pointer;"
    "text-align:center}"
    ".sthemebtn:hover{border-color:var(--primary);color:var(--primary)}"
    # Tom Select's stock stylesheet paints a white box; pages narrow it further
    ".ts-wrapper .ts-control,.ts-wrapper.single.input-active .ts-control,"
    ".ts-dropdown{background:var(--bg)!important;color:var(--text);border-color:var(--line2)}"
    ".ts-wrapper .ts-control input{color:var(--text)}"
    ".ts-dropdown .active{background:var(--hover);color:var(--text)}"
)

# Runs at the top of <body> (inside the sidebar markup), before anything that
# draws: applies a stored theme choice and defines bbTheme for the Plotly
# draw sites (ui.CHART_JS, health.HEALTH_JS, the rewards page).
HEAD_JS = r"""
(function(){
  var KEY = 'simbench_theme_v1', root = document.documentElement, plots = [], memo = {};
  try { var t = localStorage.getItem(KEY);
        if (t === 'light' || t === 'dark') root.setAttribute('data-theme', t); } catch (e) {}
  var mq = window.matchMedia ? matchMedia('(prefers-color-scheme: dark)') : null;
  function current(){
    return root.getAttribute('data-theme') || (mq && mq.matches ? 'dark' : 'light');
  }
  function tok(n){
    if (!(n in memo)) memo[n] = getComputedStyle(root).getPropertyValue(n).trim() || 'currentColor';
    return memo[n];
  }
  function resolve(v){
    if (typeof v === 'string')
      return v.indexOf('var(--') < 0 ? v
        : v.replace(/var\((--[a-z0-9-]+)\)/g, function(_, n){ return tok(n); });
    if (Array.isArray(v))
      return (v.length && typeof v[0] === 'number') ? v : v.map(resolve);
    if (v && typeof v === 'object') {
      var o = {}; for (var k in v) o[k] = resolve(v[k]); return o;
    }
    return v;
  }
  function redraw(){
    memo = {};
    plots.forEach(function(el){
      if (!el.isConnected || !window.Plotly) return;
      var s = el._bbSrc; Plotly.react(el, resolve(s[0]), resolve(s[1]), s[2]);
    });
    document.querySelectorAll('.sthemebtn').forEach(label);
    document.dispatchEvent(new CustomEvent('bbtheme', {detail: current()}));
  }
  function label(b){
    b.textContent = current() === 'dark' ? '☀️  Light mode' : '🌙  Dark mode';
  }
  window.bbTheme = {
    current: current, resolve: resolve, label: label,
    plot: function(el, data, layout, config){
      layout = Object.assign({}, layout);
      layout.font = Object.assign({family: 'IBM Plex Sans, system-ui, sans-serif'},
                                  layout.font || {});
      el._bbSrc = [data, layout, config];
      if (plots.indexOf(el) < 0) plots.push(el);
      return Plotly.newPlot(el, resolve(data), resolve(layout), config);
    },
    toggle: function(){
      var t = current() === 'dark' ? 'light' : 'dark';
      root.setAttribute('data-theme', t);
      try { localStorage.setItem(KEY, t); } catch (e) {}
      redraw();
    }
  };
  if (mq && mq.addEventListener) mq.addEventListener('change', function(){
    if (!root.getAttribute('data-theme')) redraw();
  });
  document.addEventListener('click', function(ev){
    if (ev.target.closest && ev.target.closest('.sthemebtn')) window.bbTheme.toggle();
  });
})();
"""
