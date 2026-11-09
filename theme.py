"""Colours: the BI dashboard's Streamlit theme, light and dark.

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

_LIGHT_TEXT = "49,51,63"       # #31333F
_DARK_TEXT = "250,250,250"     # #fafafa

LIGHT = {
    "bg": "#ffffff", "bg1": "#f8f9fb", "bg2": "#f0f2f6", "bg3": "#e6eaf1",
    "line": f"rgba({_LIGHT_TEXT},.1)", "line2": f"rgba({_LIGHT_TEXT},.2)",
    "line3": f"rgba({_LIGHT_TEXT},.3)",
    "text": "#31333f", "text2": f"rgba({_LIGHT_TEXT},.6)",
    "text3": f"rgba({_LIGHT_TEXT},.4)",
    "on-color": "#ffffff", "primary": "#ff4b4b",
    "hover": "rgba(151,166,195,.15)", "shadow": "rgba(0,0,0,.16)",
    "good": "#158237", "good-main": "#21c354",
    "good-bg": "rgba(33,195,84,.1)", "good-line": "rgba(33,195,84,.4)",
    "bad": "#bd4043", "bad-main": "#ff4b4b",
    "bad-bg": "rgba(255,43,43,.09)", "bad-line": "rgba(255,43,43,.35)",
    "warn": "#926c05", "warn-main": "#faca2b",
    "warn-bg": "rgba(255,255,18,.1)", "warn-line": "rgba(250,202,43,.55)",
    "orange": "#e2660c", "orange-main": "#ffa421",
    "orange-bg": "rgba(255,164,33,.1)", "orange-line": "rgba(255,164,33,.45)",
    "accent": "#0054a3", "accent-main": "#1c83e1",
    "accent-bg": "rgba(28,131,255,.1)", "accent-line": "rgba(28,131,225,.4)",
    "violet": "#583f84", "violet-main": "#803df5",
    "violet-bg": "rgba(154,93,255,.1)", "violet-line": "rgba(128,61,245,.4)",
    # chartCategoricalColors, light order
    "c0": "#0068c9", "c1": "#83c9ff", "c2": "#ff2b2b", "c3": "#ffabab",
    "c4": "#29b09d", "c5": "#7defa1", "c6": "#ff8700", "c7": "#ffd16a",
    "c8": "#6d3fc0", "c9": "#d5dae5",
    "c0-a": "rgba(0,104,201,.3)", "c2-a": "rgba(255,43,43,.38)",
    "c4-a": "rgba(41,176,157,.38)", "c6-a": "rgba(255,135,0,.38)",
    "c6-b": "rgba(255,135,0,.6)", "c8-a": "rgba(109,63,192,.5)",
}

DARK = {
    "bg": "#0e1117", "bg1": "#1a1c24", "bg2": "#262730", "bg3": "#31333f",
    "line": f"rgba({_DARK_TEXT},.1)", "line2": f"rgba({_DARK_TEXT},.2)",
    "line3": f"rgba({_DARK_TEXT},.3)",
    "text": "#fafafa", "text2": f"rgba({_DARK_TEXT},.6)",
    "text3": f"rgba({_DARK_TEXT},.4)",
    "on-color": "#0e1117", "primary": "#ff4b4b",
    "hover": "rgba(172,177,195,.15)", "shadow": "rgba(0,0,0,.6)",
    "good": "#5ce488", "good-main": "#09ab3b",
    "good-bg": "rgba(61,213,109,.2)", "good-line": "rgba(61,213,109,.45)",
    "bad": "#ff6c6c", "bad-main": "#ff2b2b",
    "bad-bg": "rgba(255,108,108,.2)", "bad-line": "rgba(255,108,108,.45)",
    "warn": "#ffffc2", "warn-main": "#ffe312",
    "warn-bg": "rgba(255,255,18,.2)", "warn-line": "rgba(255,227,18,.45)",
    "orange": "#ffbd45", "orange-main": "#ff8700",
    "orange-bg": "rgba(255,135,0,.2)", "orange-line": "rgba(255,135,0,.45)",
    "accent": "#3d9df3", "accent-main": "#0068c9",
    "accent-bg": "rgba(61,157,243,.2)", "accent-line": "rgba(61,157,243,.45)",
    "violet": "#b27eff", "violet-main": "#803df5",
    "violet-bg": "rgba(154,93,255,.2)", "violet-line": "rgba(154,93,255,.45)",
    # chartCategoricalColors, dark order
    "c0": "#83c9ff", "c1": "#0068c9", "c2": "#ffabab", "c3": "#ff2b2b",
    "c4": "#7defa1", "c5": "#29b09d", "c6": "#ffd16a", "c7": "#ff8700",
    "c8": "#6d3fc0", "c9": "#d5dae5",
    "c0-a": "rgba(131,201,255,.3)", "c2-a": "rgba(255,171,171,.38)",
    "c4-a": "rgba(125,239,161,.38)", "c6-a": "rgba(255,209,106,.38)",
    "c6-b": "rgba(255,209,106,.6)", "c8-a": "rgba(154,93,255,.5)",
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
