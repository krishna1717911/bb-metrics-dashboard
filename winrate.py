#!/usr/bin/env python3
"""Per-builder win rates over the last 24h, from the relay's own view.

The full table that `bb-scripts/builder_scorecard.py` report 1 prints, rendered
for the main page. Kept out of app.py for the same reason /rewards is: app.py is
8k lines several people edit, and this needs only a route and one panel there.

Why the relay and not our own store: `relay.mini_block_events_v2` sees EVERY
builder's submissions; our `bifrost_miniblocks.won_by_us` can only ever say
"not us", never who won, and it disagrees with the relay in both directions.
`round_chosen` is the authoritative winner for a (slot, index_in_slot).

Three things this gets right that a naive count does not, each learned the
hard way and each worth keeping:

  * TAIL ROUNDS COUNT AS WINS. They are exclusive -- one builder owns a tail
    round -- so they are counted as distinct (slot, index_in_slot) graded
    `tail`, never as accepted offers: one tail round absorbs
    many accepted offers from its single owner. Leaving them out silently
    undercounts the winner.

  * TWO DENOMINATORS, because one is misleading on its own. `win% contested`
    is wins over rounds that builder actually bid in; `share%` is wins over
    every winnable round in the window. Builders serve different connectors
    (AMS vs TYO/JP), so a single global denominator makes a smaller market
    look like a losing one. Only win% compares across regions.

  * COGENT'S REFUSALS ARE HELD OUT of the rejection count and shown in their
    own column. Cogent returns builder_not_eligible on 100% of our
    submissions by policy, which says nothing about us, and leaving it in
    buries the rejections we can act on (index_mismatch and friends) under a
    wall we do not control. Offers are NOT excluded, only the rejections.

Every query here must be bound by `timestamp` -- the table is large and the
Grafana proxy in front of it times out otherwise.
"""

import html
import os
import threading
import time

WINDOW_H = int(os.environ.get("WINRATE_WINDOW_H", "24"))
TTL_S = int(os.environ.get("WINRATE_TTL_S", "300"))

# Cogent refuses 100% of our submissions and always has. Only the two builders
# that actually serve it: JP sees different connectors and never meets Cogent,
# and the mock's 100% is BuilderConfig.mock by design, not eligibility.
COGENT = os.environ.get(
    "WINRATE_COGENT", "Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA")
COGENT_EXCLUDED = [b.strip() for b in os.environ.get(
    "WINRATE_COGENT_BUILDERS", "ASTRALANE_AMS_1,ASTRALANE_AMS_2").split(",")
    if b.strip()]

# Which builder_ids to mark as ours. The configured deployments are the
# authority, but the fleet is wider than what this dashboard instruments --
# ASTRALANE_FRA_1 wins thousands of rounds a day and is nobody's DEPLOY_* here --
# so anything carrying the house prefix is marked too. Set empty to rely on the
# configured deployments alone.
OURS_PREFIX = os.environ.get("WINRATE_OURS_PREFIX", "ASTRALANE_")

_lock = threading.Lock()
_cache = {}


def _cogent_bne():
    if not (COGENT and COGENT_EXCLUDED):
        return "0"
    ids = ",".join("'" + b.replace("'", "") + "'" for b in COGENT_EXCLUDED)
    return (f"(builder_id IN ({ids}) AND reason = 'builder_not_eligible' "
            f"AND connector_identity = '{COGENT.replace(chr(39), '')}')")


def _pct(num, den):
    return f"{100.0 * num / den:.2f}" if den else "-"


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# The relay's round log since the V3 refactor. The v1 table this used to read
# (relay.mini_block_events, with offer_rejected / tail_offer_accepted events)
# was dropped; in v2 every offer is one `submitted` row graded by `reason`:
# best / not_best / tail are accepted, anything else is the rejection.
TABLE = "relay.mini_block_events_v2"
ACCEPTED = "('best', 'not_best', 'tail')"
# a round is (connector, slot, round): two connectors never share a slot, but
# the key costs nothing and keeps a slot served twice from merging
ROUND = "(connector_identity, slot, index_in_slot)"


def _chosen_sql(where, pad_where):
    """Regular-round winners: round_chosen carries neither slot nor builder,
    so its uuid is matched back to the submitted row that owns it. The
    submitted side is read over a padded window -- the winning offer comes
    a few ms before round_chosen, and a round at the window's start would
    otherwise lose its owner."""
    return (f"(SELECT mini_block_uuid, timestamp AS t FROM {TABLE} "
            f" WHERE {where} AND event = 'round_chosen') AS c "
            "INNER JOIN (SELECT mini_block_uuid, any(builder_id) AS b "
            f" FROM {TABLE} WHERE {pad_where} AND event = 'submitted' "
            " GROUP BY mini_block_uuid) AS s "
            "ON c.mini_block_uuid = s.mini_block_uuid")


def collect(relay, hours=WINDOW_H, our_builders=(), keep_cogent=False):
    """Run the relay queries and assemble the table."""
    bne = _cogent_bne()
    dropped = "0" if keep_cogent else bne
    where = f"timestamp >= now() - INTERVAL {int(hours)} HOUR"
    pad = f"timestamp >= now() - INTERVAL {int(hours) * 60 + 5} MINUTE"

    # Tail rounds are exclusive: the tail owner's offers are graded `tail`,
    # so its wins are the distinct rounds it has such a row in -- one tail
    # round absorbs many accepted offers.
    _, rows = relay(
        "SELECT assumeNotNull(builder_id) AS builder, count() AS offered,"
        f" uniqExact({ROUND}) AS contested,"
        f" uniqExactIf({ROUND}, reason = 'tail') AS tail_wins,"
        f" countIf(reason NOT IN {ACCEPTED} AND NOT {dropped}) AS rejected,"
        f" countIf({bne}) AS cogent_bne"
        f" FROM {TABLE} WHERE {where} AND event = 'submitted'"
        "   AND builder_id != '' GROUP BY builder")
    _, won = relay(f"SELECT s.b, count() FROM {_chosen_sql(where, pad)} "
                   "GROUP BY s.b")
    round_wins = {b: _int(n) for b, n in won}

    _, totals = relay(
        "SELECT countIf(event = 'round_chosen') AS regular,"
        f" uniqExactIf({ROUND}, event = 'submitted' AND reason = 'tail') AS tails,"
        " uniqExactIf(slot, event = 'round_opened') AS slots"
        f" FROM {TABLE} WHERE {where}")
    regular, tails, slots = ([_int(v) for v in totals[0]] if totals else (0, 0, 0))
    winnable = regular + tails

    _, reason_rows = relay(
        "SELECT assumeNotNull(builder_id) AS builder, reason, count() AS n"
        f" FROM {TABLE} WHERE {where} AND event = 'submitted'"
        f"   AND reason NOT IN {ACCEPTED} AND builder_id != ''"
        f"   AND NOT {dropped} GROUP BY builder, reason")
    reasons = {}
    for builder, reason, n in reason_rows:
        reasons.setdefault(builder, {})[reason or "(none given)"] = _int(n)

    ours = {b for b in our_builders if b}

    def is_ours(b):
        return b in ours or bool(OURS_PREFIX and b.startswith(OURS_PREFIX))
    out = []
    for r in rows:
        builder = r[0]
        offered, contested, tail_wins, rejected, cbne = (_int(v) for v in r[1:6])
        rw = round_wins.get(builder, 0)
        wins = rw + tail_wins
        top = sorted(reasons.get(builder, {}).items(), key=lambda kv: -kv[1])[:1]
        out.append({
            "builder": builder, "offered": offered, "contested": contested,
            "round_wins": rw, "tail_wins": tail_wins, "wins": wins,
            "rejected": rejected, "cogent_bne": cbne,
            "win_pct": _pct(wins, contested),
            "share_pct": _pct(wins, winnable),
            "rej_pct": _pct(rejected, offered),
            "top_reason": (f"{top[0][0]} ({top[0][1]:,})" if top else "-"),
            "ours": is_ours(builder),
        })
    out.sort(key=lambda r: (-r["wins"], -r["offered"]))
    return {"hours": int(hours), "slots": slots, "regular_rounds": regular,
            "tail_rounds": tails, "winnable": winnable, "builders": out,
            "keep_cogent": keep_cogent}


def collect_history(relay, hours=WINDOW_H, our_builders=()):
    """win% contested per builder per UTC hour, the current (partial) hour
    last. A round is bucketed by when it was bid in (its submitted rows) and,
    for the win, by its round_chosen / tail row -- the two land in the same
    hour except for a round straddling the boundary."""
    start = f"toStartOfHour(now()) - INTERVAL {int(hours) - 1} HOUR"
    where = f"timestamp >= {start}"
    pad = f"timestamp >= {start} - INTERVAL 5 MINUTE"
    _, rows = relay(
        "SELECT assumeNotNull(builder_id) AS b,"
        " toString(toStartOfHour(timestamp)) AS h,"
        f" uniqExact({ROUND}) AS contested,"
        f" uniqExactIf({ROUND}, reason = 'tail') AS tail_wins"
        f" FROM {TABLE} WHERE {where} AND event = 'submitted'"
        "   AND builder_id != '' GROUP BY b, h")
    _, won = relay(f"SELECT s.b, toString(toStartOfHour(c.t)) AS h, count() "
                   f"FROM {_chosen_sql(where, pad)} GROUP BY s.b, h")
    _, now_h = relay("SELECT toString(toStartOfHour(now()))")
    cells = {}
    for b, h, contested, tw in rows:
        c = cells.setdefault(b, {}).setdefault(h[:16], [0, 0])
        c[0] += _int(contested)
        c[1] += _int(tw)
    for b, h, n in won:
        cells.setdefault(b, {}).setdefault(h[:16], [0, 0])[1] += _int(n)
    import datetime as dt
    last = dt.datetime.fromisoformat(now_h[0][0][:19]) if now_h else None
    hrs = ([(last - dt.timedelta(hours=k)).strftime("%Y-%m-%d %H:%M")
            for k in range(int(hours) - 1, -1, -1)] if last else
           sorted({h for v in cells.values() for h in v}))
    ours = {b for b in our_builders if b}

    def is_ours(b):
        return b in ours or bool(OURS_PREFIX and b.startswith(OURS_PREFIX))
    builders = []
    for b, by in cells.items():
        tot_c = sum(v[0] for v in by.values())
        tot_w = sum(v[1] for v in by.values())
        if not tot_c:
            continue
        builders.append({"builder": b, "ours": is_ours(b), "by": by,
                         "contested": tot_c, "wins": tot_w,
                         "win_pct": _pct(tot_w, tot_c)})
    # ours first, then by wins; builders that never win (mocks, idle
    # fallbacks) last
    builders.sort(key=lambda r: (not r["ours"], r["wins"] == 0, -r["wins"]))
    return {"hours": int(hours), "hrs": hrs, "builders": builders}


def cached_history(relay, our_builders=(), hours=WINDOW_H):
    key = ("hist", hours, tuple(sorted(our_builders)))
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < TTL_S:
            return hit[1]
    data = collect_history(relay, hours=hours, our_builders=our_builders)
    with _lock:
        _cache[key] = (time.time(), data)
    return data


def cached(relay, our_builders=(), hours=WINDOW_H):
    key = (hours, tuple(sorted(our_builders)))
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < TTL_S:
            return hit[1]
    data = collect(relay, hours=hours, our_builders=our_builders)
    with _lock:
        _cache[key] = (time.time(), data)
    return data


CSS = """
.wr-tbl{border-collapse:collapse;width:100%;font-size:12px;margin:2px 0 8px}
.wr-tbl th,.wr-tbl td{padding:4px 8px;border-bottom:1px solid #1b2531;
  text-align:right;white-space:nowrap}
.wr-tbl th{color:#8b9bb0;font-weight:600;text-align:right;
  border-bottom:1px solid #2a3647}
.wr-tbl td.b,.wr-tbl th.b{text-align:left}
.wr-tbl tr.ours td{background:#101b27}
.wr-tbl tr.ours td.b{font-weight:600;color:#cfe0f5}
.wr-tbl td.z{color:#5b6b80}
.wr-note{color:#8b9bb0;font-size:11px;line-height:1.55;margin:6px 0 0}
.wr-head{color:#8b9bb0;font-size:11px;margin:0 0 6px}
.wr-warn{color:#d9a441;font-size:11px;margin:6px 0 0}
.wr-histbtn{margin:8px 0 4px;background:#0c141b;border:1px solid #2a3647;
  color:#9fb3c8;border-radius:6px;padding:4px 10px;font-size:11.5px;cursor:pointer}
.wr-histbtn:hover,.wr-histbtn[aria-expanded=true]{border-color:#14b8a6;color:#5eead4}
.wr-hist{overflow-x:auto;margin:4px 0 8px}
.wr-htbl{border-collapse:collapse;font-size:11px}
.wr-htbl th,.wr-htbl td{padding:3px 5px;text-align:right;white-space:nowrap;
  border-bottom:1px solid #16202b}
.wr-htbl th{color:#8b9bb0;font-weight:600;border-bottom:1px solid #2a3647}
.wr-htbl th.b,.wr-htbl td.b{text-align:left;position:sticky;left:0;background:#0b1016}
.wr-htbl tr.ours td.b{font-weight:600;color:#cfe0f5}
.wr-htbl th.now{color:#d9a441}
.wr-htbl td.z{color:#3e4b5c}
.wr-htbl td.tot{border-left:1px solid #2a3647;color:#cfe0f5}
"""

COLS = [("b", "builder"), ("", "offered"), ("", "contested"),
        ("", "round wins"), ("", "tail wins"), ("", "wins"),
        ("", "win% contested"), ("", "share%"), ("", "rejected"),
        ("", "rej%"), ("", "cogent_bne"), ("b", "top reject reason")]


def _heat(pct):
    """Background for a win% cell: transparent at 0, teal toward 60%+."""
    a = min(1.0, pct / 60.0) * 0.55
    return f"background:rgba(20,184,166,{a:.2f})"


def render_history(data):
    """Builders down, UTC hours across, win% contested in each cell."""
    e = html.escape
    if not data["builders"]:
        return '<div class="none">no relay rows in the last %dh</div>' % data["hours"]
    hrs = data["hrs"]
    head = ('<th class="b">builder</th>'
            + "".join(f'<th class="{"now" if k == len(hrs) - 1 else ""}" '
                      f'title="{e(h)} UTC{" (this hour, still filling)" if k == len(hrs) - 1 else ""}">'
                      f"{e(h[11:13])}h</th>" for k, h in enumerate(hrs))
            + f'<th class="tot">{data["hours"]}h</th>')
    body = []
    for r in data["builders"]:
        tds = []
        for h in hrs:
            c, w = r["by"].get(h, (0, 0))
            if not c:
                tds.append('<td class="z">&middot;</td>')
                continue
            pct = 100.0 * w / c
            tds.append(f'<td style="{_heat(pct)}" title="{e(h)} UTC: {w:,} won '
                       f'of {c:,} contested">{pct:.0f}</td>')
        tds.append(f'<td class="tot" title="{r["wins"]:,} won of '
                   f'{r["contested"]:,} contested">{r["win_pct"]}</td>')
        body.append(f'<tr class="{"ours" if r["ours"] else ""}"><td class="b">'
                    + ("&#9733; " if r["ours"] else "") + e(r["builder"])
                    + "</td>" + "".join(tds) + "</tr>")
    return (f'<div class="wr-head">win% contested per UTC hour, last '
            f'{data["hours"]}h &middot; hour {e(hrs[0][5:16])} to '
            f'{e(hrs[-1][5:16])} UTC; the last column is still filling</div>'
            f'<table class="wr-htbl"><thead><tr>{head}</tr></thead>'
            f'<tbody>{"".join(body)}</tbody></table>'
            '<div class="wr-note">Each cell is wins over the rounds that '
            'builder bid in that hour, tail rounds included; hover for the '
            'counts. &middot; means it bid in no round that hour.</div>')


def render(data):
    """The table plus the footnotes that stop it being misread."""
    e = html.escape
    if not data["builders"]:
        return ('<div class="none">no relay rows in the last '
                f'{data["hours"]}h</div>')
    head = "".join(f'<th class="{c}">{e(t)}</th>' for c, t in COLS)
    body = []
    for r in data["builders"]:
        cells = [
            ("b", ("&#9733; " if r["ours"] else "") + e(r["builder"])),
            ("", f'{r["offered"]:,}'), ("", f'{r["contested"]:,}'),
            ("", f'{r["round_wins"]:,}'), ("", f'{r["tail_wins"]:,}'),
            ("", f'{r["wins"]:,}'), ("", r["win_pct"]), ("", r["share_pct"]),
            ("", f'{r["rejected"]:,}'), ("", r["rej_pct"]),
            ("z" if not r["cogent_bne"] else "", f'{r["cogent_bne"]:,}'),
            ("b", e(r["top_reason"])),
        ]
        body.append(f'<tr class="{"ours" if r["ours"] else ""}">'
                    + "".join(f'<td class="{c}">{v}</td>' for c, v in cells)
                    + "</tr>")

    won = sum(r["wins"] for r in data["builders"])
    warn = ""
    if won != data["winnable"]:
        # Every winnable round has exactly one owner, so this must balance.
        # It will not if the window clips a round whose events straddle an edge.
        warn = (f'<div class="wr-warn">wins sum to {won:,} against '
                f'{data["winnable"]:,} winnable rounds &mdash; the window clips '
                f'a round at one of its edges</div>')

    cogent_total = sum(r["cogent_bne"] for r in data["builders"])
    if data["keep_cogent"]:
        cogent_note = (f'<code>cogent_bne</code> is included in rejected '
                       f'({cogent_total:,} refusals).')
    elif cogent_total:
        cogent_note = (
            f'<code>cogent_bne</code> is {cogent_total:,} '
            f'<code>builder_not_eligible</code> refusals from Cogent against '
            f'{e(", ".join(COGENT_EXCLUDED))}, held out of rejected and rej%. '
            f'Cogent refuses 100% by policy, so it says nothing about us. '
            f'Offers are not excluded.')
    else:
        cogent_note = ('<code>cogent_bne</code> is 0 in this window &mdash; no '
                       'Cogent eligibility refusals to hold out.')

    return (
        f'<div class="wr-head">last {data["hours"]}h &middot; '
        f'{data["slots"]:,} slots &middot; {data["regular_rounds"]:,} rounds + '
        f'{data["tail_rounds"]:,} tail rounds = {data["winnable"]:,} winnable'
        '</div>'
        f'<table class="wr-tbl"><thead><tr>{head}</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table>'
        + warn +
        '<button type="button" class="wr-histbtn" aria-expanded="false">'
        f'win-rate history &middot; per hour, last {data["hours"]}h</button>'
        '<div class="wr-hist" hidden data-loaded="0"></div>' +
        '<div class="wr-note">&#9733; = ours. '
        '<b>win% contested</b> is wins over rounds that builder actually bid '
        'in; <b>share%</b> is wins over every winnable round in the window. '
        'Builders serve different connectors, so only win% compares across '
        'regions. <b>tail wins</b> are counted as distinct rounds, not as '
        'accepted offers &mdash; one tail round absorbs many. '
        + cogent_note + '</div>')
