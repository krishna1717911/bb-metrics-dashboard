#!/usr/bin/env python3
"""Per-builder win rates over the last 24h, from the relay's own view.

The full table that `bb-scripts/builder_scorecard.py` report 1 prints, rendered
for the main page. Kept out of app.py for the same reason /rewards is: app.py is
8k lines several people edit, and this needs only a route and one panel there.

Why the relay and not our own store: `relay.mini_block_events` sees EVERY
builder's submissions; our `bifrost_miniblocks.won_by_us` can only ever say
"not us", never who won, and it disagrees with the relay in both directions.
`round_chosen` is the authoritative winner for a (slot, index_in_slot).

Three things this gets right that a naive count does not, each learned the
hard way and each worth keeping:

  * TAIL ROUNDS COUNT AS WINS. They are exclusive -- one builder owns a tail
    round -- so they are counted as distinct (slot, index_in_slot) on
    `tail_offer_accepted`, never as accepted offers: one tail round absorbs
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


def collect(relay, hours=WINDOW_H, our_builders=(), keep_cogent=False):
    """Run the three relay queries and assemble the table."""
    bne = _cogent_bne()
    dropped = "0" if keep_cogent else bne
    where = f"timestamp >= now() - INTERVAL {int(hours)} HOUR"

    _, rows = relay(
        "SELECT assumeNotNull(builder_id) AS builder,"
        " countIf(event = 'submitted') AS offered,"
        " uniqExactIf((slot, index_in_slot), event = 'submitted') AS contested,"
        " countIf(event = 'round_chosen') AS round_wins,"
        " uniqExactIf((slot, index_in_slot),"
        "             event = 'tail_offer_accepted') AS tail_wins,"
        f" countIf(event = 'offer_rejected' AND NOT {dropped}) AS rejected,"
        f" countIf(event = 'offer_rejected' AND {bne}) AS cogent_bne"
        " FROM relay.mini_block_events"
        f" WHERE {where} AND builder_id IS NOT NULL GROUP BY builder")

    # Tail rounds by distinct (slot, round): one tail round absorbs many
    # accepted offers, so counting the events would inflate the denominator.
    _, totals = relay(
        "SELECT countIf(event = 'round_chosen') AS regular,"
        " uniqExactIf((slot, index_in_slot),"
        "             event = 'tail_offer_accepted') AS tails,"
        " uniqExact(slot) AS slots"
        f" FROM relay.mini_block_events WHERE {where}")
    regular, tails, slots = ([_int(v) for v in totals[0]] if totals else (0, 0, 0))
    winnable = regular + tails

    _, reason_rows = relay(
        "SELECT assumeNotNull(builder_id) AS builder, reason, count() AS n"
        " FROM relay.mini_block_events"
        f" WHERE {where} AND event = 'offer_rejected' AND builder_id IS NOT NULL"
        f"   AND NOT {dropped}"
        " GROUP BY builder, reason")
    reasons = {}
    for builder, reason, n in reason_rows:
        reasons.setdefault(builder, {})[reason or "(none given)"] = _int(n)

    ours = {b for b in our_builders if b}

    def is_ours(b):
        return b in ours or bool(OURS_PREFIX and b.startswith(OURS_PREFIX))
    out = []
    for r in rows:
        builder = r[0]
        offered, contested, round_wins, tail_wins, rejected, cbne = (
            _int(v) for v in r[1:7])
        wins = round_wins + tail_wins
        top = sorted(reasons.get(builder, {}).items(), key=lambda kv: -kv[1])[:1]
        out.append({
            "builder": builder, "offered": offered, "contested": contested,
            "round_wins": round_wins, "tail_wins": tail_wins, "wins": wins,
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
"""

COLS = [("b", "builder"), ("", "offered"), ("", "contested"),
        ("", "round wins"), ("", "tail wins"), ("", "wins"),
        ("", "win% contested"), ("", "share%"), ("", "rejected"),
        ("", "rej%"), ("", "cogent_bne"), ("b", "top reject reason")]


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
        '<div class="wr-note">&#9733; = ours. '
        '<b>win% contested</b> is wins over rounds that builder actually bid '
        'in; <b>share%</b> is wins over every winnable round in the window. '
        'Builders serve different connectors, so only win% compares across '
        'regions. <b>tail wins</b> are counted as distinct rounds, not as '
        'accepted offers &mdash; one tail round absorbs many. '
        + cogent_note + '</div>')
