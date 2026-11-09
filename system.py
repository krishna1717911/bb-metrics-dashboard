"""The BI dashboard's System section (astralane-quant/python/bi_dashboard
tabs/freshness.py and tabs/about.py), for this site's own stores.

  freshness  how old the newest row is in every store the pages read, per
             writer where a store has several, and how long the read took.
             A page that looks broken is very often a store that stopped.
  about      what this build is, which stores it reads for what, and the
             caveats that change how its numbers should be read.

Query helpers are injected by app.py; nothing here holds a credential.
"""

import concurrent.futures as cf
import datetime as dt
import html
import json
import os
import subprocess
import time

import ui

FRESH_PATH = "/system/freshness"
ABOUT_PATH = "/system/about"
F = "%Y-%m-%d %H:%M:%S"
STARTED = dt.datetime.now(dt.UTC).strftime(F)

# (ok, stale) thresholds in seconds per cadence: a live log should have
# written in the last couple of minutes; an hourly or daily one should not.
CADENCE = {"live": (120, 900), "event": (None, None),
           "daily": (36 * 3600, 60 * 3600),
           # writes only while one of its validators leads, so a quiet
           # quarter of an hour between leader windows is normal
           "leader": (900, 6 * 3600)}


def _dur(s):
    s = int(max(s, 0))
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m {s % 60:02d}s"
    if s < 172800:
        return f"{s // 3600}h {(s % 3600) // 60:02d}m"
    return f"{s // 86400}d {(s % 86400) // 3600}h"


def _rows(ch, ixs, rq, gq, fd_report, snapshot_path):
    """[(store, key, newest 'YYYY-MM-DD HH:MM:SS', cadence, extra)] -- one
    entry per (store, writer), or an exception per store."""
    day = "now() - INTERVAL 1 DAY"
    jobs = {
        "bifrost_events": lambda: [
            ("bifrost_events (our builders' event log)", r[0], r[1], "live",
             f"{int(r[2]):,} rows / 24 h")
            for r in ch("SELECT instance_id, toString(max(ts)), count() "
                        f"FROM bifrost_events WHERE ts > {day} "
                        "GROUP BY instance_id ORDER BY instance_id")[1]],
        "bifrost_miniblocks": lambda: [
            ("bifrost_miniblocks (our offers + winners)", r[0], r[1], "leader",
             "written while a validator it serves leads")
            for r in ch("SELECT instance_id, toString(max(ts)) FROM bifrost_miniblocks "
                        f"WHERE ts > {day} GROUP BY instance_id ORDER BY instance_id")[1]],
        "relay_events": lambda: [
            ("relay.mini_block_events_v2 (rounds, grades)", r[0], r[1], "leader",
             f"{int(r[2]):,} rows / 24 h; written while one of its validators leads")
            for r in rq("SELECT instance_id, toString(max(timestamp)), count() "
                        "FROM relay.mini_block_events_v2 "
                        f"WHERE timestamp > {day} GROUP BY instance_id "
                        "ORDER BY instance_id")[1]],
        "relay_tx": lambda: [
            ("relay.mini_block_transactions_v2 (offer contents)", "all relays",
             rq("SELECT toString(max(ingested_at)) FROM relay.mini_block_transactions_v2 "
                "WHERE ingested_at > now() - INTERVAL 1 HOUR")[1][0][0], "live", "")],
        "ingested": lambda: [
            ("relay.connector_ingested_orders", "all relays",
             rq("SELECT toString(max(relay_sent_at)) FROM relay.connector_ingested_orders "
                "WHERE relay_sent_at > now() - INTERVAL 1 HOUR")[1][0][0], "live", "")],
        "handshakes": lambda: [
            ("relay.connector_handshakes (connector (re)connects)", "all relays",
             rq("SELECT toString(max(timestamp)) FROM relay.connector_handshakes "
                "WHERE timestamp > now() - INTERVAL 30 DAY")[1][0][0], "event",
             "written only when a connector connects")],
        "block_summary": lambda: [
            ("solana.block_summary (chain, per block)", "all leaders",
             gq("SELECT toString(max(estimated_slot_time)) FROM solana.block_summary "
                f"WHERE estimated_slot_time > {day}")[1][0][0], "live", "")],
        "slot_info": lambda: [
            ("solana.slot_info (connector heartbeat)", "all connectors",
             gq("SELECT toString(max(start_at)) FROM solana.slot_info "
                f"WHERE start_at > {day}")[1][0][0], "live", "")],
        "influx": lambda: [
            ("Influx sim-commit (simulator)", ser.get("tags", {}).get("host_id", "?")[:12] + "…",
             ser["values"][-1][0][:19].replace("T", " "), "leader",
             "one row per round, so only in leader windows")
            for ser in ixs('SELECT last("body_us") FROM "sim-commit" '
                           'WHERE time > now() - 1d GROUP BY "host_id"')],
        "fd": lambda: [
            ("reports.firedancer.io (daily report)", "newest report day",
             fd_report()["_date"] + " 23:59:59", "daily", "published about a day late")],
        "panics": lambda: [
            ("panics.json (#bb-alerts panic snapshot)", "covers to",
             json.load(open(snapshot_path)).get("covers_to", ""), "event",
             "refreshed by hand; SLACK_BOT_TOKEN reads the channel live")],
    }
    out, timing = {}, {}

    def run(k, fn):
        t0 = time.time()
        try:
            return k, fn(), time.time() - t0
        except Exception as exc:
            return k, exc, time.time() - t0
    with cf.ThreadPoolExecutor(len(jobs)) as ex:
        for k, v, took in ex.map(lambda kv: run(*kv), jobs.items()):
            out[k], timing[k] = v, took
    return [(k, out[k], timing[k]) for k in jobs]


def freshness_page(ch, ixs, rq, gq, fd_report, snapshot_path):
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    rows, n_ok, n_stale, n_bad = [], 0, 0, 0
    for key, got, took in _rows(ch, ixs, rq, gq, fd_report, snapshot_path):
        if isinstance(got, Exception):
            n_bad += 1
            rows.append((f"<b>{html.escape(key)}</b>", "&mdash;", "&mdash;", "&mdash;",
                         '<span class="udelta bad">unreadable</span>',
                         html.escape(str(got)[:160]), f"{took * 1000:,.0f}"))
            continue
        for store, who, newest, cadence, extra in got:
            newest = str(newest or "")[:19]
            try:
                age = (now - dt.datetime.strptime(newest, F)).total_seconds()
            except ValueError:
                age = None
            ok_s, stale_s = CADENCE[cadence]
            if age is None or newest.startswith("1970"):
                st, n_bad = '<span class="udelta bad">no rows</span>', n_bad + 1
            elif ok_s is None:
                st, n_ok = '<span class="udelta flat">event-driven</span>', n_ok + 1
            elif age < ok_s:
                st, n_ok = '<span class="udelta good">fresh</span>', n_ok + 1
            elif age < stale_s:
                st, n_stale = '<span class="udelta flat">behind</span>', n_stale + 1
            else:
                st, n_bad = '<span class="udelta bad">stale</span>', n_bad + 1
            rows.append((html.escape(store), html.escape(who), html.escape(newest),
                         "&mdash;" if age is None else _dur(age), st,
                         html.escape(extra), f"{took * 1000:,.0f}"))
    head = "".join(f"<th{' class=n' if i in (3, 6) else ''}>{h}</th>" for i, h in enumerate(
        ["store", "writer / key", "newest row (UTC)", "age", "status", "note", "read ms"]))
    body = "".join("<tr>" + "".join(
        f"<td{' class=n' if i in (3, 6) else ''}{' class=m' if i in (1, 2) else ''}>{v}</td>"
        for i, v in enumerate(r)) + "</tr>" for r in rows)
    return (ui.info(
        "How old the newest row is in every store this dashboard reads, "
        "per writer where several write the same table. <b>fresh</b> = "
        "written in the last 2 min, <b>behind</b> = within 15 min, "
        "<b>stale</b> = older; for stores that only write during leader "
        "windows (offers, relay rounds, simulator commits) fresh is 15 min "
        "and stale 6 h. Ages are against this machine's UTC clock; a "
        "builder's own clock can drift (FRA_1 runs ~18 s behind since "
        "10-28), which shifts its rows by as much. Read at "
        f"{now:%Y-%m-%d %H:%M:%S} UTC.")
        + ui.kpi_row([ui.kpi("fresh", f'<span class="udelta good">{n_ok}</span>'),
                      ui.kpi("behind", f'<span class="udelta flat">{n_stale}</span>'),
                      ui.kpi("stale or unreadable",
                             f'<span class="udelta {"bad" if n_bad else "flat"}">{n_bad}</span>')])
        + f'<div class="exp" style="padding:8px 14px"><table class="utab">'
          f"<tr>{head}</tr>{body}</table></div>")


SOURCES = [
    ("ClickHouse block_builder", "bifrost_events, bifrost_miniblocks, bifrost_latency_metrics",
     "every page: our builders' events, offers and winners (events 30 d, offers 7 d)"),
    ("relay ClickHouse (direct login)", "mini_block_events_v2, mini_block_transactions_v2, "
     "connector_ingested_orders, connector_execution_results, connector_handshakes",
     "bb health, offer timelines, builder accuracy, order lookup"),
    ("relay Grafana datasource", "solana.block_summary, solana.slot_info",
     "bb health (connector view), val comparison"),
    ("InfluxDB solana", "sim-commit, sim-extend, sim-probe, bank_frozen, shreds",
     "bb health (simulator, replay, shreds), per slot analysis"),
    ("Solana RPC", "getBlock, getLeaderSchedule, getBlockTime",
     "builder accuracy, bb health (skipped slots), building card"),
    ("reports.firedancer.io", "daily per-validator report",
     "bb health (connector view), val comparison, validator names"),
    ("quant ClickHouse (db_sync, backrun_intel)", "router submissions, backruns",
     "order lookup, val comparison"),
    ("ClickStack otel_logs", "builder log lines",
     "bb health (panics: ERROR line + restart; restarts with no error)"),
    ("#bb-alerts", "builder panic lines", "bb health (panics) fallback: live with "
     "SLACK_BOT_TOKEN, else panics.json"),
]

CAVEATS = [
    "Each builder stamps its rows with its own clock. FRA_1 has drifted "
    "~1.2 ms/min since its 2026-10-28 15:06Z restart (18 s behind the relay "
    "on 11-08); compare FRA_1 timings with the relay only after measuring "
    "the offset.",
    "Panics are read from otel_logs: the panic hook's own line never ships "
    "(the process aborts first), so a panic is the panic site's ERROR line "
    "followed within 10 s by a new process start. That matched #bb-alerts on "
    "all 54 panics 10-17 -> 11-09. otel_logs keeps data from 2026-10-17.",
    "The Firedancer report is published about a day late, so its figures "
    "compare report days, not the window on screen.",
    "Mock builders are graded builder_not_eligible by policy; every rate "
    "on bb health counts production builders only.",
    "Win rate counts a tail round the builder owned as a win, and divides "
    "by the rounds it offered in (contested), not by every round.",
    "bifrost_miniblocks keeps 7 days and bifrost_events 30; an older window "
    "is empty, not zero.",
]


def _git(args):
    try:
        return subprocess.run(["git", *args], cwd=os.path.dirname(os.path.abspath(__file__)),
                              capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        return ""


def about_page(pages, sections):
    rev = _git(["rev-parse", "--short", "HEAD"]) or "unknown"
    when = _git(["log", "-1", "--format=%cd", "--date=format:%Y-%m-%d %H:%M"]) or ""
    dirty = bool(_git(["status", "--porcelain", "--untracked-files=no"]))
    tiles = ui.kpi_row([
        ui.kpi("build", html.escape(rev) + (" +local" if dirty else ""),
               sub=html.escape(f"committed {when}") if when else ""),
        ui.kpi("server up since", html.escape(STARTED[11:16]) + " UTC",
               sub=html.escape(STARTED[:10])),
        ui.kpi("pages", str(len(pages)),
               sub=", ".join(html.escape(lab) for _, lab, _ in sections)),
    ])
    src = "".join(f"<tr><td><b>{html.escape(a)}</b></td><td class=m>{html.escape(b)}</td>"
                  f"<td>{html.escape(c)}</td></tr>" for a, b, c in SOURCES)
    pg = "".join(f"<tr><td>{html.escape(p['title'])}</td><td class=m>{html.escape(p['path'])}</td>"
                 f"<td>{html.escape(p['blurb'])}</td></tr>" for p in pages)
    cav = "".join(f"<li>{html.escape(c)}</li>" for c in CAVEATS)
    return (tiles
            + ui.section("data sources", "what each store is read for")
            + f'<div class="exp" style="padding:8px 14px"><table class="utab">'
              "<tr><th>store</th><th>tables</th><th>used by</th></tr>"
              f"{src}</table></div>"
            + ui.section("known caveats", "read these before quoting a number")
            + ui.info(f'<ul style="margin:0;padding-left:18px">{cav}</ul>', "warn")
            + ui.expander("every page", '<table class="utab"><tr><th>page</th>'
                          f"<th>path</th><th>what it is for</th></tr>{pg}</table>"))
