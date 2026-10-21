"""Specific validator comparison -- the business side, not the tech side.

Two inputs:

  1. one of OUR validators, taken from the distinct connector_identity
     values the builder has actually served;
  2. a comparison set drawn from the Firedancer daily report, filtered by
     client and by a stake band around our validator.

Firedancer-run clients are never offered. The comparison is about what a
validator of our size earns on another Agave block-building client, and a
Firedancer validator is not that comparison -- it is a different validator
implementation, so the numbers would not be like for like.

This module holds no credentials: app.py injects the ClickHouse helper.
"""

import csv
import datetime as dt
import html
import json
import os
import re
import threading
import time
import urllib.request

import gbxdune

FD_API = "https://reports.firedancer.io/api/report"

# Clients whose names start with any of these never appear. Checked on a
# prefix because the report names them `Firedancer Jito`, `Firedancer BAM`,
# `Frankendancer Jito` and so on -- a new variant should be excluded by
# default rather than quietly appear in a comparison it does not belong in.
EXCLUDE_CLIENT_PREFIXES = ("Firedancer", "Frankendancer")

# Pre-ticked in the client filter. Everything else stays available.
DEFAULT_CLIENTS = ("Agave Harmonic", "Agave JitoBAM")

# The slider is a band around OUR validator's stake, in SOL, capped either
# side. 0 sits dead centre and means "same stake as ours".
STAKE_SPAN = 100_000
STAKE_STEP = 1_000

FD_TTL_S = 3600
_cache = {}
_lock = threading.Lock()


def _cached(key, fn, ttl=FD_TTL_S):
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    if val:
        with _lock:
            _cache[key] = (now, val)
    return val


# Cloudflare fronts reports.firedancer.io and answers urllib's default
# User-Agent with a 403 "error code: 1010" -- and, with an Accept header, a
# problem+json body that parses cleanly and carries none of the fields this
# code reads. The failure therefore looks like "no report available" rather
# than like a block. relay_query.py hit the same wall on the Grafana host;
# the fix is the same, and it is the header, not the credentials.
_UA = "bb-metrics-dashboard/1.0"


def _get(url):
    req = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as err:
        # A 404 still carries max_report_date, which is how the newest
        # available day is discovered -- so the body is worth reading.
        try:
            return json.loads(err.read())
        except Exception:
            raise


def fd_report():
    """The newest daily report, not a pinned date.

    Asking for today 404s until that day is published, but the 404 body
    names `max_report_date`, so one retry lands on the newest data. Pinning
    a date in code means the stake figures silently go stale."""
    def go():
        today = dt.datetime.now(dt.UTC).date().isoformat()
        doc = _get(f"{FD_API}?period=daily&date={today}")
        if not doc.get("validators"):
            newest = doc.get("max_report_date")
            if not newest:
                raise RuntimeError(doc.get("error") or "no report available")
            doc = _get(f"{FD_API}?period=daily&date={newest}")
            doc["_date"] = newest
        else:
            doc["_date"] = today
        if not doc.get("validators"):
            raise RuntimeError(doc.get("error") or "report has no validators")
        return doc
    return _cached("fd", go)


def our_validators(ch):
    """The connector identities this builder has actually served.

    From bifrost_miniblocks rather than a config: a validator we stopped
    serving should drop off this list on its own, and one we picked up
    should appear without an edit."""
    def go():
        _, rows = ch("SELECT connector_identity, count() AS n, "
                     "  uniqExact(slot) AS slots, max(ts) AS last_seen "
                     "FROM bifrost_miniblocks "
                     "WHERE ts > now() - INTERVAL 30 DAY "
                     "  AND connector_identity != '' "
                     "GROUP BY connector_identity ORDER BY n DESC")
        return [{"id": r[0], "rows": int(r[1] or 0), "slots": int(r[2] or 0),
                 "last": r[3]} for r in rows if r[0]]
    return _cached("ours", go, ttl=900)


def candidates(doc):
    """Every non-Firedancer validator in the report, as plain dicts."""
    out = []
    for v in doc.get("validators") or []:
        client = v.get("client") or "Unknown"
        if client.startswith(EXCLUDE_CLIENT_PREFIXES):
            continue
        try:
            stake = int(v.get("active_stake") or 0)
        except (TypeError, ValueError):
            stake = 0
        out.append({"id": v.get("leader") or "", "name": v.get("name") or "",
                    "client": client, "stake": stake,
                    "commission": v.get("commission"),
                    "avg_reward": v.get("avg_reward") or 0,
                    "blocks": int(v.get("blocks") or 0)})
    return out


# ------------------------------------------------- the per-day comparison
#
# avg_reward is priority + base fees per LANDED block. Two adjustments make
# it comparable between validators:
#
#   per_slot      = avg_reward * landed / blocks   -- a skipped slot earns
#                   nothing, so it has to count as a zero, not be dropped.
#   per_slot_norm = per_slot * ref_ms / own_ms     -- a validator that holds
#                   its slots longer has more time to pack each block, so
#                   per slot it flatters itself. ref_ms is the median slot
#                   time across validators with enough slots THAT DAY.
#
# Both are the method val-gbx-comp uses. GBX builder tips are not in
# avg_reward at all -- they are paid to tip wallets, not to the leader in
# block -- so they are measured separately in gbxdune and added on top,
# normalised by the same day's factor.

MIN_SLOTS_FOR_REF = 100
DAYS_DEFAULT = 10
DAYS_MAX = 30


def fd_day(date):
    """One daily report, cached. Returns {} for a day not published yet."""
    def go():
        doc = _get(f"{FD_API}?period=daily&date={date}")
        return doc if doc.get("validators") else {}
    return _cached(f"fd:{date}", go, ttl=6 * 3600)


def day_metrics(doc, ident):
    """per_slot / per_slot_norm for one validator on one day, in SOL."""
    if not doc:
        return None
    ref = doc.get("_ref_ms")
    if ref is None:
        ms = sorted(float(v["avg_slot_duration_nanoseconds"]) / 1e6
                    for v in doc["validators"]
                    if int(v.get("blocks") or 0) >= MIN_SLOTS_FOR_REF
                    and v.get("avg_slot_duration_nanoseconds"))
        ref = doc["_ref_ms"] = (ms[len(ms) // 2] if ms else None)
    v = next((x for x in doc["validators"] if x.get("leader") == ident), None)
    if not v:
        return None
    blocks, skipped = int(v.get("blocks") or 0), int(v.get("skipped") or 0)
    landed = blocks - skipped
    if landed <= 0 or v.get("avg_reward") is None:
        return None
    per_block = float(v["avg_reward"]) / 1e9
    per_slot = per_block * landed / blocks
    own = v.get("avg_slot_duration_nanoseconds")
    own_ms = float(own) / 1e6 if own else None
    return {"per_slot": per_slot,
            "per_slot_norm": (per_slot * ref / own_ms
                              if own_ms and ref else None),
            "norm_factor": (ref / own_ms if own_ms and ref else None),
            # What the validator actually earned that day: the per-landed-
            # block figure times the blocks it landed. Not normalised --
            # normalisation answers "per slot, like for like", and a day's
            # takings are a real quantity, not a rate.
            "total_sol": per_block * landed,
            "blocks": blocks, "skipped": skipped, "landed": landed,
            "slot_ms": own_ms, "ref_ms": ref}


def day_list(days):
    """The last `days` published report dates, oldest first.

    Walks back from the newest report rather than from today, because today
    is never published yet and a fixed offset would silently shed a day."""
    newest = fd_report().get("_date")
    end = dt.date.fromisoformat(newest)
    return [(end - dt.timedelta(days=k)).isoformat()
            for k in range(days - 1, -1, -1)]


# ------------------------------------------------------------- the cache
#
# A comparison over ten days is ten cheap Firedancer fetches and one Dune
# run that takes about a minute. Re-running the same pair tomorrow would
# redo all of it for the sake of one new day, so each pair keeps a CSV and
# only the missing days are fetched.
#
# THE PARTIAL-DAY RULE. Today is still accumulating: its leader slots, tips
# and block count all grow until midnight UTC. A row written now would be
# wrong forever, and silently so -- it would look like a complete day with
# low numbers. So today is computed and shown but never written, and it is
# picked up tomorrow when the day is closed.

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "cache", "valcomp")
CACHE_COLS = [
    "day",
    "ours_per_slot", "ours_per_slot_norm", "ours_total_sol", "ours_blocks",
    "ours_skipped", "ours_slot_ms", "ours_norm_factor", "ours_ref_ms",
    "tips_sol", "tips_leader_slots", "tips_slots_with_tips",
    "other_per_slot", "other_per_slot_norm", "other_total_sol",
    "other_blocks", "other_skipped", "other_slot_ms", "other_ref_ms",
]
_cache_io = threading.Lock()


def _safe(ident):
    return re.sub(r"[^A-Za-z0-9_-]", "", str(ident))[:64]


def _pair_path(ours, other):
    """One file per ordered pair, so repeat comparisons accumulate."""
    return os.path.join(CACHE_DIR, f"{_safe(ours)}__{_safe(other)}.csv")


def _today():
    return dt.datetime.now(dt.UTC).date().isoformat()


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def cache_read(ours, other):
    """day -> flat dict of cached values. Missing file is simply no cache."""
    path = _pair_path(ours, other)
    out = {}
    try:
        with _cache_io, open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                day = (r.get("day") or "").strip()
                if day:
                    out[day] = r
    except (FileNotFoundError, csv.Error, UnicodeDecodeError):
        return {}
    return out


def cache_write(ours, other, rows):
    """Merge complete days in, newest last. Today is never written."""
    today = _today()
    fresh = {r["day"]: r for r in rows if r["day"] < today}
    if not fresh:
        return 0
    have = cache_read(ours, other)
    have.update(fresh)
    path = _pair_path(ours, other)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _cache_io:
        # Whole-file rewrite under the lock: the row set is a few hundred
        # lines at most, and an append could interleave two comparisons of
        # the same pair mid-write.
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=CACHE_COLS)
            w.writeheader()
            for day in sorted(have):
                w.writerow({k: have[day].get(k, "") for k in CACHE_COLS})
        os.replace(tmp, path)
    return len(fresh)


def _row_to_cache(r):
    """A computed comparison row, flattened for the CSV."""
    o, ot, t = r.get("ours") or {}, r.get("other") or {}, r.get("tips") or {}
    out = {"day": r["day"]}
    for pre, src in (("ours", o), ("other", ot)):
        for k in ("per_slot", "per_slot_norm", "total_sol", "blocks",
                  "skipped", "slot_ms", "ref_ms"):
            v = src.get(k)
            out[f"{pre}_{k}"] = "" if v is None else v
        if pre == "ours":
            out["ours_norm_factor"] = ("" if o.get("norm_factor") is None
                                       else o["norm_factor"])
    out["tips_sol"] = t.get("gbx_tips_sol", "")
    out["tips_leader_slots"] = t.get("leader_slots", "")
    out["tips_slots_with_tips"] = t.get("slots_with_tips", "")
    return out


def _cache_to_row(c):
    """The inverse: a CSV row back into the shape compare() returns."""
    def side(pre):
        if _num(c.get(f"{pre}_blocks")) is None:
            return None
        d = {k: _num(c.get(f"{pre}_{k}"))
             for k in ("per_slot", "per_slot_norm", "total_sol", "slot_ms",
                       "ref_ms")}
        d["blocks"] = int(_num(c.get(f"{pre}_blocks")) or 0)
        d["skipped"] = int(_num(c.get(f"{pre}_skipped")) or 0)
        if pre == "ours":
            d["norm_factor"] = _num(c.get("ours_norm_factor"))
        return d
    o, ot = side("ours"), side("other")
    slots = _num(c.get("tips_leader_slots"))
    tips = None
    if slots is not None:
        tips = {"day": c["day"], "leader_slots": int(slots),
                "gbx_tips_sol": _num(c.get("tips_sol")) or 0.0,
                "slots_with_tips": int(_num(c.get("tips_slots_with_tips")) or 0)}
    raw = (tips["gbx_tips_sol"] / tips["leader_slots"]
           if tips and tips["leader_slots"] else None)
    return {"day": c["day"], "ours": o, "other": ot, "tips": tips,
            "tips_per_slot": raw,
            "tips_per_slot_norm": (raw * o["norm_factor"]
                                   if raw is not None and o
                                   and o.get("norm_factor") else None),
            "cached": True}


def compare(ours, other, days, on_step=lambda _m: None):
    """Both validators per day plus our GBX tips, cache first, Dune last.

    Order matters: whatever the CSV already holds is used as-is, and only
    the days it is missing cost anything. A repeat comparison of the same
    pair over an overlapping range does no Dune work at all."""
    dates = day_list(days)
    today = _today()
    cached = cache_read(ours, other)
    rows, missing = {}, []
    for d in dates:
        # Today is never trusted from cache either -- if a stale file does
        # carry it, it was written by an older build and is partial.
        if d in cached and d != today:
            try:
                rows[d] = _cache_to_row(cached[d])
                continue
            except (TypeError, ValueError, KeyError):
                pass
        missing.append(d)

    if rows:
        on_step(f"{len(rows)} of {len(dates)} days from cache")
    if not missing:
        return {"days": dates, "rows": [rows[d] for d in dates],
                "ours": ours, "other": other,
                "from_cache": len(rows), "fetched": 0}

    for n, d in enumerate(missing, 1):
        on_step(f"Firedancer report {n}/{len(missing)} ({d})")
        doc = fd_day(d)
        rows[d] = {"day": d, "ours": day_metrics(doc, ours),
                   "other": day_metrics(doc, other), "cached": False}

    # One Dune run spanning the missing days. They are usually contiguous --
    # a widened window, or a day that has since closed -- so a single range
    # costs the same as a single day and far less than one run per day.
    lo, hi = min(missing), max(missing)
    on_step(f"querying Dune for GBX tips, {lo} to {hi}")
    tips = {t["day"]: t for t in gbxdune.tips_per_day(ours, lo, hi, on_step)}
    for d in missing:
        r, t = rows[d], tips.get(d)
        r["tips"] = t
        o = r["ours"]
        if t and o and t["leader_slots"]:
            raw = t["gbx_tips_sol"] / t["leader_slots"]
            r["tips_per_slot"] = raw
            r["tips_per_slot_norm"] = (raw * o["norm_factor"]
                                       if o.get("norm_factor") else None)
        else:
            r["tips_per_slot"] = r["tips_per_slot_norm"] = None

    stored = cache_write(ours, other,
                         [_row_to_cache(rows[d]) for d in missing])
    on_step(f"cached {stored} complete day(s)")
    return {"days": dates, "rows": [rows[d] for d in dates],
            "ours": ours, "other": other,
            "from_cache": len(dates) - len(missing), "fetched": len(missing),
            "stored": stored}


# The fee drill-down caches per VALIDATOR-DAY, not per pair: those top
# payers are a property of one validator's slots on one day, so the same
# file serves every comparison that validator ever appears in.
FEE_COLS = ["day", "total_prio", "n", "signature", "signer", "slot", "prio",
            "cu", "success"]


def _fee_path(validator):
    return os.path.join(CACHE_DIR, "fees", f"{_safe(validator)}.csv")


def fees_cached(validator, day):
    """The stored top payers for one validator-day, or None."""
    try:
        with _cache_io, open(_fee_path(validator), newline="",
                             encoding="utf-8") as fh:
            rows = [r for r in csv.DictReader(fh) if r.get("day") == day]
    except (FileNotFoundError, csv.Error, UnicodeDecodeError):
        return None
    if not rows:
        return None
    total = _num(rows[0].get("total_prio")) or 0.0
    return {"total_prio": total, "n": int(_num(rows[0].get("n")) or 0),
            "validator": validator, "day": day, "cached": True,
            "rows": [{"signature": r.get("signature", ""),
                      "signer": r.get("signer", ""),
                      "slot": int(_num(r.get("slot")) or 0),
                      "prio": _num(r.get("prio")) or 0.0,
                      "cu": int(_num(r.get("cu")) or 0),
                      "success": r.get("success") == "1",
                      "share": ((_num(r.get("prio")) or 0.0) / total
                                if total else None)} for r in rows]}


def fees_store(validator, day, res):
    """Append one complete day. Today is still accumulating, so it is not
    stored -- its top five and its total both still move."""
    if day >= _today() or not res.get("rows"):
        return False
    have = []
    path = _fee_path(validator)
    try:
        with _cache_io, open(path, newline="", encoding="utf-8") as fh:
            have = [r for r in csv.DictReader(fh) if r.get("day") != day]
    except (FileNotFoundError, csv.Error, UnicodeDecodeError):
        have = []
    for t in res["rows"]:
        have.append({"day": day, "total_prio": res["total_prio"],
                     "n": res["n"], "signature": t["signature"],
                     "signer": t["signer"], "slot": t["slot"],
                     "prio": t["prio"], "cu": t["cu"],
                     "success": "1" if t["success"] else "0"})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _cache_io:
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FEE_COLS)
            w.writeheader()
            for r in sorted(have, key=lambda x: (x["day"],
                                                 -float(x["prio"] or 0))):
                w.writerow({k: r.get(k, "") for k in FEE_COLS})
        os.replace(tmp, path)
    return True


# ------------------------------------------------- egress by api key
#
# What we PAID the other validator, per api key, on one day. The lane is
# chosen by that validator's client -- Harmonic -> harmonic, JitoBAM ->
# jito_bam, and so on -- because that is the lane our router used to reach
# it. Source is the quant ClickHouse, db_sync.inbound_transactions_enriched:
# one row per inbound transaction, with a per-lane pivot of landing status
# and the egress tip we attached. Two de-duplications, and both matter:
#
#   - it is a ReplacingMergeTree read without FINAL, so a signature shows
#     up once per unmerged version (2-5 copies is normal). Collapsing to
#     one row per signature first is what makes sum(tip) honest: without
#     it the ingress side read ~3x too high while the egress side did not.
#   - one egress PAYMENT often covers several user transactions, so its tip
#     is shared across them pro rata to their own tips (equal split when
#     every tip is zero). Summing the payment once per member would count
#     it N times; summing it once per key is wrong whenever one payment
#     spans two keys.
#
# Done this way the per-key figures add up to landing.validator_landing_daily
# to the lamport, and that rollup -- the table the Landing API shows partner
# validators -- is read alongside as the check and for the lane-wide total.
# Only a KEY PREFIX leaves the server: an api key is a credential.

QUANT_TABLE = "db_sync.inbound_transactions_enriched"
QUANT_ROLLUP = "landing.validator_landing_daily"

LANES = {"harmonic": "Harmonic", "jito_bam": "Jito BAM", "jito": "Jito",
         "rakurai": "Rakurai"}
# Matched in order on the lower-cased client name: "Agave JitoBAM" has to
# resolve to the BAM lane, not to plain Jito.
_LANE_BY_CLIENT = (("harmonic", "harmonic"), ("bam", "jito_bam"),
                   ("rakurai", "rakurai"), ("jito", "jito"))
KEY_SHOWN = 8            # the JS caption says "first eight characters"


def lane_for_client(client):
    c = (client or "").lower()
    for needle, lane in _LANE_BY_CLIENT:
        if needle in c:
            return lane
    return None


def _key_label(key):
    if not key:
        return "(no api key)"
    return key[:KEY_SHOWN] + "…" if len(key) > KEY_SHOWN else key


def egress_by_key(qch, validator, day, lane):
    """Our egress to one validator on one day, by api key, plus the rollup."""
    if lane not in LANES:
        raise ValueError(f"no egress lane named {lane!r}")
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", validator):
        raise ValueError("validator must be a base58 pubkey")
    d0 = dt.date.fromisoformat(day)
    d1 = (d0 + dt.timedelta(days=1)).isoformat()
    # `leader` and the lane status belong in PREWHERE with the day range:
    # the table is sorted on created_at only, so a day is a full ~70M-row
    # scan either way, but with them there ClickHouse reads two cheap
    # LowCardinality/Nullable columns first and fetches the wide ones for
    # the few thousand rows that survive. Measured on stakefish 2026-10-13:
    # 32 s with them in WHERE, 3.5 s in PREWHERE -- the difference between
    # tripping a 30 s execution cap and not.
    _, rows = qch(f"""
SELECT key, count() AS txs, sum(tip) AS ingress_lam, sum(eshare) AS egress_lam
FROM (
  SELECT signature, key, tip,
         if(esig = '', 0, if(tot > 0, etip * tip / tot, etip / cnt)) AS eshare
  FROM (
    SELECT signature, key, tip, esig, etip,
           sum(tip) OVER (PARTITION BY esig) AS tot,
           count() OVER (PARTITION BY esig) AS cnt
    FROM (
      SELECT signature,
             any(ifNull(api_key, '')) AS key,
             toFloat64(greatest(any(ifNull(tip, 0)), 0)) AS tip,
             any(ifNull({lane}_egress_tip_signature, '')) AS esig,
             toFloat64(any(ifNull({lane}_egress_tip, 0))) AS etip
      FROM {QUANT_TABLE}
      PREWHERE created_at >= '{day} 00:00:00' AND created_at < '{d1} 00:00:00'
        AND leader = '{validator}' AND {lane}_landing_status = 'success'
      WHERE status = 'success'
      GROUP BY signature
    )
  )
)
GROUP BY key ORDER BY egress_lam DESC, ingress_lam DESC""")
    keys = [{"key": _key_label(r[0]), "txs": int(float(r[1] or 0)),
             "ingress_sol": float(r[2] or 0) / 1e9,
             "egress_sol": float(r[3] or 0) / 1e9} for r in rows]
    eg = sum(k["egress_sol"] for k in keys)
    ing = sum(k["ingress_sol"] for k in keys)
    for k in keys:
        k["share"] = k["egress_sol"] / eg if eg else None

    # The rollup: this validator's row is the check, every row is the lane
    # total. FINAL, because both refreshers APPEND a version per run and the
    # open day holds several -- summed raw, today reads ~3x.
    _, rr = qch(
        f"SELECT leader, {lane}_egress_lamports, {lane}_ingress_lamports, "
        f"{lane}_egress_txs FROM {QUANT_ROLLUP} FINAL "
        f"WHERE day = '{day}' AND {lane}_egress_txs > 0")
    rollup, lane_total, lane_leaders = None, 0.0, 0
    for r in rr:
        v = float(r[1] or 0) / 1e9
        lane_total += v
        lane_leaders += 1
        if r[0] == validator:
            rollup = {"egress_sol": v, "ingress_sol": float(r[2] or 0) / 1e9,
                      "txs": int(float(r[3] or 0))}
    return {"validator": validator, "day": day, "lane": lane,
            "lane_label": LANES[lane], "keys": keys, "n_keys": len(keys),
            "txs": sum(k["txs"] for k in keys),
            "egress_sol": eg, "ingress_sol": ing, "rollup": rollup,
            "lane_total_sol": lane_total if rr else None,
            "lane_leaders": lane_leaders}


# Cached per validator-day-lane, like the fee drill-down: the figure is a
# property of that validator's slots on that day, whichever of ours it is
# being compared with. Today is never stored.

def _egress_path(validator):
    return os.path.join(CACHE_DIR, "egress", f"{_safe(validator)}.json")


def _egress_doc(validator):
    try:
        with _cache_io, open(_egress_path(validator), encoding="utf-8") as fh:
            doc = json.load(fh)
        return doc if isinstance(doc, dict) else {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def egress_cached(validator, day, lane):
    hit = _egress_doc(validator).get(f"{day}|{lane}")
    if not hit:
        return None
    return dict(hit, cached=True)


def egress_store(validator, day, lane, res):
    if day >= _today():
        return False
    doc = _egress_doc(validator)
    doc[f"{day}|{lane}"] = res
    path = _egress_path(validator)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _cache_io:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, separators=(",", ":"))
        os.replace(tmp, path)
    return True


# ---------------------------------------------------------- snapshots
#
# A comparison is a minute of Dune time plus however many day drill-downs
# the reader opened. A snapshot freezes that whole working state -- the
# per-day rows AND every fee table that was opened -- so it can be reopened
# later, or by someone else, without touching Dune at all.
#
# Distinct from the cache: the cache makes the NEXT query cheap and is keyed
# by pair and day, overwritten as days close. A snapshot is a dated artefact
# of one reading and is never rewritten.

SNAP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "snapshots")


def snapshot_write(payload):
    """Freeze one comparison. Returns the snapshot id."""
    ours = str(payload.get("ours") or "")
    other = str(payload.get("other") or "")
    rows = payload.get("rows") or []
    if not ours or not other or not rows:
        raise ValueError("a snapshot needs both validators and some rows")
    days = sorted(str(r.get("day") or "")[:10] for r in rows if r.get("day"))
    now = dt.datetime.now(dt.UTC)
    sid = (f"{now.strftime('%Y%m%dT%H%M%SZ')}_{_safe(ours)[:8]}_"
           f"{_safe(other)[:8]}")
    doc = {
        "id": sid,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "ours": ours, "other": other,
        "ours_name": str(payload.get("ours_name") or "")[:120],
        "other_name": str(payload.get("other_name") or "")[:120],
        "from": days[0] if days else "", "to": days[-1] if days else "",
        "day_count": len(days),
        "rows": rows,
        # Only the days the reader actually opened. Keyed by day so the
        # viewer can serve a drill-down without going near Dune.
        "fees": payload.get("fees") or {},
        "egress": payload.get("egress") or {},
        # Lets the viewer still resolve the egress lane for a day that was
        # not opened before the snapshot was taken.
        "other_client": str(payload.get("other_client") or "")[:60],
    }
    os.makedirs(SNAP_DIR, exist_ok=True)
    path = os.path.join(SNAP_DIR, sid + ".json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    os.replace(tmp, path)
    return sid


def snapshot_list():
    """Newest first. Reads only the header fields, never the rows."""
    out = []
    try:
        names = os.listdir(SNAP_DIR)
    except FileNotFoundError:
        return []
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(SNAP_DIR, name), encoding="utf-8") as fh:
                d = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        out.append({k: d.get(k) for k in
                    ("id", "created_at", "ours", "other", "ours_name",
                     "other_name", "from", "to", "day_count")}
                   | {"fee_days": len(d.get("fees") or {}),
                      "egress_days": len(d.get("egress") or {})})
    out.sort(key=lambda r: r.get("id") or "", reverse=True)
    return out


def snapshot_read(sid):
    path = os.path.join(SNAP_DIR, _safe(sid) + ".json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ------------------------------------------------------------- the job
#
# A run takes about a minute, nearly all of it Dune, so it cannot happen in
# a request thread. The page starts a job, polls, and draws when it lands.

_jobs = {}
_jobs_lock = threading.Lock()
JOB_TTL_S = 1800


def start_job(ours, other, days):
    jid = f"{int(time.time()*1000):x}{len(_jobs):02x}"
    job = {"id": jid, "state": "running", "step": "starting",
           "started": time.time(), "result": None, "error": None}
    with _jobs_lock:
        for k, v in list(_jobs.items()):      # drop anything stale
            if time.time() - v["started"] > JOB_TTL_S:
                _jobs.pop(k, None)
        _jobs[jid] = job

    def step(msg):
        job["step"] = msg

    def go():
        try:
            job["result"] = compare(ours, other, days, step)
            job["state"] = "done"
            job["step"] = "done"
        except Exception as exc:
            job["state"] = "error"
            job["error"] = str(exc)[:300]
    threading.Thread(target=go, daemon=True).start()
    return jid


def start_fees_job(validator, day):
    """Top priority-fee payers for one validator on one day."""
    jid = f"f{int(time.time()*1000):x}{len(_jobs):02x}"
    job = {"id": jid, "state": "running", "step": "starting",
           "started": time.time(), "result": None, "error": None}
    with _jobs_lock:
        _jobs[jid] = job

    def go():
        try:
            hit = fees_cached(validator, day)
            if hit:
                job["result"] = hit
                job["state"], job["step"] = "done", "from cache"
                return
            res = gbxdune.top_priority_fees(
                validator, day, lambda m: job.__setitem__("step", m))
            res["validator"], res["day"] = validator, day
            res["cached"] = False
            fees_store(validator, day, res)
            job["result"] = res
            job["state"], job["step"] = "done", "done"
        except Exception as exc:
            job["state"], job["error"] = "error", str(exc)[:300]
    threading.Thread(target=go, daemon=True).start()
    return jid


def start_egress_job(qch, validator, day, client):
    """Our egress to `validator` on `day`, by api key, on the lane its
    client implies. `qch` is app.py's quant ClickHouse helper."""
    lane = lane_for_client(client)
    jid = f"e{int(time.time()*1000):x}{len(_jobs):02x}"
    job = {"id": jid, "state": "running", "step": "starting",
           "started": time.time(), "result": None, "error": None}
    with _jobs_lock:
        _jobs[jid] = job

    def go():
        try:
            if not lane:
                raise RuntimeError(
                    f"no egress lane is known for a {client or 'unknown'} "
                    "validator -- our router reaches Harmonic, Jito BAM, "
                    "Jito and Rakurai validators")
            hit = egress_cached(validator, day, lane)
            if hit:
                job["result"] = hit
                job["state"], job["step"] = "done", "from cache"
                return
            job["step"] = f"reading the {LANES[lane]} lane on {day}"
            res = egress_by_key(qch, validator, day, lane)
            res["partial"] = day >= _today()
            res["cached"] = False
            egress_store(validator, day, lane, res)
            job["result"] = res
            job["state"], job["step"] = "done", "done"
        except Exception as exc:
            job["state"], job["error"] = "error", str(exc)[:300]
    threading.Thread(target=go, daemon=True).start()
    return jid


def job_status(jid):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        return {"state": "unknown",
                "error": "no such job -- it may have expired; run it again"}
    out = {"state": job["state"], "step": job["step"],
           "elapsed": int(time.time() - job["started"])}
    if job["state"] == "error":
        out["error"] = job["error"]
    if job["state"] == "done":
        out["result"] = job["result"]
    return out


CSS = """
.vcwrap{padding:18px 28px 50px;max-width:1180px}
.vcform{display:flex;gap:24px;flex-wrap:wrap;align-items:flex-start}
.vcf{display:flex;flex-direction:column;gap:6px}
.vcf>label{color:#6b7f96;font-size:11px;text-transform:uppercase;
  letter-spacing:.04em}
.vcf select,.vcf input{background:#0e151d;border:1px solid #22303f;
  border-radius:6px;color:#cfe0f0;padding:6px 11px;
  font:12px ui-monospace,Menlo,monospace}
.vcf select:focus,.vcf input:focus{outline:none;border-color:#5eead4}
.vcf select{min-width:340px}
.vcclients{display:flex;flex-wrap:wrap;gap:7px;max-width:420px}
.vcc{display:flex;align-items:center;gap:5px;border:1px solid #22303f;
  border-radius:6px;padding:4px 9px;font-size:11.5px;color:#8fa6bf;
  cursor:pointer}
.vcc:hover{border-color:#2f4459}
.vcc input{accent-color:#14b8a6;margin:0}
.vcc.on{border-color:#14b8a655;color:#cfe0f0;background:#0f766e18}
.vcu{color:#6b7f96;font-weight:400}
/* Two range inputs stacked on one track. Each input is transparent and
   pointer-transparent except for its own thumb, so both handles stay
   grabbable even where they overlap -- a single input cannot express a
   range, and two side by side cannot share a track. */
.rng{position:relative;width:272px;height:46px;margin-top:2px}
.rng-mid{position:absolute;top:0;left:0;right:0;text-align:center;
  color:#cfe0f0;font-size:12px;font-weight:600}
.rng-track{position:absolute;top:26px;left:9px;right:9px;height:4px;
  border-radius:2px;background:#2f4459}
/* The 0 mark is a position on the track, not a number to work out. */
.rng-zero{position:absolute;left:50%;top:-4px;width:1px;height:12px;
  background:#4d5c70}
.rng-fill{position:absolute;top:0;height:100%;border-radius:2px;
  background:#5eead4}
.rng input[type=range]{position:absolute;top:18px;left:0;width:100%;
  margin:0;height:20px;background:none;-webkit-appearance:none;
  appearance:none;pointer-events:none}
.rng input[type=range]:focus{outline:none}
.rng input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;
  pointer-events:auto;width:18px;height:18px;border-radius:50%;
  background:#5eead4;border:2px solid #0b1016;cursor:grab}
.rng input[type=range]::-moz-range-thumb{pointer-events:auto;width:18px;
  height:18px;border-radius:50%;background:#5eead4;border:2px solid #0b1016;
  cursor:grab}
.rng input[type=range]::-webkit-slider-runnable-track{background:none}
.rng input[type=range]::-moz-range-track{background:none}
.rng-ends{position:absolute;top:34px;left:0;right:0;display:flex;
  justify-content:space-between;color:#6b7f96;
  font:11px ui-monospace,Menlo,monospace}
.rng-ends span:nth-child(2){color:#4d5c70}
.vcgo{background:#0f766e;border:1px solid #14b8a6;color:#eafffb;
  border-radius:6px;padding:7px 18px;font-size:12.5px;cursor:pointer}
.vcgo:hover{background:#14b8a6}
.vcgo:disabled{opacity:.5;cursor:default}
.vcstat{margin-top:20px;color:#8fa6bf;font-size:12.5px}
.vcstat.err{color:#fca5a5}
/* Three series, validated against this surface: OKLCH lightness inside the
   dark band, chroma above the grey floor, adjacent CVD separation clear.
   The obvious brighter picks all failed the band check. */
.vcchart{margin-top:22px}
.vcchart svg{width:100%;height:280px;display:block;overflow:visible}
.vcax{stroke:#1e2937;stroke-width:1}
.vcgl{stroke:#16202b;stroke-width:1}
.vctick{fill:#6b7f96;font:10.5px ui-monospace,Menlo,monospace}
/* The series class carries the STROKE only. It used to set fill too, which
   beat .vcln{fill:none} on specificity and rendered every line as a filled
   wedge between its own points. Dots get their fill from a two-class rule
   so they stay solid without reaching the paths. */
.vcln{fill:none;stroke-width:1.25;stroke-linejoin:round;stroke-linecap:round}
.vcdot{stroke:#0b1016;stroke-width:1.25}
.s0{stroke:#2563eb}
.s1{stroke:#ea580c}
circle.vcdot.s0{fill:#2563eb}
circle.vcdot.s1{fill:#ea580c}
.vcleg{display:flex;gap:20px;flex-wrap:wrap;margin-top:12px;font-size:12px;
  color:#8fa6bf}
.vcleg i{display:inline-block;width:11px;height:11px;border-radius:3px;
  margin-right:6px;vertical-align:-1px}
.vctab2{margin-top:20px;width:100%;border-collapse:collapse;font-size:12px}
.vctab2 th{text-align:right;color:#6b7f96;font-size:10.5px;font-weight:600;
  text-transform:uppercase;letter-spacing:.04em;padding:0 12px 7px 0;
  border-bottom:1px solid #1e2937}
.vctab2 th:first-child{text-align:left}
.vctab2 td{padding:7px 12px 7px 0;border-bottom:1px solid #16202b;
  color:#cfe0f0;text-align:right;font:11.5px ui-monospace,Menlo,monospace}
.vctab2 td:first-child{text-align:left}
.vcsrc2{margin-top:18px;color:#4d5c70;font-size:11px;line-height:1.6}
.vcrow{cursor:pointer}
.vcrow:hover td,.vcrow:focus td{background:#101a23;outline:none}
.vcrow.on td{background:#0f766e18;color:#eafffb}
.vcdet td{padding:0 0 14px;border-bottom:1px solid #16202b}
.vcload{color:#6b7f96;font-size:12px;padding:12px 2px}
.vcload.err{color:#fca5a5}
.vcdh{color:#8fa6bf;font-size:12.5px;padding:12px 2px 9px}
.vcdh b{color:#cfe0f0}
.vcfee{width:100%;border-collapse:collapse;font-size:12px}
.vcfee th{text-align:left;color:#6b7f96;font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.04em;padding:0 12px 6px 0;
  border-bottom:1px solid #1e2937}
.vcfee th.n,.vcfee td.n{text-align:right}
.vcfee td{padding:6px 12px 6px 0;border-bottom:1px solid #16202b;
  color:#cfe0f0}
.vcfee td.m{font:11.5px ui-monospace,Menlo,monospace}
.vcfail{color:#fca5a5;font-size:10px}
.snapbar{margin-top:22px;display:flex;gap:14px;align-items:center;
  flex-wrap:wrap}
.snapbar span{color:#8fa6bf;font-size:12.5px}
.snapgrid{display:grid;gap:12px;padding-top:6px;
  grid-template-columns:repeat(auto-fill,minmax(310px,1fr))}
.snapcard{display:block;padding:15px 17px;border:1px solid #22303f;
  border-radius:10px;background:#0e151d;text-decoration:none;color:inherit;
  transition:.12s}
.snapcard:hover{border-color:#5eead4;background:#101a23}
.snapv{color:#5eead4;font-size:13.5px;font-weight:600}
.snapv span{color:#4d5c70;font-weight:400;margin:0 4px}
.snapr{margin-top:6px;color:#cfe0f0;
  font:12px ui-monospace,Menlo,monospace}
.snapm{margin-top:5px;color:#6b7f96;font-size:11.5px}
.snapnone{color:#8fa6bf;font-size:13px;padding:24px 0;max-width:62ch;
  line-height:1.6}
.snaphead{padding:4px 0 18px;border-bottom:1px solid #1e2937;margin-bottom:6px}
.snaphead h2{margin:10px 0 4px;font-size:15px;color:#cfe0f0}
.snaphead span{color:#6b7f96;font-size:12px}
.vclink{color:#5eead4;text-decoration:none;border-bottom:1px solid #14b8a644}
.vclink:hover,.vclink:focus{border-bottom-color:#5eead4;outline:none}
/* The copy control sits beside a link here, so unlike the bare .cpv value
   it needs its own label -- there is no value text to recolour. */
.cpsm{margin-left:7px;padding:0 5px;border:1px solid #2b3a4b;border-radius:4px;
  font-size:9.5px;color:#6b7f96;cursor:pointer;text-transform:uppercase;
  letter-spacing:.03em}
.cpsm:hover,.cpsm:focus{color:#5eead4;border-color:#14b8a655;outline:none}
/* The egress-by-key bars: one series, so one hue (the page accent), the
   fold-up "N more" bar in the muted grey, and hover brightens. */
/* Wider than the page column on purpose -- the table it sits in grows
   with it. Readability of the data beats the layout here. */
.vcegc{width:70vw;min-width:100%}
.vcegc svg{height:auto;aspect-ratio:760/260}
.vcegctl{color:#8fa6bf;font-size:12px;padding:0 2px 10px}
.vcegn{width:64px;background:#0e151d;border:1px solid #22303f;border-radius:6px;
  color:#cfe0f0;padding:3px 8px;font:12px ui-monospace,Menlo,monospace;margin:0 6px}
.vcegn:focus{outline:none;border-color:#5eead4}
button.cpsm{background:none;font-family:inherit;line-height:1.5}
.vcbar{fill:#14b8a6}
.vcbar.oth{fill:#4d5c70}
.vcbar:hover{fill:#5eead4}
.vcval{fill:#cfe0f0;font:10px ui-monospace,Menlo,monospace}
.vcpart{color:#fbbf24}
.vcfeebox{margin-top:22px}
.vcup{margin-top:16px}
"""

# The viewer runs in three steps: this sets up the globals the main
# script expects, then the main script loads (defining draw() and exposing
# it, but wiring no controls because there are none), then SNAP_POST_JS
# draws. Shipping only this half left __VC_DRAW__ undefined and the page
# blank.
SNAP_PRE_JS = r'''
(function(){
  var D = window.__SNAP__; if (!D) return;
  window.__VC__ = {ours: {}, cands: [{id: D.other, name: D.other_name || '',
                                      client: D.other_client || '', stake: 0}]};
  window.__VC__.ours[D.ours] = {name: D.ours_name || ''};
  window.__VC_SNAP_FEES__ = D.fees || {};
  window.__VC_SNAP_EGRESS__ = D.egress || {};
})();
'''

SNAP_POST_JS = r'''
(function(){
  var D = window.__SNAP__;
  if (!D || !window.__VC_DRAW__) return;
  window.__VC_DRAW__({rows: D.rows, days: D.rows.map(function(r){
    return r.day; }), ours: D.ours, other: D.other});
})();
'''

JS = r'''
(function(){
  var D = window.__VC__; if (!D) return;
  var SNAP_PATH = '/econ/validator-comparison/snapshots';
  var outEl;                       // assigned below, used by draw()
  // Per-day drill-down caches. Declared up here, ABOVE the LIVE early
  // return: a `var` below it is hoisted but never assigned in the
  // snapshot viewer, so the first row click there threw.
  var feeSeen = {}, openedFees = {}, egSeen = {}, openedEgress = {}, egTop = {},
      egAll = {};                    // day -> key table expanded to every key
  // Same reason. How many keys get their own bar before the rest fold
  // into one; the panel has an input to change it per day.
  var TOP_DEFAULT = 10;
  var ours = document.getElementById('vcours'),
      other = document.getElementById('vcother'),
      loEl = document.getElementById('vclo'),
      hiEl = document.getElementById('vchi'),
      readEl = document.getElementById('vcread'),
      fillEl = document.getElementById('vcfill'),
      SPAN = hiEl ? parseFloat(hiEl.max) : 100000;
  // The snapshot viewer has the output container but none of the controls.
  var LIVE = !!(ours && other && loEl && hiEl);
  function brief(n){
    var s = n < 0 ? '-' : (n > 0 ? '+' : ''), a = Math.abs(n);
    if (a >= 1e6) return s + (a/1e6).toFixed(a % 1e6 ? 1 : 0) + 'M';
    if (a >= 1e3) return s + Math.round(a/1e3) + 'K';
    return s + a;
  }
  function sol(l){ return (l/1e9).toLocaleString(undefined,
      {maximumFractionDigits:0}); }
  function redraw(){
    var mine = D.ours[ours.value] || null;
    // Either handle can be dragged past the other, so read them as a pair
    // rather than letting one shove the other along.
    var a = parseFloat(loEl.value), b = parseFloat(hiEl.value);
    var vLo = Math.min(a, b), vHi = Math.max(a, b);
    readEl.textContent = (vLo <= -SPAN && vHi >= SPAN)
      ? 'Any within \u00b1' + brief(SPAN).slice(1)
      : brief(vLo) + ' \u2026 ' + brief(vHi);
    // 0 is the centre of the track, so a value maps to (v + SPAN) / 2*SPAN.
    fillEl.style.left = (100 * (vLo + SPAN) / (2*SPAN)) + '%';
    fillEl.style.width = (100 * (vHi - vLo) / (2*SPAN)) + '%';

    var base = mine ? mine.stake : 0;
    var sLo = Math.max(0, base + vLo * 1e9), sHi = base + vHi * 1e9;
    var want = [];
    document.querySelectorAll('.vcc input:checked').forEach(function(c){
      want.push(c.value); });
    var keep = D.cands.filter(function(v){
      return want.indexOf(v.client) !== -1 &&
             v.stake >= sLo && v.stake <= sHi &&
             v.id !== ours.value;
    }).sort(function(x, y){ return y.stake - x.stake; });
    var had = other.value;
    other.innerHTML = keep.length ? keep.map(function(v){
      return '<option value="' + v.id + '">' +
        (v.name || v.id.slice(0,14) + '\u2026') + ' \u00b7 ' +
        v.client + ' \u00b7 ' + sol(v.stake) + ' SOL</option>';
    }).join('') : '<option value="">no validator in that client set and ' +
      'stake band</option>';
    other.disabled = !keep.length;
    if (keep.some(function(v){ return v.id === had; })) other.value = had;
  }
  window.__VC_DRAW__ = draw;        // the snapshot viewer calls this
  if (LIVE){
    ours.addEventListener('change', redraw);
    loEl.addEventListener('input', redraw);
    hiEl.addEventListener('input', redraw);
    document.querySelectorAll('.vcc input').forEach(function(c){
      c.addEventListener('change', function(){
        c.closest('.vcc').classList.toggle('on', c.checked); redraw(); });
    });
    redraw();
  }

  // ---------------------------------------------------------- the run
  //
  // outEl is what draw() writes into, so it has to be resolved BEFORE the
  // LIVE guard -- the snapshot viewer has this container and none of the
  // controls, and leaving it below meant draw() found it undefined.
  outEl = document.getElementById('vcout');
  if (!LIVE) return;
  var runBtn = document.getElementById('vcrun'),
      daysEl = document.getElementById('vcdays'),
      statEl = document.getElementById('vcstat');

  function say(msg, bad){
    statEl.hidden = false;
    statEl.className = 'vcstat' + (bad ? ' err' : '');
    statEl.textContent = msg;
  }
  function fmt(v, dp){ return v == null ? '\u2014' : v.toFixed(dp == null ? 5 : dp); }

  function draw(res){
    var rows = res.rows;
    // Three series. 0 = the other validator, 1 = ours on chain, 2 = ours
    // with GBX builder tips on top. All slot-time normalised.
    // Two lines, one per validator. Ours is its on-chain figure plus GBX
    // builder tips, because that is what our validator actually earns --
    // splitting it into a third line compared us against ourselves.
    // Never "other": an unnamed validator in the report still has an
    // identity, and a truncated pubkey is a name you can act on.
    function nameOf(id, rec){
      return (rec && rec.name) || (id ? id.slice(0, 8) + '\u2026' + id.slice(-4)
                                      : '\u2014');
    }
    var ourName = nameOf(res.ours, D.ours[res.ours]);
    var othRec = D.cands.filter(function(v){ return v.id === res.other; })[0];
    var othName = nameOf(res.other, othRec);
    var othClient = (othRec && othRec.client) || '';
    var S = [
      {cls:'s0', label:othName,
       at:function(r){ return r.other && r.other.per_slot_norm; }},
      {cls:'s1', label:ourName + ' (incl. GBX tips)',
       at:function(r){ var o = r.ours && r.ours.per_slot_norm;
                       return o == null ? null : o + (r.tips_per_slot_norm || 0); }}
    ];
    var vals = [];
    S.forEach(function(s){ rows.forEach(function(r){
      var v = s.at(r); if (v != null) vals.push(v); }); });
    if (!vals.length) { say('no overlapping data for those two validators', 1); return; }
    var hi = Math.max.apply(null, vals), lo = Math.min.apply(null, vals);
    var pad = (hi - lo) * 0.15 || hi * 0.1; hi += pad; lo = Math.max(0, lo - pad);
    var W = 760, H = 240, L = 58, R = 12, T = 10, B = 28;
    var px = function(i){ return L + (W - L - R) * (rows.length < 2 ? 0.5 : i / (rows.length - 1)); };
    var py = function(v){ return T + (H - T - B) * (1 - (v - lo) / (hi - lo || 1)); };
    var g = '';
    for (var t = 0; t <= 4; t++){
      var v = lo + (hi - lo) * t / 4, y = py(v);
      g += '<line class="vcgl" x1="' + L + '" y1="' + y + '" x2="' + (W-R) + '" y2="' + y + '"/>' +
           '<text class="vctick" x="' + (L-8) + '" y="' + (y+3.5) + '" text-anchor="end">' + v.toFixed(4) + '</text>';
    }
    rows.forEach(function(r, i){
      if (rows.length > 8 && i % 2) return;
      g += '<text class="vctick" x="' + px(i) + '" y="' + (H-10) + '" text-anchor="middle">' +
           r.day.slice(8) + '/' + r.day.slice(5,7) + '</text>';
    });
    S.forEach(function(s){
      var d = '', open = false;
      rows.forEach(function(r, i){
        var v = s.at(r);
        if (v == null) { open = false; return; }
        d += (open ? 'L' : 'M') + px(i).toFixed(1) + ' ' + py(v).toFixed(1) + ' ';
        open = true;
      });
      if (d) g += '<path class="vcln ' + s.cls + '" d="' + d + '"/>';
      rows.forEach(function(r, i){
        var v = s.at(r);
        if (v != null) g += '<circle class="vcdot ' + s.cls + '" cx="' + px(i).toFixed(1) +
          '" cy="' + py(v).toFixed(1) + '" r="2.5"><title>' + s.label + ' \u00b7 ' +
          r.day + ' \u00b7 ' + v.toFixed(5) + ' SOL/slot</title></circle>';
      });
    });
    var leg = S.map(function(s){
      var c = s.cls === 's0' ? '#2563eb' : '#ea580c';
      return '<span><i style="background:' + c + '"></i>' + s.label + '</span>'; }).join('');
    function num(n){ return n == null ? '\u2014' : n.toLocaleString(); }
    function sol(n){ return n == null ? '\u2014'
      : n.toLocaleString(undefined, {minimumFractionDigits:2,
                                     maximumFractionDigits:2}); }
    // Every row is clickable: it opens that day's biggest priority-fee
    // payers for the validator we are comparing against.
    // The per-slot pipeline, one column per step, so the total can be
    // checked by eye: fees per slot, GBX tips per slot, their sum, the
    // slot-time factor, and the sum times that factor. The factor is
    // linear, so normalising each part and adding gives the same number --
    // but shown that way the parts did not visibly add up to the total.
    function fac(side){
      return side && side.slot_ms && side.ref_ms ? side.ref_ms / side.slot_ms : null;
    }
    function x4(v){ return v == null ? '\u2014' : '\u00d7' + v.toFixed(4); }
    var tb = '<table class="vctab2"><tr><th>day</th>' +
      '<th>' + ourName + '<br>fees/slot</th><th>' + ourName + '<br>GBX tips/slot</th>' +
      '<th>' + ourName + '<br>normalised</th><th>' + ourName + '<br>day SOL</th>' +
      '<th>' + ourName + '<br>slots</th>' +
      '<th>' + othName + '<br>fees/slot</th><th>' + othName + '<br>slot time</th>' +
      '<th>' + othName + '<br>normalised</th><th>' + othName + '<br>day SOL</th>' +
      '<th>' + othName + '<br>slots</th></tr>' + rows.map(function(r){
        var o = r.ours || {}, ot = r.other || {};
        var fees = o.per_slot, tips = r.tips_per_slot;
        return '<tr class="vcrow" data-day="' + r.day + '" tabindex="0" ' +
          'title="see our egress to ' + othName + ' by api key, and its ' +
          'biggest priority-fee payers, on ' + r.day + '">' +
          '<td>' + r.day + '</td><td>' + fmt(fees) + '</td><td>' + fmt(tips) +
          '</td><td>' + fmt(S[1].at(r)) + '</td><td>' + sol(o.total_sol) + '</td><td>' +
          num(o.blocks) + '</td><td>' + fmt(ot.per_slot) + '</td><td>' +
          x4(fac(ot)) + '</td><td>' + fmt(ot.per_slot_norm) + '</td><td>' +
          sol(ot.total_sol) + '</td><td>' + num(ot.blocks) + '</td></tr>' +
          '<tr class="vcdet" data-day="' + r.day + '" hidden><td colspan=11>' +
          '</td></tr>';
      }).join('') + '</table>';
    outEl.innerHTML = '<div class="vcchart"><svg viewBox="0 0 ' + W + ' ' + H +
      '" role="img" aria-label="block reward per leader slot, by day">' + g +
      '<line class="vcax" x1="' + L + '" y1="' + (H-B) + '" x2="' + (W-R) + '" y2="' + (H-B) + '"/>' +
      '</svg></div><div class="vcleg">' + leg + '</div>' + tb +
      '<div class="vcsrc2">SOL per leader slot. <b>fees/slot</b> is the Firedancer daily ' +
      'report\u2019s avg_reward (priority + base fees per landed block) scaled by ' +
      'landed/leader slots, so a skipped slot counts as zero. <b>GBX tips/slot</b> are ' +
      'builder tips from Dune over the slots our validator led \u2014 they are paid to tip ' +
      'wallets, not to the leader in block, so avg_reward never sees them. <b>normalised</b> ' +
      'is the two added together, times the <b>slot time</b> factor: ' +
      'the day\u2019s median slot time across validators over this validator\u2019s own, so ' +
      'a validator that holds its slots longer is not flattered. The chart plots the ' +
      'normalised column. Slot counts are each validator\u2019s own leader slots that ' +
      'day, the denominator its per-slot figures use.</div>';
    if (LIVE){
      outEl.innerHTML += '<div class="snapbar">' +
        '<button id="vcsnap" type="button" class="vcgo">snapshot this ' +
        'comparison</button><span id="vcsnapmsg"></span></div>';
      var sb = document.getElementById('vcsnap');
      sb.addEventListener('click', function(){
        var msg = document.getElementById('vcsnapmsg');
        sb.disabled = true; msg.textContent = 'saving\u2026';
        fetch('/api/valcomp/snapshot', {method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ours: res.ours, other: res.other,
            ours_name: ourName, other_name: othName,
            rows: res.rows,
            // only the days actually opened travel with it
            fees: openedFees, egress: openedEgress,
            other_client: othClient})})
          .then(function(r){ return r.json(); })
          .then(function(j){
            sb.disabled = false;
            if (j.error){ msg.textContent = j.error; return; }
            msg.innerHTML = 'saved \u2014 <a class="vclink" href="' +
              SNAP_PATH + '?id=' + encodeURIComponent(j.id) +
              '">open it</a> or see <a class="vclink" href="' + SNAP_PATH +
              '">all snapshots</a>';
          })
          .catch(function(e){ sb.disabled = false;
                              msg.textContent = 'could not save: ' + e; });
      });
    }
    outEl.hidden = false;
    wireRows(res, ourName, othName, othClient);
  }

  // ------------------------------------------------- the per-day drill-down
  //
  // Two panels per opened day, loaded side by side: our egress to the other
  // validator by api key (quant ClickHouse, seconds) above, and its biggest
  // priority-fee payers (Dune, about a minute) below. Each keeps its own
  // cache, so reopening a day costs nothing and a snapshot carries both.
  function wireRows(res, ourName, othName, othClient){
    outEl.querySelectorAll('.vcrow').forEach(function(tr){
      function open(){
        var day = tr.getAttribute('data-day');
        var det = outEl.querySelector('.vcdet[data-day="' + day + '"]');
        if (!det) return;
        if (!det.hidden){ det.hidden = true; tr.classList.remove('on'); return; }
        outEl.querySelectorAll('.vcdet').forEach(function(d){ d.hidden = true; });
        outEl.querySelectorAll('.vcrow').forEach(function(x){
          x.classList.remove('on'); });
        det.hidden = false; tr.classList.add('on');
        var cell = det.firstElementChild;
        if (!cell.querySelector('.vcegbox'))
          cell.innerHTML = '<div class="vcegbox"></div><div class="vcfeebox"></div>';
        loadEgress(day, cell.querySelector('.vcegbox'), res, ourName, othName,
                   othClient);
        loadFees(day, cell.querySelector('.vcfeebox'), res.other, othName);
      }
      tr.addEventListener('click', open);
      tr.addEventListener('keydown', function(ev){
        if (ev.key === 'Enter' || ev.key === ' '){ ev.preventDefault(); open(); }
      });
    });
  }

  function pollJob(jid, onStep){
    return new Promise(function(resolve, reject){
      (function tick(){
        fetch('/api/valcomp/status?job=' + encodeURIComponent(jid))
          .then(function(r){ return r.json(); })
          .then(function(st){
            if (st.state === 'running'){ onStep(st); setTimeout(tick, 2000); return; }
            if (st.state !== 'done') reject(new Error(st.error || 'failed'));
            else resolve(st.result);
          }).catch(reject);
      })();
    });
  }

  function loadFees(day, box, otherId, otherName){
    if (feeSeen[day]){ box.innerHTML = feeSeen[day]; return; }
    var frozen = (window.__VC_SNAP_FEES__ || {})[day];
    if (frozen){
      feeSeen[day] = feeTable(frozen, otherName, day);
      box.innerHTML = feeSeen[day];
      return;
    }
    box.innerHTML = '<div class="vcload">asking Dune for ' +
      otherName + '’s biggest priority-fee payers on ' + day +
      '…</div>';
    fetch('/api/valcomp/fees', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({validator: otherId, day: day})})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.error) throw new Error(j.error);
        return pollJob(j.job, function(st){
          box.innerHTML = '<div class="vcload">' + st.step +
            '… (' + st.elapsed + 's)</div>'; });
      })
      .then(function(result){
        openedFees[day] = result;
        feeSeen[day] = feeTable(result, otherName, day);
        box.innerHTML = feeSeen[day];
      })
      .catch(function(e){
        box.innerHTML = '<div class="vcload err">' + e.message + '</div>';
      });
  }

  function renderEgress(box, day, eg, row, ourName, othName){
    egSeen[day] = eg;
    var n = egTop[day] || TOP_DEFAULT;
    box.innerHTML = egressPanel(eg, row, ourName, othName, n, !!egAll[day]);
    var inp = box.querySelector('.vcegn');
    if (inp) inp.addEventListener('change', function(){
      var v = parseInt(inp.value, 10);
      if (!(v >= 1)) v = TOP_DEFAULT;
      egTop[day] = v;
      renderEgress(box, day, eg, row, ourName, othName);
    });
    var all = box.querySelector('.vcegall');
    if (all) all.addEventListener('click', function(){
      egAll[day] = !egAll[day];
      renderEgress(box, day, eg, row, ourName, othName);
    });
    var dl = box.querySelector('.vcegdl');
    if (dl) dl.addEventListener('click', function(){ downloadEgress(eg, othName); });
  }

  // Every key, not just the ones on screen, as a file. Built here from the
  // result already in hand, so no second query and no server endpoint.
  function csvCell(v){
    v = v == null ? '' : String(v);
    return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
  }
  function downloadEgress(eg, othName){
    var head = ['day', 'validator', 'validator_name', 'lane', 'api_key_prefix',
                'egress_sol', 'share_of_day', 'landed_txs', 'ingress_tip_sol',
                'kept_sol'];
    var lines = [head.join(',')];
    (eg.keys || []).forEach(function(k){
      lines.push([eg.day, eg.validator, othName, eg.lane, k.key.replace(/\u2026$/, ''),
                  k.egress_sol.toFixed(9), k.share == null ? '' : k.share.toFixed(6),
                  k.txs, k.ingress_sol.toFixed(9),
                  (k.ingress_sol - k.egress_sol).toFixed(9)].map(csvCell).join(','));
    });
    var blob = new Blob([lines.join('\n') + '\n'], {type: 'text/csv;charset=utf-8'});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'egress_by_key_' + eg.lane + '_' + eg.day + '_' +
      String(eg.validator).slice(0, 8) + '.csv';
    document.body.appendChild(a); a.click();
    setTimeout(function(){ URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }

  function loadEgress(day, box, res, ourName, othName, othClient){
    var row = res.rows.filter(function(r){ return r.day === day; })[0];
    if (egSeen[day]){ renderEgress(box, day, egSeen[day], row, ourName, othName); return; }
    var frozen = (window.__VC_SNAP_EGRESS__ || {})[day];
    if (frozen){ renderEgress(box, day, frozen, row, ourName, othName); return; }
    box.innerHTML = '<div class="vcload">reading our egress to ' + othName +
      ' on ' + day + ', by api key…</div>';
    fetch('/api/valcomp/egress', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({validator: res.other, day: day, client: othClient})})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.error) throw new Error(j.error);
        return pollJob(j.job, function(st){
          box.innerHTML = '<div class="vcload">' + st.step +
            '… (' + st.elapsed + 's)</div>'; });
      })
      .then(function(result){
        openedEgress[day] = result;
        renderEgress(box, day, result, row, ourName, othName);
      })
      .catch(function(e){
        box.innerHTML = '<div class="vcload err">' + e.message + '</div>';
      });
  }

  // One bar per api key, biggest first; past topN the rest fold into a
  // single muted bar. Data over polish: the chart is 70vw wide, every bar
  // carries its value while they fit, labels only rotate when they must.
  function s4(n){ return n == null ? '—' : n.toFixed(4); }
  function pct(a, b){ return b ? (100 * a / b).toFixed(1) + '%' : '—'; }

  function egressPanel(eg, row, ourName, othName, topN, showAll){
    var keys = eg.keys || [];
    topN = topN || TOP_DEFAULT;
    if (!keys.length)
      return '<div class="vcload">none of our transactions landed on ' +
        othName + ' through the ' + eg.lane_label + ' lane on ' + eg.day +
        '</div>' + upliftBlock(eg, row, ourName, othName);
    var head = '<div class="vcdh">Our egress that landed in <b>' + othName +
      '</b>\u2019s slots on ' + eg.day + ' (' + eg.lane_label + ' lane) \u2014 <b>' +
      s4(eg.egress_sol) + ' SOL</b> paid across ' + eg.n_keys + ' api key' +
      (eg.n_keys === 1 ? '' : 's') + ' and ' + eg.txs.toLocaleString() +
      ' landed transactions, which tipped us ' + s4(eg.ingress_sol) + ' SOL' +
      (eg.rollup ? ' · daily rollup ' + s4(eg.rollup.egress_sol) + ' SOL' : '') +
      (eg.partial ? ' · <span class="vcpart">today — still accumulating</span>'
                  : '') + '</div>' +
      '<div class="vcegctl">show top <input class="vcegn" type="number" min="1" max="' +
      keys.length + '" step="1" value="' + Math.min(topN, keys.length) +
      '" aria-label="how many api keys get their own bar"> of ' + keys.length +
      ' keys; the rest fold into one bar' +
      ' <button type="button" class="cpsm vcegdl" title="every key as CSV, not ' +
      'just the ones shown">download csv</button></div>';

    var top = keys.slice(0, topN), rest = keys.slice(topN);
    var rv = 0, ri = 0, rt = 0;
    rest.forEach(function(k){ rv += k.egress_sol; ri += k.ingress_sol; rt += k.txs; });
    var bars = top.map(function(k){
      return {label: k.key, v: k.egress_sol, oth: false,
        title: k.key + ' · ' + s4(k.egress_sol) + ' SOL egress · ' +
          pct(k.egress_sol, eg.egress_sol) + ' of the day · ' +
          k.txs.toLocaleString() + ' txs · tipped us ' + s4(k.ingress_sol) +
          ' SOL'}; });
    if (rest.length)
      bars.push({label: rest.length + ' more', v: rv, oth: true,
        title: rest.length + ' smaller keys · ' + s4(rv) + ' SOL egress · ' +
          rt.toLocaleString() + ' txs'});

    var n = bars.length, flat = n <= 16;     // room for upright labels?
    var W = 760, H = flat ? 250 : 270, L = 58, R = 12, T = 18, B = flat ? 34 : 70,
        base = H - B;
    var hi = (Math.max.apply(null, bars.map(function(b){ return b.v; })) || 1) * 1.1;
    var slot = (W - L - R) / n, bw = Math.max(3, Math.min(flat ? 44 : 26, slot - 2));
    var py = function(v){ return T + (base - T) * (1 - v / hi); };
    var g = '';
    for (var t = 0; t <= 4; t++){
      var v = hi * t / 4, y = py(v);
      g += '<line class="vcgl" x1="' + L + '" y1="' + y + '" x2="' + (W - R) + '" y2="' + y + '"/>' +
        '<text class="vctick" x="' + (L - 8) + '" y="' + (y + 3.5) + '" text-anchor="end">' +
        v.toFixed(3) + '</text>';
    }
    bars.forEach(function(b, i){
      var x = L + i * slot + (slot - bw) / 2, y = py(b.v), h = base - y;
      var r = Math.max(0, Math.min(4, bw / 2, h));
      // Rounded at the data end only, square on the baseline.
      var d = 'M' + x.toFixed(1) + ' ' + base + ' V' + (y + r).toFixed(1) +
        ' Q' + x.toFixed(1) + ' ' + y.toFixed(1) + ' ' + (x + r).toFixed(1) + ' ' + y.toFixed(1) +
        ' H' + (x + bw - r).toFixed(1) +
        ' Q' + (x + bw).toFixed(1) + ' ' + y.toFixed(1) + ' ' + (x + bw).toFixed(1) + ' ' +
        (y + r).toFixed(1) + ' V' + base + ' Z';
      g += '<path class="vcbar' + (b.oth ? ' oth' : '') + '" d="' + d + '"><title>' +
        b.title + '</title></path>';
      if (flat || (i < 3 && !b.oth))
        g += '<text class="vcval" x="' + (x + bw / 2).toFixed(1) + '" y="' +
          (y - 4).toFixed(1) + '" text-anchor="middle">' + s4(b.v) + '</text>';
      g += flat
        ? '<text class="vctick" x="' + (x + bw / 2).toFixed(1) + '" y="' + (base + 14) +
          '" text-anchor="middle">' + b.label + '</text>'
        : '<text class="vctick" transform="translate(' + (x + bw / 2).toFixed(1) + ',' +
          (base + 9) + ') rotate(-42)" text-anchor="end">' + b.label + '</text>';
    });
    g += '<line class="vcax" x1="' + L + '" y1="' + base + '" x2="' + (W - R) + '" y2="' +
      base + '"/>';
    var chart = '<div class="vcchart vcegc"><svg viewBox="0 0 ' + W + ' ' + H +
      '" role="img" aria-label="our egress tips to ' + othName +
      ' by api key, SOL">' + g + '</svg></div>';

    var shown = showAll ? keys : top;
    var tb = '<table class="vcfee"><tr><th>api key</th><th class=n>egress SOL</th>' +
      '<th class=n>% of day</th><th class=n>landed txs</th><th class=n>tipped us</th>' +
      '<th class=n>we kept</th></tr>' + shown.map(function(k){
        return '<tr><td class="m">' + k.key + '</td><td class="n m">' + s4(k.egress_sol) +
          '</td><td class="n m">' + pct(k.egress_sol, eg.egress_sol) +
          '</td><td class="n m">' + k.txs.toLocaleString() + '</td><td class="n m">' +
          s4(k.ingress_sol) + '</td><td class="n m">' +
          s4(k.ingress_sol - k.egress_sol) + '</td></tr>';
      }).join('') + (rest.length && !showAll ?
        '<tr><td>' + rest.length + ' more keys <button type="button" class="cpsm vcegall">' +
        'show all ' + keys.length + '</button></td><td class="n m">' + s4(rv) +
        '</td><td class="n m">' + pct(rv, eg.egress_sol) + '</td><td class="n m">' +
        rt.toLocaleString() + '</td><td class="n m">' + s4(ri) + '</td><td class="n m">' +
        s4(ri - rv) + '</td></tr>' : '') + (showAll && rest.length ?
        '<tr><td colspan=6>all ' + keys.length + ' keys <button type="button" ' +
        'class="cpsm vcegall">show top ' + top.length + ' only</button></td></tr>' : '') +
      '</table>';

    return head + chart + tb + upliftBlock(eg, row, ourName, othName) +
      '<div class="vcsrc2">Egress is the tip our router attached to land these ' +
      'transactions in ' + othName + '’s slots through the ' + eg.lane_label +
      ' lane; “tipped us” is what the same transactions carried when they ' +
      'reached us, so the last column is our margin on them. One payment often ' +
      'covers several transactions and is shared across them pro rata before ' +
      'being summed by key; every transaction is counted once however many ' +
      'versions of it the source table holds. Keys are shown by their first ' +
      'eight characters only.</div>';
  }

  // The estimate: that day's egress spread over OUR validator's leader
  // slots, scaled by the same slot-time factor as its own figure, added to
  // what it earned per slot. Two rows -- this one validator, and every
  // validator we reached through the lane that day.
  function upliftBlock(eg, row, ourName, othName){
    var o = row && row.ours;
    if (!o || !o.blocks)
      return '<div class="vcload vcup">no leader slots are recorded for ' + ourName +
        ' on ' + eg.day + ', so there is no per-slot figure to add to</div>';
    var basePs = o.per_slot_norm;
    if (basePs == null)
      return '<div class="vcload vcup">' + ourName + ' has no per-slot figure for ' +
        eg.day + '</div>';
    basePs += (row.tips_per_slot_norm || 0);
    var nf = o.norm_factor || 1;
    function line(label, sol){
      if (sol == null) return '';
      var add = sol / o.blocks * nf;
      return '<tr><td>' + label + '</td><td class="n m">' + s4(sol) +
        '</td><td class="n m">' + fmt(add) + '</td><td class="n m">' + fmt(basePs + add) +
        '</td><td class="n m">' + pct(add, basePs) + '</td></tr>';
    }
    var oth = row.other && row.other.per_slot_norm;
    return '<div class="vcup"><div class="vcdh">If that egress had landed on <b>' +
      ourName + '</b> instead — it earned <b>' + fmt(basePs) +
      ' SOL/slot</b> on ' + eg.day + ' (incl. GBX tips, slot-time normalised) over ' +
      o.blocks.toLocaleString() + ' leader slots' +
      (oth != null ? '; ' + othName + ' earned ' + fmt(oth) : '') + '</div>' +
      '<table class="vcfee"><tr><th>scenario</th><th class=n>SOL redirected</th>' +
      '<th class=n>+ per slot</th><th class=n>new per slot</th><th class=n>uplift</th></tr>' +
      line('this day’s egress to ' + othName, eg.egress_sol) +
      line('all our ' + eg.lane_label + '-lane egress that day, every validator (' +
           eg.lane_leaders + ')', eg.lane_total_sol) +
      '</table><div class="vcsrc2">Redirected SOL is spread over ' + ourName +
      '’s leader slots that day and scaled by its slot-time factor (×' +
      nf.toFixed(3) + '), the same treatment its GBX tips get. It assumes the ' +
      'orderflow would have landed with us at the same tip, which makes it an ' +
      'upper bound: a tip bid to win a ' + eg.lane_label + ' slot is not ' +
      'automatically what the same sender would pay on ours.</div></div>';
  }

  function feeTable(res, otherName, day){
    if (!res.rows || !res.rows.length)
      return '<div class="vcload">no transaction paid a priority fee in ' +
             otherName + '\u2019s slots on ' + day + '</div>';
    // The signature and the signer go to Solscan; the copy control beside
    // each keeps the raw value available for a query, which the link cannot
    // do. The slot stays copy-only. Declared here, NOT inside the return
    // expression below -- spliced in there it parses as a function operand
    // of the string concatenation and the whole table comes out wrong.
    function solscan(kind, value, shown, title){
      return '<a class="vclink" href="https://solscan.io/' + kind + '/' +
        encodeURIComponent(value) + '" target="_blank" rel="noopener" ' +
        'title="' + title + ' on Solscan">' + shown + '</a>' +
        '<span class="cpv cpsm" data-c="' + value + '" role="button" ' +
        'tabindex="0" title="click to copy">copy</span>';
    }
    var head = '<div class="vcdh">Biggest priority-fee payers to <b>' +
      otherName + '</b> on ' + day + ' \u2014 day total <b>' +
      (res.total_prio/1e9).toFixed(4) + ' SOL</b> across ' +
      res.n.toLocaleString() + ' paying transactions</div>';
    return head + '<table class="vcfee"><tr><th>signature</th><th>signer</th>' +
      '<th class=n>slot</th><th class=n>priority SOL</th>' +
      '<th class=n>% of day</th><th class=n>CU</th></tr>' +
      res.rows.map(function(t){
        return '<tr><td class="m">' +
          solscan('tx', t.signature, t.signature.slice(0, 12) + '\u2026',
                  'transaction') +
          (t.success ? '' : ' <span class="vcfail">failed</span>') + '</td>' +
          '<td class="m">' +
          solscan('account', t.signer, t.signer.slice(0, 10) + '\u2026',
                  'account') + '</td>' +
          '<td class="n m"><span class="cpv" data-c="' + t.slot +
          '" role="button" tabindex="0" title="click to copy">' + t.slot +
          '</span></td>' +
          '<td class="n m">' + (t.prio/1e9).toFixed(5) + '</td>' +
          '<td class="n m">' + (t.share == null ? '\u2014'
            : (100*t.share).toFixed(3) + '%') + '</td>' +
          '<td class="n m">' + t.cu.toLocaleString() + '</td></tr>';
      }).join('') + '</table>';
  }

  function poll(jid){
    fetch('/api/valcomp/status?job=' + encodeURIComponent(jid))
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.state === 'running'){ say(j.step + '\u2026 (' + j.elapsed + 's)');
          setTimeout(function(){ poll(jid); }, 2000); return; }
        runBtn.disabled = false;
        if (j.state === 'done'){ statEl.hidden = true; draw(j.result); }
        else say(j.error || 'the run failed', 1);
      })
      .catch(function(e){ runBtn.disabled = false; say('lost the job: ' + e, 1); });
  }

  runBtn.addEventListener('click', function(){
    if (!ours.value || !other.value){ say('pick both validators first', 1); return; }
    runBtn.disabled = true; outEl.hidden = true;
    say('starting\u2026');
    fetch('/api/valcomp/run', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ours: ours.value, other: other.value,
                            days: parseInt(daysEl.value, 10) || 10})})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.error){ runBtn.disabled = false; say(j.error, 1); return; }
        poll(j.job);
      })
      .catch(function(e){ runBtn.disabled = false; say('could not start: ' + e, 1); });
  });
})();
'''


def snapshots_body(view_id, self_path):
    """The snapshot index, or one frozen comparison."""
    if view_id:
        try:
            doc = snapshot_read(view_id)
        except (OSError, json.JSONDecodeError, ValueError):
            return ('<div class="vcwrap"><div class="herr">no such snapshot: '
                    f"{html.escape(view_id)[:80]}</div></div>")
        label = (f'{html.escape(doc.get("ours_name") or doc["ours"][:10])}'
                 f' vs {html.escape(doc.get("other_name") or doc["other"][:10])}')
        payload = json.dumps(doc, separators=(",", ":"))
        return (
            '<div class="vcwrap">'
            f'<div class="snaphead"><a class="vclink" href="{html.escape(self_path)}">'
            "&larr; all snapshots</a>"
            f'<h2>{label}</h2>'
            f'<span>{html.escape(doc.get("from") or "?")} &rarr; '
            f'{html.escape(doc.get("to") or "?")} &middot; '
            f'{doc.get("day_count", 0)} days &middot; taken '
            f'{html.escape(doc.get("created_at") or "")} UTC</span></div>'
            '<div id="vcout"></div></div>'
            f"<script>window.__SNAP__={payload};</script>"
            f"<script>{SNAP_PRE_JS}</script>"
            f"<script>{JS}</script>"
            f"<script>{SNAP_POST_JS}</script>")

    snaps = snapshot_list()
    if not snaps:
        return ('<div class="vcwrap"><div class="snapnone">No snapshots yet. '
                "Run a comparison, open the days you care about, then use "
                "<b>snapshot this comparison</b> to freeze it here.</div>"
                "</div>")
    cards = []
    for sn in snaps:
        ours = html.escape(sn.get("ours_name") or (sn.get("ours") or "")[:10])
        other = html.escape(sn.get("other_name") or (sn.get("other") or "")[:10])
        fee = max(sn.get("fee_days") or 0, sn.get("egress_days") or 0)
        cards.append(
            f'<a class="snapcard" href="{html.escape(self_path)}'
            f'?id={urllib.parse.quote(sn["id"])}">'
            f'<div class="snapv">{ours} <span>vs</span> {other}</div>'
            f'<div class="snapr">{html.escape(sn.get("from") or "?")} '
            f'&rarr; {html.escape(sn.get("to") or "?")}</div>'
            f'<div class="snapm">{sn.get("day_count", 0)} days'
            + (f' &middot; {fee} day{"" if fee == 1 else "s"} drilled into'
               if fee else "")
            + f' &middot; {html.escape(sn.get("created_at") or "")} UTC'
            "</div></a>")
    return (f'<div class="vcwrap"><div class="snapgrid">{"".join(cards)}</div>'
            "</div>")


def body(ch):
    try:
        doc = fd_report()
    except Exception as exc:
        return ('<div class="vcwrap"><div class="herr">the Firedancer report '
                f"could not be fetched: {html.escape(str(exc)[:200])}</div>"
                "</div>")
    try:
        ours = our_validators(ch)
    except Exception as exc:
        return ('<div class="vcwrap"><div class="herr">our connector list '
                f"could not be read: {html.escape(str(exc)[:200])}</div>"
                "</div>")
    cands = candidates(doc)
    by_id = {v["id"]: v for v in cands}
    mine, opts = {}, []
    for o in ours:
        v = by_id.get(o["id"]) or {}
        mine[o["id"]] = {"name": v.get("name", ""),
                         "client": v.get("client", ""),
                         "stake": v.get("stake", 0)}
        label = v.get("name") or o["id"][:14] + "\u2026"
        stake = (f' \u00b7 {v["stake"] / 1e9:,.0f} SOL' if v.get("stake")
                 else " \u00b7 not in report")
        opts.append(f'<option value="{html.escape(o["id"])}">'
                    f"{html.escape(label)}{stake}</option>")
    chips = "".join(
        f'<label class="vcc{" on" if c in DEFAULT_CLIENTS else ""}">'
        f'<input type="checkbox" value="{html.escape(c)}"'
        f'{" checked" if c in DEFAULT_CLIENTS else ""}>{html.escape(c)}'
        "</label>" for c in sorted({v["client"] for v in cands}))
    payload = json.dumps({"ours": mine, "cands": cands},
                         separators=(",", ":"))
    return (
        '<div class="vcwrap"><div class="vcform">'
        '<div class="vcf"><label for="vcours">our validator</label>'
        f'<select id="vcours">{"".join(opts)}</select></div>'
        '<div class="vcf"><label for="vcother">compare with</label>'
        '<select id="vcother"></select></div>'
        '<div class="vcf"><label>client</label>'
        f'<div class="vcclients">{chips}</div></div>'
        '<div class="vcf"><label for="vclo">stake band '
        '<span class="vcu">SOL, relative to ours</span></label>'
        '<div class="rng">'
        '<div class="rng-mid" id="vcread"></div>'
        '<div class="rng-track"><div class="rng-zero"></div>'
        '<div class="rng-fill" id="vcfill"></div></div>'
        f'<input id="vclo" type="range" min="-{STAKE_SPAN}" '
        f'max="{STAKE_SPAN}" step="{STAKE_STEP}" value="-{STAKE_SPAN}" '
        'aria-label="lower bound, SOL relative to our stake">'
        f'<input id="vchi" type="range" min="-{STAKE_SPAN}" '
        f'max="{STAKE_SPAN}" step="{STAKE_STEP}" value="{STAKE_SPAN}" '
        'aria-label="upper bound, SOL relative to our stake">'
        f'<div class="rng-ends"><span>-{STAKE_SPAN // 1000}K</span>'
        '<span>0</span>'
        f'<span>+{STAKE_SPAN // 1000}K</span></div>'
        "</div></div>"
        '<div class="vcf"><label for="vcdays">days</label>'
        f'<input id="vcdays" type="number" min="1" max="{DAYS_MAX}" '
        f'step="1" value="{DAYS_DEFAULT}" style="width:74px"></div>'
        '<div class="vcf"><label>&nbsp;</label>'
        '<button id="vcrun" type="button" class="vcgo">compare</button></div>'
        "</div>"
        '<div id="vcstat" class="vcstat" hidden></div>'
        '<div id="vcout" hidden></div>'
        "</div>"
        f"<script>window.__VC__={payload};</script><script>{JS}</script>")
