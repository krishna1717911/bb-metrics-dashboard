"""Builder accuracy -- who won each round, and did its contents land.

Pick a slot, a time range, or the last N hours. The page lists every round
the relay chose in that range with its winner; expanding a slot fetches that
slot's winning miniblocks and the block itself, and marks each order landed
or not.

WHY IT IS SPLIT THAT WAY. Doing the landing check for a whole range eagerly
means pulling every order of every winner: measured, 300 slots is 328,479
rows out of relay.mini_block_transactions. The summary needs none of that --
relay.mini_block_events_v2 already carries the winner and the order count --
so the expensive half runs per slot, on demand, and is ~900 rows and one RPC
call.

TWO THINGS THE RELAY'S v2 SCHEMA FORCES.

  `round_chosen` carries ONLY mini_block_uuid. No slot, no index, no
  builder, no connector -- all blank on 100% of rows. The winner is
  recovered by joining that uuid back to the `submitted` row, which has all
  of them. There is no other route; this is not an optimisation.

  `mini_block_transactions` holds every OFFER, not just winners. Filtering
  it by slot rather than by winning uuid pulls ~450 miniblocks per slot
  instead of ~3 -- 2.1M rows for 20 slots, against 23,736 filtered properly.

Query helpers are injected by app.py so this module holds no credentials.
"""

import base64
import collections
import concurrent.futures as cf
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
import txprice


def _csv_cell(v):
    v = "" if v is None else str(v)
    return '"' + v.replace('"', '""') + '"' if any(
        c in v for c in ',"\n') else v


def copy_csv(slot, rnd, misses, label):
    """Copy EVERY unlanded order for this round as CSV.

    Not just the rows on screen: the table stops at SIG_CAP because a
    thousand rows of signatures is unreadable, but the copy is for pasting
    into a query, where the cap would silently lose rows.

    Carries more than the table shows -- the member index, whether we held
    the order, and the two verdicts (did it land anywhere else, what did
    the validator say) -- because the point of the copy is to be joined
    against, and the table has no room for columns nobody scans visually.

    Newlines go into the attribute as &#10; -- a bare newline in an HTML
    attribute is legal but gets normalised by some parsers, and a CSV that
    arrives as one long line is useless."""
    lines = ["slot,round,type,signature,sigprefix,bundle_id,bundle_tx_index,"
             "held,landed_slot,landed_failed,validator_reason,"
             "validator_instances"]
    for i in misses:
        lslot, lerr = _chain_csv(i)
        vr = i.get("validator") or []
        lines.append(",".join(_csv_cell(x) for x in (
            slot, rnd, i.get("type") or "", i.get("sig") or "",
            i.get("prefix") or "", i.get("bundle_id") or "",
            "" if i.get("bundle_tx_index") is None else i["bundle_tx_index"],
            "yes" if i.get("ours") else "no", lslot, lerr,
            " ".join(sorted({x["reason"] for x in vr})),
            " ".join(sorted({x["instance"] for x in vr})))))
    payload = html.escape("\n".join(lines), quote=True).replace("\n", "&#10;")
    return (f'<span class="cpv cpcsv" data-c="{payload}" role="button" '
            f'tabindex="0" title="copy all {len(misses)} rows as CSV">'
            f"{label}</span>")


def copy_val(value, shown=None):
    """The value itself as the copy control -- see app.py's copy_val. The
    `.cpv` stylesheet and click handler are global (shell ships COPY_JS on
    every page), so matching the markup is all that is needed.

    `shown` displays something shorter while copying the whole value: a
    bundle id is 64 hex characters and only the head is worth the column
    width, but a truncated id pasted into a query matches nothing."""
    v = html.escape(str(value), quote=True)
    label = html.escape(str(shown), quote=True) if shown is not None else v
    return (f'<span class="cpv" data-c="{v}" role="button" tabindex="0" '
            f'title="click to copy {v}">{label}</span>')


# ============================================================ CONFIG
# Newest-first cap on how many slots a range expands to. The summary is
# cheap, so this is about page size rather than query cost.
MAX_SLOTS = 300
DEFAULT_HOURS = 1
# The `submitted` row precedes its `round_chosen` by up to a round, so the
# half of the join that supplies slot/builder looks back slightly further.
JOIN_LOOKBACK_MIN = 10
# ======================================================== end of config

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_IDX = {c: i for i, c in enumerate(B58)}


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + B58_IDX[ch]
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + raw


def b58encode(raw):
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58[r] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + out


def sigprefix(sig):
    """Full signature -> the 16-byte SigPrefix our own tables store.

    Exact and O(1) in this direction. The reverse is not a truncation, which
    is why everything here keys on full signatures and derives the prefix,
    never the other way round."""
    try:
        return b58encode(b58decode(sig)[:16])
    except Exception:
        return ""


def _q(v):
    return str(v).replace("\\", "").replace("'", "")


# ------------------------------------------------------------- the range

SLOT_RE = re.compile(r"^\d{6,}$")
# A connector identity is the validator's pubkey, base58. Anything else in
# the query string is dropped rather than quoted into SQL.
PUBKEY_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def parse_range(qs):
    """-> dict(mode, slot, lo, hi, label, err). One of three shapes."""
    one = lambda k: (qs.get(k, [""])[0] or "").strip()
    mode = one("mode") or ("slot" if one("slot") else
                           "range" if one("from") else "hours")
    now = dt.datetime.now(dt.UTC)
    out = {"mode": mode, "slot": None, "lo": None, "hi": None, "err": "",
           "hours": one("hours") or str(DEFAULT_HOURS),
           "from": one("from"), "to": one("to"), "slot_in": one("slot"),
           "metric": one("metric") if one("metric") in METRICS else LANDING,
           "region": (one("region") if one("region") in REGIONS
                      else DEFAULT_REGION),
           # one validator (connector identity) within the region, or ""
           "connector": (one("connector") if PUBKEY_RE.match(one("connector"))
                         else "")}
    if mode == "slot":
        s = one("slot")
        if not SLOT_RE.match(s):
            out["err"] = "a slot is a number, e.g. 456429791"
            return out
        out["slot"] = int(s)
        out["label"] = f"slot {out['slot']}"
        return out
    if mode == "range":
        try:
            lo = dt.datetime.fromisoformat(one("from")).replace(tzinfo=dt.UTC)
            hi = (dt.datetime.fromisoformat(one("to")).replace(tzinfo=dt.UTC)
                  if one("to") else now)
        except ValueError:
            out["err"] = "use YYYY-MM-DD HH:MM (UTC)"
            return out
        if hi <= lo:
            out["err"] = "the end of the range is before its start"
            return out
        out["lo"], out["hi"] = lo, hi
        out["label"] = (f"{lo:%Y-%m-%d %H:%M} to {hi:%Y-%m-%d %H:%M} UTC")
        return out
    try:
        h = float(one("hours") or DEFAULT_HOURS)
    except ValueError:
        h = DEFAULT_HOURS
    h = max(0.05, min(h, 168.0))
    out["hours"] = f"{h:g}"
    out["lo"], out["hi"] = now - dt.timedelta(hours=h), now
    out["label"] = f"last {h:g}h"
    return out


def slot_window(relay_q, slot):
    """When a slot's rows were written, so the join can be bounded by time.

    `round_chosen` has no slot column -- it carries only the uuid -- so a
    slot cannot be filtered on that side of the join. Left unbounded it
    scans every chosen round in retention and the Grafana proxy answers
    400. Resolving the window from the `submitted` rows first (cheap: slot
    is selective, 0.6s) turns it into a two-second scan."""
    _, rows = relay_q(
        "SELECT toString(min(timestamp)) AS lo, toString(max(timestamp)) AS hi "
        "FROM relay.mini_block_events_v2 "
        f"WHERE timestamp > now() - INTERVAL 14 DAY AND slot = {int(slot)}")
    if not rows or not rows[0][0] or rows[0][0].startswith("1970"):
        return None, None
    pad = dt.timedelta(minutes=2)
    lo = dt.datetime.fromisoformat(rows[0][0][:26]) - pad
    hi = dt.datetime.fromisoformat(rows[0][1][:26]) + pad
    return lo, hi


def _bounds(r):
    """SQL predicates for the chosen half and the submitted half."""
    f = "%Y-%m-%d %H:%M:%S"
    if r["slot"] is not None:
        lo, hi = r["win_lo"], r["win_hi"]
        if lo is None:
            return ("1 = 0", "1 = 0")
        return (f"c.timestamp >= '{lo:{f}}' AND c.timestamp <= '{hi:{f}}'",
                f"timestamp >= '{lo:{f}}' AND timestamp <= '{hi:{f}}' "
                f"AND slot = {r['slot']}")
    lo, hi = r["lo"], r["hi"]
    jl = lo - dt.timedelta(minutes=JOIN_LOOKBACK_MIN)
    return (f"c.timestamp >= '{lo:{f}}' AND c.timestamp <= '{hi:{f}}'",
            f"timestamp >= '{jl:{f}}' AND timestamp <= '{hi:{f}}'")


# ------------------------------------------------------- the round table

def rounds(relay_q, r, cap=MAX_SLOTS, region="all", connector=""):
    """Every round the relay chose in the range, newest slot first.

    -> (rows, slots_shown, slots_total)"""
    if r["slot"] is not None and "win_lo" not in r:
        r["win_lo"], r["win_hi"] = slot_window(relay_q, r["slot"])
    chosen_w, sub_w = _bounds(r)
    # Every aggregate is aliased to a w_ name. `any(slot) AS slot` shadows
    # the raw column in this subquery's own WHERE, and ClickHouse rejects
    # the whole statement with ILLEGAL_AGGREGATION -- which the Grafana
    # proxy then reports only as a bare HTTP 400.
    sub = ("SELECT mini_block_uuid, any(slot) AS w_slot, "
           "  any(index_in_slot) AS w_idx, any(builder_id) AS w_builder, "
           "  any(connector_identity) AS w_conn, "
           "  toString(min(timestamp)) AS w_sub, "
           "  any(order_count) AS w_orders, any(builder_payment) AS w_pay, "
           "  any(jito_tips) AS w_jito, any(protocol_fees) AS w_fees, "
           "  any(is_last) AS w_last "
           "FROM relay.mini_block_events_v2 "
           f"WHERE {sub_w} AND event = 'submitted'{_region_sql(region)}"
           f"{_connector_sql(connector)} "
           "GROUP BY mini_block_uuid")
    join = (f"FROM relay.mini_block_events_v2 AS c INNER JOIN ({sub}) AS s "
            "  ON c.mini_block_uuid = s.mini_block_uuid "
            f"WHERE {chosen_w} AND c.event = 'round_chosen'")
    _, tot = relay_q(f"SELECT uniqExact(s.w_slot) AS n {join}")
    total = int(tot[0][0]) if tot else 0
    cap_sql = ""
    if r["slot"] is None:
        cap_sql = (" AND s.w_slot IN (SELECT DISTINCT s.w_slot " + join +
                   f" ORDER BY s.w_slot DESC LIMIT {int(cap)})")
    _, rows = relay_q(
        "SELECT s.w_slot AS slot, s.w_idx AS idx, s.w_builder AS winner, "
        "  s.w_conn AS connector, toString(c.mini_block_uuid) AS uuid, "
        "  s.w_orders AS order_count, s.w_pay AS payment, "
        "  s.w_jito AS jito, s.w_fees AS fees, s.w_last AS is_last, "
        "  s.w_sub AS sub_at, toString(c.timestamp) AS at "
        + join + cap_sql + " ORDER BY slot DESC, idx ASC")
    out = []
    for x in rows:
        out.append({"slot": int(x[0] or 0), "idx": int(x[1] or 0),
                    "winner": x[2] or "", "connector": x[3] or "",
                    "uuid": x[4] or "", "order_count": int(x[5] or 0),
                    "payment": int(x[6] or 0), "jito": int(x[7] or 0),
                    "fees": int(x[8] or 0),
                    "is_last": str(x[9]).lower() in ("1", "true"),
                    # when the winning OFFER was submitted, which is the
                    # instant the round was decided -- not when
                    # round_chosen fired, which is ~11 ms later
                    "sub_at": x[10] or "", "at": x[11] or ""})
    return out, len({o["slot"] for o in out}), total


# --------------------------------------------- one slot, with the chain

def winner_orders(relay_q, uuids, window=None):
    """The winning miniblocks' contents, by uuid.

    Filtered on the winning uuids, never on the slot -- see the module
    docstring. order_index is the winner's execution order.

    `window` narrows `ingested_at` to (lo, hi). The uuid is not the sort
    key, so without it every call rescans 14 days: measured 5.99s against
    0.85s for the same 1,425 rows, row-for-row identical. The caller knows
    when its rounds happened, so it can always say.
    """
    if not uuids:
        return {}
    lst = ", ".join(f"toUUID('{_q(u)}')" for u in uuids)
    if window and window[0] and window[1]:
        f = "%Y-%m-%d %H:%M:%S"
        bound = (f"ingested_at >= '{window[0]:{f}}' "
                 f"AND ingested_at <= '{window[1]:{f}}'")
    else:
        bound = "ingested_at > now() - INTERVAL 14 DAY"
    _, rows = relay_q(
        "SELECT toString(mini_block_uuid) AS uuid, order_index, order_type, "
        "  base58Encode(signature) AS sig, "
        # the same 16-byte head sigprefix() computes, but for free and on
        # the server -- checked identical over 200 rows. Everything
        # downstream keys on the prefix so the fallback can join to it.
        "  base58Encode(substring(signature, 1, 16)) AS pfx, "
        "  bundle_id, bundle_tx_index "
        f"FROM relay.mini_block_transactions_v2 WHERE {bound} "
        f"  AND mini_block_uuid IN ({lst}) "
        "ORDER BY uuid, order_index, bundle_tx_index")
    out = {}
    for u, oi, ot, sig, pfx, bid, bti in rows:
        out.setdefault(u, []).append({
            "order_index": int(oi or 0), "type": ot or "", "sig": sig or "",
            "prefix": pfx or "", "bundle_id": bid or "",
            "bundle_tx_index": None if bti in (None, "") else int(bti)})
    return out


# -------------------------------------------------- what a landed order paid
#
# Two different things, kept apart because they are paid to different people:
#
#   fee  -- base + priority, the whole `fee` the transaction paid. Solana
#           stores no priority component, so there is nothing to derive: the
#           total IS what the payer spent. It goes to the leader.
#   tip  -- lamports moved into a known builder/Jito tip account inside the
#           same transaction, read off preBalances/postBalances. This is how
#           a bundle actually pays, and it is NOT in `fee`.
#
# Only the Jito and builder tip accounts count. The GRAVITY keys in
# gbx_tip_accounts.txt are deliberately excluded: their own source comments
# call them "astralane searchers" / "titan searchers", i.e. searcher revenue
# rather than anything the block paid out, and counting them would inflate
# every number here.
JITO_TIP_ACCOUNTS = ['96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5', 'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe', 'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY', 'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49', 'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh', 'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt', 'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL', '3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT']
TIP_ACCOUNTS = frozenset(JITO_TIP_ACCOUNTS
                         + gbxdune.TITAN_TIP_ACCOUNTS
                         + gbxdune.BIFROST_TIP_ACCOUNTS)


def block_rewards(rpc_url, slot):
    """-> {signature: {"fee": int, "tip": int, "err": bool}} for one block.

    transactionDetails=accounts carries fee, err and the balance arrays
    without the instruction payloads -- 5.5 MB against 0.14 MB for
    signatures alone, and 0.84 s against 0.36 s. Worth it for one expanded
    slot; NOT worth it per slot across a range scan, which is why the
    exports stay on the signature fetch.

    maxSupportedTransactionVersion must be 1. At 0 the node refuses the
    whole block with -32015 the moment one v1 transaction is in it, which
    is most blocks.
    """
    body = {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [
        int(slot), {"encoding": "json", "transactionDetails": "accounts",
                    "rewards": False, "maxSupportedTransactionVersion": 1}]}
    req = urllib.request.Request(
        rpc_url, headers={"Content-Type": "application/json"},
        data=json.dumps(body).encode())
    with urllib.request.urlopen(req, timeout=90) as resp:
        d = json.loads(resp.read())
    if d.get("error"):
        if d["error"].get("code") in (-32009, -32007):
            return {}                      # skipped slot: nothing landed
        raise RuntimeError(str(d["error"].get("message", ""))[:160])
    out = {}
    for tx in (d.get("result") or {}).get("transactions") or []:
        meta, inner = tx.get("meta") or {}, tx.get("transaction") or {}
        keys = inner.get("accountKeys") or []
        pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
        tip = 0
        for i, k in enumerate(keys):
            pk = k.get("pubkey") if isinstance(k, dict) else k
            if pk in TIP_ACCOUNTS and i < len(pre) and i < len(post):
                # credits only -- a tip account that also pays a fee would
                # otherwise net its own cost out of someone else's tip
                tip += max(0, int(post[i]) - int(pre[i]))
        rec = {"fee": int(meta.get("fee") or 0), "tip": tip,
               "err": meta.get("err") is not None}
        for sig in inner.get("signatures") or []:
            out[sig] = rec
    return out


def block_prefixes(sigs):
    """{SigPrefix: full signature} for a block.

    Forward derivation only -- exact and O(1) per signature, unlike going
    the other way, which is a bounding prefix search. This is what makes
    the fallback work: our own store names the winner's orders by 16-byte
    SigPrefix and ships bytes for almost none of them (5 of 259 on a
    measured round), but every signature that LANDED can be reduced to its
    prefix here and matched. 1,420 signatures gave 1,420 distinct
    prefixes, no collisions."""
    return {sigprefix(x): x for x in (sigs or ())}


def bundle_members(ch, bundle_ids, lo=None, hi=None):
    """bundle_id -> its member signatures, from our own ingest.

    `scheduler/received` with entity_kind='bundle' carries the members in
    the `signatures` array and keys them on `entity`, which is the bare
    hex bundle id -- the same value order_bundle_ids holds, no translation.
    Every builder that saw the bundle logs its own row, so the arrays are
    unioned across instances.

    This is what makes a bundle ref decidable. Without it a bundle is a
    32-byte id with no member signatures and its landing cannot be tested
    at all. Measured on slot 457305047: 49 of 59 refs resolved (83%), and
    every one of those had ALL members in the block -- no partials, which
    is what atomicity predicts.

    Bound tightly: the receive lands within a second of the slot (min -1s,
    max 0s over those 49), so the round window is already generous.
    """
    out = {}
    ids = sorted({b for b in bundle_ids if b})
    if not ids:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo:{f}}' AND ts <= '{hi:{f}}' AND " if lo and hi
          else "ts > now() - INTERVAL 2 HOUR AND ")
    for i in range(0, len(ids), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(b) for b in ids[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT entity, arrayStringConcat("
                "  groupUniqArrayArray(signatures), ' ') AS sg "
                f"FROM bifrost_events WHERE {tb}"
                "  stage = 'scheduler' AND event = 'received' "
                f"  AND entity_kind = 'bundle' AND entity IN ({lst}) "
                "GROUP BY entity")
        except Exception:
            continue
        for ent, sg in rows:
            mem = [x for x in (sg or "").split() if x]
            if mem:
                out[ent] = mem
    return out


def ours_orders(ch, uuids, pmap=None, window=None):
    """Winner/offer contents from OUR store, keyed by the relay's uuid.

    The fallback for relay.mini_block_transactions. bifrost_miniblocks.uuid
    is the relay's mini_block_uuid, so the same uuids join straight across.

    What it cannot give, and why:
      * `order_tx_prefixes` is 16-byte SigPrefixes, not signatures. A full
        signature comes back only when the order LANDED (recovered from
        `pmap`) or when we happened to hold its bytes.
      * `order_bundle_ids` is a bare 32-byte id with no members attached,
        so a bundle ref cannot be tested against the block at all. Those
        orders get landed = None, not False -- undecidable is not a miss.
      * the decoded columns split tx refs and bundle refs into two arrays,
        which loses the interleave, so order_index is the position within
        its own array, not the winner's true execution order.
    """
    out = {}
    uuids = [u for u in uuids if u]
    if not uuids:
        return out
    pmap = pmap or {}
    for i in range(0, len(uuids), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(u) for u in uuids[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT toString(uuid) AS u, "
                "  arrayStringConcat(order_tx_prefixes, ' ') AS tx, "
                "  arrayStringConcat(order_bundle_ids, ' ') AS bids, "
                "  arrayStringConcat(sig_prefixes, ' ') AS sp, "
                "  arrayStringConcat(signatures, ' ') AS sg "
                "FROM bifrost_miniblocks "
                f"WHERE toString(uuid) IN ({lst}) "
                "  AND ts > now() - INTERVAL 14 DAY")
        except Exception:
            continue
        for u, tx, bids, sp, sg in rows:
            if u in out:
                continue
            # the orders we DO hold bytes for, prefix -> signature
            held = dict(zip((sp or "").split(), (sg or "").split()))
            items = []
            for n, pfx in enumerate((tx or "").split()):
                items.append({
                    "order_index": n, "type": "transaction", "prefix": pfx,
                    "sig": pmap.get(pfx) or held.get(pfx, ""),
                    "bundle_id": "", "bundle_tx_index": None})
            base = len(items)
            for n, bid in enumerate((bids or "").split()):
                items.append({
                    "order_index": base + n, "type": "bundle", "prefix": "",
                    "sig": "", "bundle_id": bid, "bundle_tx_index": None})
            out[u] = items
    return _expand_bundles(ch, out, window)


def _expand_bundles(ch, by_uuid, window=None):
    """Turn each resolvable bundle ref into one row PER MEMBER.

    That is the shape relay.mini_block_transactions produces, so the two
    sources become directly comparable: same granularity, same key, same
    columns. A ref we cannot resolve stays a single row with no signature
    and is still reported as undecidable rather than as a miss."""
    want = {i["bundle_id"] for items in by_uuid.values() for i in items
            if i["type"] == "bundle" and i["bundle_id"]}
    if not want:
        return by_uuid
    lo, hi = (window or (None, None))
    mem = bundle_members(ch, want, lo, hi)
    if not mem:
        return by_uuid
    for u, items in by_uuid.items():
        grown = []
        for i in items:
            sigs = mem.get(i["bundle_id"]) if i["type"] == "bundle" else None
            if not sigs:
                grown.append(i)
                continue
            for k, sg in enumerate(sigs):
                grown.append(dict(i, sig=sg, prefix=sigprefix(sg),
                                  bundle_tx_index=k))
        by_uuid[u] = grown
    return by_uuid


def order_contents(relay_q, ch, uuids, window, pmap=None):
    """-> (contents, source). The relay if it has them, our store if not.

    Automatic rather than a switch: relay.mini_block_transactions stopped
    taking rows at 2026-10-15 10:47:46 UTC with no warning and the other
    three relay tables carried on, so "is it up" is a per-query fact, not
    a setting anyone can be expected to keep current."""
    uuids = [u for u in uuids if u]
    if not uuids:
        return {}, RELAY_SRC
    try:
        got = winner_orders(relay_q, uuids, window)
    except Exception:
        got = {}
    if got:
        return got, RELAY_SRC
    return ours_orders(ch, uuids, pmap, window), OURS_SRC


def block_signatures(rpc_url, slot):
    """-> (set of signatures, blockTime) or (None, None) if there is no block.

    transactionDetails=signatures keeps this at ~0.09 MB and ~0.3s; the full
    block is ~8 MB and carries nothing else this needs."""
    if not rpc_url:
        raise RuntimeError("SOLANA_RPC is not set")
    body = {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [
        int(slot), {"encoding": "json", "transactionDetails": "signatures",
                    "rewards": False, "maxSupportedTransactionVersion": 0}]}
    req = urllib.request.Request(
        rpc_url, headers={"Content-Type": "application/json"},
        data=json.dumps(body).encode())
    with urllib.request.urlopen(req, timeout=45) as resp:
        d = json.loads(resp.read())
    if d.get("error"):
        code = d["error"].get("code")
        # -32009 / -32007: skipped slot, or no block produced. That is an
        # answer, not a failure: nothing in it landed.
        if code in (-32009, -32007):
            return None, None
        raise RuntimeError(str(d["error"].get("message", ""))[:160])
    res = d.get("result") or {}
    return set(res.get("signatures") or []), res.get("blockTime")


def our_prefixes(ch, slot):
    """SigPrefixes our own builder holds bytes for in this slot.

    Only a marker on the detail table -- it says "we had this order", which
    is a different claim from "we offered it" and from "it landed"."""
    try:
        _, rows = ch(
            "SELECT arrayJoin(arrayConcat(sig_prefixes, order_tx_prefixes)) AS p "
            f"FROM bifrost_miniblocks WHERE slot = {int(slot)} "
            "  AND ts > now() - INTERVAL 14 DAY GROUP BY p")
        return {r[0] for r in rows if r and r[0]}
    except Exception:
        return set()


# getSignatureStatuses takes at most 256 signatures per call, and both
# lookups below are bounded so a skipped slot -- where EVERY order of every
# winner is a miss -- cannot turn one expansion into an unbounded scan.
STATUS_BATCH = 256
ENTITY_BATCH = 400
LOOKUP_CAP = 2000
# None already means "nowhere on chain", so "we could not find out"
# needs a value of its own.
UNKNOWN = "?"


def landed_elsewhere(rpc_url, sigs):
    """Did each signature land in SOME block, and which one.

    `searchTransactionHistory` makes this search the whole ledger rather
    than the recent status cache, so a null answer means the signature is
    nowhere on chain -- not merely outside the last ~150 slots. Checked
    against a known-landed signature, which comes back with its own slot.

    -> {sig: {"slot": int, "err": bool}} for one that landed, {sig: None}
    for one that is nowhere. A signature ABSENT from the dict is one the
    lookup could not answer for, which the caller renders as unknown
    rather than as a finding.
    """
    out = {}
    sigs = [x for x in sigs if x][:LOOKUP_CAP]
    if not (rpc_url and sigs):
        return out
    for i in range(0, len(sigs), STATUS_BATCH):
        chunk = sigs[i:i + STATUS_BATCH]
        body = {"jsonrpc": "2.0", "id": 1, "method": "getSignatureStatuses",
                "params": [chunk, {"searchTransactionHistory": True}]}
        req = urllib.request.Request(
            rpc_url, headers={"Content-Type": "application/json"},
            data=json.dumps(body).encode())
        with urllib.request.urlopen(req, timeout=60) as resp:
            d = json.loads(resp.read())
        if d.get("error"):
            raise RuntimeError(str(d["error"].get("message", ""))[:160])
        for sig, v in zip(chunk, (d.get("result") or {}).get("value") or []):
            out[sig] = (None if v is None else
                        {"slot": int(v.get("slot") or 0),
                         "err": v.get("err") is not None})
    return out


def validator_results(ch, entities, lo, hi):
    """What the validator said about each order, from bifrost_events.

    stage=validator event=executed is the per-order execution result the
    builder logs on the way back from the connector. `entity` is the
    22-char SigPrefix for a transaction and the bare hex bundle id for a
    bundle -- the same values mini_block_transactions carries, so nothing
    needs translating. A bundle's reason arrives wrapped as `Some(REASON)`
    and a transaction's bare; they are unwrapped to the same vocabulary.

    Every builder in the fleet logs its own row, so one order usually has
    several. They are kept apart rather than deduplicated: the instances
    are the evidence that a reason is fleet-wide and not one node's.

    Returns {} rather than raising -- this is an annotation on a table
    that is already useful without it.

    -> {entity: [{"instance", "reason", "slot", "cu"}]}
    """
    out = {}
    ents = [e for e in entities if e][:LOOKUP_CAP]
    if not (ents and lo and hi):
        return out
    f = "%Y-%m-%d %H:%M:%S"
    # The transport sends SQL in the URL, so the IN list is chunked rather
    # than built whole: a skipped slot makes EVERY order a miss, and a
    # thousand 64-char bundle ids in one query is a ~250 KB URL.
    for i in range(0, len(ents), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(e) for e in ents[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT entity, instance_id, reason, nums['slot'] AS vslot, "
                "  nums['cu'] AS vcu FROM bifrost_events "
                f"WHERE ts >= '{lo:{f}}' AND ts <= '{hi:{f}}' "
                "  AND stage = 'validator' AND event = 'executed' "
                f"  AND entity IN ({lst}) ORDER BY entity, instance_id")
        except Exception:
            continue
        for ent, inst, reason, vslot, vcu in rows:
            r = (reason or "").strip()
            # a bundle reports Some(REASON), a transaction reports it bare
            if r.startswith("Some(") and r.endswith(")"):
                r = r[5:-1]
            out.setdefault(ent, []).append(
                {"instance": inst or "", "reason": r or "?",
                 "slot": int(vslot or 0), "cu": int(vcu or 0)})
    return out


def _vkey(it):
    """How an order is keyed in bifrost_events.

    A bundle is ONE order to the validator however many members it has, so
    every member of it carries the bundle's verdict, not its own."""
    return it["bundle_id"] or it["prefix"]


def _event_window(rows):
    """A ts window around this slot's chosen rounds, to bound the scan.

    Padded generously on purpose: the relay's clock runs seconds ahead of
    the builders' (measured at +3.44 s), and this is a scan bound, not a
    measurement, so being loose costs nothing and being tight loses rows.
    """
    ats = [r["at"] for r in rows if r.get("at")]
    if not ats:
        return None, None
    try:
        stamps = [dt.datetime.fromisoformat(a[:26]) for a in ats]
    except ValueError:
        return None, None
    return (min(stamps) - dt.timedelta(minutes=2),
            max(stamps) + dt.timedelta(minutes=5))


def our_prefixes_many(ch, slots):
    """our_prefixes for a batch of slots -> {slot: {prefix, ...}}.

    Only ever called for the slots that actually HAVE a miss, which is a
    small minority -- asking it for every slot in a range would pull every
    prefix of every miniblock we built, hundreds of thousands of rows to
    answer a question about a handful."""
    out = {}
    slots = sorted({int(x) for x in slots})
    if not slots:
        return out
    for i in range(0, len(slots), ENTITY_BATCH):
        lst = ", ".join(str(x) for x in slots[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT slot, arrayJoin(arrayConcat(sig_prefixes, "
                "  order_tx_prefixes)) AS p FROM bifrost_miniblocks "
                f"WHERE slot IN ({lst}) AND ts > now() - INTERVAL 14 DAY "
                "GROUP BY slot, p")
        except Exception:
            continue
        for slot, pfx in rows:
            if pfx:
                out.setdefault(int(slot), set()).add(pfx)
    return out


def slot_detail(relay_q, ch, rpc_url, slot, rows, blk_sigs=None):
    """Per-round order lists for one slot, each order marked landed or not."""
    uuids = [r["uuid"] for r in rows if r["uuid"]]
    ev_lo, ev_hi = _event_window(rows)
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        # the caller may already hold the block (the selection half fetches
        # a richer form of it), so do not pay for it twice
        f_blk = (None if blk_sigs is not None
                 else pool.submit(block_signatures, rpc_url, slot))
        f_our = pool.submit(our_prefixes, ch, slot)
        sigs, btime, blk_err = blk_sigs, None, ""
        if f_blk is not None:
            try:
                sigs, btime = f_blk.result()
            except Exception as exc:
                sigs, btime, blk_err = None, None, str(exc)[:160]
        ours = f_our.result()
    # the block's prefix map has to exist before the contents are fetched:
    # the fallback uses it to put a full signature back on an order our
    # store names only by prefix
    pmap = block_prefixes(sigs)
    orders, source = order_contents(relay_q, ch, uuids, (ev_lo, ev_hi), pmap)
    out = []
    for r in rows:
        items = orders.get(r["uuid"], [])
        for it in items:
            if not it.get("prefix") and it["sig"]:
                it["prefix"] = sigprefix(it["sig"])
            # Landing is tested on the PREFIX, not the signature, so the
            # two sources answer the same question: our store never names
            # the winner's orders by signature.
            if blk_err:
                it["landed"] = None          # the fetch failed, not an answer
            elif not it["prefix"]:
                # a bundle ref carries no member signatures -- undecidable,
                # and calling it a miss would invent one
                it["landed"] = None if source == OURS_SRC else (
                    False if sigs is None else it["sig"] in sigs)
            elif sigs is None:
                it["landed"] = False         # no block: nothing landed
            else:
                it["landed"] = it["prefix"] in pmap
            it["ours"] = it["prefix"] in ours if ours else False
        out.append({"round": r, "orders": items})

    # Both annotations are about the MISSES only -- a landed order needs
    # neither -- so they run once the landed flag is known, not before.
    miss = [it for blk in out for it in blk["orders"]
            if it["landed"] is False]
    chain_err = ""
    if miss:
        lo, hi = ev_lo, ev_hi
        with cf.ThreadPoolExecutor(max_workers=2) as pool:
            f_st = pool.submit(landed_elsewhere, rpc_url,
                               sorted({it["sig"] for it in miss}))
            f_vr = pool.submit(validator_results, ch,
                               sorted({_vkey(it) for it in miss}), lo, hi)
            try:
                status = f_st.result()
            except Exception as exc:
                status, chain_err = {}, str(exc)[:160]
            vres = f_vr.result()
        for it in miss:
            # Three states, and they are not the same: a dict is "landed
            # there", None is "nowhere on chain", UNKNOWN is "we could not
            # find out". Collapsing the last two would invent a finding.
            it["elsewhere"] = (status.get(it["sig"], UNKNOWN)
                               if it["sig"] else UNKNOWN)
            it["validator"] = vres.get(_vkey(it), [])
        for blk in out:
            for it in blk["orders"]:
                it["round_idx"] = blk["round"]["idx"]
        attach_recv(ch, miss, slot, lo, hi, round_of=lambda x: x["round_idx"],
                    prefix_of=lambda x: x.get("prefix"))
    # handed on to the selection diff so it does not re-fetch the block
    return {"slot": slot, "rounds": out, "block_sigs": sigs,
            "source": source, "block": sigs is not None,
            "blockTime": btime,
            "block_n": len(sigs) if sigs else 0, "err": blk_err,
            "chain_err": chain_err,
            "skipped": sigs is None and not blk_err}


# ---------------------------------------------- the selection diff metric
#
# Why did one builder win the round and not the other. The landing metric
# above asks "did the winner's content reach the chain"; this asks the
# prior question -- what was in each side's offer that the other's was not.
#
# THE BID IS NOT `builder_payment`. That column is 0 for most of the extend
# ladder and then jumps to the SAME value for every builder in the round
# (10,320 for all four on 457294795 r0), so it is a round-level figure, not
# a per-builder bid, and argMax over it picks an arbitrary rung. What the
# relay actually grades on is the total: payment + jito_tips +
# protocol_fees. Measured on that round, TITAN_FRA_1 bid 2,048,919 against
# ASTRALANE_FRA_1's 1,774,081 and was graded `best` for nearly the whole
# ladder. So each builder's "offer" here is its highest-bidding rung.

OURS_PREFIX = "ASTRALANE"
TITAN_PREFIX = "TITAN"
# The mocks bid on every round and can never be chosen (BuilderConfig.mock
# -> builder_not_eligible), so including them doubles the row count and
# adds no selection signal.
MOCK_MARK = "MOCK"
BID = "builder_payment + jito_tips + protocol_fees"
# A rejected rung never competed and the relay keeps no transaction rows for
# it, so picking one as "our offer" silently diffs the winner against an
# EMPTY set -- which reads as "Titan included 240 things we didn't" when the
# truth is "our best-paying rung arrived after the round closed". Only these
# three reasons mean the offer was actually graded.
ACCEPTED = "('best', 'not_best', 'tail')"

SEL_COLS = ["fee", "tip", "reward",
            "slot", "round", "connector", "won_by", "we_won",
            "our_builder", "titan_builder", "builder_id", "verdict",
            "type", "signature", "sigprefix", "bundle_id",
            "bundle_tx_index", "order_index", "landed",
            "our_bid", "titan_bid", "bid_gap",
            "our_cost", "titan_cost", "our_orders", "titan_orders",
            "our_reason", "titan_reason", "is_last",
            "sched_event", "sched_reason", "sched_detail"]


def _is_ours(b):
    return b.startswith(OURS_PREFIX) and MOCK_MARK not in b


def _is_titan(b):
    return b.startswith(TITAN_PREFIX)


def best_offers(relay_q, slots, window, chosen_at=None):
    """Each builder's offer AS IT STOOD when the relay chose the round.

    NOT its highest-bidding rung. The auction closes at an instant, and
    what competed is whatever that builder had on the table at that
    instant. Picking by max bid is wrong twice over: the ladder does not
    climb monotonically -- a builder's bid went 244,035 -> 245,103 ->
    244,035 across three consecutive rungs -- so the best rung can be one
    that was already superseded; and a rung that arrived after the close
    never competed at all, whatever it was worth.

    `chosen_at` maps (slot, idx) to the moment the round was decided --
    the WINNING OFFER's submission time, which rounds() carries as
    `sub_at`. Not round_chosen's event timestamp: that fires ~11 ms
    later, and a rung we put in during those 11 ms was graded against an
    offer already on the table and never had a chance at the round.
    Both stamps are relay-side, so no clock correction applies. Without it, the last accepted
    rung is used, which is the same answer whenever the round closed
    after the final submission.

    -> {(slot, idx, builder): {uuid, bid, cost, n, reason, accepted,
                               rungs, at}}
    """
    out = {}
    slots = sorted({int(x) for x in slots})
    if not slots:
        return out
    chosen_at = chosen_at or {}
    f = "%Y-%m-%d %H:%M:%S"
    tb = ""
    if window and window[0] and window[1]:
        tb = (f"timestamp >= '{window[0]:{f}}' "
              f"AND timestamp <= '{window[1]:{f}}' AND ")
    # the rungs themselves, not an aggregate: "the last one before the
    # close" cannot be expressed as argMax over a per-round bound without
    # a three-way join through round_chosen, which carries no slot
    rows = []
    for i in range(0, len(slots), ENTITY_BATCH):
        lst = ", ".join(str(x) for x in slots[i:i + ENTITY_BATCH])
        _, part = relay_q(
            "SELECT slot, index_in_slot AS idx, builder_id, "
            "  toString(timestamp) AS t, "
            "  toString(mini_block_uuid) AS uuid, "
            f"  {BID} AS bid, execution_cost AS cost, "
            "  order_count AS n, reason "
            "FROM relay.mini_block_events_v2 "
            f"WHERE {tb}event = 'submitted' AND slot IN ({lst}) "
            "ORDER BY slot, idx, builder_id, t")
        rows += part

    ladders = {}
    for slot, idx, bld, t, uuid, bid, cost, n, reason in rows:
        ladders.setdefault((int(slot), int(idx), bld), []).append(
            {"t": t or "", "uuid": uuid or "", "bid": int(bid or 0),
             "cost": int(cost or 0), "n": int(n or 0),
             "reason": reason or ""})
    for key, rungs in ladders.items():
        close = chosen_at.get((key[0], key[1]), "")
        ok = [x for x in rungs if x["reason"].strip("()").replace("Some", "")
              in ("best", "not_best", "tail")]
        standing = [x for x in ok if not close or x["t"] <= close]
        pick = standing[-1] if standing else None
        out[key] = {
            "bid": pick["bid"] if pick else 0,
            "uuid": pick["uuid"] if pick else "",
            "cost": pick["cost"] if pick else 0,
            "n": pick["n"] if pick else 0,
            "at": pick["t"] if pick else "",
            "accepted": len(standing), "rungs": len(rungs),
            # with nothing standing, the only honest thing to report is
            # why -- either every rung was refused, or they all landed
            # after the round had already closed
            "reason": (pick["reason"] if pick else
                       ("too_late" if ok else
                        (rungs[-1]["reason"] if rungs else "rejected")))}
    return out


def _pair(offers, slot, idx, won_by, won_uuid):
    """-> (titan_builder, titan_side, [(our_builder, our_side), ...]).

    The side that WON is represented by the miniblock the relay actually
    chose, not by its best rung -- those can differ, and what won is the
    thing worth comparing against."""
    here = [(b, v) for (s, i, b), v in offers.items() if s == slot and i == idx]
    if not here:
        return None, None, []
    titans = sorted([(b, v) for b, v in here
                     if _is_titan(b) and v.get("accepted")],
                    key=lambda bv: -bv[1]["bid"])
    ours = sorted([(b, v) for b, v in here if _is_ours(b)],
                  key=lambda bv: -bv[1]["bid"])
    if not titans or not ours:
        return None, None, []
    tb, tv = titans[0]
    if _is_titan(won_by) and won_uuid:
        tb, tv = won_by, dict(offers.get((slot, idx, won_by), tv),
                              uuid=won_uuid)
    fixed = []
    for b, v in ours:
        if b == won_by and won_uuid:
            v = dict(v, uuid=won_uuid)
        fixed.append((b, v))
    return tb, tv, fixed


# The scheduler events worth attaching to an order. Everything here is a
# REFUSAL of some kind -- the order reached the builder and something
# decided against it -- which is the question the diff raises and cannot
# answer on its own.
SCHED_EVENTS = ("dropped", "check_dropped", "winner_order_refused",
                "pin_skipped", "promote_mismatch", "prefix_diverged",
                "bundle_selection")


def scheduler_notes(ch, entities, lo=None, hi=None):
    """Why the scheduler turned an order away -> {entity: [note, ...]}.

    `entity` is the 22-char SigPrefix for a transaction and the bare hex
    bundle id for a bundle, so both sides of an expanded bundle row can be
    looked up: the member by its prefix, the bundle by its id.

    attrs and nums come back as raw JSON via toJSONString -- CLAUDE.md
    warns that toString(Map) 500s on this login, toJSONString does not --
    and are passed through untouched. They are worth reading verbatim: the
    nums keys are visibly unreliable (the same 102 arrives as
    nums['slot'] on one instance and nums['wait_us'] on another, and some
    maps carry the same key twice), so anything that parsed them into
    named fields would be quietly wrong.
    """
    out = {}
    ents = sorted({e for e in entities if e})
    if not ents:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo:{f}}' AND ts <= '{hi:{f}}' AND " if lo and hi
          else "ts > now() - INTERVAL 2 HOUR AND ")
    evs = ", ".join("'%s'" % e for e in SCHED_EVENTS)
    for i in range(0, len(ents), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(e) for e in ents[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT entity, toString(ts) AS at, instance_id, event, "
                "  reason, toJSONString(attrs) AS a, toJSONString(nums) AS n "
                f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
                f"  AND event IN ({evs}) AND entity IN ({lst}) "
                "ORDER BY entity, ts")
        except Exception:
            continue
        for ent, at, inst, ev, rsn, a, n in rows:
            out.setdefault(ent, []).append({
                "at": at or "", "instance": inst or "", "event": ev or "",
                "reason": rsn or "", "attrs": a or "{}", "nums": n or "{}"})
    return out


def _sched_keys(rows):
    """Both identities an order can be logged under."""
    out = set()
    for x in rows:
        if x.get("sigprefix"):
            out.add(x["sigprefix"])
        if x.get("bundle_id"):
            out.add(x["bundle_id"])
    return out


def attach_notes(ch, rows, lo=None, hi=None):
    """Hang the scheduler's verdicts on each diff row, in place."""
    notes = scheduler_notes(ch, _sched_keys(rows), lo, hi)
    if not notes:
        for x in rows:
            x.setdefault("sched", [])
        return rows
    for x in rows:
        got = []
        for k in (x.get("sigprefix"), x.get("bundle_id")):
            if k:
                got += notes.get(k, [])
        got.sort(key=lambda z: z["at"])
        x["sched"] = got
    return rows


def round_marks(ch, slot, lo=None, hi=None):
    """{(instance, round): {"open": ts, "win": ts}} -- each builder
    instance's own round_started / round_winner for this slot, on that
    instance's clock."""
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo:{f}}' AND ts <= '{hi:{f}}' AND " if lo and hi
          else "ts > now() - INTERVAL 2 HOUR AND ")
    out = {}
    try:
        _, rows = ch(
            "SELECT instance_id, nums['index'], event, toString(min(ts)) "
            f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
            "  AND event IN ('round_started', 'round_winner') "
            f"  AND nums['slot'] = {int(slot)} "
            "GROUP BY instance_id, nums['index'], event")
    except Exception:
        return out
    for inst, idx, e, t in rows:
        out.setdefault((inst, int(idx)), {})["open" if e == "round_started"
                                            else "win"] = t
    return out


def received_at(ch, entities, lo=None, hi=None):
    """{entity: {instance: first scheduler/received ts}} for SigPrefixes
    and bundle ids -- an order is logged once per instance at admission."""
    out = {}
    ents = sorted({e for e in entities if e})
    if not ents:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo:{f}}' AND ts <= '{hi:{f}}' AND " if lo and hi
          else "ts > now() - INTERVAL 2 HOUR AND ")
    for i in range(0, len(ents), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(e) for e in ents[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT entity, instance_id, toString(min(ts)) "
                f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
                f"  AND event = 'received' AND entity IN ({lst}) "
                "GROUP BY entity, instance_id")
        except Exception:
            continue
        for ent, inst, t in rows:
            out.setdefault(ent, {})[inst] = t
    return out


def attach_recv(ch, rows, slot, lo=None, hi=None, round_of=lambda x: x["round"],
                prefix_of=lambda x: x.get("sigprefix"),
                bundle_of=lambda x: x.get("bundle_id")):
    """Hang, on each row, when the builder admitted the order relative to
    its round: {instance: (ms after round_started, ms before round_winner)}.

    The order is looked up by its SigPrefix and, for a bundle member, by
    the bundle id (that is the entity a bundle's `received` carries); the
    earlier of the two is the admission. Both stamps are the same
    instance's clock, so no correction applies. Negative "after open"
    means it arrived before that round opened -- normal for a round that
    is not the slot's first."""
    marks = round_marks(ch, slot, lo, hi)
    if not marks:
        for x in rows:
            x.setdefault("recv", {})
        return rows
    ents = set()
    for x in rows:
        ents.update(k for k in (prefix_of(x), bundle_of(x)) if k)
    recv = received_at(ch, ents, lo, hi)
    for x in rows:
        got = {}
        for key in (prefix_of(x), bundle_of(x)):
            for inst, t in (recv.get(key) or {}).items():
                if inst not in got or t < got[inst]:
                    got[inst] = t
        rec = {}
        for inst, t in got.items():
            m = marks.get((inst, int(round_of(x))))
            if not m or "open" not in m:
                continue
            after = _ms(m["open"], t)
            before = _ms(t, m["win"]) if "win" in m else None
            if after is not None:
                rec[inst] = (after, before)
        x["recv"] = rec
    return rows


def _recv_cells(x, prefer=None):
    """Two cells: ms after the round opened, ms before its winner. One
    instance is shown -- `prefer` (the row's own builder) if it logged the
    order, else the earliest admission -- and every instance is in the
    title."""
    rec = x.get("recv") or {}
    if not rec:
        return '<td class="n dim">&mdash;</td><td class="n dim">&mdash;</td>'
    inst = prefer if prefer in rec else min(rec, key=lambda k: rec[k][0])
    after, before = rec[inst]
    title = html.escape("; ".join(
        f"{k}: {v[0]:+.1f} ms after open" +
        (f", {v[1]:.1f} ms before winner" if v[1] is not None else "")
        for k, v in sorted(rec.items())))
    def cell(v, cls=""):
        if v is None:
            return '<td class="n dim">&mdash;</td>'
        return (f'<td class="n m{cls}" title="{title}">'
                f'{v:+.1f}</td>' if cls else
                f'<td class="n m" title="{title}">{v:.1f}</td>')
    return (cell(after, " neg" if after < 0 else " pos")
            + cell(before, " neg" if before is not None and before < 0 else ""))


def _sched_pairs(x):
    seen, out = set(), []
    for n in x.get("sched") or []:
        k = (n["event"], n["reason"])
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _okey(o):
    """What two offers are compared on.

    The prefix, not the signature: our own store never carries the
    winner's signatures, only its 16-byte SigPrefixes, and a prefix is
    derivable from a signature but not the reverse. A bundle has no prefix
    of its own, so it is keyed by id under a tag that cannot collide with
    a base58 prefix."""
    return o["prefix"] or ("bundle:" + o["bundle_id"])


def _sel_rows(rnd, titan_builder, titan_side, our_sides, contents,
              blk_sigs, rewards=None):
    """The diff itself, at SIGNATURE level.

    Not at order-ref level: a transaction can be standalone in one builder's
    offer and a bundle member in another's, and comparing bundle ids would
    score that as "neither has it". Comparing signatures catches it, and
    `type`/`bundle_id` still say how each side carried it."""
    rows = []
    tset = {_okey(o): o for o in contents.get(titan_side["uuid"], [])}
    for our_builder, our in our_sides:
        oset = {_okey(o): o for o in contents.get(our["uuid"], [])}
        if not tset and not oset:
            continue
        base = {
            "slot": rnd["slot"], "round": rnd["idx"],
            "connector": rnd["connector"], "won_by": rnd["winner"],
            "we_won": _is_ours(rnd["winner"]),
            "our_builder": our_builder, "titan_builder": titan_builder,
            "our_bid": our["bid"], "titan_bid": titan_side["bid"],
            "bid_gap": our["bid"] - titan_side["bid"],
            "our_cost": our["cost"], "titan_cost": titan_side["cost"],
            "our_orders": our["n"], "titan_orders": titan_side["n"],
            "our_reason": our["reason"], "titan_reason": titan_side["reason"],
            "is_last": rnd["is_last"]}
        if (our["accepted"] and titan_side["accepted"]
                and not (oset and tset)):
            # Both offers were graded, but one has no stored content. Under
            # the fallback that is expected for a LOSING Titan offer --
            # bifrost_miniblocks holds the winner announcement and our own
            # offers, never a rival's losing rung. Reporting it as "Titan
            # included nothing we had" would be a fabrication.
            rows.append(dict(base, verdict="no_content",
                             builder_id=(titan_builder if not tset
                                         else our_builder),
                             type="", signature="", sigprefix="",
                             bundle_id="", bundle_tx_index=None,
                             order_index="", landed=""))
            continue
        if not our["accepted"] or not titan_side["accepted"]:
            # Not a content finding: one side never had an offer in play.
            # One row, so the CSV still records that the round happened and
            # why we were not in it.
            rows.append(dict(base, verdict="no_accepted_offer",
                             builder_id=(our_builder if not our["accepted"]
                                         else titan_builder),
                             type="", signature="", sigprefix="",
                             bundle_id="", bundle_tx_index=None,
                             order_index="", landed=""))
            continue
        for verdict, src, owner in (("us_inc_not_titan", oset.keys() - tset.keys(),
                                     our_builder),
                                    ("titan_inc_not_us", tset.keys() - oset.keys(),
                                     titan_builder)):
            pool = oset if verdict == "us_inc_not_titan" else tset
            other = tset if verdict == "us_inc_not_titan" else oset
            for key in src:
                o = pool[key]
                # A bundle member is one row of the diff, but the reader
                # needs the bundle it came from: which other members
                # travelled with it, what each paid, and which of them the
                # other side also carried (those are NOT diff rows and
                # would otherwise be invisible here).
                members = None
                if o["bundle_id"]:
                    members = sorted(
                        ({"sig": m["sig"], "prefix": m["prefix"],
                          "bundle_tx_index": m["bundle_tx_index"],
                          "landed": _landed_mark(m, blk_sigs),
                          "in_diff": _okey(m) in src,
                          "shared": _okey(m) in other,
                          **_reward_of(m, blk_sigs, rewards)}
                         for m in pool.values()
                         if m["bundle_id"] == o["bundle_id"]),
                        key=lambda m: (m["bundle_tx_index"] is None,
                                       m["bundle_tx_index"] or 0))
                rows.append(dict(base, verdict=verdict, builder_id=owner,
                                 members=members,
                                 type=o["type"], signature=o["sig"],
                                 sigprefix=o["prefix"],
                                 bundle_id=o["bundle_id"],
                                 bundle_tx_index=o["bundle_tx_index"],
                                 order_index=o["order_index"],
                                 landed=_landed_mark(o, blk_sigs),
                                 **_reward_of(o, blk_sigs, rewards)))
    return rows


def _reward_of(o, blk_pfx, rewards):
    """What this order paid, but only if it is in THIS block.

    Keyed on the full signature, not the prefix: the prefix is what proves
    it landed, the signature is what the block indexes its fee by. Under
    the fallback an order that landed always has its signature recovered
    from the block, so the two always arrive together.

    Blank, not zero, when it did not land or we could not price it --
    0 lamports is a real answer and must not be confused with "no data".
    """
    if not rewards or not o.get("sig"):
        return {"fee": "", "tip": "", "reward": ""}
    if blk_pfx is not None and o.get("prefix") and o["prefix"] not in blk_pfx:
        return {"fee": "", "tip": "", "reward": ""}
    rec = rewards.get(o["sig"])
    if not rec:
        return {"fee": "", "tip": "", "reward": ""}
    return {"fee": rec["fee"], "tip": rec["tip"],
            "reward": rec["fee"] + rec["tip"]}


def price_from_packets(relayer_q, rows, window=(None, None)):
    """Price the diff rows the block could not: from the transaction bytes
    in the relay's packet log, in place.

    `_reward_of` prices an order only if it landed in THIS block, so a
    `us_inc_not_titan` row -- our order the winner left out, which then
    mostly did not land -- shows no reward at all. Our own store cannot
    help: `bifrost_miniblocks.payload` carries 16-byte refs, not bytes
    (42 bytes per order, measured). `relayer.validator_packet_events` keeps
    the bytes of every packet that reached a served validator's TPU, which
    on a measured slot covered 2 of 5 of our rows and 21 of 33 of Titan's.
    Priced with the builder's own arithmetic (see txprice); verified to the
    lamport against the block on every landed sample.

    Rows priced here carry `priced = "bytes"` so the cell and the totals
    can say so -- it is what the order WOULD have paid, not what changed
    hands."""
    want = {}
    for x in rows:
        if x.get("reward") in ("", None) and x.get("signature"):
            want.setdefault(x["signature"], []).append(x)
    if not want:
        return rows
    lo, hi = window or (None, None)
    f = "%Y-%m-%d %H:%M:%S"
    # the table is partitioned on created_at; an hour either side covers
    # relay/builder clock skew and a packet that arrived well before the
    # round it was offered in
    tb = (f"created_at >= '{lo - dt.timedelta(hours=1):{f}}' "
          f"AND created_at <= '{hi + dt.timedelta(hours=1):{f}}'"
          if lo and hi else "created_at >= now() - INTERVAL 1 DAY")
    sigs = sorted(want)
    for i in range(0, len(sigs), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(x) for x in sigs[i:i + ENTITY_BATCH])
        try:
            got = relayer_q(
                "SELECT signature, any(tx_body) FROM validator_packet_events "
                f"WHERE {tb} AND signature IN ({lst}) GROUP BY signature")
        except Exception:
            return rows
        for sig, body in got:
            if not body:
                continue
            try:
                pr = txprice.price(base64.b64decode(body), TIP_ACCOUNTS)
            except Exception:
                continue
            for x in want.get(sig, []):
                x.update(fee=pr["fee"], tip=pr["tip"],
                         reward=pr["fee"] + pr["tip"], priced="bytes",
                         tip_lower_bound=bool(pr["lookups"]))
    return rows


def _landed_mark(o, blk_pfx):
    """yes / no / blank. Blank is "cannot tell", which a bundle ref always
    is -- it names no member signatures."""
    if blk_pfx is None:
        return ""
    if not o["prefix"]:
        return ""
    return "yes" if o["prefix"] in blk_pfx else "no"


def selection_diff(relay_q, ch, rounds_, window, blocks=None,
                   contents=None, rewards=None):
    """Every us-vs-titan content difference across these rounds."""
    slots = {r["slot"] for r in rounds_}
    # The instant the round was decided is when the WINNING OFFER was
    # submitted, not when round_chosen fired. Measured on 457382947 r0
    # the gap is 11 ms, and we put five more rungs in during it -- all
    # graded not_best against an offer already on the table. Anchoring on
    # round_chosen credited us with 352 orders when 338 were standing
    # when Titan won, and disagreed with the race on the same round.
    closed = {(r["slot"], r["idx"]): (r.get("sub_at") or r.get("at") or "")
              for r in rounds_}
    offers = best_offers(relay_q, slots, window, chosen_at=closed)
    need, plan = set(), []
    for rnd in rounds_:
        tb, tv, ours = _pair(offers, rnd["slot"], rnd["idx"],
                             rnd["winner"], rnd["uuid"])
        if not tb:
            continue
        need.add(tv["uuid"])
        need.update(v["uuid"] for _b, v in ours)
        plan.append((rnd, tb, tv, ours))
    need.discard("")
    # one prefix map per slot, shared by the fallback (to put signatures
    # back on prefix-only orders) and by the landed column
    pmaps = {sl: block_prefixes(sg)
             for sl, sg in (blocks or {}).items() if sg is not None}
    flat = {}
    for mp in pmaps.values():
        flat.update(mp)
    if contents is None:
        contents, _src = order_contents(relay_q, ch, sorted(need), window,
                                        flat)
    rows = []
    for rnd, tb, tv, ours in plan:
        rows.append(_sel_rows(rnd, tb, tv, ours, contents,
                              pmaps.get(rnd["slot"])
                              if rnd["slot"] in pmaps else None,
                              (rewards or {}).get(rnd["slot"])))
    flat_rows = [x for part in rows for x in part]
    # the chunk's own window, so the scan stays narrow even over a range
    attach_notes(ch, flat_rows, *(window or (None, None)))
    lo, hi = window or (None, None)
    for sl in {x["slot"] for x in flat_rows}:
        attach_recv(ch, [x for x in flat_rows if x["slot"] == sl], sl, lo, hi)
    return flat_rows


def _sel_flat(x, col):
    """The three scheduler columns are derived, not stored."""
    if col == "sched_event":
        return ";".join(dict.fromkeys(e for e, _r in _sched_pairs(x)))
    if col == "sched_reason":
        return ";".join(dict.fromkeys(r for _e, r in _sched_pairs(x) if r))
    if col == "sched_detail":
        got = x.get("sched") or []
        return json.dumps(got, separators=(",", ":")) if got else ""
    return x.get(col, "")


def _sel_csv(rows):
    lines = [",".join(SEL_COLS)]
    for x in rows:
        lines.append(",".join(_csv_cell(_sel_flat(x, c)) for c in SEL_COLS))
    return "\n".join(lines)



def slot_selection(relay_q, ch, rpc_url, slot, rounds_, blk_sigs=None,
                   rewards=None):
    """The selection diff for one slot, for the page expansion."""
    win = _chunk_window(rounds_)
    if blk_sigs is None:
        try:
            blk_sigs, _bt = block_signatures(rpc_url, slot)
        except Exception:
            blk_sigs = None
    return selection_diff(relay_q, ch, rounds_, win, blocks={slot: blk_sigs},
                          rewards={slot: rewards} if rewards else None)


def relay_tx_lag(relay_q):
    """When relay.mini_block_transactions_v2 last took a row.

    The un-suffixed table appeared to "stop" at 2026-10-15 10:47:46 UTC.
    It did not break -- it was RENAMED to _v2, the same migration the
    events table went through on 2026-10-09. The old name still resolves
    and still returns rows up to that instant, which is why it reads as
    an outage rather than a move. Kept as a liveness check on the real
    table; the bifrost_miniblocks fallback below stays as the safety net.
    """
    try:
        _, rows = relay_q(
            "SELECT toString(max(ingested_at)) AS hi "
            "FROM relay.mini_block_transactions_v2 "
            "WHERE ingested_at > now() - INTERVAL 24 HOUR")
    except Exception:
        return None
    hi = rows[0][0] if rows and rows[0] else ""
    if not hi or hi.startswith("1970"):
        return None
    try:
        return dt.datetime.fromisoformat(hi[:26])
    except ValueError:
        return None


LANDING = "landing"
SELECTION = "selection"
BOTH = "both"
# The wording is the user's: "won but not landed" and "winner txn vs ours
# diff". Kept verbatim so the control reads the way the question is asked.
TIMELINE = "timeline"
# Region comes off the RELAY host, not the builder name: instance_id is
# sol-relay-lat-fra / sol-relay-1-lat-fra / sol-relay-2-lat-ams /
# sol-relay-1-lat-ams / sol-relay-tsw-tyo, so the trailing token is the
# region and a new relay in an existing region needs no code change.
#
# It is TYO, not JP. Astralane has no builder there at all -- 0 offers
# against Titan's 39,397 over 24 h -- so the option exists to show that,
# not because we compete in it.
REGIONS = {"all": "all regions", "fra": "FRA", "ams": "AMS",
           "tyo": "TYO (Tokyo)"}
DEFAULT_REGION = "all"


def _region_sql(region, col="instance_id"):
    if region in ("", "all") or region not in REGIONS:
        return ""
    return f" AND endsWith({col}, '-{region}')"


def _connector_sql(connector):
    return f" AND connector_identity = '{connector}'" if connector else ""


CONNECTOR_LIST_DAYS = 7


# Our builders by region, for the connectors only our own store knows.
OUR_BUILDER_REGION = {"ams": "ASTRALANE_AMS%", "fra": "ASTRALANE_FRA%",
                      "tyo": "ASTRALANE_JP%"}


def _our_region_sql(region):
    pat = OUR_BUILDER_REGION.get(region)
    return f" AND local_builder_id LIKE '{pat}'" if pat else ""


def connectors_in(relay_q, r, region="all", ch=None):
    """The validators the region's relays opened rounds for in the last
    CONNECTOR_LIST_DAYS, busiest first -> [(identity, slots, source)].

    Deliberately NOT the window being viewed: a validator that happened to
    lead nothing in the last hour would vanish from the list, and the list
    is for picking a validator, not for describing the window. From
    round_opened, so a validator nobody bid on still appears.

    Gattaca's store only knows Gattaca's relays. A validator served through
    a relay of our own (DaRKu4…, 2026-10-21) leaves no trace there but
    every trace in bifrost_miniblocks, so our store is unioned in and such
    an entry is tagged `ours` -- the relay-side pages will be empty for it
    and fall back to our own records."""
    _, rows = relay_q(
        "SELECT connector_identity, uniqExact(slot) AS n "
        "FROM relay.mini_block_events_v2 "
        f"WHERE timestamp >= now() - INTERVAL {CONNECTOR_LIST_DAYS} DAY "
        f"  AND event = 'round_opened'{_region_sql(region)} "
        "  AND connector_identity != '' "
        "GROUP BY connector_identity ORDER BY n DESC")
    out = {c: [int(n or 0), "relay"] for c, n in rows if c}
    if ch is not None:
        try:
            _, ours = ch(
                "SELECT connector_identity, uniqExact(slot) AS n "
                "FROM bifrost_miniblocks "
                f"WHERE ts >= now() - INTERVAL {CONNECTOR_LIST_DAYS} DAY "
                f"  AND connector_identity != ''{_our_region_sql(region)} "
                "GROUP BY connector_identity")
        except Exception:
            ours = []
        for c, n in ours:
            if c and c not in out:
                out[c] = [int(n or 0), "ours"]
    return sorted(((c, n, src) for c, (n, src) in out.items()),
                  key=lambda x: -x[1])


def _connector_select(cur, options):
    """`options` is [(identity, name, rounds)], already scoped to the
    region on screen; the label says so because the list only refreshes
    on run."""
    opts = ['<option value="">all validators</option>']
    seen = False
    for opt in options:
        ident, name, n = opt[:3]
        src = opt[3] if len(opt) > 3 else "relay"
        seen = seen or ident == cur
        shown = (html.escape(name) if name
                 else html.escape(ident[:10]) + "\u2026")
        opts.append(
            f'<option value="{html.escape(ident)}"'
            f'{" selected" if ident == cur else ""} '
            f'title="{html.escape(ident)}">{shown} &middot; {n:,} slots / '
            f"{CONNECTOR_LIST_DAYS}d"
            + (" &middot; own relay, from our builders' records"
               if src == "ours" else "") + "</option>")
    if cur and not seen:
        # Carried over from another region or window: keep it selectable
        # so the URL still means what it says, but say it is not in this list.
        opts.append(f'<option value="{html.escape(cur)}" selected>'
                    f"{html.escape(cur[:10])}\u2026 &middot; not seen here</option>")
    return ('<label>validator <span class="dim">in this region, last '
            f'{CONNECTOR_LIST_DAYS}d</span>'
            f'<select name=connector>{"".join(opts)}</select></label>')


SLOT_MS = 200          # nominal since SIMD-0525; measured 204-217


def _rpc(rpc_url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params}).encode()
    req = urllib.request.Request(rpc_url, data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        d = json.loads(resp.read())
    if d.get("error"):
        raise RuntimeError(str(d["error"].get("message", ""))[:160])
    return d.get("result")


def next_leader_slot(rpc_url, identity):
    """The validator's next leader slot and when it is due.

    From the chain's leader schedule, not the relay: a validator keeps its
    slots whether or not any relay serves it. Looks in the current epoch
    first, then the next (its schedule is known one epoch ahead). The
    ETA assumes SLOT_MS per slot from the current slot, which is what the
    page's countdown then runs down from; `now_ms` goes along so the
    browser can correct for its own clock."""
    if not rpc_url or not identity:
        return None
    info = _rpc(rpc_url, "getEpochInfo", [])
    cur, idx, per = (int(info["absoluteSlot"]), int(info["slotIndex"]),
                     int(info["slotsInEpoch"]))
    epoch_start = cur - idx
    sched = _rpc(rpc_url, "getLeaderSchedule", [None, {"identity": identity}])
    slots = sorted(int(x) for x in (sched or {}).get(identity, []))
    leading = [epoch_start + x for x in slots
               if epoch_start + x <= cur < epoch_start + x + 1]
    nxt = next((epoch_start + x for x in slots if epoch_start + x > cur), None)
    epoch = int(info["epoch"])
    if nxt is None:
        nxt_start = epoch_start + per
        sched = _rpc(rpc_url, "getLeaderSchedule",
                     [nxt_start, {"identity": identity}])
        slots = sorted(int(x) for x in (sched or {}).get(identity, []))
        nxt = nxt_start + slots[0] if slots else None
        epoch += 1
    if nxt is None:
        return {"current_slot": cur, "next_slot": None, "epoch": epoch}
    ahead = nxt - cur
    return {"current_slot": cur, "next_slot": nxt, "epoch": epoch,
            "slots_ahead": ahead, "eta_ms": ahead * SLOT_MS,
            "leading_now": bool(leading),
            "now_ms": int(time.time() * 1000),
            "in_epoch": epoch == int(info["epoch"])}


def slot_age_html(slot, ref):
    """'~3m 12s ago' beside a slot, estimated from the current slot at page
    load and SLOT_MS per slot -- the page's age ticker then keeps it
    running. `ref` is (current_slot, now_epoch_s) or None."""
    if not ref:
        return ""
    cur, now = ref
    behind = cur - int(slot)
    if behind < 0:
        return ' <span class="sage dim">upcoming</span>'
    at = now - behind * SLOT_MS / 1000
    s_ = int(behind * SLOT_MS / 1000)
    d, h, m, sec = s_ // 86400, s_ % 86400 // 3600, s_ % 3600 // 60, s_ % 60
    txt = (f"{d}d {h}h" if d else f"{h}h {m}m" if h else f"{m}m {sec}s" if m
           else f"{sec}s")
    return (f' <span class="age sage dim" data-t="{at:.1f}" title="estimated: '
            f"{behind:,} slots before the current slot at page load, at "
            f'{SLOT_MS} ms per slot">{txt} ago</span>')


def next_leader_html(nl, label):
    """The countdown line. Rendered once with the server's numbers; the JS
    keeps `.nl-eta` and `.nl-at` ticking from data-* on the span."""
    if not nl:
        return ""
    if nl.get("next_slot") is None:
        return (f'<div class="nextlead">{html.escape(label)} has no leader '
                f"slot in epoch {nl['epoch']} or the next</div>")
    eta = nl["eta_ms"] / 1000
    at = (dt.datetime.now(dt.UTC) + dt.timedelta(milliseconds=nl["eta_ms"]))
    return (f'<div class="nextlead" data-slot="{nl["next_slot"]}" '
            f'data-cur="{nl["current_slot"]}" data-eta="{nl["eta_ms"]}" '
            f'data-now="{nl["now_ms"]}" data-slotms="{SLOT_MS}">'
            + ("<b>leading now</b> &middot; " if nl.get("leading_now") else "")
            + f'next leader slot for <b>{html.escape(label)}</b>: '
            f'<span class="m">{copy_val(str(nl["next_slot"]))}</span> '
            f'(window {nl["next_slot"]}&ndash;{nl["next_slot"] + 3}'
            + ("" if nl.get("in_epoch", True) else f', epoch {nl["epoch"]}')
            + f') &middot; <b class="nl-eta">{eta:,.1f} s</b> from now, '
            f'~<span class="nl-at">{at:%H:%M:%S}</span> UTC '
            f'<span class="aqcap">({nl["slots_ahead"]:,} slots ahead at '
            f"{SLOT_MS} ms/slot, from slot {nl['current_slot']:,})</span></div>")


def _region_select(cur):
    opts = "".join(
        f'<option value="{k}"{" selected" if k == cur else ""}>'
        f"{html.escape(v)}</option>" for k, v in REGIONS.items())
    return f"<label>region<select name=region>{opts}</select></label>"
METRICS = {LANDING: "won but not landed",
           SELECTION: "winner txn vs ours diff",
           BOTH: "both"}
# which export kind each metric offers
METRIC_KIND = {LANDING: "misses", SELECTION: "selection"}


def _metric_select(cur):
    opts = "".join(
        f'<option value="{k}"{" selected" if k == cur else ""}>'
        f"{html.escape(v)}</option>" for k, v in METRICS.items())
    return f"<label>metric<select name=metric>{opts}</select></label>"


RELAY_SRC = "relay"
OURS_SRC = "ours"
SRC_NOTE = {
    RELAY_SRC: "",
    OURS_SRC: ("content from <b>bifrost_miniblocks</b> (our own store) "
               "because relay.mini_block_transactions_v2 is not taking rows. "
               "Transactions are matched by 16-byte SigPrefix against the "
               "block, which is exact. A bundle ref carries no member "
               "signatures, so its members are recovered from our own "
               "<b>scheduler/received</b> events and expanded one row "
               "each; a ref we never ingested stays undecidable and is "
               "left out rather than counted as a miss."),
}


STALE_AFTER_MIN = 30


def stale_banner(lag):
    """Say so when relay.mini_block_transactions has stopped taking rows.

    Without this the page degrades into "the relay kept no transaction
    rows for this miniblock" on every round, which looks like a bug here
    rather than an outage there. mini_block_events_v2 keeps flowing, so
    the round table above still fills in normally -- that contrast is the
    confusing part."""
    if not lag:
        return ""
    age = (dt.datetime.now(dt.UTC).replace(tzinfo=None) - lag).total_seconds()
    if age < STALE_AFTER_MIN * 60:
        return ""
    return ('<div class="herr" style="margin:14px 28px 0">'
            "<b>relay.mini_block_transactions has taken no rows since "
            f'{lag.strftime("%Y-%m-%d %H:%M:%S UTC")}</b> '
            f"({age / 3600:.1f} h ago). The round table below is from "
            "mini_block_events_v2 and is current, but expanding a slot "
            "newer than that will find no transactions, and both exports "
            "will come back empty for it.</div>")


SEL_SHOW = 40


def _sel_round_key(x):
    return (x["round"], x["our_builder"])


def _sel_head(x):
    gap = x["bid_gap"]
    cls = "y" if gap >= 0 else "n"
    return (f'<div class="selhead">r{x["round"]} &middot; '
            f'<b>{html.escape(x["our_builder"])}</b> vs '
            f'{html.escape(x["titan_builder"])} &middot; won by '
            f'<b class="land {"y" if x["we_won"] else "n"}">'
            f'{html.escape(x["won_by"] or "?")}</b></div>'
            '<div class="selbid">bid '
            f'<b>{x["our_bid"]:,}</b> vs {x["titan_bid"]:,} '
            f'<span class="land {cls}">{gap:+,}</span> &middot; orders '
            f'{x["our_orders"]:,} vs {x["titan_orders"]:,} &middot; cost '
            f'{x["our_cost"]:,} vs {x["titan_cost"]:,} &middot; graded '
            f'{html.escape(x["our_reason"])}/{html.escape(x["titan_reason"])}'
            ' &middot; <span class="dim">each side as it stood when the '
            "winning offer was submitted</span></div>")


def _sel_table(group):
    out = ['<table class="otab"><tr><th>#</th><th>verdict</th><th>type</th>'
           "<th>signature</th><th>sigprefix</th><th>bundle</th>"
           '<th>landed</th><th class="n" title="landed: fee and tip from the '
           'block. offered: from the transaction bytes in the relay packet log, '
           'for an order that did not land here">reward</th>'
           '<th class="n" title="ms from this round&#39;s round_started to the builder admitting the order (scheduler/received), same clock; negative = it arrived before the round opened">recv +open</th>'
           '<th class="n" title="ms from the builder admitting the order to this round&#39;s round_winner">win &minus;recv</th>'
           "<th>scheduler said</th></tr>"]
    real = [i for i in group
            if i["verdict"] not in ("no_accepted_offer", "no_content")]
    shown = _sel_show(group)
    front = {id(i) for i in shown}
    # the capped order first, then everything else -- hidden, not dropped
    ordered = shown + sorted((i for i in real if id(i) not in front),
                             key=lambda i: (str(i["verdict"]), i["order_index"]))
    for i in ordered:
        extra = ' class="selx" hidden' if id(i) not in front else ""
        bid = (copy_val(i["bundle_id"], i["bundle_id"][:12] + "\u2026")
               if i["bundle_id"] else '<span class="dim">&mdash;</span>')
        bx = ""
        if i["bundle_id"] and i.get("members"):
            n = len(i["members"])
            pos = next((k + 1 for k, m in enumerate(i["members"])
                        if m["sig"] == i["signature"]), None)
            bxid = f'bx{abs(hash((i["slot"], i["round"], i["order_index"], i["signature"], i["verdict"], i.get("our_builder"), i.get("builder_id")))):x}'
            bid += (f' <span class="scopen bxo" data-sx="{bxid}" role="button" '
                    f'tabindex="0" title="every member of this bundle">'
                    f'#{pos or "?"} of {n}</span>')
            bx = (f'<tr class="sxrow" id="{bxid}" hidden><td colspan=11>'
                  + _bundle_panel(i) + "</td></tr>")
        vcls = "us" if i["verdict"] == "us_inc_not_titan" else "ti"
        pairs = _sched_pairs(i)
        # One panel per RENDERED row. The same order is rendered once per
        # our-builder group (AMS_1 vs Titan, AMS_2 vs Titan), so the
        # builder has to be in the key or the ids collide and a click in
        # the second group opens the first group's panel.
        rid = f'sx{abs(hash((i["slot"], i["round"], i["order_index"], i["signature"], i["verdict"], i.get("our_builder"), i.get("builder_id")))):x}'
        if pairs:
            lab = " ".join(
                f'<span class="sc">{html.escape(e)}'
                + (f'<i>{html.escape(rs[:34])}</i>' if rs else "") + "</span>"
                for e, rs in pairs[:2])
            if len(pairs) > 2:
                lab += f' <span class="dim">+{len(pairs) - 2}</span>'
            cell = (f'<span class="scopen" data-sx="{rid}" role="button" '
                    f'tabindex="0">{lab}</span>')
        else:
            cell = '<span class="dim">&mdash;</span>'
        # data-b lets a hover on any value in the row light up every row
        # of the same bundle -- see the hover handler in ACCURACY_JS.
        battr = (f' data-b="{html.escape(i["bundle_id"], quote=True)}"'
                 if i["bundle_id"] else "")
        out.append(f'<tr{battr}{extra}><td class="n">{i["order_index"]}</td>'
                   f'<td><span class="vd {vcls}">{i["verdict"]}</span></td>'
                   f'<td>{html.escape(i["type"])}</td>'
                   f'<td>{copy_val(i["signature"])}</td>'
                   f'<td>{copy_val(i["sigprefix"])}</td>'
                   f"<td>{bid}</td>"
                   f'<td class="land {"y" if i["landed"] == "yes" else "n"}">'
                   f'{i["landed"] or "?"}</td>'
                   f'<td class="n">{_rw_cell(i)}</td>'
                   f'{_recv_cells(i, i.get("our_builder"))}'
                   f"<td>{cell}</td></tr>")
        if bx:
            out.append(bx)
        if pairs:
            out.append(f'<tr class="sxrow" id="{rid}" hidden><td colspan=11>'
                       + _sched_panel(i) + "</td></tr>")
    out.append("</table>")
    if len(real) > len(shown):
        sm = sum(1 for i in shown if i["verdict"] == "us_inc_not_titan")
        st = len(shown) - sm
        tm = sum(1 for i in real if i["verdict"] == "us_inc_not_titan")
        out.append(f'<div class="more">showing {st} of {len(real) - tm:,} '
                   f"titan_inc_not_us and {sm} of {tm:,} us_inc_not_titan "
                   f'<button type="button" class="cpsm selmore">show all '
                   f"{len(real):,}</button></div>")
    return "".join(out)


def _bundle_panel(i):
    """Every member of the bundle this row came from, in bundle order.

    `in diff` marks the members that are rows of this table; a member
    without it was carried by BOTH sides and so is not a difference -- but
    it is still part of the bundle, and usually the one paying the tip."""
    rows = []
    for m in i["members"]:
        tag = ('<span class="vd us">in diff</span>' if m["in_diff"]
               else ('<span class="vd na">both sides</span>' if m["shared"]
                     else ""))
        rows.append(
            f'<tr><td class="n">{"" if m["bundle_tx_index"] is None else m["bundle_tx_index"]}</td>'
            f'<td>{copy_val(m["sig"]) if m["sig"] else "&mdash;"}</td>'
            f'<td>{copy_val(m["prefix"]) if m["prefix"] else "&mdash;"}</td>'
            f'<td class="land {"y" if m["landed"] == "yes" else "n"}">'
            f'{m["landed"] or "?"}</td>'
            f'<td class="n">{_rw_cell(m)}</td><td>{tag}</td></tr>')
    return ('<div class="sxbox"><div class="sxh">bundle '
            + copy_val(i["bundle_id"], i["bundle_id"][:16] + "\u2026")
            + f' &middot; {len(i["members"])} member'
            + ("" if len(i["members"]) == 1 else "s") + "</div>"
            '<table class="otab"><tr><th>#</th><th>signature</th>'
            "<th>sigprefix</th><th>landed</th>"
            '<th class="n">reward</th><th></th></tr>'
            + "".join(rows) + "</table></div>")


def _sched_panel(i):
    """The raw attrs and nums for one order, verbatim and copyable.

    Not reformatted into columns: these maps are what the builder wrote,
    and the keys cannot be trusted enough to name them (see
    scheduler_notes). The whole thing is also offered as one JSON blob so
    it can go straight into a ticket."""
    blob = json.dumps({"slot": i["slot"], "round": i["round"],
                       "signature": i["signature"],
                       "sigprefix": i["sigprefix"],
                       "bundle_id": i["bundle_id"],
                       "events": i.get("sched") or []}, indent=2)
    out = ['<div class="sxbox">',
           '<div class="sxh">scheduler events &middot; '
           + copy_val(blob, "copy all as JSON") + "</div>"]
    for n in i.get("sched") or []:
        out.append(
            '<div class="sxev"><div class="sxmeta">'
            f'{html.escape(n["at"][:23])} &middot; '
            f'{html.escape(n["instance"])} &middot; '
            f'<b>{html.escape(n["event"])}</b>'
            + (f' / {html.escape(n["reason"])}' if n["reason"] else "")
            + "</div>"
            f'<pre class="sxpre">attrs {html.escape(n["attrs"])}\n'
            f'nums  {html.escape(n["nums"])}</pre></div>')
    out.append("</div>")
    return "".join(out)


def _rw_cell(i):
    if i.get("reward") == "" or i.get("reward") is None:
        return '<span class="dim">&mdash;</span>'
    t = (f' <span class="dim">+{i["tip"]:,} tip</span>' if i.get("tip")
         else "")
    if i.get("priced") == "bytes":
        # Not what changed hands -- what the offered transaction would
        # have paid, read off its bytes in the relay packet log.
        why = ("priced from the transaction bytes in the relay packet "
               "log, not from the block: this order did not land here")
        if i.get("tip_lower_bound"):
            why += ("; the message uses address tables, so a tip to a "
                    "table-resolved account would not show -- tip is a "
                    "lower bound")
        t += f' <span class="dim" title="{why}">offered</span>'
    return f'{i["fee"]:,}{t}'


def _sel_show(group):
    """Half the rows from each side, not the first N of the list.

    The rows are generated one verdict at a time, so a flat head cut shows
    ONLY the first verdict -- on a round with 87 ours and 182 theirs, all
    40 visible rows were ours and the reader concluded we had offered more
    than the winner, while the count line directly above said the
    opposite. Each side now gets its own half of the budget."""
    half = SEL_SHOW // 2
    mine = [i for i in group if i["verdict"] == "us_inc_not_titan"]
    theirs = [i for i in group if i["verdict"] == "titan_inc_not_us"]
    # whatever one side does not use, the other may have
    take_m = min(len(mine), max(half, SEL_SHOW - len(theirs)))
    take_t = min(len(theirs), SEL_SHOW - take_m)
    out = theirs[:take_t] + mine[:take_m]
    out.sort(key=lambda i: (str(i["verdict"]), i["order_index"]))
    return out


def _sel_counts(group):
    c = collections.Counter(i["verdict"] for i in group)
    land = collections.Counter(
        (i["verdict"], i["landed"]) for i in group if i["landed"])
    bits = []
    for v in ("titan_inc_not_us", "us_inc_not_titan",
              "no_accepted_offer", "no_content"):
        if not c[v]:
            continue
        diff = v in ("titan_inc_not_us", "us_inc_not_titan")
        cls = "us" if v == "us_inc_not_titan" else ("ti" if diff else "na")
        yes, und = land[(v, "yes")], c[v] - land[(v, "yes")] - land[(v, "no")]
        bits.append(f'<span class="vd {cls}">{v}</span> <b>{c[v]:,}</b>'
                    + (f' <span class="dim">({yes} landed'
                       + (f", {und} undecidable" if und else "") + ")</span>"
                       if diff else "")
                    + _rw_total(group, v))
    return " &middot; ".join(bits)


def _rw_total(group, verdict):
    """What the landed orders of one verdict actually paid.

    Only the ones in THIS block carry a price, so this is the reward that
    really changed hands, not a valuation of everything in the offer. On
    titan_inc_not_us it is what the winner collected and we did not; on
    us_inc_not_titan it is what landed anyway without the winner carrying
    it."""
    fee = tip = 0
    ofee = otip = 0            # priced from bytes: offered, did not land
    for i in group:
        if i["verdict"] != verdict or i.get("reward") in ("", None):
            continue
        if i.get("priced") == "bytes":
            ofee += int(i["fee"] or 0)
            otip += int(i["tip"] or 0)
            continue
        fee += int(i["fee"] or 0)
        tip += int(i["tip"] or 0)
    out = ""
    if fee or tip:
        out = f' <b class="rwt">{fee + tip:,}</b> <span class="dim">lamports'
        if tip:
            out += f" ({tip:,} tip)"
        out += "</span>"
    if ofee or otip:
        # Kept apart from the landed figure on purpose: one is money that
        # moved, the other is what the unlanded offers carried.
        out += (f' <span class="dim">&middot; offered, not landed: '
                f"{ofee + otip:,}")
        if otip:
            out += f" ({otip:,} tip)"
        out += "</span>"
    return out


def selection_html(rows, slot):
    """The two sections: the full matrix, then the rounds we lost."""
    if not rows:
        return ('<div class="dmeta">no round in this slot had both an '
                "Astralane and a Titan offer accepted, so there is nothing "
                "to compare</div>")
    groups = {}
    for x in rows:
        groups.setdefault(_sel_round_key(x), []).append(x)

    out = ['<div class="selsec"><div class="seltitle">winner_inc_metric '
           '<span class="dim">&mdash; every round, both directions</span> '
           + copy_sel(rows, f"copy {len(rows):,} as CSV") + "</div>"]
    for key in sorted(groups):
        g = groups[key]
        extra = ""
        if any(i["verdict"] == "no_content" for i in g):
            extra = ('<div class="selcnt dim">no stored content for '
                     "the losing side &mdash; our own store keeps the "
                     "winner announcement and our offers, never a "
                     "rival's losing rung</div>")
        out.append('<div class="selblk">' + _sel_head(g[0])
                   + f'<div class="selcnt">{_sel_counts(g)}</div>'
                   + extra + _sel_table(g) + "</div>")
    out.append("</div>")

    lost = [x for x in rows if not x["we_won"]]
    out.append('<div class="selsec"><div class="seltitle">offered but not '
               'landed <span class="dim">&mdash; what we put up that the '
               "winner did not take, rounds we lost, by builder</span> ")
    if lost:
        out[-1] += copy_sel(lost, f"copy {len(lost):,} as CSV")
    out[-1] += "</div>"
    if not lost:
        out.append('<div class="dmeta">we won every comparable round in '
                   "this slot</div>")
    else:
        by_b = {}
        for x in lost:
            by_b.setdefault(x["our_builder"], {}).setdefault(
                x["round"], []).append(x)
        for b in sorted(by_b):
            out.append(f'<div class="selblk"><div class="selhead">'
                       f"<b>{html.escape(b)}</b></div>")
            for rd in sorted(by_b[b]):
                g = by_b[b][rd]
                out.append(f'<div class="selcnt">r{rd} &middot; lost to '
                           f'{html.escape(g[0]["titan_builder"])} &middot; '
                           f'bid <span class="land n">{g[0]["bid_gap"]:+,}</span>'
                           f" &middot; {_sel_counts(g)}</div>")
            out.append("</div>")
    out.append("</div>")
    return "".join(out)


def copy_sel(rows, label):
    payload = html.escape(_sel_csv(rows), quote=True).replace("\n", "&#10;")
    return (f'<span class="cpv cpcsv" data-c="{payload}" role="button" '
            f'tabindex="0" title="copy all {len(rows)} rows as CSV">'
            f"{label}</span>")



# ----------------------------------------------------------- offer timelines
#
# When each builder put its offers in, and what they were worth, measured
# from the moment the relay opened the round.
#
# Everything here is one relay table and one clock. `round_opened` carries
# the slot, `round_chosen` does not (uuid only), and `deadline` is epoch on
# every row -- so the anchor is round_opened's timestamp and the end of the
# round is found by joining the chosen uuid back to its `submitted` row.
# Both stamps are relay-side, so the offsets have no cross-clock skew in
# them at all; the +3.44 s the relay runs ahead of us cancels.

TL_SHOW = 80
TL_COLS = ["slot", "round", "t_ms", "builder_id", "ours", "chosen",
           "bid", "builder_payment", "jito_tips", "protocol_fees",
           "execution_cost", "order_count", "reason", "uuid", "at"]


def _ms(a, b):
    """b - a in milliseconds, from two ClickHouse timestamp strings."""
    try:
        return (dt.datetime.fromisoformat(b[:26])
                - dt.datetime.fromisoformat(a[:26])).total_seconds() * 1000.0
    except (ValueError, TypeError):
        return None


def timeline_slots(relay_q, r, cap=MAX_SLOTS, region="all", connector="",
                   ch=None):
    """Slots where ANY builder offered, newest first, with who offered.

    Keyed on `submitted`, not on round_chosen: a round nobody won still
    had offers in it, and those are exactly the ones worth looking at.
    """
    # _bounds needs the slot's own time window in slot mode, and only
    # rounds() fills it -- this page never calls that, so resolve it here
    # or every one-slot query dies on a missing win_lo.
    if r["slot"] is not None and "win_lo" not in r:
        r["win_lo"], r["win_hi"] = slot_window(relay_q, r["slot"])
    sub_w, _ = _bounds(r)[1], None
    _, rows = relay_q(
        "SELECT slot, index_in_slot AS idx, builder_id, count() AS offers, "
        f"  max({BID}) AS best_bid, max(order_count) AS n, "
        "  toString(min(timestamp)) AS first_t, "
        "  toString(max(timestamp)) AS last_t "
        "FROM relay.mini_block_events_v2 "
        f"WHERE {sub_w} AND event = 'submitted'{_region_sql(region)}"
        f"{_connector_sql(connector)} "
        "GROUP BY slot, idx, builder_id ORDER BY slot DESC, idx ASC")
    by = {}
    for slot, idx, bld, offers, bid, n, ft, lt in rows:
        by.setdefault(int(slot), {}).setdefault(int(idx), []).append({
            "builder": bld or "", "offers": int(offers or 0),
            "bid": int(bid or 0), "n": int(n or 0),
            "first": ft or "", "last": lt or ""})
    # With one validator picked, the question changes from "where were
    # offers" to "what happened on EVERY slot it led". Add the slots the
    # relay opened rounds for and the blocks the chain says it produced,
    # whether or not anyone offered; a slot with no offer at all stays in
    # with an empty round list, and the table paints it.
    led = {}
    if connector:
        _, opened = relay_q(
            "SELECT slot, count() AS rounds FROM relay.mini_block_events_v2 "
            f"WHERE {sub_w} AND event = 'round_opened'{_region_sql(region)}"
            f"{_connector_sql(connector)} GROUP BY slot")
        for slot, n in opened:
            led[int(slot)] = {"rounds_opened": int(n or 0), "produced": False}
        lo = r.get("win_lo") if r["slot"] is not None else r["lo"]
        hi = r.get("win_hi") if r["slot"] is not None else r["hi"]
        if lo and hi:
            f = "%Y-%m-%d %H:%M:%S"
            _, blocks = relay_q(
                "SELECT slot FROM solana.block_summary "
                f"WHERE estimated_slot_time >= '{lo:{f}}' "
                f"  AND estimated_slot_time <= '{hi:{f}}' "
                f"  AND leader = '{connector}'")
            for (slot,) in blocks:
                led.setdefault(int(slot), {"rounds_opened": 0,
                                           "produced": False})["produced"] = True
        for slot in led:
            by.setdefault(slot, {})
        # Our own records, for rounds the relay's store never saw (a relay
        # of our own). Added only where the relay has nothing for that
        # (slot, round, builder), so nothing is counted twice.
        if ch is not None and lo and hi:
            f = "%Y-%m-%d %H:%M:%S"
            try:
                _, mine = ch(
                    "SELECT slot, index_in_slot AS idx, local_builder_id, "
                    "  countIf(kind = 'selected') AS offers, "
                    "  maxIf(reward, kind = 'selected') AS best_bid, "
                    "  maxIf(order_count, kind = 'selected') AS n, "
                    "  toString(minIf(ts, kind = 'selected')) AS first_t, "
                    "  toString(maxIf(ts, kind = 'selected')) AS last_t, "
                    "  countIf(kind = 'winner' AND won_by_us) AS won "
                    "FROM bifrost_miniblocks "
                    f"WHERE ts >= '{lo - dt.timedelta(minutes=2):{f}}' "
                    f"  AND ts <= '{hi + dt.timedelta(minutes=2):{f}}' "
                    f"  AND connector_identity = '{connector}' "
                    "GROUP BY slot, idx, local_builder_id")
            except Exception:
                mine = []
            for slot, idx, bld, offers, bid, n, ft, lt, won in mine:
                slot, idx = int(slot), int(idx)
                rounds_ = by.setdefault(slot, {})
                if any(b["builder"] == bld for b in rounds_.get(idx, [])):
                    continue
                if int(offers or 0):
                    rounds_.setdefault(idx, []).append({
                        "builder": bld or "", "offers": int(offers or 0),
                        "bid": int(bid or 0), "n": int(n or 0),
                        "first": ft or "", "last": lt or "",
                        "won": int(won or 0) > 0, "source": "ours"})
                led.setdefault(slot, {"rounds_opened": 0, "produced": False})
                led[slot]["ours_store"] = True
    slots = sorted(by, reverse=True)
    total = len(slots)
    slots = slots[:cap]
    return ([{"slot": sl, "rounds": by[sl], "led": led.get(sl)} for sl in slots],
            len(slots), total)


def slot_timeline(relay_q, slot, window, region="all"):
    """Every offer in every round of one slot, on the round's own clock."""
    f = "%Y-%m-%d %H:%M:%S"
    lo, hi = window
    tb = (f"timestamp >= '{lo:{f}}' AND timestamp <= '{hi:{f}}'"
          if lo and hi else "timestamp > now() - INTERVAL 14 DAY")
    _, rows = relay_q(
        "SELECT index_in_slot AS idx, event, builder_id, "
        "  toString(timestamp) AS t, builder_payment AS pay, "
        "  jito_tips AS jito, protocol_fees AS fees, "
        "  execution_cost AS cost, order_count AS n, reason, "
        "  toString(mini_block_uuid) AS uuid, is_last "
        f"FROM relay.mini_block_events_v2 WHERE {tb} AND slot = {int(slot)}"
        f"{_region_sql(region)} ORDER BY idx, t")
    # round_chosen carries no slot, so the winner is the chosen uuid
    # matched back to whichever submitted row owns it
    _, ch_rows = relay_q(
        "SELECT toString(mini_block_uuid) AS uuid, toString(timestamp) AS t "
        f"FROM relay.mini_block_events_v2 WHERE {tb} "
        f"  AND event = 'round_chosen'{_region_sql(region)}")
    chosen = {u: t for u, t in ch_rows if u}

    rounds_ = {}
    for idx, ev, bld, t, pay, jito, fees, cost, n, reason, uuid, is_last in rows:
        i = int(idx)
        d = rounds_.setdefault(i, {"idx": i, "opened": "", "offers": [],
                                   "sealed": "", "tail": "",
                                   "is_last": False})
        if ev == "round_opened":
            d["opened"] = d["opened"] or t
            continue
        if ev == "block_sealed":
            d["sealed"] = t
            continue
        if ev == "tail_opened":
            d["tail"] = t
            continue
        if ev != "submitted":
            continue
        d["is_last"] = d["is_last"] or str(is_last).lower() in ("1", "true")
        d["offers"].append({
            "at": t, "builder": bld or "", "uuid": uuid or "",
            "pay": int(pay or 0), "jito": int(jito or 0),
            "fees": int(fees or 0),
            "bid": int(pay or 0) + int(jito or 0) + int(fees or 0),
            "cost": int(cost or 0), "n": int(n or 0),
            "reason": reason or ""})
    out = []
    for i in sorted(rounds_):
        d = rounds_[i]
        d["offers"].sort(key=lambda z: z["at"])
        anchor = d["opened"] or (d["offers"][0]["at"] if d["offers"] else "")
        d["anchor"] = anchor
        win_at, win_b = "", ""
        for o in d["offers"]:
            o["t_ms"] = _ms(anchor, o["at"]) if anchor else None
            o["chosen"] = o["uuid"] in chosen
            if o["chosen"]:
                win_at, win_b = o["at"], o["builder"]
        d["won_by"] = win_b
        d["chosen_ms"] = _ms(anchor, win_at) if anchor and win_at else None
        d["sealed_ms"] = _ms(anchor, d["sealed"]) if anchor and d["sealed"] else None
        d["builders"] = sorted({o["builder"] for o in d["offers"]})
        out.append(d)
    return {"slot": slot, "rounds": out}


def slot_timeline_ours(ch, slot, connector=""):
    """The same shape as slot_timeline, from bifrost_miniblocks + the
    builder's round events, for a slot the relay's store never saw.

    The anchor of each round is our builder's own `round_started`; the
    winner is the `winner` row's uuid, which names one of our offers only
    when we won -- otherwise the round shows as won by "another builder",
    because our store never learns who."""
    _, mb = ch(
        "SELECT kind, index_in_slot AS idx, local_builder_id, toString(ts), "
        "  uuid, ifNull(reward, 0), ifNull(execution_cost, 0), order_count, "
        "  won_by_us FROM bifrost_miniblocks "
        "WHERE ts >= now() - INTERVAL 7 DAY "
        f"  AND slot = {int(slot)}"
        + (f" AND connector_identity = '{connector}'" if connector else "")
        + " ORDER BY ts")
    if not mb:
        return {"slot": slot, "rounds": [], "source": "ours"}
    insts = ", ".join("'%s'" % b for b in {r[2] for r in mb})
    _, ev = ch(
        "SELECT instance_id, nums['index'], event, toString(ts) "
        "FROM bifrost_events WHERE ts >= now() - INTERVAL 7 DAY "
        f"  AND instance_id IN ({insts}) AND stage = 'scheduler' "
        "  AND event IN ('round_started', 'round_winner', 'round_committed') "
        f"  AND nums['slot'] = {int(slot)} ORDER BY ts")
    opened = {}
    for inst, idx, e, t in ev:
        if e == "round_started":
            opened.setdefault(int(idx), t)
    rounds_ = {}
    winners = {}
    for kind, idx, bld, t, uuid, reward, cost, n, won in mb:
        i = int(idx)
        d = rounds_.setdefault(i, {"idx": i, "opened": opened.get(i, ""),
                                   "offers": [], "sealed": "", "tail": "",
                                   "is_last": False, "source": "ours"})
        if kind == "winner":
            winners[i] = (uuid, won == "true" or won == "1", bld)
            continue
        d["offers"].append({"at": t, "builder": bld or "", "uuid": uuid or "",
                            "pay": 0, "jito": 0, "fees": 0,
                            "bid": int(reward or 0), "cost": int(cost or 0),
                            "n": int(n or 0), "reason": "offered"})
    out = []
    for i in sorted(rounds_):
        d = rounds_[i]
        d["offers"].sort(key=lambda z: z["at"])
        anchor = d["opened"] or (d["offers"][0]["at"] if d["offers"] else "")
        d["anchor"] = anchor
        w = winners.get(i)
        win_at, win_b = "", ""
        for o in d["offers"]:
            o["t_ms"] = _ms(anchor, o["at"]) if anchor else None
            o["chosen"] = bool(w) and w[1] and o["uuid"] == w[0]
            if o["chosen"]:
                win_at, win_b = o["at"], o["builder"]
        d["won_by"] = win_b or ("another builder (not in the relay's store)"
                                if w and not w[1] else "")
        d["chosen_ms"] = _ms(anchor, win_at) if anchor and win_at else None
        d["sealed_ms"] = None
        d["builders"] = sorted({o["builder"] for o in d["offers"]})
        out.append(d)
    return {"slot": slot, "rounds": out, "source": "ours"}


# ------------------------------------------------------------- the race
#
# The same picture the slot explorer draws for an auction -- time running
# down a spine, one column per builder, one bullet per offer -- fed from
# mini_block_events_v2 instead of the dead v1 table its own query still
# reads. The renderer (app.OFFERS_JS) and its CSS are shared; only the
# data shape has to be matched.
#
# Three things v2 changed that the shape has to absorb:
#   `reward` is gone, so a bullet is sized by builder_payment + jito_tips
#   + protocol_fees, the figure the relay actually grades on.
#   `offer_best` / `offer_not_best` / `offer_rejected` are gone -- the
#   verdict is now the `reason` on the submitted row itself, so it is read
#   straight off the bid instead of joined back by uuid.
#   `deadline` is epoch on every row, so there is no deadline flag to draw.

RACE_HUES = ["#3987e5", "#c98500", "#d55181", "#008300"]
RACE_SHAPES = ["circle", "square", "diamond", "triangle", "plus", "bar"]
RACE_OTHER = "#6b7f96"


def _epoch_ms(t):
    try:
        return int(dt.datetime.fromisoformat(t[:26]).timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def race_data(tl):
    """slot_timeline output -> the shape OFFERS_JS expects."""
    out = {}
    for rnd in tl["rounds"]:
        if not rnd["offers"]:
            continue
        bids, chosen = [], None
        for o in rnd["offers"]:
            ms = _epoch_ms(o["at"])
            bids.append({"t": ms, "b": o["builder"], "r": o["bid"],
                         "o": o["n"], "u": o["uuid"],
                         "v": o["reason"].replace("_", " ")})
            if o["chosen"]:
                chosen = {"b": o["builder"], "r": o["bid"], "o": o["n"],
                          "t": ms, "u": o["uuid"]}
        out[rnd["idx"]] = {
            "round": rnd["idx"],
            "opened": _epoch_ms(rnd["anchor"]) or bids[0]["t"],
            "deadline": None,          # epoch on every v2 row, so not drawn
            "bids": bids, "chosen": chosen,
            "sealed": _epoch_ms(rnd["sealed"]) if rnd["sealed"] else None}
    return out


def race_html(tl):
    """The legend, the data blob and the mount point. The drawing is done
    by OFFERS_JS, which the page loads once and re-invokes after each
    expansion."""
    data = race_data(tl)
    if not data:
        return ""
    builders = sorted({b["b"] for r in data.values() for b in r["bids"]})
    ours = next((b for b in builders if _is_ours(b)), "")
    style = {}
    for i, b in enumerate(builders):
        style[b] = {"c": RACE_HUES[i] if i < len(RACE_HUES) else RACE_OTHER,
                    "s": RACE_SHAPES[i % len(RACE_SHAPES)]}
    legend = "".join(
        f'<span class="obuilder"><i class="om {style[b]["s"]}" '
        f'style="background:{style[b]["c"]}"></i>{html.escape(b)}'
        + ('<b class="ousp">ours</b>' if _is_ours(b) else "")
        + "</span>" for b in builders)
    blob = json.dumps({"rounds": list(data.values()), "style": style,
                       "ours": ours}, separators=(",", ":"))
    n = sum(len(r["bids"]) for r in data.values())
    return (
        '<div class="racewrap"><div class="dtlhead">the race'
        f'<span class="note">{n:,} offers across {len(data)} rounds '
        "&middot; time runs down, one column per builder, one bullet per "
        "offer &middot; hover a bullet for the bid and the order count"
        "</span></div>"
        f'<div class="olegend">{legend}'
        '<span class="omark"><i class="omk ohalo2"></i>highest bid</span>'
        '<span class="omark"><i class="omk owon2"></i>chosen by the relay'
        "</span></div>"
        '<div class="ozoomall"><span>zoom every round</span>'
        '<span id="offzoomall"></span>'
        '<span class="dim">stretches time only &mdash; offers land '
        "milliseconds apart at the close</span></div>"
        '<div id="offwrap"></div>'
        f'<script id="offdata" type="application/json">{blob}</script>'
        "</div>")



def _tl_window(offers):
    """Which offers to actually draw: the opening, the close, and always
    the one that WON.

    Showing a flat first-N is the wrong cut. A round is hundreds of rungs
    and the decisive one is at the end -- on a measured round the chosen
    offer was #247 of 249, so a 80-row head cut hid the only row anyone
    opens the page for. Head and tail with the middle elided keeps both
    the ramp-up and the finish; the chosen row is spliced back in if it
    somehow falls in the gap."""
    n = len(offers)
    if n <= TL_SHOW:
        return list(offers)
    half = TL_SHOW // 2
    head, tail = offers[:half], offers[-half:]
    out = head + [None] + tail
    if not any(o["chosen"] for o in head + tail):
        win = next((o for o in offers if o["chosen"]), None)
        if win is not None:
            out = head + [None, win, None] + tail
    return out


def _tl_csv(slot, rnd):
    lines = [",".join(TL_COLS)]
    for o in rnd["offers"]:
        lines.append(",".join(_csv_cell(v) for v in (
            slot, rnd["idx"],
            "" if o["t_ms"] is None else f"{o['t_ms']:.1f}",
            o["builder"], "yes" if _is_ours(o["builder"]) else "no",
            "yes" if o["chosen"] else "no", o["bid"], o["pay"], o["jito"],
            o["fees"], o["cost"], o["n"], o["reason"], o["uuid"], o["at"])))
    return "\n".join(lines)


def timeline_html(d):
    """The race first, then the same offers as numbers underneath."""
    if not d["rounds"]:
        return ('<div class="dmeta">the relay recorded no offer for this '
                "slot in the window</div>")
    out = [race_html(d)]
    for rnd in d["rounds"]:
        n = len(rnd["offers"])
        head = (f'<div class="rhead">round {rnd["idx"]} &middot; '
                f"{n:,} offers from {len(rnd['builders'])} builders")
        if rnd["won_by"]:
            cls = "y" if _is_ours(rnd["won_by"]) else "n"
            head += (' &middot; won by <b class="land ' + cls + '">'
                     + html.escape(rnd["won_by"]) + "</b>")
        else:
            head += ' &middot; <span class="dim">no winner</span>'
        if rnd["chosen_ms"] is not None:
            head += f' &middot; chosen at <b>+{rnd["chosen_ms"]:.0f} ms</b>'
        if rnd["sealed_ms"] is not None:
            head += f' &middot; sealed +{rnd["sealed_ms"]:.0f} ms'
        if rnd["is_last"]:
            head += ' &middot; <span class="dim">last round</span>'
        head += " " + _copy_raw(_tl_csv(d["slot"], rnd),
                                f"copy {n:,} as CSV") + "</div>"
        if rnd["anchor"]:
            head += ('<div class="selbid">opened '
                     f'{html.escape(rnd["anchor"][11:23])} &middot; offsets '
                     "are from round_opened, relay clock both sides</div>")
        out.append(head)
        span = max((o["t_ms"] or 0) for o in rnd["offers"]) or 1
        rowsh = []
        for o in _tl_window(rnd["offers"]):
            if o is None:                       # the elided middle
                rowsh.append('<tr class="tlgap"><td colspan=7>'
                             "&middot;&middot;&middot;</td></tr>")
                continue
            pct = max(0.0, min(100.0, 100.0 * (o["t_ms"] or 0) / span))
            mine = _is_ours(o["builder"])
            rowsh.append(
                "<tr%s><td class=\"n\">%s</td>"
                '<td class="%s">%s</td><td class="n">%s</td>'
                '<td class="n">%s</td><td class="n">%s</td>'
                '<td>%s</td><td class="tlbar"><i style="width:%.1f%%"></i>'
                "</td></tr>" % (
                    ' class="tlwin"' if o["chosen"] else "",
                    "&mdash;" if o["t_ms"] is None else f"+{o['t_ms']:.0f}",
                    "ours" if mine else "",
                    html.escape(o["builder"])
                    + (' <span class="tlstar">chosen</span>'
                       if o["chosen"] else ""),
                    f"{o['bid']:,}", f"{o['cost']:,}", f"{o['n']:,}",
                    html.escape(o["reason"]), pct))
        out.append('<table class="otab tltab"><tr><th>t +ms</th>'
                   "<th>builder</th><th>bid</th><th>exec cost</th>"
                   "<th>orders</th><th>graded</th><th></th></tr>"
                   + "".join(rowsh) + "</table>")
        if n > TL_SHOW:
            out.append(f'<div class="more">{n - TL_SHOW:,} offers elided '
                       "from the middle &mdash; the CSV has all of them"
                       "</div>")
    return "".join(out)


def _copy_raw(text, label):
    payload = html.escape(text, quote=True).replace("\n", "&#10;")
    return (f'<span class="cpv cpcsv" data-c="{payload}" role="button" '
            f'tabindex="0" title="copy as CSV">{label}</span>')


def timeline_table_html(r, slots, shown, total, ours_re, slot_ref=None):
    """The slot list for the timeline metric: who OFFERED, not who won."""
    if r["err"]:
        return (f'<div class="herr" style="margin:20px 28px">'
                f'{html.escape(r["err"])}</div>')
    if not slots:
        return ('<div class="aqhead">No builder submitted an offer in '
                f'<b>{html.escape(r["label"])}</b>.</div>')
    cap = ""
    if total > shown:
        cap = (f' &middot; <span class="aqcap">showing the newest {shown:,} '
               f"of {total:,} slots</span>")
    reg = r["region"]
    note = ""
    if reg != "all":
        note = f' &middot; <b>{html.escape(REGIONS[reg])}</b> only'
    if r.get("connector"):
        lab = r.get("connector_label") or r["connector"][:10] + "\u2026"
        note += (f' &middot; validator <b>{html.escape(lab)}</b> '
                 f'<span class="m">{copy_val(r["connector"])}</span> only')
    if not any(_is_ours(b["builder"]) for srow in slots
               for v in srow["rounds"].values() for b in v):
        note += (' &middot; <span class="aqcap">no Astralane builder offers '
                 "here</span>")
    nobody = sum(1 for srow in slots if not any(
        b for v in srow["rounds"].values() for b in v))
    what = (f"{shown:,} slots led"
            f' &middot; <b class="redn">{nobody:,} with no offer from any builder</b> '
            '<span class="aqcap">(red)</span>'
            if r.get("connector") else f"{shown:,} slots with offers")
    out = [f'<div class="aqhead"><b>{html.escape(r["label"])}</b> &mdash; '
           f"{what}{note}{cap}. Click a slot for the "
           "offers in arrival order, timed from when the relay opened the "
           "round.</div>",
           '<table class="atab2"><tr><th>slot</th><th class="n">rounds</th>'
           '<th class="n">offers</th><th>builders</th>'
           '<th class="n">best bid</th></tr>'
           '<tbody id="aqbody" data-metric="timeline" '
           'data-frag="/tech/offer-timelines/slot" '
           f'data-region="{html.escape(r["region"])}">']
    for srow in slots:
        sl, rs = srow["slot"], srow["rounds"]
        offers = sum(b["offers"] for v in rs.values() for b in v)
        blds = sorted({b["builder"] for v in rs.values() for b in v})
        best = max((b["bid"] for v in rs.values() for b in v), default=0)
        tags = "".join(
            f'<span class="wtag{" us" if ours_re and ours_re.match(b) else ""}">'
            f"{html.escape(b)}</span>" for b in blds)
        led = srow.get("led") or {}
        # Red = the validator led this slot and NO builder at all offered
        # into it. The title says how we know it led: rounds the relay
        # opened, a block on chain, or both.
        cls = "srow" + (" noours" if r.get("connector") and not blds else "")
        if led.get("ours_store"):
            won = sum(1 for v in rs.values() for b in v if b.get("won"))
            tags += (f' <span class="wtag own" title="the relay\'s store has '
                     "nothing for this slot; this is from bifrost_miniblocks"
                     f'">own relay{" &middot; won " + str(won) if won else ""}'
                     "</span>")
        why = ""
        if r.get("connector") and not blds:
            why = (f'{led.get("rounds_opened", 0)} round(s) opened'
                   + (", block produced" if led.get("produced") else
                      ", no block on chain yet") + ", nobody offered")
            tags = '<span class="dim">&mdash;</span>'
        out.append(
            f'<tr class="{cls}" data-slot="{sl}"'
            + (f' title="{html.escape(why)}"' if why else "") + ">"
            f'<td class="m">{copy_val(str(sl))}{slot_age_html(sl, slot_ref)}</td>'
            f'<td class="n">{len(rs)}</td><td class="n">{offers:,}</td>'
            f"<td>{tags}</td><td class=\"n\">{best:,}</td></tr>"
            f'<tr class="drow" id="d{sl}" hidden><td colspan=5>'
            '<div class="dbox"></div></td></tr>')
    out.append("</tbody></table>")
    return "".join(out)



# ------------------------------------------------ the whole range, to CSV
#
# The page expands ONE slot at a time because that is how it is read. This
# does the identical work across every slot in the range and writes the
# result to a file, which is a different problem: 508 slots is ~671,000
# order rows, so the orders are streamed a chunk at a time and only the
# misses -- a small fraction -- are ever held.
#
# It also never touches the gattaca proxy. That credential is capped at
# 2,000 queries an hour and is shared with every other tool; the Grafana
# datasource reads both relay tables just as well and is on its own quota.

EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "cache", "accuracy")
EXPORT_COLS = ["slot", "round", "winner", "connector", "order_index", "type",
               "signature", "sigprefix", "bundle_id", "bundle_tx_index",
               "held", "landed_slot", "landed_failed", "validator_reason",
               "validator_instances"]
# Slots per relay round trip. 30 measured at 2.19s for 39,623 rows; the
# win over a bigger chunk is latency only, and a bigger one widens the
# ingested_at window that makes the query fast in the first place.
EXPORT_SLOT_CHUNK = 30
EXPORT_MAX_SLOTS = 3000
BLOCK_WORKERS = 8


def _chunk_window(rows, pad_lo=2, pad_hi=5):
    ats = [r["at"] for r in rows if r.get("at")]
    if not ats:
        return None, None
    try:
        st = [dt.datetime.fromisoformat(a[:26]) for a in ats]
    except ValueError:
        return None, None
    return (min(st) - dt.timedelta(minutes=pad_lo),
            max(st) + dt.timedelta(minutes=pad_hi))


def _stale_mark(lag):
    if not lag:
        return ""
    old = (dt.datetime.now(dt.UTC).replace(tzinfo=None)
           - lag).total_seconds() > STALE_AFTER_MIN * 60
    return lag.strftime("%Y-%m-%d %H:%M UTC") if old else ""


def scan_misses(relay_q, ch, rpc_url, r, on_step=lambda _m: None):
    """Every unlanded winner order across the whole range -> a CSV file.

    Same definition as the per-slot expansion, deliberately: an order is a
    miss when the winning miniblock named it and the block for that slot
    does not contain its signature. The relay's own not_included_reason
    would be cheaper but it is a different claim, and the number on the
    page would stop matching the number in the file.
    """
    on_step("reading the chosen rounds")
    rounds_, _shown, total = rounds(relay_q, r, cap=EXPORT_MAX_SLOTS,
                                    region=r.get("region", "all"))
    if not rounds_:
        raise RuntimeError("the relay recorded no chosen round in this range")
    lag = relay_tx_lag(relay_q)
    by_slot = {}
    for x in rounds_:
        by_slot.setdefault(x["slot"], []).append(x)
    slots = sorted(by_slot)
    truncated = max(0, total - len(slots))

    miss, n_orders, no_block, failed = [], 0, [], []
    undecided, sources = 0, set()
    for i in range(0, len(slots), EXPORT_SLOT_CHUNK):
        part = slots[i:i + EXPORT_SLOT_CHUNK]
        on_step(f"slots {i + 1}-{i + len(part)} of {len(slots):,} "
                f"&middot; {len(miss):,} misses so far")
        rws = [x for sl in part for x in by_slot[sl]]
        # blocks first: their prefix map is what lets the fallback put a
        # signature back on an order our store names only by prefix
        with cf.ThreadPoolExecutor(max_workers=BLOCK_WORKERS) as pool:
            blocks = dict(zip(part, pool.map(
                lambda sl: _block_safe(rpc_url, sl), part)))
        pmaps = {sl: block_prefixes(blocks[sl][0]) for sl in part
                 if blocks[sl][0] is not None}
        flat = {}
        for mp in pmaps.values():
            flat.update(mp)
        orders, src = order_contents(
            relay_q, ch, [x["uuid"] for x in rws if x["uuid"]],
            _chunk_window(rws), flat)
        sources.add(src)
        for sl in part:
            sigs, err = blocks[sl]
            if err:
                failed.append(sl)
                continue
            if sigs is None:
                no_block.append(sl)
            pm = pmaps.get(sl)
            for rnd in by_slot[sl]:
                for it in orders.get(rnd["uuid"], []):
                    n_orders += 1
                    if not it.get("prefix") and it["sig"]:
                        it["prefix"] = sigprefix(it["sig"])
                    if not it["prefix"]:
                        # an unresolvable bundle ref: undecidable, and
                        # calling it a miss would invent one
                        undecided += 1
                        continue
                    if pm is not None and it["prefix"] in pm:
                        continue
                    it["slot"] = sl
                    it["round"] = rnd["idx"]
                    it["winner"] = rnd["winner"]
                    it["connector"] = rnd["connector"]
                    miss.append(it)

    if miss:
        on_step(f"{len(miss):,} misses &middot; did they land elsewhere")
        try:
            status = landed_elsewhere(rpc_url, sorted({x["sig"] for x in miss}))
        except Exception:
            status = {}
        on_step(f"{len(miss):,} misses &middot; reading the validator events")
        vres = validator_results(ch, sorted({_vkey(x) for x in miss}),
                                 *_chunk_window(rounds_))
        held = our_prefixes_many(ch, {x["slot"] for x in miss})
        for x in miss:
            x["elsewhere"] = status.get(x["sig"], UNKNOWN)
            x["validator"] = vres.get(_vkey(x), [])
            x["ours"] = x["prefix"] in held.get(x["slot"], ())

    on_step("writing the file")
    miss.sort(key=lambda x: (x["slot"], x["round"], x["order_index"]))
    os.makedirs(EXPORT_DIR, exist_ok=True)
    name = ("misses_%s_%s.csv"
            % (re.sub(r"[^0-9a-zA-Z]+", "-", r["label"]).strip("-"),
               dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")))
    path = os.path.join(EXPORT_DIR, name)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(EXPORT_COLS)
        for x in miss:
            lslot, lerr = _chain_csv(x)
            vr = x.get("validator") or []
            w.writerow([
                x["slot"], x["round"], x["winner"], x["connector"],
                x["order_index"], x["type"], x["sig"], x["prefix"],
                x["bundle_id"],
                "" if x["bundle_tx_index"] is None else x["bundle_tx_index"],
                "yes" if x.get("ours") else "no", lslot, lerr,
                " ".join(sorted({v["reason"] for v in vr})),
                " ".join(sorted({v["instance"] for v in vr}))])
    return {"file": name, "path": path, "rows": len(miss),
            "slots": len(slots), "rounds": len(rounds_),
            "orders": n_orders, "truncated": truncated,
            "undecided": undecided,
            "source": "+".join(sorted(sources)) if sources else "",
            "stale": _stale_mark(lag),
            "no_block": len(no_block), "block_failed": len(failed),
            "label": r["label"],
            "bytes": os.path.getsize(path)}


def scan_selection(relay_q, ch, rpc_url, r, on_step=lambda _m: None):
    """The selection diff across every slot in the range -> a CSV file.

    Same chunking as scan_misses and the same reason for it: the contents
    of four miniblocks per round over five hundred slots is far too much to
    hold, and the ingested_at bound is what keeps each relay read fast."""
    on_step("reading the chosen rounds")
    rounds_, _shown, total = rounds(relay_q, r, cap=EXPORT_MAX_SLOTS,
                                    region=r.get("region", "all"))
    if not rounds_:
        raise RuntimeError("the relay recorded no chosen round in this range")
    lag = relay_tx_lag(relay_q)
    by_slot = {}
    for x in rounds_:
        by_slot.setdefault(x["slot"], []).append(x)
    slots = sorted(by_slot)
    out, no_block = [], []
    for i in range(0, len(slots), EXPORT_SLOT_CHUNK):
        part = slots[i:i + EXPORT_SLOT_CHUNK]
        on_step(f"slots {i + 1}-{i + len(part)} of {len(slots):,} "
                f"&middot; {len(out):,} rows so far")
        rws = [x for sl in part for x in by_slot[sl]]
        with cf.ThreadPoolExecutor(max_workers=BLOCK_WORKERS) as pool:
            blocks = dict(zip(part, pool.map(
                lambda sl: _block_safe(rpc_url, sl)[0], part)))
        no_block += [sl for sl in part if blocks[sl] is None]
        out += selection_diff(relay_q, ch, rws, _chunk_window(rws),
                              blocks=blocks)

    on_step("writing the file")
    out.sort(key=lambda x: (x["slot"], x["round"], x["our_builder"],
                            x["verdict"], str(x["order_index"])))
    os.makedirs(EXPORT_DIR, exist_ok=True)
    name = ("selection_%s_%s.csv"
            % (re.sub(r"[^0-9a-zA-Z]+", "-", r["label"]).strip("-"),
               dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")))
    path = os.path.join(EXPORT_DIR, name)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(SEL_COLS)
        for x in out:
            w.writerow([_sel_flat(x, c) for c in SEL_COLS])
    vc = collections.Counter(x["verdict"] for x in out)
    return {"file": name, "path": path, "rows": len(out),
            "slots": len(slots), "rounds": len(rounds_),
            "truncated": max(0, total - len(slots)),
            "no_block": len(no_block), "block_failed": 0,
            "lost_rows": sum(1 for x in out if not x["we_won"]),
            "us_inc": vc["us_inc_not_titan"],
            "titan_inc": vc["titan_inc_not_us"],
            "no_offer": vc["no_accepted_offer"],
            "stale": _stale_mark(lag),
            "label": r["label"], "bytes": os.path.getsize(path)}


def _block_safe(rpc_url, slot):
    """block_signatures that reports its failure instead of raising.

    One unreachable slot must not void a scan of five hundred -- but it
    must not be silently counted as "nothing landed" either, which is why
    the error comes back rather than None."""
    try:
        sigs, _bt = block_signatures(rpc_url, slot)
        return sigs, ""
    except Exception as exc:
        return None, str(exc)[:120]


_jobs = {}
_jobs_lock = threading.Lock()
JOB_TTL_S = 3600


SCANS = {"misses": None, "selection": None}      # filled below


def start_export(relay_q, ch, rpc_url, r, kind="misses"):
    fn = SCANS.get(kind) or scan_misses
    jid = f"a{int(time.time() * 1000):x}"
    job = {"state": "running", "step": "starting", "started": time.time(),
           "result": None, "error": None}
    with _jobs_lock:
        for k, v in list(_jobs.items()):
            if time.time() - v["started"] > JOB_TTL_S:
                _drop(k)
        _jobs[jid] = job

    def go():
        try:
            job["result"] = fn(relay_q, ch, rpc_url, r,
                               lambda m: job.__setitem__("step", m))
            job["result"]["kind"] = kind
            job["state"], job["step"] = "done", "done"
        except Exception as exc:
            job["state"], job["error"] = "error", str(exc)[:300]
    threading.Thread(target=go, daemon=True).start()
    return jid


def _drop(jid):
    """Expire a job AND the file it wrote -- the dir is a scratch area, not
    an archive, and nothing else ever cleans it."""
    job = _jobs.pop(jid, None)
    res = (job or {}).get("result") or {}
    if res.get("path"):
        try:
            os.remove(res["path"])
        except OSError:
            pass


SCANS["misses"] = scan_misses
SCANS["selection"] = scan_selection


def export_status(jid):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        return {"state": "unknown",
                "error": "no such export -- it may have expired, run it again"}
    out = {"state": job["state"], "step": job["step"],
           "elapsed": int(time.time() - job["started"])}
    if job["state"] == "error":
        out["error"] = job["error"]
    if job["state"] == "done":
        out["result"] = job["result"]
    return out


def export_file(jid):
    """-> (filename, bytes) for a finished export, or (None, None).

    Looked up BY JOB ID, never by a name from the query string: the client
    cannot name a path, so there is no traversal to defend against."""
    with _jobs_lock:
        job = _jobs.get(jid)
    res = (job or {}).get("result") or {}
    if not res.get("path") or not os.path.exists(res["path"]):
        return None, None
    with open(res["path"], "rb") as fh:
        return res["file"], fh.read()



# ------------------------------------------------------------------ render

CSS = """
.cpcsv{margin-left:10px;padding:1px 8px;border:1px solid #2b3a4b;
  border-radius:5px;font-size:10px;color:#6b7f96;cursor:pointer;
  text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.cpcsv:hover,.cpcsv:focus{color:#5eead4;border-color:#14b8a655;outline:none}
.aqform{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap;
  padding:16px 28px;border-bottom:1px solid #1e2937}
.aqform label[hidden]{display:none}
.aqform label{display:flex;flex-direction:column;gap:4px;color:#61748b;
  font-size:10.5px;text-transform:uppercase;letter-spacing:.04em}
.aqform input,.aqform select{background:#0c141b;border:1px solid #22303f;
  border-radius:6px;color:#cfe0f0;padding:6px 10px;
  font:12px ui-monospace,Menlo,monospace}
.aqform input:focus,.aqform select:focus{outline:none;border-color:#5eead4}
.aqform button{background:#0f766e;border:1px solid #14b8a6;color:#eafffb;
  border-radius:6px;padding:7px 16px;font-size:12px;cursor:pointer}
.aqform button:hover{background:#14b8a6}
.aqhead{padding:14px 28px 0;color:#8fa6bf;font-size:12.5px}
.aqhead b{color:#dbe4ee}
.aqcap{color:#fbbf24}
.atab2{margin:12px 28px 40px;border-collapse:collapse;width:calc(100% - 56px);
  max-width:1400px;font-size:12.5px}
.atab2 th{text-align:left;color:#6b7f96;font-size:10.5px;font-weight:600;
  text-transform:uppercase;letter-spacing:.04em;padding:0 12px 7px 0;
  border-bottom:1px solid #1e2937}
.atab2 th.n,.atab2 td.n{text-align:right}
.atab2 td{padding:7px 12px 7px 0;border-bottom:1px solid #16202b;color:#cfe0f0}
.atab2 td.m{font:11.5px ui-monospace,Menlo,monospace}
.srow{cursor:pointer}
.srow:hover{background:#101a23}
.srow td:first-child::before{content:"\\25B8 ";color:#5eead4}
.srow.open td:first-child::before{content:"\\25BE "}
/* a slot the picked validator led that no builder at all offered into */
.srow.noours td{background:#2a0e10}
.srow.noours td:first-child{color:#fca5a5}
.srow.noours td:first-child::before{color:#fca5a5}
.srow.noours:hover td{background:#3a1317}
.redn{color:#fca5a5}
.otab td.neg{color:#fbbf24}
.sage{margin-left:8px;font-size:10.5px;font-variant-numeric:tabular-nums}
.wtag.own{color:#fbbf24;border-color:#fbbf2455}
.nextlead{margin:10px 28px 0;color:#8fa6bf;font-size:12.5px}
.nextlead b{color:#cfe0f0}
.nextlead .nl-eta{color:#5eead4;font:600 13px ui-monospace,Menlo,monospace}
.nextlead .nl-eta.past{color:#fbbf24}
.ours{color:#5eead4;font-weight:600}
.wtag{font:10.5px ui-monospace,Menlo,monospace;padding:1px 7px;
  border-radius:4px;border:1px solid #22303f;color:#8fa6bf}
.wtag.us{border-color:#14b8a6;color:#5eead4;background:#0f766e22}
.drow>td{padding:0 0 0 0;background:#0a1118}
.dbox{padding:12px 28px 16px 20px}
.dmeta{color:#6b7f96;font-size:11.5px;margin-bottom:9px}
.otab{border-collapse:collapse;width:100%;font-size:11.5px}
.otab th{text-align:left;color:#61748b;font-size:10px;text-transform:uppercase;
  padding:0 10px 5px 0;border-bottom:1px solid #1e2937}
.otab td{padding:4px 10px 4px 0;border-bottom:1px solid #121a23;
  font:11px ui-monospace,Menlo,monospace;color:#b9ccdf}
.land{font-weight:650}
.land.y{color:#5eead4}
.land.n{color:#fca5a5}
.land.u{color:#6b7f96}
.aqexp{display:flex;align-items:center;gap:12px;margin-left:auto}
.aqexp button{background:#0c141b;border:1px solid #22303f;color:#8fa6bf;
  border-radius:6px;padding:7px 14px;font-size:12px;cursor:pointer}
.aqexp button:hover:not(:disabled){border-color:#14b8a6;color:#5eead4}
.aqexp button:disabled{opacity:.55;cursor:default}
.aqexpmsg{font-size:11.5px;color:#6b7f96;max-width:460px}
.aqexpmsg a{color:#5eead4;font-weight:650}
.aqexpmsg b{color:#dbe4ee}
.aqexpmsg.bad{color:#fca5a5}
.selsec{margin-top:18px;border-top:1px solid #1e2937;padding-top:12px}
.seltitle{color:#dbe4ee;font-size:12px;font-weight:650;margin-bottom:8px;
  text-transform:lowercase;letter-spacing:.02em}
.selblk{margin:10px 0 14px;padding-left:10px;border-left:2px solid #1e2937}
.selhead{color:#8fa6bf;font-size:11.5px;font-weight:600}
.selbid{color:#6b7f96;font-size:11px;margin:2px 0 4px;
  font:11px ui-monospace,Menlo,monospace}
.selcnt{color:#8fa6bf;font-size:11px;margin:3px 0 6px}
.vd{font:10px ui-monospace,Menlo,monospace;padding:1px 6px;border-radius:4px;
  border:1px solid #22303f}
.vd.us{color:#5eead4;border-color:#14b8a655}
.vd.ti{color:#fbbf24;border-color:#fbbf2455}
.vd.na{color:#6b7f96}
.rwt{color:#dbe4ee}
.tltab td{vertical-align:middle}
.tltab .tlbar{width:28%;padding-right:0}
.tltab .tlbar i{display:block;height:5px;border-radius:3px;
  background:#22303f;min-width:2px}
.racewrap{margin:4px 0 18px}
.tlwin{background:#0f766e18}
.tlgap td{color:#3b4a5c;text-align:center;letter-spacing:4px;padding:2px 0}
.tlwin .tlbar i{background:#14b8a6}
.tlstar{font:9px ui-monospace,Menlo,monospace;color:#5eead4;
  border:1px solid #14b8a655;border-radius:4px;padding:0 5px;margin-left:6px;
  text-transform:uppercase;letter-spacing:.05em}
.sc{font:10px ui-monospace,Menlo,monospace;color:#fca5a5;margin-right:6px;
  white-space:nowrap}
.sc i{color:#8fa6bf;font-style:normal;margin-left:4px}
.scopen{cursor:pointer;border-bottom:1px dotted #2b3a4b}
.scopen:hover,.scopen:focus{border-bottom-color:#5eead4;outline:none}
/* A hovered value GLOWS, not just recolours: these are 88-char
   signatures in an 11.5px table, and the eye needs help finding the one
   under the pointer. Hovering any value in a bundle row gives the same
   glow to every value in every row of that bundle, so it reads as one
   thing (the class is set by the hover handler in ACCURACY_JS). */
.cpv:hover,.cpv:focus,.cpv.glow{color:#5eead4;background:#0f766e22;
  text-shadow:0 0 6px #14b8a6cc,0 0 14px #14b8a666}
.scopen::after{content:" \\25B8";color:#5eead4;font-size:9px}
.scopen.open::after{content:" \\25BE"}
.sxrow>td{background:#0a1118;padding:0}
.sxbox{padding:8px 10px 10px;border-left:2px solid #22303f;margin:4px 0 8px}
.sxh{color:#8fa6bf;font-size:11px;margin-bottom:6px}
.sxev{margin-bottom:7px}
.sxmeta{color:#6b7f96;font-size:10.5px}
.sxmeta b{color:#fca5a5}
.sxpre{margin:2px 0 0;padding:6px 8px;background:#0c141b;
  border:1px solid #16202b;border-radius:5px;color:#b9ccdf;
  font:10.5px ui-monospace,Menlo,monospace;white-space:pre-wrap;
  word-break:break-all}
.nowhere{color:#fca5a5}
.vr{font-weight:650}
.vr.y{color:#5eead4}
.vr.n{color:#fbbf24}
.rhead{margin:10px 0 5px;color:#8fa6bf;font-size:11.5px;font-weight:600}
.more{color:#6b7f96;font-size:11px;font-style:italic;margin-top:6px}
.more .cpsm{font-style:normal;margin-left:8px}
.bxo{margin-left:6px;font-size:10px;color:#8fa6bf}
"""

ACCURACY_JS = r'''
(function(){
  // Expansion is lazy on purpose: the landing check for a whole range is
  // ~328k rows of relay data, against ~900 for one slot.
  var tbody = document.getElementById('aqbody');
  if (!tbody) return;
  tbody.addEventListener('click', function(ev){
    var tr = ev.target.closest('tr.srow');
    if (!tr) return;
    var slot = tr.getAttribute('data-slot');
    var det = document.getElementById('d' + slot);
    if (!det) return;
    var open = tr.classList.toggle('open');
    det.hidden = !open;
    var box = det.querySelector('.dbox');
    if (!open || box.getAttribute('data-loaded') === '1') return;
    box.setAttribute('data-loaded', '1');
    box.innerHTML = '<div class="dmeta">fetching the block and the winning miniblocks…</div>';
    var metric = tbody.getAttribute('data-metric') || 'landing';
    var region = tbody.getAttribute('data-region') || 'all';
    // the endpoint comes from the table, not from here: builder accuracy
    // and offer timelines share this handler and expand against
    // different fragments
    var frag = tbody.getAttribute('data-frag') || '/tech/accuracy/slot';
    fetch(frag + '?slot=' + encodeURIComponent(slot) +
          '&metric=' + encodeURIComponent(metric) +
          '&region=' + encodeURIComponent(region))
      .then(function(r){ return r.text(); })
      .then(function(t){
        box.innerHTML = t;
        // the panel ships its data as a <script type="application/json">,
        // which innerHTML inserts but never executes -- so the renderer
        // has to be called by hand once the node is in the DOM
        if (window.__renderOffers) window.__renderOffers();
      })
      .catch(function(e){
        box.setAttribute('data-loaded', '0');
        box.innerHTML = '<div class="dmeta">could not load: ' + e + '</div>';
      });
  });
})();

(function(){
  // The mode dropdown decides which inputs mean anything, so it also
  // decides which are on screen. Server-rendered correct already; this
  // only keeps it right when the select changes without a reload.
  var sel = document.querySelector('.aqform select[name=mode]');
  if (!sel) return;
  function sync(){
    [].forEach.call(document.querySelectorAll('.aqform .qf'), function(el){
      el.hidden = el.getAttribute('data-m') !== sel.value;
    });
  }
  sel.addEventListener('change', sync);
  document.addEventListener('click', function(ev){
    var b = ev.target.closest && ev.target.closest('.selmore');
    if (!b) return;
    ev.preventDefault();
    var note = b.closest('.more'), tab = note && note.previousElementSibling;
    if (tab) tab.querySelectorAll('tr.selx').forEach(function(tr){ tr.hidden = false; });
    if (note) note.remove();
  });
  // The next-leader-slot countdown. The server sent its own clock
  // (data-now) with the ETA, so the browser's clock only has to be
  // consistent with itself: remaining = eta - (Date.now() - now0) + skew.
  var nl = document.querySelector('.nextlead[data-eta]');
  if (nl){
    var eta0 = parseFloat(nl.getAttribute('data-eta')),
        now0 = parseFloat(nl.getAttribute('data-now')),
        skew = Date.now() - now0,            // includes network + clock skew
        slotms = parseFloat(nl.getAttribute('data-slotms')) || 200,
        etaEl = nl.querySelector('.nl-eta'), atEl = nl.querySelector('.nl-at');
    var due = Date.now() + eta0 - Math.max(0, Math.min(skew, 5000));
    function pad(n){ return (n < 10 ? '0' : '') + n; }
    function tick(){
      var left = due - Date.now();
      if (etaEl){
        etaEl.textContent = (left < 0 ? '-' : '') + (Math.abs(left) / 1000).toFixed(1) + ' s';
        etaEl.classList.toggle('past', left < 0);
        if (left < -4 * slotms) etaEl.textContent += ' (window over; reload for the next)';
      }
      if (atEl){ var d = new Date(due); atEl.textContent = pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()); }
    }
    tick(); setInterval(tick, 100);
  }
  var reg = document.querySelector('.aqform select[name=region]'),
      con = document.querySelector('.aqform select[name=connector]');
  if (reg && con) reg.addEventListener('change', function(){ con.value = ''; });
  sync();
})();

(function(){
  // The raw attrs/nums panel. Delegated on document, not on the slot
  // table: these rows are injected by the expansion fetch long after the
  // page loaded, and they sit inside a tr.drow, which closest('tr.srow')
  // walks straight past -- so the two handlers never collide.
  document.addEventListener('click', function(ev){
    var el = ev.target.closest && ev.target.closest('.scopen');
    if (!el) return;
    ev.preventDefault(); ev.stopPropagation();
    // The panel is the row right after the opener's own row. Prefer that
    // to the id lookup: it cannot pick another row's panel even if two
    // ids ever collide again.
    var tr = el.closest('tr'), row = tr && tr.nextElementSibling;
    if (!(row && row.classList.contains('sxrow') &&
          row.id === el.getAttribute('data-sx')))
      row = document.getElementById(el.getAttribute('data-sx'));
    if (!row) return;
    row.hidden = !row.hidden;
    el.classList.toggle('open', !row.hidden);
  });
  document.addEventListener('keydown', function(ev){
    if (ev.key !== 'Enter' && ev.key !== ' ') return;
    var el = ev.target.closest && ev.target.closest('.scopen');
    if (!el) return;
    ev.preventDefault(); el.click();
  });
})();

(function(){
  // The export runs the same scan the expansions do, over every slot in
  // the range at once, so it is a background job with a progress line
  // rather than a request anyone waits on.
  var btns = [document.getElementById('aqexport'),
              document.getElementById('aqexportsel')].filter(Boolean);
  var msg = document.getElementById('aqexpmsg');
  if (!btns.length || !msg) return;
  var timer = null;

  function say(t, bad){
    msg.innerHTML = t;
    msg.className = 'aqexpmsg' + (bad ? ' bad' : '');
  }
  function stop(){ if (timer) { clearInterval(timer); timer = null; }
                   btns.forEach(function(b){ b.disabled = false; }); }

  function poll(job){
    fetch('/api/accuracy/status?job=' + encodeURIComponent(job))
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.state === 'running'){ say(d.step + ' &middot; ' + d.elapsed + 's'); return; }
        stop();
        if (d.state !== 'done'){ say(d.error || 'the export failed', true); return; }
        var x = d.result, bits = [];
        if (x.kind === 'selection'){
          bits.push('<b>' + x.rows.toLocaleString() + '</b> differences over ' +
                    x.slots.toLocaleString() + ' slots, ' +
                    x.rounds.toLocaleString() + ' rounds');
          bits.push('titan_inc_not_us ' + x.titan_inc.toLocaleString() +
                    ' / us_inc_not_titan ' + x.us_inc.toLocaleString());
          bits.push(x.lost_rows.toLocaleString() + ' in rounds we lost');
          if (x.no_offer) bits.push(x.no_offer.toLocaleString() +
                                    ' rounds with no accepted offer');
        } else {
          bits.push('<b>' + x.rows.toLocaleString() + '</b> misses from ' +
                    x.orders.toLocaleString() + ' orders over ' +
                    x.slots.toLocaleString() + ' slots');
        }
        if (x.stale) bits.push('<b>relay tx log stale since ' + x.stale + '</b>');
        if (x.truncated) bits.push(x.truncated.toLocaleString() + ' slots past the cap');
        if (x.no_block)  bits.push(x.no_block.toLocaleString() + ' slots had no block');
        if (x.block_failed) bits.push(x.block_failed.toLocaleString() + ' unreadable');
        say(bits.join(' &middot; ') +
            ' &middot; <a href="/tech/accuracy/export.csv?job=' +
            encodeURIComponent(job) + '">download CSV</a>');
      })
      .catch(function(e){ stop(); say('' + e, true); });
  }

  btns.forEach(function(btn){ btn.addEventListener('click', function(){
    btns.forEach(function(b){ b.disabled = true; });
    say('starting…');
    // The page's OWN query string, so the file covers exactly the range
    // on screen -- not whatever is currently typed into the inputs.
    var qs = window.location.search || '?';
    fetch('/api/accuracy/export' + qs + '&kind=' + btn.getAttribute('data-kind'))
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.error || !d.job){ stop(); say(d.error || 'could not start', true); return; }
        say('starting…');
        poll(d.job);
        timer = setInterval(function(){ poll(d.job); }, 1500);
      })
      .catch(function(e){ stop(); say('' + e, true); });
  }); });
})();
(function(){
  // Hover any copyable value in a row that belongs to a bundle, and every
  // value in every row of that bundle (within the same table) takes the
  // hover colour too. Delegated on the document: the per-slot tables are
  // fetched into the page after load, so nothing is wired per table.
  var lit = [];
  function clear(){
    lit.forEach(function(el){ el.classList.remove('glow'); });
    lit = [];
  }
  function light(v){ if (!v.classList.contains('glow')){ v.classList.add('glow'); lit.push(v); } }
  document.addEventListener('mouseover', function(ev){
    var el = ev.target.closest && ev.target.closest('.cpv');
    if (!el || !el.closest('.otab')) return;
    clear();
    // every other occurrence of this exact value, page-wide: the same
    // signature or bundle id shows up once per our-builder group
    var val = el.getAttribute('data-c');
    if (val) document.querySelectorAll('.otab .cpv[data-c]').forEach(function(v){
      if (v.getAttribute('data-c') === val) light(v);
    });
    // and, in a bundle row, every value of every row of that bundle
    var tr = el.closest('tr[data-b]');
    if (tr){
      var b = tr.getAttribute('data-b');
      document.querySelectorAll('.otab tr[data-b] .cpv').forEach(function(v){
        if (v.closest('tr').getAttribute('data-b') === b) light(v);
      });
    }
  });
  document.addEventListener('mouseout', function(ev){
    var el = ev.target.closest && ev.target.closest('.cpv');
    if (el && el.closest('.otab')) clear();
  });
})();

'''


def _sol(lamports):
    return f"{lamports / 1e9:.6f}"


def form_html(r, path, metric=True, region=False, connectors=None):
    """The query controls. Built as a list, not one implicitly concatenated
    string: half the parts are conditional on the metric, and a `+` in the
    middle of an implicit concatenation is a syntax error waiting to
    happen."""
    m = r["mode"]
    sel = lambda v: " selected" if m == v else ""
    # Only the inputs the chosen mode actually reads are shown. With all
    # five on screen it is natural to type a from/to while the mode is
    # still "last N hours" -- the server then correctly ignores them and
    # the filter looks broken. Hidden rather than dropped, so switching
    # back keeps what was typed, and the server-rendered state is already
    # right before any JS runs.
    def fld(mode_, body):
        hide = "" if mode_ == m else " hidden"
        return f'<label class="qf" data-m="{mode_}"{hide}>{body}</label>'

    out = [f'<form class="aqform" method="get" action="{path}">',
           "<label>mode<select name=mode>",
           f'<option value="hours"{sel("hours")}>last N hours</option>',
           f'<option value="range"{sel("range")}>time range</option>',
           f'<option value="slot"{sel("slot")}>one slot</option>',
           "</select></label>",
           fld("hours", "hours<input name=hours size=5 "
               f'value="{html.escape(r["hours"])}">'),
           fld("range", 'from (UTC)<input name=from size=17 '
               f'placeholder="2026-10-13 09:00" value="{html.escape(r["from"])}">'),
           fld("range", 'to (UTC)<input name=to size=17 placeholder="now" '
               f'value="{html.escape(r["to"])}">'),
           fld("slot", 'slot<input name=slot size=12 placeholder="456429791" '
               f'value="{html.escape(r["slot_in"])}">'),
           _metric_select(r["metric"]) if metric else "",
           _region_select(r["region"]) if region else "",
           (_connector_select(r.get("connector", ""), connectors)
            if connectors is not None else ""),
           "<button type=submit>run</button>",
           # Outside the inputs, because it acts on the range already run,
           # not on whatever half-edited values sit in the boxes. One button
           # per section actually on screen.
           '<span class="aqexp">']
    if not metric:
        out.append('<span class="aqexpmsg">every round carries its own '
                   "copy-as-CSV</span>")
    if metric and r["metric"] in (LANDING, BOTH):
        out.append('<button type=button id="aqexport" data-kind="misses">'
                   "export won-but-not-landed as CSV</button>")
    if metric and r["metric"] in (SELECTION, BOTH):
        out.append('<button type=button id="aqexportsel" '
                   'data-kind="selection">export winner-vs-ours as CSV'
                   "</button>")
    out.append('<span id="aqexpmsg" class="aqexpmsg"></span></span></form>')
    return "".join(out)


def table_html(r, rows, shown, total, ours_re, slot_ref=None):
    if r["err"]:
        return f'<div class="herr" style="margin:20px 28px">{html.escape(r["err"])}</div>'
    if not rows:
        return ('<div class="aqhead">No round was chosen in '
                f'<b>{html.escape(r["label"])}</b>. The relay records a '
                "round_chosen only when a round actually closed with a "
                "winner.</div>")
    cap = ""
    if total > shown:
        cap = (f' &middot; <span class="aqcap">showing the newest {shown:,} '
               f"of {total:,} slots</span>")
    reg = r.get("region", "all")
    note = ""
    if reg != "all":
        note = f' &middot; <b>{html.escape(REGIONS[reg])}</b> only'
    if r.get("connector"):
        lab = r.get("connector_label") or r["connector"][:10] + "\u2026"
        note += (f' &middot; validator <b>{html.escape(lab)}</b> '
                 f'<span class="m">{copy_val(r["connector"])}</span> only')
    if reg != "all":
        if not any(ours_re and ours_re.match(x["winner"] or "") for x in rows):
            note += (' &middot; <span class="aqcap">no Astralane builder '
                     "won a round here</span>")
    head = (f'<div class="aqhead"><b>{html.escape(r["label"])}</b> &mdash; '
            f"{len(rows):,} rounds across {shown:,} slots{note}{cap}. "
            "Click a slot to fetch its block and check what landed.</div>")
    by_slot = {}
    for x in rows:
        by_slot.setdefault(x["slot"], []).append(x)
    body = []
    for slot in sorted(by_slot, reverse=True):
        rs = sorted(by_slot[slot], key=lambda z: z["idx"])
        wins = []
        for z in rs:
            us = bool(ours_re and ours_re.match(z["winner"]))
            wins.append(f'<span class="wtag{" us" if us else ""}">r{z["idx"]}'
                        f' {html.escape(z["winner"] or "?")}</span>')
        pay = sum(z["payment"] for z in rs)
        orders = sum(z["order_count"] for z in rs)
        conn = rs[0]["connector"]
        body.append(
            f'<tr class="srow" data-slot="{slot}">'
            f'<td class="m">{copy_val(slot)}{slot_age_html(slot, slot_ref)}</td>'
            f'<td class="n m">{len(rs)}</td>'
            f'<td>{" ".join(wins)}</td>'
            f'<td class="n m">{orders:,}</td>'
            f'<td class="n m">{_sol(pay)}</td>'
            f'<td class="m dim">{html.escape(conn[:8])}&hellip;</td></tr>'
            f'<tr class="drow" id="d{slot}" hidden><td colspan=6>'
            '<div class="dbox" data-loaded="0"></div></td></tr>')
    return (head + '<table class="atab2"><thead><tr><th>slot</th>'
            "<th class=n>rounds</th><th>winner by round</th>"
            "<th class=n>orders</th><th class=n>paid (SOL)</th>"
            "<th>connector</th></tr></thead>"
            '<tbody id="aqbody" data-frag="/tech/accuracy/slot" '
            f'data-region="{html.escape(r.get("region", "all"))}" '
            f'data-metric="{html.escape(r["metric"])}">'
            f'{"".join(body)}</tbody></table>')


SIG_CAP = 60


def _chain_csv(it):
    """(landed_slot, landed_failed) for the CSV. Blank means unknown."""
    st = it.get("elsewhere", UNKNOWN)
    if st == UNKNOWN:
        return "", ""
    if st is None:
        return "nowhere", ""
    return st["slot"], "yes" if st["err"] else "no"


def _chain_cell(it):
    """Did this order land in some OTHER block.

    It is not in this slot's block -- that is why the row is here. So the
    question left is whether it landed at all, and a signature that is
    nowhere in ledger history is a different story from one that landed a
    slot later."""
    st = it.get("elsewhere", UNKNOWN)
    if st == UNKNOWN:
        return '<span class="dim">?</span>'
    if st is None:
        return '<span class="nowhere">nowhere</span>'
    return (copy_val(str(st["slot"]))
            + (' <span class="land n">failed</span>' if st["err"] else ""))


def _val_cell(it):
    """What the validator said, grouped by reason.

    The instances behind each reason go in the tooltip rather than the
    cell: which builders agree matters when you are reading one row, and
    is noise down a column of sixty."""
    rs = it.get("validator") or []
    if not rs:
        return '<span class="dim">&mdash;</span>'
    by = {}
    for x in rs:
        by.setdefault(x["reason"], set()).add(x["instance"])
    parts = []
    for reason, insts in sorted(by.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        cls = "y" if reason == "included" else "n"
        parts.append(
            f'<span class="vr {cls}" title="{html.escape(", ".join(sorted(insts)))}">'
            f"{html.escape(reason)}</span>"
            + (f' <span class="dim">&times;{len(insts)}</span>'
               if len(insts) > 1 else ""))
    return " ".join(parts)




def detail_html(d):
    """Only the orders that were in the winning miniblock and did not land.

    The full list was every order with a landed flag, and it was nearly all
    noise: a winning miniblock normally lands in full, so the table was
    hundreds of rows of "landed" hiding the handful that matter. Here every
    row shown IS the finding, so the column saying so is gone."""
    if d["err"]:
        return f'<div class="dmeta">block unavailable: {html.escape(d["err"])}</div>'
    if d["skipped"]:
        meta = ('<div class="dmeta"><b>No block for this slot</b> &mdash; it '
                "was skipped, so nothing in any winning miniblock landed. "
                "Every order below is a miss for that reason alone.</div>")
    else:
        when = (dt.datetime.fromtimestamp(d["blockTime"], dt.UTC)
                .strftime("%Y-%m-%d %H:%M:%S UTC") if d["blockTime"] else "?")
        meta = (f'<div class="dmeta">Block has {d["block_n"]:,} signatures '
                f"&middot; {when}</div>")
    if SRC_NOTE.get(d.get("source")):
        meta += ('<div class="dmeta aqcap">'
                 f'{SRC_NOTE[d["source"]]}</div>')
    if d.get("chain_err"):
        meta += ('<div class="dmeta aqcap">the "landed elsewhere" lookup '
                 f'failed: {html.escape(d["chain_err"])}</div>')
    out = [meta]
    for blk in d["rounds"]:
        r, items = blk["round"], blk["orders"]
        miss = [i for i in items if i["landed"] is False]
        bundles = {i["bundle_id"] for i in items if i["bundle_id"]}
        miss_b = {i["bundle_id"] for i in miss if i["bundle_id"]}
        head = (f'<div class="rhead">round {r["idx"]} &mdash; '
                f'{html.escape(r["winner"] or "?")} &middot; {len(items):,} '
                f"orders ({r['order_count']:,} refs) &middot; ")
        if not items:
            out.append(head + "</div>"
                       '<div class="more">the relay kept no transaction rows '
                       "for this miniblock</div>")
            continue
        if not miss:
            out.append(head + '<b class="land y">all landed</b></div>')
            continue
        pct = 100 * len(miss) / len(items)
        out.append(
            head + f'<b class="land n">{len(miss):,} did not land</b> '
            f"({pct:.1f}%)"
            + (f' &middot; {len(miss_b)}/{len(bundles)} bundles affected'
               if bundles else "")
            + " " + copy_csv(d.get("slot", ""), r["idx"], miss,
                             f"copy {len(miss):,} as CSV")
            + "</div>")
        rowsh = []
        for i in miss[:SIG_CAP]:
            # The member index stays OUTSIDE the copy control: copying a
            # bundle id should give the id, not the id plus "#3".
            bid = ((copy_val(i["bundle_id"], i["bundle_id"][:12] + "\u2026")
                    + (f" #{i['bundle_tx_index']}"
                       if i["bundle_tx_index"] is not None else ""))
                   if i["bundle_id"] else '<span class="dim">&mdash;</span>')
            battr = (f' data-b="{html.escape(i["bundle_id"], quote=True)}"'
                     if i["bundle_id"] else "")
            rowsh.append(
                f'<tr{battr}><td class="n">{i["order_index"]}</td>'
                f'<td>{html.escape(i["type"])}</td>'
                f'<td>{copy_val(i["sig"])}</td>'
                f'<td>{copy_val(i["prefix"])}</td>'
                f"<td>{bid}</td>"
                f'<td class="ours">{"ours" if i["ours"] else ""}</td>'
                f"<td>{_chain_cell(i)}</td>"
                f"{_recv_cells(i)}"
                f"<td>{_val_cell(i)}</td></tr>")
        out.append('<table class="otab"><tr><th>#</th><th>type</th>'
                   "<th>signature</th><th>sigprefix</th><th>bundle</th>"
                   "<th>held</th><th>landed elsewhere</th>"
                   '<th class="n" title="ms from this round&#39;s round_started to the builder admitting the order (scheduler/received), same clock; negative = it arrived before the round opened">recv +open</th>'
           '<th class="n" title="ms from the builder admitting the order to this round&#39;s round_winner">win &minus;recv</th>'
                   "<th>validator said</th></tr>"
                   + "".join(rowsh) + "</table>")
        if len(miss) > SIG_CAP:
            out.append(f'<div class="more">{len(miss) - SIG_CAP:,} more '
                       "misses not shown</div>")
    return "".join(out)
