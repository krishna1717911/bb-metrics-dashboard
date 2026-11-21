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
import math
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


# The validator's leader schedule on Staking Facilities' Firedancer explorer:
# the next slots it leads, with the client and skip history -- quicker than
# our own countdown when the question is "is this validator actually up".
STAKINGFAC_URL = "https://fd-mainnet.stakingfacilities.com/leaderSchedule?searchText="


def stakingfac_link(pubkey):
    if not PUBKEY_RE.match(pubkey or ""):
        return ""
    url = html.escape(STAKINGFAC_URL + pubkey, quote=True)
    return (f' <a class="extbtn" href="{url}" target="_blank" rel="noopener" '
            f'title="leader schedule for {html.escape(pubkey)} on Staking '
            f'Facilities">staking fac</a>')


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
JITO_TIP_SET = frozenset(JITO_TIP_ACCOUNTS)


def weigh(fee, jito, builder, bps):
    """-> (tip, reward) the way the relay scores them: Jito tips at the
    connector's Jito weight (bps / 10000), builder tips and fees at face
    value. See score_sql."""
    tip = int(jito) * int(bps) // 10000 + int(builder)
    return tip, int(fee) + tip


def block_rewards(rpc_url, slot):
    """-> {signature: {"fee", "tip", "jito", "builder", "err"}} for one
    block. `tip` = jito + builder, unweighted; see weigh().

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
        tip = jito = 0
        for i, k in enumerate(keys):
            pk = k.get("pubkey") if isinstance(k, dict) else k
            if pk in TIP_ACCOUNTS and i < len(pre) and i < len(post):
                # credits only -- a tip account that also pays a fee would
                # otherwise net its own cost out of someone else's tip
                got = max(0, int(post[i]) - int(pre[i]))
                tip += got
                if pk in JITO_TIP_SET:
                    jito += got
        rec = {"fee": int(meta.get("fee") or 0), "tip": tip, "jito": jito,
               "builder": tip - jito,
               "err": meta.get("err") is not None}
        for sig in inner.get("signatures") or []:
            out[sig] = rec
    return out


def block_tx_bytes(rpc_url, slot, consumed=None):
    """-> {signature: raw transaction bytes} for every tx in the block.
    `consumed`, a dict, also receives {signature: meta.computeUnitsConsumed}
    -- the CU each landed transaction actually burned.

    The same block fetched with base64 bodies, for what block_rewards'
    accounts view cannot give: the transaction's own compute-unit limit.
    Measured on 460494455: 3.4 MB / 0.6 s against 2.9 MB / 0.5 s for the
    accounts view. This is the only source of bytes for a landed bundle
    member -- our relayer never sees most of those (3 of 17 on a measured
    round)."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [
        int(slot), {"encoding": "base64", "transactionDetails": "full",
                    "rewards": False, "maxSupportedTransactionVersion": 1}]}
    req = urllib.request.Request(
        rpc_url, headers={"Content-Type": "application/json"},
        data=json.dumps(body).encode())
    with urllib.request.urlopen(req, timeout=90) as resp:
        d = json.loads(resp.read())
    if d.get("error"):
        if d["error"].get("code") in (-32009, -32007):
            return {}
        raise RuntimeError(str(d["error"].get("message", ""))[:160])
    out = {}
    for tx in (d.get("result") or {}).get("transactions") or []:
        try:
            raw = base64.b64decode((tx.get("transaction") or [""])[0])
            sig = txprice.parse(raw)["sig"]
            out[sig] = raw
            cu = (tx.get("meta") or {}).get("computeUnitsConsumed")
            if consumed is not None and cu is not None:
                consumed[sig] = int(cu)
        except Exception:
            continue
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


def market_ticks(relay_q, window=None):
    """SigPrefixes of the relay's market ticks opened in `window`.

    Since 2026-10-23 12:57Z sol-relay-2-lat-ams signs one tick per round
    and sends it to every builder as the round's mandatory prefix, so our
    offers carry it at order_index 0 (Titan's don't; the relay does not
    enforce it yet). It is the relay's transaction, not a builder's
    choice, and most never land (460768261/0: 4ot9Yzam..., nowhere on
    chain), so left in it reads as a miss and as a diff row on every
    round we bid. Keyed on the prefix so it filters both content sources.
    The column is Nullable(FixedString(64)) -- base58Encode needs
    assumeNotNull."""
    if window and window[0] and window[1]:
        f = "%Y-%m-%d %H:%M:%S"
        bound = (f"timestamp >= '{window[0] - dt.timedelta(minutes=2):{f}}' "
                 f"AND timestamp <= '{window[1] + dt.timedelta(minutes=2):{f}}'")
    else:
        bound = "timestamp > now() - INTERVAL 14 DAY"
    _, rows = relay_q(
        "SELECT DISTINCT base58Encode(substring(assumeNotNull("
        "  market_tick_signature), 1, 16)) "
        f"FROM relay.mini_block_events_v2 WHERE {bound} "
        "  AND event IN ('round_opened', 'tail_opened') "
        "  AND market_tick_signature IS NOT NULL")
    return {r[0] for r in rows if r and r[0]}


def order_contents(relay_q, ch, uuids, window, pmap=None):
    """-> (contents, source). The relay if it has them, our store if not.

    Automatic rather than a switch: relay.mini_block_transactions stopped
    taking rows at 2026-10-15 10:47:46 UTC with no warning and the other
    three relay tables carried on, so "is it up" is a per-query fact, not
    a setting anyone can be expected to keep current.

    The relay's market ticks are dropped from either source -- see
    market_ticks()."""
    uuids = [u for u in uuids if u]
    if not uuids:
        return {}, RELAY_SRC
    try:
        got = winner_orders(relay_q, uuids, window)
    except Exception:
        got = {}
    src = RELAY_SRC
    if not got:
        got, src = ours_orders(ch, uuids, pmap, window), OURS_SRC
    try:
        ticks = market_ticks(relay_q, window)
    except Exception:
        ticks = set()
    if ticks:
        got = {u: [i for i in items
                   if (i.get("prefix") or (sigprefix(i["sig"]) if i.get("sig")
                                           else "")) not in ticks]
               for u, items in got.items()}
    return got, src


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
BID = "builder_payment + jito_tips + protocol_fees"     # raw, for display

# -------------------------------------------------- how the relay ranks
#
# Not by the raw sum. MiniBlockReward::score (relay messages.rs) is
#   protocol_fees + jito_tips * weight / 10000 + builder_payment
# with weight = the connector's Jito weight net of the relay's 600 bps Jito
# fee. Cogent's is 400 bps, so 2.25M of Jito tips scored 89,835 and a
# lower raw offer won (460514195 r0: AMS_2 raw 5.65M / score 3.49M lost to
# TITAN_AMS_3 raw 4.42M / score 4.14M). relay.connector_handshakes records
# the weight in effect; it changed for every connector at 2026-10-15
# ~10:09Z (10,000 -> 9,400; Cogent 1,000 -> 400), so each offer is scored
# with the weight in force at its own timestamp. Unknown connectors get
# 10,000 - 600.
DEFAULT_JITO_WEIGHT = 9400
WEIGHT_TTL_S = 600
_WEIGHTS = {"at": 0.0, "segs": {}}
_WEIGHTS_LOCK = threading.Lock()


def jito_weights(relay_q):
    """connector -> [(from_timestamp, bps)], oldest first, cached."""
    with _WEIGHTS_LOCK:
        if _WEIGHTS["segs"] and time.time() - _WEIGHTS["at"] < WEIGHT_TTL_S:
            return _WEIGHTS["segs"]
    _, rows = relay_q(
        "SELECT connector_identity, toString(timestamp), jito_tip_weight_bps "
        "FROM relay.connector_handshakes "
        "WHERE timestamp > now() - INTERVAL 60 DAY "
        "ORDER BY connector_identity, timestamp")
    segs = {}
    for c, t, w in rows:
        if not c:
            continue
        lst = segs.setdefault(c, [])
        if not lst or lst[-1][1] != int(w):
            lst.append((t, int(w)))
    with _WEIGHTS_LOCK:
        _WEIGHTS.update(at=time.time(), segs=segs)
    return segs


def round_weight(segs, rnd):
    """The Jito weight the relay used for this round's offers."""
    return weight_at(segs, rnd.get("connector", ""),
                     rnd.get("sub_at") or rnd.get("at") or "")


def weight_at(segs, connector, ts):
    """The weight `connector` had at `ts` (relay timestamp string)."""
    lst = (segs or {}).get(connector)
    if not lst:
        return DEFAULT_JITO_WEIGHT
    w = lst[0][1]
    for start, bps in lst:
        if ts and start <= ts:
            w = bps
    return w


def score_of(fees, jito, pay, bps):
    return int(fees) + int(jito) * int(bps) // 10000 + int(pay)


def score_sql(relay_q):
    """SQL for the relay's score of a mini_block_events_v2 row."""
    try:
        segs = jito_weights(relay_q)
    except Exception:
        segs = {}
    conds = []
    for c, lst in segs.items():
        for i in range(len(lst) - 1, -1, -1):
            start, bps = lst[i]
            cond = f"connector_identity = '{_q(c)}'"
            if i:
                cond += f" AND timestamp >= '{_q(start)}'"
            conds.append(f"{cond}, {bps}")
    w = (f"multiIf({', '.join(conds)}, {DEFAULT_JITO_WEIGHT})" if conds
         else str(DEFAULT_JITO_WEIGHT))
    return ("(protocol_fees + intDiv(jito_tips * toUInt64(" + w
            + "), 10000) + builder_payment)")
# A rejected rung never competed and the relay keeps no transaction rows for
# it, so picking one as "our offer" silently diffs the winner against an
# EMPTY set -- which reads as "Titan included 240 things we didn't" when the
# truth is "our best-paying rung arrived after the round closed". Only these
# three reasons mean the offer was actually graded.
ACCEPTED = "('best', 'not_best', 'tail')"

SEL_COLS = ["kind", "received_by_us",
            "fee", "tip", "reward", "jito_raw", "jito_weight_bps",
            "wait_us", "titan_off_ms", "titan_off_err_ms", "titan_off_on",
            "titan_first_by",
            "slot", "round", "connector", "won_by", "we_won",
            "our_builder", "titan_builder", "builder_id", "verdict",
            "type", "signature", "sigprefix", "bundle_id",
            "bundle_tx_index", "order_index", "landed",
            "our_bid", "titan_bid", "bid_gap",
            "our_cost", "titan_cost", "our_orders", "titan_orders",
            "our_reason", "titan_reason", "is_last",
            "sched_event", "sched_reason", "sched_detail",
            "our_uuid", "trace_on", "trace_at_our_offer", "trace_then",
            "trace_window_closed", "trace_path",
            "similar_bundles", "similar_at_our_offer", "overlap_bundles"]


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
    score = score_sql(relay_q)
    for i in range(0, len(slots), ENTITY_BATCH):
        lst = ", ".join(str(x) for x in slots[i:i + ENTITY_BATCH])
        _, part = relay_q(
            "SELECT slot, index_in_slot AS idx, builder_id, "
            "  toString(timestamp) AS t, "
            "  toString(mini_block_uuid) AS uuid, "
            f"  {score} AS bid, execution_cost AS cost, "
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


def scheduler_notes(ch, entities, lo=None, hi=None, kind=None):
    """Why the scheduler turned an order away -> {entity: [note, ...]}.

    `kind` narrows to one entity_kind ('tx' / 'bundle'): a member's
    SigPrefix as a tx entity is the plain copy of that transaction, the
    one that arrived on its own, not the bundle it is also inside.

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
    kf = f"  AND entity_kind = '{_q(kind)}' " if kind else ""
    for i in range(0, len(ents), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(e) for e in ents[i:i + ENTITY_BATCH])
        try:
            _, rows = ch(
                "SELECT entity, toString(ts) AS at, instance_id, event, "
                "  reason, toJSONString(attrs) AS a, toJSONString(nums) AS n, "
                "  entity_kind "
                f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
                f"  AND event IN ({evs}) AND entity IN ({lst}) {kf}"
                "ORDER BY entity, ts")
        except Exception:
            continue
        for ent, at, inst, ev, rsn, a, n, ek in rows:
            out.setdefault(ent, []).append({
                "at": at or "", "instance": inst or "", "event": ev or "",
                "reason": rsn or "", "attrs": a or "{}", "nums": n or "{}",
                "kind": ek or ""})
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


def _own_keys(x, by_kind):
    """The entities a row's own events are logged under. With `by_kind`
    a bundle row is its bundle id ONLY: a member's SigPrefix as a tx entity
    is that transaction arriving on its own, a different order, and hanging
    its events on the bundle row reads as if the bundle had them. Those
    live in the bundle's member panel instead."""
    if by_kind and x.get("bundle_id"):
        return (x["bundle_id"],)
    return tuple(k for k in (x.get("sigprefix"), x.get("bundle_id")) if k)


def attach_notes(ch, rows, lo=None, hi=None, by_kind=False):
    """Hang the scheduler's verdicts on each diff row, in place."""
    notes = scheduler_notes(ch, {k for x in rows
                                 for k in _own_keys(x, by_kind)}, lo, hi)
    if not notes:
        for x in rows:
            x.setdefault("sched", [])
        return rows
    for x in rows:
        got = []
        for k in _own_keys(x, by_kind):
            got += notes.get(k, [])
        got.sort(key=lambda z: z["at"])
        x["sched"] = got
    return rows


def round_marks(ch, slot, lo=None, hi=None):
    """{(instance, round): {"open": ts, "win": ts}} -- each builder
    instance's own round_started, and the ts of its own `winner` row in
    bifrost_miniblocks for this slot and round, on that instance's clock.
    round_winner stands in where the instance wrote no winner row; the
    two are ~2 us apart (460768261, every round, all three AMS
    instances)."""
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
    try:
        _, wins = ch(
            "SELECT instance_id, index_in_slot, toString(min(ts)) "
            f"FROM bifrost_miniblocks WHERE {tb}kind = 'winner' "
            f"  AND slot = {int(slot)} GROUP BY instance_id, index_in_slot")
    except Exception:
        wins = []
    for inst, idx, t in wins:
        out.setdefault((inst, int(idx)), {})["win"] = t
    return out


def received_at(ch, entities, lo=None, hi=None):
    """{entity: {instance: (first scheduler/received ts, wait_us, source)}}
    for SigPrefixes and bundle ids -- an order is logged once per instance
    at admission. wait_us is the relay's hold (nums['wait_us'], builder
    main >= #117); None when that admission did not log the key, which is
    not the same as a zero hold."""
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
                "SELECT entity, instance_id, toString(min(ts)), "
                "  argMin(if(has(mapKeys(nums), 'wait_us'), "
                "            toString(nums['wait_us']), ''), ts), "
                "  argMin(attrs['source'], ts) "
                f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
                f"  AND event = 'received' AND entity IN ({lst}) "
                "GROUP BY entity, instance_id")
        except Exception:
            continue
        for ent, inst, t, w, src in rows:
            out.setdefault(ent, {})[inst] = (
                t, int(w) if w not in ("", None) else None, src or "")
    return out


def received_events(ch, entities, lo=None, hi=None):
    """{entity: {instance: event}} -- each instance's FIRST
    scheduler/received for the entity, with its attrs and nums verbatim
    (toJSONString, see scheduler_notes) plus the three fields worth a
    column: source, route and wait_us (None when not logged)."""
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
                "SELECT entity, instance_id, toString(min(ts)), "
                "  argMin(attrs['source'], ts), argMin(attrs['route'], ts), "
                "  argMin(if(has(mapKeys(nums), 'wait_us'), "
                "            toString(nums['wait_us']), ''), ts), "
                "  argMin(toJSONString(attrs), ts), "
                "  argMin(toJSONString(nums), ts) "
                f"FROM bifrost_events WHERE {tb}stage = 'scheduler' "
                f"  AND event = 'received' AND entity IN ({lst}) "
                "GROUP BY entity, instance_id")
        except Exception:
            continue
        for ent, inst, t, src, route, w, a, n in rows:
            out.setdefault(ent, {})[inst] = {
                "at": t, "source": src or "", "route": route or "",
                "wait": int(w) if w not in ("", None) else None,
                "attrs": a or "{}", "nums": n or "{}"}
    return out


def attach_member_recv(ch, rows, marks, lo=None, hi=None):
    """On every member of a bundle row, its own scheduler/received -- the
    member's SigPrefix as an entity, i.e. the transaction arriving on its
    own (the relay's feed, mostly), not inside this bundle. `recv_alone`
    is [(instance, ms after that instance's round_started or None,
    event)], one per instance that logged it; empty = never alone.

    `sched_alone` is the same copy's later life: every SCHED_EVENTS row
    the scheduler wrote with the member's SigPrefix as a tx entity
    (entity_kind = 'tx' -- the bundle's own rows carry the bundle id and
    sit on the parent row), as [(instance, ms after that instance's
    round_started or None, note)] in time order; empty = nothing logged."""
    pfx = {m["prefix"] for x in rows for m in (x.get("members") or [])
           if m.get("prefix")}
    if not pfx:
        return rows
    got = received_events(ch, pfx, lo, hi)
    notes = scheduler_notes(ch, pfx, lo, hi, kind="tx")
    for x in rows:
        for m in x.get("members") or []:
            out = []
            for inst, ev in sorted((got.get(m.get("prefix")) or {}).items()):
                mk = marks.get((inst, int(x["round"]))) or {}
                after = _ms(mk["open"], ev["at"]) if "open" in mk else None
                out.append((inst, after, ev))
            m["recv_alone"] = out
            sched = []
            for n in notes.get(m.get("prefix")) or []:
                mk = marks.get((n["instance"], int(x["round"]))) or {}
                after = _ms(mk["open"], n["at"]) if "open" in mk else None
                sched.append((n["instance"], after, n))
            sched.sort(key=lambda z: z[2]["at"])
            m["sched_alone"] = sched
    return rows


def attach_recv(ch, rows, slot, lo=None, hi=None, round_of=lambda x: x["round"],
                prefix_of=lambda x: x.get("sigprefix"),
                bundle_of=lambda x: x.get("bundle_id"), marks=None,
                by_kind=False):
    """Hang, on each row, when the builder admitted the order relative to
    its round: {instance: (ms after round_started, ms before that
    instance's `winner` row in bifrost_miniblocks -- see round_marks)}.

    The order is looked up by its SigPrefix and, for a bundle member, by
    the bundle id (that is the entity a bundle's `received` carries); the
    earlier of the two is the admission. Both stamps are the same
    instance's clock, so no correction applies. Negative "after open"
    means it arrived before that round opened -- normal for a round that
    is not the slot's first.

    `by_kind`: a bundle row is looked up by its bundle id alone -- the
    member arriving as a plain tx is not the bundle being received (that
    copy is in the member panel, attach_member_recv)."""
    if marks is None:
        marks = round_marks(ch, slot, lo, hi)

    def keys(x):
        if by_kind and bundle_of(x):
            return ((bundle_of(x), "bundle"),)
        return ((prefix_of(x), "tx"), (bundle_of(x), "bundle"))
    ents = set()
    for x in rows:
        ents.update(k for k, _kind in keys(x) if k)
    recv = received_at(ch, ents, lo, hi)
    for x in rows:
        got = {}
        for key, kind in keys(x):
            for inst, (t, w, src) in (recv.get(key) or {}).items():
                if inst not in got or t < got[inst][0]:
                    got[inst] = (t, w, src, kind)
        # any builder instance at all, round marks or not: "did we get it"
        x["received_by_us"] = bool(got)
        # the raw admission stamp per instance, on that instance's clock --
        # titan_off puts it on the relay's clock
        x["recv_at"] = {inst: v[0] for inst, v in got.items()}
        rec = {}
        for inst, (t, w, src, kind) in got.items():
            m = marks.get((inst, int(round_of(x))))
            if not m or "open" not in m:
                continue
            after = _ms(m["open"], t)
            before = _ms(t, m["win"]) if "win" in m else None
            if after is not None:
                rec[inst] = (after, before, w, src, kind)
        x["recv"] = rec
    return rows


def _recv_cells(x, prefer=None):
    """Three cells: ms after the round opened, ms before its winner, and
    the relay's hold (wait_us) on that admission. One instance is shown --
    `prefer` (the row's own builder) if it logged the order, else the
    earliest admission -- and every instance is in the title."""
    rec = x.get("recv") or {}
    if not rec:
        return ('<td class="n dim">&mdash;</td>' * 3)
    inst = prefer if prefer in rec else min(rec, key=lambda k: rec[k][0])
    after, before, wait, _src, _kind = rec[inst]
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

    def hold(k, v):
        a, _b, w, src, kind = v
        if w is None:
            return f"{k}: no wait_us logged ({kind} via {src or '?'})"
        return (f"{k}: {w / 1000:.3f} ms hold on the {kind} via "
                f"{src or '?'}, extendable at {a + w / 1000:+.1f} ms after "
                "open")
    wtitle = html.escape("; ".join(hold(k, v) for k, v in sorted(rec.items())))
    if wait is None:
        wcell = f'<td class="n dim" title="{wtitle}">&mdash;</td>'
    else:
        wcell = (f'<td class="n m{" held" if wait else ""}" '
                 f'title="{wtitle}">{wait / 1000:.1f}</td>')
    return (cell(after, " neg" if after < 0 else " pos")
            + cell(before, " neg" if before is not None and before < 0 else "")
            + wcell)


# ---------------------------------------------------------- order traces
#
# builder_order_traces (builder #134, on AMS_1/AMS_2/MOCK since build
# d57f5721b; FRA_1 and JP_1 do not write it yet): one row per state change
# of every non-vote order the scheduler admitted, written only when the
# status, reason or round changes. `trace_seq` is a per-instance, per-run
# counter shared by every row the scheduler writes, including the
# `offer_dispatched` row for each mini-block (entity_id = its uuid), so
# "what state was this order in when that offer went out" is a sequence
# comparison on one counter -- no clock involved.
#
# The diff compares one offer of ours per round (our best as it stood when
# the winning offer was submitted). The trace answers why THAT offer did
# not carry an order: its last state at or before that offer's
# offer_dispatched row. Rows after it say what became of the order.
#
# Limits, from the builder's docs/order-tracing.md: delivery is best
# effort (a missing row does not prove the order was never received);
# included_in_prefix is not proof of offer or landing; `liveness`
# retirement is broad -- after a round it almost always means the
# winner carried the order. Rows live 7 days.

TRACE_CAP = 300                 # rows per order per instance, oldest first
TRACE_PAD = dt.timedelta(minutes=3)   # admitted well before its round
# a_only / b_only: the offer diff page, two offers picked by hand
PAIR_A, PAIR_B = "a_only", "b_only"
DIFF_VERDICTS = ("titan_inc_not_us", "us_inc_not_titan", PAIR_A, PAIR_B)

# status -> (words, colour class). Order: the pipeline.
TRACE_STATUS = {
    "check_pending": ("in check", "pend"),
    "probe_pending": ("in probe", "pend"),
    "auction_ready": ("ready, not picked", "pend"),
    "selection_skipped": ("skipped", "hold"),
    "extend_pending": ("in extend", "pend"),
    "included_in_prefix": ("in our prefix", "inc"),
    "deferred": ("deferred", "pend"),
    "branch_pending": ("branch pending", "pend"),
    "branch_accepted": ("branch accepted", "pend"),
    "removed_from_prefix": ("removed by branch switch", "bad"),
    "retired": ("retired", "bad"),
}
# what each state means, for hovers
TRACE_HELP = {
    "check_pending": "received, waiting for Check",
    "probe_pending": "a bundle waiting for Probe (simulation = queued or in "
                     "flight, target_write = #118 target-account park, "
                     "retry_cooldown = 50 ms cooldown, target_slot = pinned "
                     "to a later slot)",
    "auction_ready": "passed Check/Probe and eligible for a cut, but no cut "
                     "had taken it yet (rank: cheaper orders sort last)",
    "selection_skipped": "a cut looked at it and left it out (relay_delay = "
                         "the relay's wait_us hold not over, compute_budget "
                         "= would not fit the round's CU, signature_conflict "
                         "= shares a signature or nonce account with an "
                         "order already in, dependency_depth = deeper than "
                         "16)",
    "extend_pending": "picked and sent to the simulator in an Extend batch, "
                      "reply not back yet",
    "included_in_prefix": "the simulator applied it to our draft",
    "deferred": "parked until the winner's commit lands (needs_round)",
    "removed_from_prefix": "a branch switch cut it out of our draft",
    "retired": "finished (liveness = executed elsewhere / blockhash or "
               "nonce dead -- usually the winner carried it; check_failed, "
               "probe_failed, extend_failed = the simulator said no, error "
               "text under 'scheduler said'; retry_expired = 10 s target "
               "wait ran out; superseded; retention_horizon; "
               "branch_evaluation_rejected)",
}
# keys that are not a builder state
TRACE_SPECIAL = {
    "after": ("arrived after our offer", "na",
              "the builder admitted it only after the offer compared here "
              "had been dispatched"),
    "notrecv": ("not received here", "na",
                "no trace row on this instance: it never admitted the order"),
    "untraced": ("not traced", "na",
                 "this builder runs a build without order traces (#134)"),
    "noboundary": ("offer not in trace", "na",
                   "the offer compared here has no offer_dispatched row "
                   "(trace delivery is best effort); showing its last state"),
    "": ("", "na", ""),
}


def _trace_key(status, reason):
    return f"{status}:{reason}" if reason else status


def trace_label(key):
    """-> (words, class, hover) for a trace key ('status[:reason]' or one
    of TRACE_SPECIAL)."""
    if key in TRACE_SPECIAL:
        return TRACE_SPECIAL[key]
    st, _, rs = key.partition(":")
    words, cls = TRACE_STATUS.get(st, (st, "pend"))
    if st == "selection_skipped" and rs == "relay_delay":
        words, cls = "held by relay delay", "hold"
    elif st == "retired" and rs == "liveness":
        words = "retired"
    return ((words + (f" {rs}" if rs and not words.startswith("held") else "")),
            cls, TRACE_HELP.get(st, ""))


def order_traces(ch, keys, insts, lo, hi):
    """{(kind, id)} -> {(kind, id, instance): [row, ...]} in trace order.

    The table is sorted (instance_id, entity_kind, entity_id, run_id,
    trace_seq), so an instance + id list is an index seek, not a scan."""
    out = {}
    keys, insts = sorted(k for k in keys if k[1]), sorted(i for i in insts if i)
    if not keys or not insts:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo - TRACE_PAD:{f}}' AND ts <= '{hi:{f}}'" if lo and hi
          else "ts > now() - INTERVAL 2 HOUR")
    il = ", ".join("'%s'" % _q(i) for i in insts)
    for i in range(0, len(keys), ENTITY_BATCH):
        part = keys[i:i + ENTITY_BATCH]
        conds = []
        for kind in ("tx", "bundle"):
            ids = [e for k, e in part if k == kind]
            if ids:
                conds.append(f"(entity_kind = '{kind}' AND entity_id IN ("
                             + ", ".join("'%s'" % _q(e) for e in ids) + "))")
        _, rows = ch(
            "SELECT entity_kind, entity_id, instance_id, toString(run_id), "
            "  trace_seq, first_trace_seq, toString(ts), "
            "  ifNull(toString(slot), ''), ifNull(toString(round_index), ''), "
            "  event, status, reason, ifNull(toString(bank_slot), ''), "
            "  ifNull(toString(branch_id), ''), "
            "  ifNull(toString(checkpoint_id), ''), "
            "  ifNull(toString(branch_request_id), ''), "
            "  ifNull(toString(common_prefix_len), '') "
            f"FROM builder_order_traces WHERE instance_id IN ({il}) "
            f"  AND {tb} AND ({' OR '.join(conds)}) "
            "ORDER BY instance_id, entity_kind, entity_id, run_id, trace_seq "
            f"LIMIT {TRACE_CAP} BY instance_id, entity_kind, entity_id")
        for (k, e, inst, run, seq, first, at, sl, rd, ev, st, rs, bsl, br,
             ck, req, cpl) in rows:
            out.setdefault((k, e, inst), []).append({
                "kind": k, "run": run, "seq": int(seq), "first": int(first),
                "at": at, "slot": sl, "round": rd, "event": ev,
                "status": st, "reason": rs, "bank_slot": bsl, "branch": br,
                "checkpoint": ck, "request": req, "common": cpl})
    return out


def offer_boundaries(ch, uuids, insts, lo, hi):
    """{(instance, uuid): {run, seq, at}} -- each of our offers'
    offer_dispatched row."""
    out = {}
    uuids, insts = sorted(u for u in uuids if u), sorted(i for i in insts if i)
    if not uuids or not insts:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo - TRACE_PAD:{f}}' AND ts <= '{hi:{f}}'" if lo and hi
          else "ts > now() - INTERVAL 2 HOUR")
    il = ", ".join("'%s'" % _q(i) for i in insts)
    for i in range(0, len(uuids), ENTITY_BATCH):
        ul = ", ".join("'%s'" % _q(u) for u in uuids[i:i + ENTITY_BATCH])
        _, rows = ch(
            "SELECT instance_id, entity_id, toString(run_id), min(trace_seq), "
            "  toString(min(ts)) "
            f"FROM builder_order_traces WHERE instance_id IN ({il}) "
            f"  AND entity_kind = 'offer' AND entity_id IN ({ul}) AND {tb} "
            "GROUP BY instance_id, entity_id, run_id")
        for inst, u, run, seq, at in rows:
            out[(inst, u)] = {"run": run, "seq": int(seq), "at": at}
    return out


def traced_instances(ch, insts, lo, hi):
    """Which of these instances wrote any offer_dispatched row in the
    window -- the ones running a build with order traces."""
    insts = sorted(i for i in insts if i)
    if not insts:
        return set()
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo:{f}}' AND ts <= '{hi:{f}}'" if lo and hi
          else "ts > now() - INTERVAL 2 HOUR")
    out = set()
    for inst in insts:
        _, rows = ch(
            "SELECT 1 FROM builder_order_traces "
            f"WHERE instance_id = '{_q(inst)}' AND entity_kind = 'offer' "
            f"  AND {tb} LIMIT 1")
        if rows:
            out.add(inst)
    return out


def _states(rows):
    """Rows that set a state (order_transition and branch pending/accepted);
    branch_evaluated / probe_invalidated / ... are observations only."""
    return [r for r in rows if r["status"]]


def window_closes(ch, insts, lo, hi):
    """{instance: [(run, seq, at)]} -- each `window_closed` boundary: our
    leader window ended and every order still in flight was dropped
    without a retire row of its own."""
    out = {}
    insts = sorted(i for i in insts if i)
    if not insts:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo - TRACE_PAD:{f}}' AND ts <= '{hi:{f}}'" if lo and hi
          else "ts > now() - INTERVAL 2 HOUR")
    il = ", ".join("'%s'" % _q(i) for i in insts)
    _, rows = ch(
        "SELECT instance_id, toString(run_id), trace_seq, toString(ts) "
        f"FROM builder_order_traces WHERE instance_id IN ({il}) "
        f"  AND entity_kind = 'window' AND {tb} ORDER BY trace_seq")
    for inst, run, seq, at in rows:
        out.setdefault(inst, []).append((run, int(seq), at))
    return out


def _state_at(rows, off):
    """(key, row) -- the last state at or before the offer, or ('after',
    None) when the order only arrived later, ('', None) without rows."""
    st = _states(rows)
    if not st:
        return "", None
    if not off:
        return "noboundary", None
    before = [r for r in st if r["run"] == off["run"] and r["seq"] <= off["seq"]]
    if not before:
        return "after", None
    b = before[-1]
    return _trace_key(b["status"], b["reason"]), b


def _trace_one(tr, keys, inst, off, traced, marks, closes=()):
    """One order on one instance -> its trace record.

    `keys` in preference order (a bundle's own id before its member's
    plain-tx copy). The state at our offer comes from the first key that
    was in hand by then; if none was, from the first that has rows at all
    ("arrived after our offer"). Every key's rows go in the timeline,
    tagged by entity kind."""
    rec = {"inst": inst, "rows": [], "key": "", "then": "", "offer": off,
           "by": "", "open": (marks or {}).get("open"),
           "win": (marks or {}).get("win"), "closed": None}
    if inst not in traced:
        rec["key"] = "untraced"
        return rec
    per = [(k, tr.get((k[0], k[1], inst)) or []) for k in keys]
    rec["rows"] = sorted((r for _k, rs in per for r in rs),
                         key=lambda r: (r["at"], r["seq"]))
    have = [(k, rs) for k, rs in per if _states(rs)]
    if not have:
        rec["key"] = "notrecv"
        return rec
    pick = None
    for k, rs in have:
        key, row = _state_at(rs, off)
        if key not in ("after", ""):
            pick = (k, rs, key, row)
            break
    if pick is None:
        k, rs = have[0]
        key, row = _state_at(rs, off)
        pick = (k, rs, key, row)
    k, rs, key, row = pick
    rec["key"], rec["at_row"], rec["by"] = key, row, k[0]
    rec["truncated"] = len(rs) >= TRACE_CAP
    last = _states(rs)[-1]
    rec["then"] = _trace_key(last["status"], last["reason"])
    if last["status"] != "retired":
        nxt = [c for c in closes if c[0] == last["run"] and c[1] > last["seq"]]
        if nxt:
            rec["closed"] = nxt[0]
    return rec


def attach_traces(ch, rows, lo=None, hi=None, marks_by_slot=None):
    """On every diff row, `trace`: the order's builder_order_traces history
    on the row's own builder, and its state when the offer compared here
    went out. A bundle row is traced as the bundle (entity_kind = bundle)
    and nothing else; its members get `trace` of their own, the member as
    a plain tx, shown in the member panel. Every similar bundle
    (attach_similar) gets its own `trace` as well."""
    want = [x for x in rows if x.get("verdict") in DIFF_VERDICTS]
    if not want:
        return rows
    keys, insts, uuids = set(), set(), set()
    for x in want:
        insts.add(x.get("our_builder"))
        uuids.add(x.get("our_uuid"))
        if x.get("bundle_id"):
            keys.add(("bundle", x["bundle_id"]))
        if x.get("sigprefix"):
            keys.add(("tx", x["sigprefix"]))
        for m in x.get("members") or []:
            if m.get("prefix"):
                keys.add(("tx", m["prefix"]))
        for s in x.get("similar") or []:
            keys.add(("bundle", s["id"]))
    insts.discard(None)
    traced = traced_instances(ch, insts, lo, hi)
    tr = order_traces(ch, keys, traced, lo, hi)
    ob = offer_boundaries(ch, uuids, traced, lo, hi)
    wc = window_closes(ch, traced, lo, hi)
    for x in want:
        inst = x.get("our_builder")
        mk = ((marks_by_slot or {}).get(x["slot"]) or {}).get(
            (inst, int(x["round"])))
        off = ob.get((inst, x.get("our_uuid")))
        ks = ([("bundle", x["bundle_id"])] if x.get("bundle_id")
              else [("tx", x["sigprefix"])] if x.get("sigprefix") else [])
        x["trace"] = _trace_one(tr, ks, inst, off, traced, mk,
                                wc.get(inst, ()))
        for m in x.get("members") or []:
            if m.get("prefix") and "trace" not in m:
                m["trace"] = _trace_one(tr, [("tx", m["prefix"])], inst, off,
                                        traced, mk, wc.get(inst, ()))
        for s in x.get("similar") or []:
            s["trace"] = _trace_one(tr, [("bundle", s["id"])], inst, off,
                                    traced, mk, wc.get(inst, ()))
    return rows


# ------------------------------------------------------- similar bundles
#
# A bundle id is the hash of its packets plus whatever the gateway folds
# in (target slot, target accounts, a dropping mask), so the same
# transactions often reach us under a DIFFERENT id than the one in the
# winner: another searcher's wrapper, a resend, our router's GBX-lane
# copy. Matching on ids calls those "never received". These are the
# bundles we did receive (scheduler/received, entity_kind = bundle, whose
# `signatures` array is the member list) that carry every signature of
# the diff row -- the bundle's members, or a plain tx's one signature --
# with their own trace on our builder. Bundles that share only some of
# them are kept apart as overlaps.

SIM_BATCH = 100            # signatures per query; the SQL travels in the URL
SIM_PAD = dt.timedelta(minutes=1)
SIM_SHOW = 12              # similar bundles listed per row; the rest counted


def received_bundles_with(ch, sigs, lo, hi):
    """{bundle_id: {"sigs": [...], "recv": {instance: first ts}}} for every
    bundle we admitted that contains any of these signatures."""
    out = {}
    sigs = sorted({s for s in sigs if s})
    if not sigs:
        return out
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"ts >= '{lo - SIM_PAD:{f}}' AND ts <= '{hi:{f}}'" if lo and hi
          else "ts > now() - INTERVAL 2 HOUR")
    for i in range(0, len(sigs), SIM_BATCH):
        lst = ", ".join("'%s'" % _q(s) for s in sigs[i:i + SIM_BATCH])
        _, rows = ch(
            "SELECT entity, instance_id, toString(min(ts)), "
            "  arrayStringConcat(argMin(signatures, ts), ' ') "
            f"FROM bifrost_events WHERE {tb} AND stage = 'scheduler' "
            "  AND event = 'received' AND entity_kind = 'bundle' "
            f"  AND hasAny(signatures, [{lst}]) "
            "GROUP BY entity, instance_id")
        for ent, inst, at, sg in rows:
            b = out.setdefault(ent, {"sigs": [], "recv": {}})
            if not b["sigs"]:
                b["sigs"] = [z for z in (sg or "").split() if z]
            b["recv"][inst] = at
    return out


def _row_sig_list(x):
    if x.get("members"):
        return [m["sig"] for m in x["members"] if m.get("sig")]
    return [x["signature"]] if x.get("signature") else []


def attach_similar(ch, rows, lo=None, hi=None, marks_by_slot=None):
    """On every diff row: `similar` = bundles we received, other than the
    row's own, that contain ALL of the row's signatures; `overlap` = ones
    that contain only some. Each with its members, which of them are not
    the row's, and when each of our instances admitted it (ms from that
    instance's round_started)."""
    want = [x for x in rows if x.get("verdict") in DIFF_VERDICTS]
    got = received_bundles_with(
        ch, {s for x in want for s in _row_sig_list(x)}, lo, hi)
    for x in want:
        mine = _row_sig_list(x)
        if not mine:
            x["similar"], x["overlap"], x["similar_why"] = [], [], "no signature"
            continue
        want_set = set(mine)
        mk = (marks_by_slot or {}).get(x["slot"]) or {}
        sim, ovl = [], []
        for bid, b in got.items():
            if bid == x.get("bundle_id"):
                continue
            have = want_set.intersection(b["sigs"])
            if not have:
                continue
            recv = {}
            for inst, at in b["recv"].items():
                m = mk.get((inst, int(x["round"]))) or {}
                recv[inst] = (at, _ms(m["open"], at) if "open" in m else None)
            rec = {"id": bid, "n": len(b["sigs"]), "sigs": b["sigs"],
                   "shared": len(have), "of": len(want_set),
                   "extra": [z for z in b["sigs"] if z not in want_set],
                   "recv": recv,
                   "first": min(at for at, _a in recv.values())}
            (sim if len(have) == len(want_set) else ovl).append(rec)
        # the row's own builder first: that is where the trace is read
        own = x.get("our_builder")
        sim.sort(key=lambda r: (own not in r["recv"], r["first"]))
        ovl.sort(key=lambda r: (own not in r["recv"], -r["shared"],
                                r["first"]))
        x["similar"], x["overlap"] = sim, ovl
    return rows


def _trace_path(rec):
    """'r190/2 check_pending > auction_ready > selection_skipped:relay_delay
    | r191/0 ...' -- the state path, the round written when it changes,
    '*' at the offer compared here."""
    if not rec or not rec.get("rows"):
        return ""
    out, cur = [], None
    at = rec.get("at_row")
    for r in rec["rows"]:
        if not r["status"] and r["event"] == "order_transition":
            continue
        rd = f"{r['slot']}/{r['round']}" if r["slot"] else "pre"
        lab = (_trace_key(r["status"], r["reason"]) if r["status"]
               else f"{r['event']}:{r['reason']}")
        if r["kind"] == "bundle" and any(z["kind"] == "tx" for z in rec["rows"]):
            lab = "bundle " + lab
        if r is at:
            lab += "*"
        if rd != cur:
            out.append(f"| r{rd} {lab}" if out else f"r{rd} {lab}")
            cur = rd
        else:
            out.append(f"> {lab}")
    return " ".join(out)


# ------------------------------------------------------------- titan_off
#
# How long after we admitted an order Titan first offered it:
#
#   titan_off = Titan's first offer carrying the signature (relay clock)
#               - our scheduler/received for it (builder clock -> relay clock)
#
# Titan's side is mini_block_transactions_v2.ingested_at, which is the
# relay's `submitted` stamp for that offer to the microsecond (checked
# 2026-11-14 on every uuid of a 5-min sample). "First" = earliest TITAN_*
# offer on the round's connector in this slot or the three before it (one
# leader window): an order Titan put up in an earlier round counts from
# then, not from the round it finally won.
#
# Our side is on the admitting builder's own clock, which is NOT the
# relay's: FRA_1 ran 13-18 s behind it in early November and drifted 20 ppm
# until builder #139; it is within 0.3 ms today. So every value is
# corrected by the relay - builder offset measured AROUND THAT ADMISSION,
# two-sided, per 20 s bucket:
#   upper = min(relay `submitted` - our `selected` row)  over our offers
#           (offset + uplink latency  >= offset)
#   lower = max(relay `round_chosen` - our `winner` row) over the rounds
#           (offset - downlink latency <= offset)
# midpoint = offset, half the gap = the error bar shown on hover. A bucket
# missing a bound widens to the neighbours (up to +-40 s); a pair that
# crosses means the clock moved inside the span, and no value is better
# than a wrong one.

TITAN_LOOKBACK_SLOTS = 3
SKEW_BUCKET_US = 20_000_000
SKEW_WIDEN = 2                  # buckets each side, at most
_EPOCH = dt.datetime(1970, 1, 1)


def _us(t):
    """ClickHouse timestamp string (UTC) -> epoch microseconds."""
    try:
        d = dt.datetime.fromisoformat(str(t)[:26])
    except (ValueError, TypeError):
        return None
    return (d.replace(tzinfo=None) - _EPOCH) // dt.timedelta(microseconds=1)


def _ts(us):
    return (_EPOCH + dt.timedelta(microseconds=us)).strftime(
        "%Y-%m-%d %H:%M:%S")


def _pfx_hex(p):
    try:
        raw = b58decode(p)
    except Exception:
        return ""
    return raw.hex().upper() if len(raw) == 16 else ""


def titan_first_offers(relay_q, rows, window=None):
    """{(connector, prefix hex): [(slot, relay us, builder, round)]} --
    per slot, the earliest TITAN_* offer carrying the transaction."""
    want = {}
    for x in rows:
        h = _pfx_hex(x.get("sigprefix") or "")
        if h and x.get("connector"):
            w = want.setdefault(x["connector"], [set(), [], []])
            w[0].add(h)
            w[1].append(int(x["slot"]))
    out = {}
    tb = ""
    if window and window[0] and window[1]:
        f = "%Y-%m-%d %H:%M:%S"
        tb = (f"ingested_at >= '{window[0]:{f}}' "
              f"AND ingested_at <= '{window[1]:{f}}' AND ")
    for conn, (hs, slots, _) in want.items():
        if not PUBKEY_RE.match(conn):
            continue
        hs = sorted(hs)
        lo, hi = min(slots) - TITAN_LOOKBACK_SLOTS, max(slots)
        for i in range(0, len(hs), ENTITY_BATCH):
            lst = ", ".join(f"unhex('{h}')" for h in hs[i:i + ENTITY_BATCH])
            _, got = relay_q(
                "SELECT hex(substring(signature, 1, 16)) AS h, slot, "
                "  toUnixTimestamp64Micro(min(ingested_at)) AS t_first, "
                "  argMin(builder_id, ingested_at) AS b_first, "
                "  argMin(index_in_slot, ingested_at) AS r_first "
                "FROM relay.mini_block_transactions_v2 "
                f"WHERE {tb}connector_identity = '{conn}' "
                f"  AND slot BETWEEN {lo} AND {hi} "
                "  AND startsWith(builder_id, 'TITAN') "
                f"  AND substring(signature, 1, 16) IN ({lst}) "
                "GROUP BY h, slot")
            for h, sl, t, b, r in got:
                out.setdefault((conn, str(h).upper()), []).append(
                    (int(sl), int(t), b or "", int(r or 0)))
    return out


def relay_offsets(relay_q, ch, insts, lo_us, hi_us):
    """-> offset(inst, at_us) -> (relay - builder us, error us) or None.
    See the block comment above for the two bounds."""
    insts = sorted({i for i in insts if i and re.match(r"^[A-Z0-9_]+$", i)})
    if not insts:
        return lambda _i, _t: None
    il = ", ".join(f"'{i}'" for i in insts)
    _, ours = ch(
        "SELECT instance_id, kind, toString(uuid), "
        "  toUnixTimestamp64Micro(min(ts)) "
        f"FROM bifrost_miniblocks WHERE ts >= '{_ts(lo_us)}' "
        f"  AND ts <= '{_ts(hi_us)}' AND instance_id IN ({il}) "
        "  AND kind IN ('selected', 'winner') "
        "GROUP BY instance_id, kind, uuid")
    # the relay side is padded: its clock is the one we are measuring
    # against, and our stamps were as much as ~20 s off in the past
    _, rel = relay_q(
        "SELECT toString(mini_block_uuid) AS u, event, "
        "  toUnixTimestamp64Micro(min(timestamp)) AS t_first "
        "FROM relay.mini_block_events_v2 "
        f"WHERE timestamp >= '{_ts(lo_us - 60_000_000)}' "
        f"  AND timestamp <= '{_ts(hi_us + 60_000_000)}' "
        f"  AND (event = 'round_chosen' OR (event = 'submitted' "
        f"       AND builder_id IN ({il}))) GROUP BY u, event")
    sub = {u: int(t) for u, e, t in rel if e == "submitted"}
    cho = {u: int(t) for u, e, t in rel if e == "round_chosen"}
    up, lw = {}, {}
    for inst, kind, u, t in ours:
        t = int(t)
        k = (inst, t // SKEW_BUCKET_US)
        if kind == "selected" and u in sub:
            up[k] = min(up.get(k, sub[u] - t), sub[u] - t)
        elif kind == "winner" and u in cho:
            lw[k] = max(lw.get(k, cho[u] - t), cho[u] - t)

    def offset(inst, at_us):
        b = at_us // SKEW_BUCKET_US
        for w in range(SKEW_WIDEN + 1):
            us = [up[(inst, b + d)] for d in range(-w, w + 1)
                  if (inst, b + d) in up]
            ls = [lw[(inst, b + d)] for d in range(-w, w + 1)
                  if (inst, b + d) in lw]
            if not us or not ls:
                continue
            u, l = min(us), max(ls)
            if l > u:
                return None
            return ((u + l) / 2.0, (u - l) / 2.0)
        return None
    return offset


def attach_titan_off(relay_q, ch, rows, window=None):
    """x["titan_off"] = {ms, err, inst, by, slot, round, off} or None,
    with x["titan_off_why"] saying why it is blank."""
    for x in rows:
        x["titan_off"], x["titan_off_why"] = None, ""
    rows = [x for x in rows if x.get("sigprefix")]
    if not rows:
        return
    firsts = titan_first_offers(relay_q, rows, window)
    ats = [(inst, _us(t)) for x in rows
           for inst, t in (x.get("recv_at") or {}).items()]
    ats = [(i, t) for i, t in ats if t is not None]
    offset = (relay_offsets(relay_q, ch, {i for i, _ in ats},
                            min(t for _, t in ats) - 60_000_000,
                            max(t for _, t in ats) + 60_000_000)
              if ats else (lambda _i, _t: None))
    for x in rows:
        sl = int(x["slot"])
        cand = [e for e in firsts.get((x.get("connector"),
                                       _pfx_hex(x["sigprefix"])), [])
                if sl - TITAN_LOOKBACK_SLOTS <= e[0] <= sl]
        if not cand:
            x["titan_off_why"] = (f"no Titan offer carried it in slots "
                                  f"{sl - TITAN_LOOKBACK_SLOTS}-{sl}")
            continue
        tslot, tus, tby, tround = min(cand, key=lambda e: e[1])
        rec = {i: _us(t) for i, t in (x.get("recv_at") or {}).items()}
        rec = {i: t for i, t in rec.items() if t is not None}
        if not rec:
            x["titan_off_why"] = ("Titan offered it, but no builder of "
                                  "ours logged scheduler/received for it")
            continue
        # the row's own builder first, then by admission time
        b = x.get("our_builder")
        order = ([b] if b in rec else []) + sorted(
            (i for i in rec if i != b), key=lambda i: rec[i])
        for inst in order:
            off = offset(inst, rec[inst])
            if off is None:
                continue
            x["titan_off"] = {
                "ms": (tus - off[0] - rec[inst]) / 1000.0,
                "err": off[1] / 1000.0, "off": off[0] / 1000.0,
                "inst": inst, "by": tby, "slot": tslot, "round": tround}
            break
        else:
            x["titan_off_why"] = ("no clock bound for "
                                  + ", ".join(order) + " around its "
                                  "admission (no matched offer/winner pair)")


def _toff_cell(x):
    t = x.get("titan_off")
    if not t:
        why = html.escape(x.get("titan_off_why") or "", quote=True)
        return f'<td class="n dim" title="{why}">&mdash;</td>'
    title = html.escape(
        f"{t['by']} first offered it in {t['slot']}/{t['round']}, "
        f"{t['ms']:+.1f} ms after {t['inst']} admitted it "
        f"(relay - {t['inst']} clock {t['off']:+.2f} +- {t['err']:.2f} ms, "
        "already applied). Negative = Titan offered it before we had it.",
        quote=True)
    pm = (f'<span class="dim"> &plusmn;{t["err"]:.0f}</span>'
          if t["err"] >= 1 else "")
    return (f'<td class="n m {"neg" if t["ms"] < 0 else "pos"}" '
            f'title="{title}">{t["ms"]:+.1f}{pm}</td>')


# ------------------------------------------- our own API's view of an order
#
# A diff row none of our builders received can still have come in through
# our own APIs (txn-router): as an MEV bundle, or as a plain transaction
# wrapped into one. The quant store keeps both. API keys are credentials:
# only their first 8 characters are ever rendered.

API_SIG_BATCH = 100        # signatures per query; the SQL travels in the URL


def _row_sigs(x):
    """Every signature the row stands for: a bundle row's members (as the
    offer carried them), else its own."""
    if x.get("members"):
        return [m["sig"] for m in x["members"] if m.get("sig")]
    return [x["signature"]] if x.get("signature") else []


def _key8(k):
    return (k[:8] + "…") if k else ""


def attach_api(quant_q, rows, window):
    """Hang `api_bundles` / `api_txs` on each diff row nobody of ours
    received, from db_sync.inbound_mev_bundles_enriched and
    inbound_transactions_enriched. Both are ReplacingMergeTrees sorted on
    created_at first, so the window bound is what makes this cheap; the
    newest version of each submission wins."""
    want = [x for x in rows if not x.get("received_by_us")
            and x.get("verdict") in DIFF_VERDICTS]
    sigs = sorted({s_ for x in want for s_ in _row_sigs(x)})
    if not sigs:
        return rows
    lo, hi = window or (None, None)
    f = "%Y-%m-%d %H:%M:%S"
    tb = (f"created_at >= '{lo:{f}}' AND created_at <= '{hi:{f}}'"
          if lo and hi else "created_at >= now() - INTERVAL 1 HOUR")
    bundles, txs = {}, {}
    for i in range(0, len(sigs), API_SIG_BATCH):
        part = sigs[i:i + API_SIG_BATCH]
        lst = ", ".join("'%s'" % _q(x) for x in part)
        _, rb = quant_q(
            "SELECT bundle_submission_id, ifNull(toString(api_key), ''), toString(created_at), "
            "  mev_variant, num_transactions, ifNull(toString(tip_wallet), ''), tip_wallet_type, "
            "  ifNull(toString(total_wire_tip_lamports), ''), ifNull(toString(provider_direct_tip_lamports), ''), "
            "  bundle_landing_status, ifNull(toString(slot_landed), ''), "
            "  ifNull(toString(gravity_blocks_landing_status), ''), ifNull(toString(gravity_blocks_slot_landed), ''), "
            "  ifNull(toString(gravity_blocks_egress_tip), ''), ifNull(toString(gravity_blocks_bundle_id), ''), "
            "  arrayStringConcat(signatures, ' ') "
            "FROM db_sync.inbound_mev_bundles_enriched "
            f"WHERE {tb} AND hasAny(signatures, [{lst}]) "
            "ORDER BY updated_at DESC LIMIT 1 BY bundle_submission_id")
        for r in rb:
            bundles[r[0]] = {
                "id": r[0], "key": _key8(r[1] or ""), "at": r[2],
                "variant": r[3], "n": r[4], "tip_wallet": r[5] or "",
                "tip_wallet_type": r[6] or "", "wire_tip": r[7],
                "direct_tip": r[8], "status": r[9] or "", "slot": r[10],
                "gb_status": r[11] or "", "gb_slot": r[12],
                "gb_tip": r[13], "gb_bundle": r[14] or "",
                "sigs": (r[15] or "").split()}
        _, rt = quant_q(
            "SELECT signature, transaction_type, ifNull(toString(api_key), ''), "
            "  bundle_submission_id, status, ifNull(toString(slot_landed), ''), leader, "
            "  ifNull(toString(gravity_blocks_landing_status), ''), ifNull(toString(gravity_blocks_slot_landed), ''), "
            "  ifNull(toString(gravity_blocks_egress_tip), ''), ifNull(toString(gravity_blocks_egress_tip_signature), ''), "
            "  ifNull(toString(gravity_blocks_bundle_id), ''), toString(created_at) "
            "FROM db_sync.inbound_transactions_enriched "
            f"WHERE {tb} AND signature IN ({lst}) "
            "ORDER BY updated_at DESC LIMIT 1 BY id")
        for r in rt:
            txs.setdefault(r[0], []).append({
                "sig": r[0], "type": r[1] or "", "key": _key8(r[2] or ""),
                "bsid": r[3] or "", "status": r[4] or "", "slot": r[5],
                "leader": r[6] or "", "gb_status": r[7] or "",
                "gb_slot": r[8], "gb_tip": r[9], "gb_tip_sig": r[10] or "",
                "gb_bundle": r[11] or "", "at": r[12]})
    for x in want:
        mine = set(_row_sigs(x))
        x["api_checked"] = True
        x["api_bundles"] = [b for b in bundles.values()
                            if mine.intersection(b["sigs"])]
        x["api_txs"] = [t for s_ in sorted(mine) for t in txs.get(s_, [])]
    return rows


def _api_tag(i, rid):
    """The opener in the type cell; '' when nothing was looked up."""
    if not i.get("api_checked"):
        return ""
    nb, nt = len(i.get("api_bundles") or []), len(i.get("api_txs") or [])
    if not nb and not nt:
        return ('<br><span class="dim" title="no MEV bundle or transaction '
                'carrying these signatures in our API logs (quant db_sync) '
                'around this slot">not via our API</span>')
    bits = []
    if nb:
        bits.append(f"{nb} bundle{'s' if nb > 1 else ''}")
    if nt:
        bits.append(f"{nt} tx{'s' if nt > 1 else ''}")
    return (f'<br><span class="scopen apio" data-sx="{rid}" role="button" '
            f'tabindex="0" title="sent to our own API (txn-router), from '
            f'db_sync">our API: {", ".join(bits)}</span>')


def _api_panel(i):
    mine = set(_row_sigs(i))
    out = []
    v = lambda x: "&mdash;" if x in (None, "", "\\N") else html.escape(str(x))
    if i.get("api_bundles"):
        out.append('<div class="sxh">MEV bundles sent to our API that '
                   "carry this order &middot; db_sync.inbound_mev_bundles_"
                   "enriched</div>"
                   '<table class="otab"><tr><th>bundle_submission_id</th>'
                   "<th>api key</th><th>created</th><th>variant</th>"
                   "<th>txs</th><th>tip wallet</th><th>wallet type</th>"
                   '<th class="n">wire tip</th><th class="n">direct tip</th>'
                   "<th>landing</th><th>slot</th><th>gravity status</th>"
                   '<th>gravity slot</th><th class="n">gravity egress tip</th>'
                   "<th>gravity bundle</th><th>signatures</th></tr>")
        for b in i["api_bundles"]:
            sigs = "".join(
                f'<div class="{"apimatch" if sg in mine else ""}">'
                f"{copy_val(sg, sg[:20] + chr(8230))} "
                f'<span class="dim">{copy_val(sigprefix(sg))}</span></div>'
                for sg in b["sigs"])
            out.append(
                f'<tr><td>{copy_val(b["id"], b["id"][:12] + chr(8230))}</td>'
                f'<td class="m">{v(b["key"])}</td><td>{v(b["at"][11:23])}</td>'
                f'<td class="n">{v(b["variant"])}</td><td class="n">{v(b["n"])}</td>'
                f'<td>{copy_val(b["tip_wallet"], b["tip_wallet"][:10] + chr(8230)) if b["tip_wallet"] else v("")}</td>'
                f'<td>{v(b["tip_wallet_type"])}</td><td class="n">{v(b["wire_tip"])}</td>'
                f'<td class="n">{v(b["direct_tip"])}</td><td>{v(b["status"])}</td>'
                f'<td class="n">{v(b["slot"])}</td><td>{v(b["gb_status"])}</td>'
                f'<td class="n">{v(b["gb_slot"])}</td><td class="n">{v(b["gb_tip"])}</td>'
                f'<td>{copy_val(b["gb_bundle"], b["gb_bundle"][:12] + chr(8230)) if b["gb_bundle"] else v("")}</td>'
                f"<td>{sigs}</td></tr>")
        out.append("</table>")
    if i.get("api_txs"):
        out.append('<div class="sxh">transactions sent to our API (plain, '
                   "wrapped as bundles) &middot; db_sync.inbound_"
                   "transactions_enriched</div>"
                   '<table class="otab"><tr><th>signature</th><th>sigprefix</th>'
                   "<th>type</th><th>api key</th><th>created</th>"
                   "<th>bundle_submission_id</th><th>status</th><th>slot</th>"
                   "<th>leader</th><th>gravity status</th><th>gravity slot</th>"
                   '<th class="n">gravity egress tip</th><th>egress tip sig</th>'
                   "<th>gravity bundle</th></tr>")
        for t in i["api_txs"]:
            out.append(
                f'<tr><td>{copy_val(t["sig"], t["sig"][:20] + chr(8230))}</td>'
                f'<td>{copy_val(sigprefix(t["sig"]))}</td><td>{v(t["type"])}</td>'
                f'<td class="m">{v(t["key"])}</td><td>{v(t["at"][11:23])}</td>'
                f'<td>{copy_val(t["bsid"], t["bsid"][:12] + chr(8230)) if t["bsid"] else v("")}</td>'
                f'<td>{v(t["status"])}</td><td class="n">{v(t["slot"])}</td>'
                f'<td>{copy_val(t["leader"], t["leader"][:8] + chr(8230)) if t["leader"] else v("")}</td>'
                f'<td>{v(t["gb_status"])}</td><td class="n">{v(t["gb_slot"])}</td>'
                f'<td class="n">{v(t["gb_tip"])}</td>'
                f'<td>{copy_val(t["gb_tip_sig"], t["gb_tip_sig"][:16] + chr(8230)) if t["gb_tip_sig"] else v("")}</td>'
                f'<td>{copy_val(t["gb_bundle"], t["gb_bundle"][:12] + chr(8230)) if t["gb_bundle"] else v("")}</td></tr>')
        out.append("</table>")
    return '<div class="sxbox apibox">' + "".join(out) + "</div>"


def _sched_key(x):
    """The row's scheduler verdicts as filter keys, "event:reason|..."."""
    pairs = _sched_pairs(x)
    return "|".join(f"{e}:{r}" for e, r in pairs) if pairs else "__none"


def _sched_label(k):
    if k == "__none":
        return "nothing logged"
    e, _, r = k.partition(":")
    return f"{e} {r}".strip()


def _kind_mix(group, verdict="titan_inc_not_us", label=None):
    """One line: what share of this verdict's rows were bundle / txn,
    and how many of them any builder of ours received."""
    rows = _units([i for i in group if i["verdict"] == verdict])
    if not rows:
        return ""
    n = len(rows)
    c = collections.Counter(i.get("kind") or "txn" for i in rows)
    got = sum(1 for i in rows if i.get("received_by_us"))
    mix = " &middot; ".join(
        f'<span class="kd {k}">{k}</span> {c[k]:,} ({100 * c[k] / n:.0f}%)'
        for k in ("txn", "bundle") if c[k])
    return (f'<div class="selcnt"><span class="vd ti">{label or verdict}</span> '
            f"by type: {mix} <span class=\"dim\">(a bundle counts once)</span>"
            " &middot; received by us "
            f'<b>{got:,}</b> of {n:,} ({100 * got / n:.0f}%) '
            '<span class="dim">(a scheduler/received on any builder of '
            "ours, for the tx or its bundle)</span></div>")


def _sel_filters(rows):
    """Per-slot filters over every diff row: verdict, kind, and what the
    scheduler said -- each option with its count. Applied in the browser
    (ACCURACY_JS); while one is set the 40-row cap is lifted."""
    real = _units([i for i in rows
                   if i["verdict"] in ("titan_inc_not_us",
                                       "us_inc_not_titan")])
    if not real:
        return ""
    vc = collections.Counter(i["verdict"] for i in real)
    kc = collections.Counter(i.get("kind") or "txn" for i in real)
    sc = collections.Counter()
    for i in real:
        for k in _sched_key(i).split("|"):
            sc[k] += 1
    vopts = "".join(f'<option value="{v}">{v} ({vc[v]:,})</option>'
                    for v in ("titan_inc_not_us", "us_inc_not_titan")
                    if vc[v])
    oc = collections.Counter(i.get("our_builder") or "" for i in real)
    oopts = "".join(
        f'<option value="{html.escape(b, quote=True)}" '
        f'data-l="{html.escape(b, quote=True)}">{html.escape(b)} '
        f"({n:,})</option>" for b, n in sorted(oc.items()) if b)
    kbox = "".join(
        f'<label class="sfk"><input type="checkbox" class="sf-t" '
        f'value="{k}" checked> <span class="kd {k}">{k}</span> '
        f'(<span class="sf-c">{kc[k]:,}</span>)</label>'
        for k in ("txn", "bundle") if kc[k])
    sopts = "".join(
        f'<option value="{html.escape(k, quote=True)}" '
        f'data-l="{html.escape(_sched_label(k), quote=True)}">'
        f"{html.escape(_sched_label(k))} ({n:,})</option>"
        for k, n in sorted(sc.items(), key=lambda kv: -kv[1]))
    rc = collections.Counter(_trace_fkey(i) for i in real)
    ropts = "".join(
        f'<option value="{html.escape(k, quote=True)}" '
        f'data-l="{html.escape(_trace_flabel(k), quote=True)}">'
        f"{html.escape(_trace_flabel(k))} ({n:,})</option>"
        for k, n in sorted(rc.items(), key=lambda kv: -kv[1]))
    return ('<div class="selfilt"><span class="dim">filter</span> '
            f'<label>verdict <select class="sf-v"><option value="">both '
            f"({len(real):,})</option>{vopts}</select></label> "
            '<label>our builder <select class="sf-o"><option value="">'
            f"all</option>{oopts}"
            "</select></label> "
            f'<span class="sfks">{kbox}</span> '
            '<label>scheduler said <select class="sf-s"><option value="">'
            f"anything</option>{sopts}</select></label> "
            '<label title="builder_order_traces: the state on our builder '
            "when the compared offer was dispatched\">at our offer "
            '<select class="sf-r"><option value="">anything</option>'
            f"{ropts}</select></label> "
            '<span class="sf-n dim"></span> '
            '<span class="sf-use" role="button" tabindex="0" hidden '
            'title="drop this slot\'s own filter and follow the page '
            'filter again">use page filter</span></div>')


def global_filter_html(builders=()):
    """The page-wide default for every slot's diff filter. Set here, it is
    pushed into each slot as it opens and into every open slot that has
    not been changed by hand; a slot's own filter, once touched, wins until
    "use page filter". Remembered in the browser. The scheduler list fills
    from the slots opened so far -- its values are per-slot verdicts."""
    bopts = "".join(f'<option value="{html.escape(b, quote=True)}">'
                    f"{html.escape(b)}</option>" for b in builders)
    kbox = "".join(
        f'<label class="sfk"><input type="checkbox" class="gf-t" '
        f'value="{k}" checked> <span class="kd {k}">{k}</span></label>'
        for k in ("txn", "bundle"))
    return ('<div class="selfilt gfilt"><span class="dim">filter for every '
            "slot</span> "
            '<label>verdict <select class="gf-v"><option value="">both</option>'
            '<option value="titan_inc_not_us">titan_inc_not_us</option>'
            '<option value="us_inc_not_titan">us_inc_not_titan</option>'
            "</select></label> "
            '<label>our builder <select class="gf-o"><option value="">all'
            f"</option>{bopts}</select></label> "
            f'<span class="sfks">{kbox}</span> '
            '<label>scheduler said <select class="gf-s"><option value="">'
            "anything</option></select></label> "
            '<label>at our offer <select class="gf-r"><option value="">'
            "anything</option></select></label> "
            '<span class="gfnote dim">a slot\'s own filter overrides this '
            "once changed</span></div>")


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
              blk_sigs, rewards=None, bps=None):
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
            "our_uuid": our.get("uuid") or "",
            "our_bid": our["bid"], "titan_bid": titan_side["bid"],
            "bid_gap": our["bid"] - titan_side["bid"],
            "our_cost": our["cost"], "titan_cost": titan_side["cost"],
            "our_orders": our["n"], "titan_orders": titan_side["n"],
            "our_reason": our["reason"], "titan_reason": titan_side["reason"],
            "is_last": rnd["is_last"], "jito_weight_bps": bps}
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
            # execution order, not set order: set order changes per process
            # (hash seed), and with it which rows fit under the display cap
            for key in sorted(src, key=lambda k: (
                    pool[k]["order_index"], pool[k]["bundle_tx_index"] or 0,
                    k)):
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
                          **_reward_of(m, blk_sigs, rewards, bps)}
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
                                 **_reward_of(o, blk_sigs, rewards, bps)))
    return rows


def _reward_of(o, blk_pfx, rewards, bps=None):
    """What this order paid, but only if it is in THIS block.

    Keyed on the full signature, not the prefix: the prefix is what proves
    it landed, the signature is what the block indexes its fee by. Under
    the fallback an order that landed always has its signature recovered
    from the block, so the two always arrive together.

    Blank, not zero, when it did not land or we could not price it --
    0 lamports is a real answer and must not be confused with "no data".
    """
    if not rewards or not o.get("sig"):
        return {"fee": "", "tip": "", "jito_raw": "", "reward": ""}
    if blk_pfx is not None and o.get("prefix") and o["prefix"] not in blk_pfx:
        return {"fee": "", "tip": "", "jito_raw": "", "reward": ""}
    rec = rewards.get(o["sig"])
    if not rec:
        return {"fee": "", "tip": "", "jito_raw": "", "reward": ""}
    bps = DEFAULT_JITO_WEIGHT if bps is None else bps
    jito = rec.get("jito", 0)
    tip, reward = weigh(rec["fee"], jito, rec["tip"] - jito, bps)
    return {"fee": rec["fee"], "tip": tip, "jito_raw": jito,
            "reward": reward}


def packet_bodies(relayer_q, sigs, window=(None, None)):
    """-> {signature: raw bytes} from relayer.validator_packet_events.

    That table is written by OUR relayer (txn-router mev-relay), not by
    Gattaca's relay: everything it received -- the Gattaca relay's mempool
    feed, sendTransaction, validator-side forwarders, what our builders
    forward. Votes are never written, and searcher-gateway bundle members
    mostly never pass through it."""
    sigs = sorted({x for x in sigs if x})
    out = {}
    if not sigs:
        return out
    lo, hi = window or (None, None)
    f = "%Y-%m-%d %H:%M:%S"
    # partitioned on created_at; an hour either side covers clock skew and
    # a packet that arrived well before the round it was offered in
    tb = (f"created_at >= '{lo - dt.timedelta(hours=1):{f}}' "
          f"AND created_at <= '{hi + dt.timedelta(hours=1):{f}}'"
          if lo and hi else "created_at >= now() - INTERVAL 1 DAY")
    for i in range(0, len(sigs), ENTITY_BATCH):
        lst = ", ".join("'%s'" % _q(x) for x in sigs[i:i + ENTITY_BATCH])
        got = relayer_q(
            "SELECT signature, any(tx_body) FROM validator_packet_events "
            f"WHERE {tb} AND signature IN ({lst}) GROUP BY signature")
        for sig, body in got:
            if not body:
                continue
            try:
                out[sig] = base64.b64decode(body)
            except Exception:
                continue
    return out


def price_from_packets(relayer_q, rows, window=(None, None), bodies=None):
    """Price the diff rows the block could not: from the transaction bytes
    in our relayer's packet log, in place. `bodies` is an already-fetched
    {signature: bytes}; only signatures missing from it are queried.

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
    have = dict(bodies or {})
    try:
        have.update(packet_bodies(relayer_q, set(want) - set(have), window))
    except Exception:
        pass
    for sig, raw in have.items():
        if sig not in want:
            continue
        try:
            pr = txprice.price(raw, TIP_ACCOUNTS, JITO_TIP_SET)
        except Exception:
            continue
        for x in want[sig]:
            bps = x.get("jito_weight_bps")
            tip, reward = weigh(pr["fee"], pr["jito"], pr["builder"],
                                DEFAULT_JITO_WEIGHT if bps is None else bps)
            x.update(fee=pr["fee"], tip=tip, jito_raw=pr["jito"],
                     reward=reward, priced="bytes",
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


def offer_plan(relay_q, ch, rounds_, window, blocks=None, contents=None):
    """-> (plan, contents, pmaps): which offers each round compares --
    [(round, titan_builder, titan_side, [(our_builder, our_side)])] --
    and their full contents by uuid. Shared by the diff and the CU/reward
    profile, so the profile can be built without the diff."""
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
    return plan, contents, pmaps


def selection_diff(relay_q, ch, rounds_, window, blocks=None,
                   contents=None, rewards=None, keep=None):
    """Every us-vs-titan content difference across these rounds.

    `keep`, a dict, receives the compared offers (`plan`) and their full
    contents, so the CU/reward profile reuses them instead of reading the
    relay a second time."""
    plan, contents, pmaps = offer_plan(relay_q, ch, rounds_, window, blocks,
                                       contents)
    if keep is not None:
        keep.update(plan=plan, contents=contents)
    try:
        segs = jito_weights(relay_q)
    except Exception:
        segs = {}
    rows = []
    for rnd, tb, tv, ours in plan:
        rows.append(_sel_rows(rnd, tb, tv, ours, contents,
                              pmaps.get(rnd["slot"])
                              if rnd["slot"] in pmaps else None,
                              (rewards or {}).get(rnd["slot"]),
                              round_weight(segs, rnd)))
    flat_rows = [x for part in rows for x in part]
    # the chunk's own window, so the scan stays narrow even over a range
    attach_notes(ch, flat_rows, *(window or (None, None)), by_kind=True)
    lo, hi = window or (None, None)
    marks_by_slot = {}
    for sl in {x["slot"] for x in flat_rows}:
        part = [x for x in flat_rows if x["slot"] == sl]
        marks = marks_by_slot[sl] = round_marks(ch, sl, lo, hi)
        attach_recv(ch, part, sl, lo, hi, marks=marks, by_kind=True)
        attach_member_recv(ch, part, marks, lo, hi)
    try:
        attach_similar(ch, flat_rows, lo, hi, marks_by_slot)
    except Exception as exc:
        for x in flat_rows:
            x["similar_why"] = f"could not be read: {str(exc)[:120]}"
    try:
        attach_traces(ch, flat_rows, lo, hi, marks_by_slot)
    except Exception as exc:
        for x in flat_rows:
            x["trace_why"] = f"could not be read: {str(exc)[:120]}"
    try:
        attach_titan_off(relay_q, ch, flat_rows, window)
    except Exception as exc:
        for x in flat_rows:
            x["titan_off"] = None
            x["titan_off_why"] = f"could not be read: {str(exc)[:120]}"
    for x in flat_rows:
        x["kind"] = ("bundle" if x.get("bundle_id") or x.get("type") == "bundle"
                     else "txn")
    return flat_rows


def _sel_flat(x, col):
    """The three scheduler columns, wait_us and titan_off are derived,
    not stored."""
    if col.startswith("titan_off") or col == "titan_first_by":
        t = x.get("titan_off")
        if not t:
            return ""
        return {"titan_off_ms": round(t["ms"], 3),
                "titan_off_err_ms": round(t["err"], 3),
                "titan_off_on": t["inst"], "titan_first_by": t["by"]}[col]
    if col in ("similar_bundles", "similar_at_our_offer", "overlap_bundles"):
        sim = x.get("similar") or []
        if col == "overlap_bundles":
            return ";".join(f'{o["id"]}:{o["shared"]}/{o["of"]}'
                            for o in x.get("overlap") or [])
        if col == "similar_bundles":
            return ";".join(z["id"] for z in sim)
        return ";".join((z.get("trace") or {}).get("key") or "" for z in sim)
    if col.startswith("trace_"):
        t = x.get("trace")
        if not t:
            return x.get("trace_why", "") if col == "trace_at_our_offer" else ""
        return {"trace_on": t["inst"], "trace_at_our_offer": t["key"],
                "trace_then": t["then"],
                "trace_window_closed": "yes" if t.get("closed") else "",
                "trace_path": _trace_path(t)}[col]
    if col == "wait_us":
        rec = x.get("recv") or {}
        if not rec:
            return ""
        b = x.get("our_builder")
        k = b if b in rec else min(rec, key=lambda i: rec[i][0])
        return "" if rec[k][2] is None else rec[k][2]
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
                   rewards=None, keep=None):
    """The selection diff for one slot, for the page expansion."""
    win = _chunk_window(rounds_)
    if blk_sigs is None:
        try:
            blk_sigs, _bt = block_signatures(rpc_url, slot)
        except Exception:
            blk_sigs = None
    return selection_diff(relay_q, ch, rounds_, win, blocks={slot: blk_sigs},
                          rewards={slot: rewards} if rewards else None,
                          keep=keep)


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
PROFILE = "profile"
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


# The validator picker: Tom Select over the plain <select> above, so the
# page still works if the CDN is unreachable. Typing filters on name and
# identity; a full identity not in the list can be entered as is; picking
# one runs the query straight away.
TOM_SELECT = "https://cdn.jsdelivr.net/npm/tom-select@2.3.1/dist"
CONNECTOR_PICKER = (
    f'<link rel="stylesheet" href="{TOM_SELECT}/css/tom-select.css">'
    f'<script src="{TOM_SELECT}/js/tom-select.complete.min.js"></script>'
    """<script>(function(){
  var s = document.querySelector('.aqform select[name=connector]');
  if (!s || !window.TomSelect) return;
  new TomSelect(s, {
    plugins: ['dropdown_input'],
    create: true,
    // a well-formed identity that is not already listed
    createFilter: function(v){
      return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(v) && !this.options[v];
    },
    searchField: ['text', 'value'],
    maxOptions: null,
    allowEmptyOption: true,
    selectOnTab: true,
    placeholder: 'type a name or identity',
    render: {
      option: function(d, esc){
        return '<div>' + esc(d.text) + (d.value ?
          ' <span class="dim">' + esc(d.value) + '</span>' : '') + '</div>';
      },
      option_create: function(d, esc){
        return '<div class="create">use identity <b>' + esc(d.input) + '</b></div>';
      },
      no_results: function(){
        return '<div class="no-results">no match &middot; paste a full identity to use it</div>';
      }
    },
    onChange: function(){
      var f = s.form;
      if (f.requestSubmit) f.requestSubmit(); else f.submit();
    }
  });
})();</script>""")


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
           BOTH: "both",
           PROFILE: "declared CU / reward graph"}
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


def _row_copy(i, kind):
    """One diff row as a single line of JSON, the numbers as the row shows
    them: recv/win/wait from the same instance _recv_cells picks."""
    rec = i.get("recv") or {}
    b = i.get("our_builder")
    inst = (b if b in rec else min(rec, key=lambda k: rec[k][0])) if rec else ""
    after, before, wait = (rec[inst][:3] if rec else (None, None, None))
    rnd = lambda v: None if v is None else round(v, 1)
    return json.dumps({
        "type": kind, "signature": i["signature"], "sigprefix": i["sigprefix"],
        "signatures": [r["signature"] for r in i.get("_rows") or [i]],
        "bundle_id": i["bundle_id"] or "", "landed": i["landed"] or "",
        "reward": "" if i.get("reward") is None else i["reward"],
        "recv_open": rnd(after), "win_recv": rnd(before), "wait_us": wait,
        "recv_on": inst.replace("ASTRALANE_", ""), "verdict": i["verdict"],
        "titan_off_ms": (round(i["titan_off"]["ms"], 1)
                         if i.get("titan_off") else None),
        "titan_first_by": (i["titan_off"]["by"] if i.get("titan_off")
                           else None),
        "scheduler": "; ".join(e + (f" {r}" if r else "")
                               for e, r in _sched_pairs(i)),
        "trace_at_our_offer": (i.get("trace") or {}).get("key") or None,
        "trace_then": (i.get("trace") or {}).get("then") or None,
        "trace_path": _trace_path(i.get("trace")) or None,
        "similar_bundles": [
            {"bundle_id": z["id"], "members": z["n"],
             "at_our_offer": (z.get("trace") or {}).get("key") or None}
            for z in i.get("similar") or []],
        "slot": i["slot"], "rn": i["round"]}, separators=(",", ":"))


def _units(rows):
    """Diff rows -> display rows: a bundle is ONE row, not one per member.

    The diff is per signature (see _sel_rows) and the CSV stays that way;
    on screen, every member row of the same bundle on the same side
    collapses into one, which shows the bundle's FIRST transaction and
    opens the rest. Sums and unions are over the members that are in the
    diff; the panel lists every member, in-diff or not."""
    out, at = [], {}
    for i in rows:
        b = i.get("bundle_id")
        if not b or i["verdict"] not in DIFF_VERDICTS:
            out.append(i)
            continue
        k = (i["verdict"], i.get("our_builder"), b)
        if k in at:
            at[k]["_rows"].append(i)
            continue
        at[k] = dict(i, _rows=[i])
        out.append(at[k])
    for u in at.values():
        rs = sorted(u["_rows"], key=lambda r: (r["bundle_tx_index"] is None,
                                               r["bundle_tx_index"] or 0))
        u["_rows"] = rs
        first = (u.get("members") or [None])[0] or {
            "sig": rs[0]["signature"], "prefix": rs[0]["sigprefix"]}
        u["signature"], u["sigprefix"] = first["sig"], first["prefix"]
        u["order_index"] = min(r["order_index"] for r in rs)
        marks = {r["landed"] for r in rs}
        u["landed"] = (marks.pop() if len(marks) == 1 else
                       ("partial" if "yes" in marks else ""))
        priced = [r for r in rs if r.get("reward") not in ("", None)]
        if priced:
            u["fee"] = sum(int(r["fee"] or 0) for r in priced)
            u["tip"] = sum(int(r["tip"] or 0) for r in priced)
            u["jito_raw"] = sum(int(r.get("jito_raw") or 0) for r in priced)
            u["reward"] = sum(int(r["reward"] or 0) for r in priced)
            u["priced"] = ("bytes" if all(r.get("priced") == "bytes"
                                          for r in priced) else "")
            u["tip_lower_bound"] = any(r.get("tip_lower_bound")
                                       for r in priced)
        else:
            u["reward"] = ""
        seen, sched = set(), []
        for r in rs:
            for n in r.get("sched") or []:
                key = (n["instance"], n["event"], n["reason"], n["at"])
                if key not in seen:
                    seen.add(key)
                    sched.append(n)
        u["sched"] = sched
        u["received_by_us"] = any(r.get("received_by_us") for r in rs)
        # the earliest Titan offer of any member: Titan offers a bundle
        # whole, so this is normally every member's value anyway
        offs = [r for r in rs if r.get("titan_off")]
        if offs:
            u["titan_off"] = min((r["titan_off"] for r in offs),
                                 key=lambda t: t["ms"])
        else:
            u["titan_off_why"] = next((r.get("titan_off_why") for r in rs
                                       if r.get("titan_off_why")), "")
    return out


def _trace_chip(key):
    """No hover of its own: the help text is long and the chip repeats in
    every row; the cell's title carries it once."""
    words, cls, _hlp = trace_label(key)
    return f'<span class="tr {cls}">{html.escape(words)}</span>'


def _trace_fkey(x):
    """The filter key: the state at our offer, '__none' without a trace."""
    t = x.get("trace")
    return (t or {}).get("key") or "__none"


def _trace_flabel(k):
    return "no trace" if k == "__none" else trace_label(k)[0]


def _trace_summary(t):
    """Chip for the state at our offer, then what became of the order."""
    if not t:
        return '<span class="dim">&mdash;</span>'
    out = _trace_chip(t["key"])
    if t.get("then") and t["then"] != t["key"]:
        out += '<div class="trthen">then ' + _trace_chip(t["then"]) + "</div>"
    if t.get("closed"):
        out += ('<div class="trthen" title="our leader window closed with '
                "the order still pending; no retire row is written for "
                'that">window closed</div>')
    if t.get("truncated"):
        out += f'<div class="trthen">first {TRACE_CAP} rows</div>'
    return out


def _trace_cell(x, tid):
    t = x.get("trace")
    if not t:
        why = html.escape(x.get("trace_why") or "no trace looked up",
                          quote=True)
        return f'<td class="trc dim" title="{why}">&mdash;</td>'
    off = t.get("offer") or {}
    title = html.escape(
        f"{t['inst']}: the {'bundle' if x.get('bundle_id') else 'order'}'s"
        " own last state (builder_order_traces, entity_kind = "
        f"{'bundle' if x.get('bundle_id') else 'tx'}) when our offer "
        f"{(x.get('our_uuid') or '?')[:8]} (the one compared here) was "
        f"dispatched" + (f", trace_seq {off['seq']}" if off else "")
        + ". " + (trace_label(t["key"])[2] or "")
        + " Click for every state change.", quote=True)
    body = _trace_summary(t)
    if t.get("rows"):
        body = (f'<span class="scopen" data-sx="{tid}" role="button" '
                f'tabindex="0">{body}</span>')
    return f'<td class="trc" title="{title}">{body}</td>'


def _similar_cell(x, sid):
    sim, ovl = x.get("similar"), x.get("overlap") or []
    if sim is None:
        why = html.escape(x.get("similar_why") or "not looked up", quote=True)
        return f'<td class="dim" title="{why}">&mdash;</td>'
    if not sim and not ovl:
        return ('<td class="dim" title="no bundle we received carries these '
                'signatures">none</td>')
    what = "these signatures" if x.get("bundle_id") else "this transaction"
    body = ""
    if sim:
        top = sim[0]
        body = (f'<b>{len(sim)}</b> bundle{"" if len(sim) == 1 else "s"} '
                + _trace_chip((top.get("trace") or {}).get("key") or "")
                + ("" if len(sim) == 1 else
                   ' <span class="dim">(first)</span>'))
    if ovl:
        body += (f'<div class="trthen">{len(ovl)} share some</div>')
    title = html.escape(
        f"bundles we received under another id that carry ALL of {what} "
        f"({len(sim)}), and ones that carry only some ({len(ovl)}). The "
        "chip is the first one's own state (received on our builder first, "
        "then earliest) when our compared offer was "
        "dispatched. Click for each, with its trace.", quote=True)
    return (f'<td class="trc" title="{title}"><span class="scopen" '
            f'data-sx="{sid}" role="button" tabindex="0">{body}</span></td>')


def _recv_list(r):
    return " ".join(
        f'{html.escape(inst.replace("ASTRALANE_", ""))} '
        f'<span class="m">{after:+.1f}</span>' if after is not None else
        f'{html.escape(inst.replace("ASTRALANE_", ""))} '
        f'<span class="m">{html.escape(at[11:23])}</span>'
        for inst, (at, after) in sorted(r["recv"].items()))


def _similar_panel(x, sid):
    """Each similar bundle: id, members (the row's + any extra), when each
    instance admitted it, and its own trace on our builder, openable."""
    sim, ovl = x.get("similar") or [], x.get("overlap") or []
    out = ['<div class="sxbox"><div class="sxh">bundles we received that '
           "carry " + ("every signature of this bundle" if x.get("bundle_id")
                       else "this transaction")
           + ' <span class="dim">&middot; scheduler/received with '
           "entity_kind = bundle, matched on its signatures array; ms from "
           "round_started on each instance; trace on "
           + html.escape(x.get("our_builder") or "") + "</span></div>"]
    if sim:
        out.append('<table class="otab"><tr><th>bundle</th>'
                   '<th class="n">txs</th><th>extra members</th>'
                   "<th>received (ms after open)</th><th>at our offer</th>"
                   "</tr>")
        for n, z in enumerate(sim[:SIM_SHOW]):
            t = z.get("trace")
            zid = f"{sid}s{n}"
            tc = _trace_summary(t)
            if t and t.get("rows"):
                tc = (f'<span class="scopen" data-sx="{zid}" role="button" '
                      f'tabindex="0" title="{html.escape(_trace_path(t), quote=True)}">'
                      f"{tc}</span>")
            extra = (" ".join(copy_val(e, e[:12] + "\u2026") for e in z["extra"])
                     or '<span class="dim">none &mdash; same content</span>')
            out.append(f'<tr><td>{copy_val(z["id"], z["id"][:16] + "\u2026")}</td>'
                       f'<td class="n">{z["n"]}</td><td>{extra}</td>'
                       f"<td>{_recv_list(z)}</td><td class=\"trc\">{tc}</td></tr>")
            if t and t.get("rows"):
                out.append(f'<tr class="sxrow" id="{zid}" hidden><td colspan=5>'
                           + _trace_panel(dict(x, bundle_id=z["id"], trace=t))
                           + "</td></tr>")
        out.append("</table>")
        if len(sim) > SIM_SHOW:
            out.append(f'<div class="dim">+{len(sim) - SIM_SHOW} more: '
                       + " ".join(copy_val(z["id"], z["id"][:12] + "\u2026")
                                  for z in sim[SIM_SHOW:]) + "</div>")
    if ovl:
        out.append('<div class="sxh">share only some of them</div>'
                   '<table class="otab"><tr><th>bundle</th>'
                   '<th class="n">shared</th><th class="n">txs</th>'
                   "<th>received (ms after open)</th></tr>")
        for z in ovl[:SIM_SHOW]:
            out.append(f'<tr><td>{copy_val(z["id"], z["id"][:16] + "\u2026")}</td>'
                       f'<td class="n">{z["shared"]} of {z["of"]}</td>'
                       f'<td class="n">{z["n"]}</td><td>{_recv_list(z)}</td></tr>')
        out.append("</table>")
        if len(ovl) > SIM_SHOW:
            out.append(f'<div class="dim">+{len(ovl) - SIM_SHOW} more</div>')
    out.append("</div>")
    return "".join(out)


def _trace_panel(x):
    """Every row the builder traced for this order on our builder, in
    order, with the round's boundaries on the same clock: round opened,
    our compared offer dispatched, the winner announced, window closed.
    ms are from this round's round_started on that instance."""
    t = x["trace"]
    opn = t.get("open")
    first = t["rows"][0]["seq"] if t["rows"] else 0
    ev = []
    for r in t["rows"]:
        lab = (r["status"] or r["event"])
        ck = " / ".join(v for v in (
            f"branch {r['branch']}" if r["branch"] else "",
            f"ckpt {r['checkpoint']}" if r["checkpoint"] else "",
            f"req {r['request']}" if r["request"] else "",
            f"common {r['common']}" if r["common"] else "") if v)
        cls = trace_label(_trace_key(r["status"], r["reason"]))[1] \
            if r["status"] else "na"
        mark = " trat" if r is t.get("at_row") else ""
        ev.append((r["at"], r["seq"], 1,
                   f'<tr class="trrow{mark}"><td class="n m">'
                   f'{r["seq"]}<span class="dim"> +{r["seq"] - first}</span>'
                   f'</td><td class="n m">{_fmt_ms(opn, r["at"])}</td>'
                   f'<td class="m">{r["slot"] + "/" + r["round"] if r["slot"] else "&mdash;"}</td>'
                   f'<td class="kd {"bundle" if r["kind"] == "bundle" else "txn"}">'
                   f'{"bundle" if r["kind"] == "bundle" else "tx"}</td>'
                   f'<td class="dim">{html.escape(r["event"])}</td>'
                   f'<td><span class="tr {cls}">{html.escape(r["status"])}'
                   f'</span></td><td>{html.escape(r["reason"])}</td>'
                   f'<td class="dim">{html.escape(ck)}</td></tr>'))

    def marker(at, seq, text):
        if at:
            ev.append((at, seq if seq is not None else -1, 0,
                       f'<tr class="trmark"><td class="n m">'
                       f'{"" if seq is None else seq}</td>'
                       f'<td class="n m">{_fmt_ms(opn, at)}</td>'
                       f"<td colspan=6>{text}</td></tr>"))
    off = t.get("offer")
    marker(opn, None, f"round r{x['round']} opened (round_started)")
    if off:
        marker(off["at"], off["seq"],
               f"<b>our offer {html.escape((x.get('our_uuid') or '')[:8])} "
               "dispatched</b> &mdash; the one compared on this row")
    marker(t.get("win"), None, "winner announced to us (winner row)")
    if t.get("closed"):
        marker(t["closed"][2], t["closed"][1], "our leader window closed")
    ev.sort(key=lambda e: (e[0], e[1]))
    # compact on purpose: one panel per diff row, and a pretty-printed
    # dict per trace row made the panels 40% of a slot's page
    blob = json.dumps({
        "slot": x["slot"], "round": x["round"], "instance": t["inst"],
        "our_offer": x.get("our_uuid"),
        "offer_trace_seq": (off or {}).get("seq"),
        "at_our_offer": t["key"], "then": t["then"],
        "sigprefix": x.get("sigprefix"), "bundle_id": x.get("bundle_id"),
        "cols": ["trace_seq", "ts", "slot/round", "entity", "event",
                 "status", "reason", "branch", "checkpoint"],
        "rows": [[r["seq"], r["at"], f"{r['slot']}/{r['round']}",
                  r["kind"], r["event"], r["status"], r["reason"],
                  r["branch"], r["checkpoint"]] for r in t["rows"]]},
        separators=(",", ":"))
    what = ("bundle " + html.escape((x.get("bundle_id") or "")[:12]) + "\u2026"
            if x.get("bundle_id") else
            "tx " + html.escape(x.get("sigprefix") or ""))
    return ('<div class="sxbox"><div class="sxh">trace of '
            f'<b>{what}</b> on '
            f'<b>{html.escape(t["inst"])}</b> &middot; at our offer '
            f'<span class="tr {trace_label(t["key"])[1]}">'
            f'{html.escape(trace_label(t["key"])[0])}</span> &middot; '
            + copy_val(blob, "copy as JSON")
            + ' <span class="dim">&middot; builder_order_traces: one row per '
            "change of status, reason or round; ms from round_started on the "
            "same instance</span></div>"
            '<table class="otab trtab"><tr><th class="n">trace_seq</th>'
            '<th class="n">ms</th><th>slot/r</th><th>entity</th>'
            "<th>event</th><th>status</th><th>reason</th>"
            "<th>branch / checkpoint</th></tr>"
            + "".join(e[3] for e in ev) + "</table></div>")


def _fmt_ms(a, b):
    v = _ms(a, b) if a and b else None
    return "&mdash;" if v is None else f"{v:+.1f}"


def _trace_mix(group, verdict="titan_inc_not_us"):
    """One line: for this verdict's orders that our builder received,
    where each stood when our compared offer went out."""
    rows = [i for i in _units([i for i in group if i["verdict"] == verdict])
            if i.get("trace") and i["trace"]["key"] not in ("", "untraced")]
    if not rows:
        t = next((i.get("trace") for i in group if i.get("trace")), None)
        if t and t["key"] == "untraced":
            return (f'<div class="selcnt dim">no order traces: '
                    f"{html.escape(t['inst'])} runs a build without them "
                    "(builder #134)</div>")
        return ""
    c = collections.Counter(i["trace"]["key"] for i in rows)
    bits = " &middot; ".join(f"{_trace_chip(k)} <b>{n:,}</b>"
                             for k, n in c.most_common())
    return (f'<div class="selcnt"><span class="vd ti">{verdict}</span> '
            f"at our offer: {bits} <span class=\"dim\">(state of each order "
            "on our builder when the compared offer was dispatched)</span>"
            "</div>")


def _sel_table(group):
    out = ['<table class="otab"><tr><th>#</th><th>verdict</th><th>type</th>'
           "<th>signature</th><th>sigprefix</th><th>bundle</th>"
           '<th>landed</th><th class="n" title="landed: fee and tip from the '
           'block. offered: from the transaction bytes in the relay packet log, '
           'for an order that did not land here">reward</th>'
           '<th class="n" title="ms from this round&#39;s round_started to the builder admitting the order (scheduler/received), same clock; negative = it arrived before the round opened">recv +open</th>'
           '<th class="n" title="ms from the builder admitting the order to that builder&#39;s own winner row (bifrost_miniblocks kind = winner) for this slot and round, same clock">win &minus;recv</th>'
           '<th class="n" title="the relay&#39;s hold on the order (scheduler/received nums wait_us), ms: the builder may not extend it before admission + wait. 0 = no hold (votes, non-nonce bundles, searcher flow); blank = not logged">wait</th>'
           '<th class="n" title="titan_off: ms from our builder admitting the order (scheduler/received) to Titan&#39;s FIRST offer carrying it on this connector, this slot or the 3 before (relay submitted stamp). Our stamp is moved onto the relay clock with the relay - builder offset measured around that moment (two-sided bound, error on hover). Negative = Titan offered it before we had it">titan off</th>'
           '<th title="builder_order_traces: the order&#39;s state on our builder when the offer compared here was dispatched (same trace_seq counter, no clock), then what became of it. Click for the full sequence">at our offer</th>'
           '<th title="bundles we received under a different id that carry every signature of this row (a plain tx: bundles containing it), with their own state at our offer. Click for each">similar bundles</th>'
           "<th>scheduler said</th></tr>"]
    real = _units([i for i in group
                   if i["verdict"] not in ("no_accepted_offer",
                                           "no_content")])
    # the cap picks from the display rows themselves: picking from the
    # per-signature rows put every bundle on the page twice, once as its
    # first member's row and once as the merged row
    shown = _sel_show(real)
    front = {id(i) for i in shown}
    # the capped order first, then everything else -- hidden, not dropped
    ordered = shown + sorted((i for i in real if id(i) not in front),
                             key=lambda i: (str(i["verdict"]), i["order_index"]))
    for i in ordered:
        extra = ' class="selx" hidden' if id(i) not in front else ""
        bid = (copy_val(i["bundle_id"], i["bundle_id"][:12] + "\u2026")
               if i["bundle_id"] else '<span class="dim">&mdash;</span>')
        bx = ""
        sigcell = copy_val(i["signature"])
        if i["bundle_id"] and i.get("members"):
            n = len(i["members"])
            k = len(i.get("_rows") or [i])
            bxid = f'bx{abs(hash((i["slot"], i["round"], i["bundle_id"], i["verdict"], i.get("our_builder"), i.get("builder_id")))):x}'
            # the signature list IS the opener: the bundle's first
            # transaction, and a click shows every one of them
            short = (i["signature"] or "")[:24] + "\u2026" if i["signature"] else "&mdash;"
            sigcell = (f'<span class="scopen sigl" data-sx="{bxid}" '
                       'role="button" tabindex="0" title="the bundle&#39;s '
                       'first transaction; click for every transaction in '
                       f'the bundle">{short} <span class="dim">'
                       f'{n} txn{"" if n == 1 else "s"}</span></span>')
            bid += (f' <span class="dim bxo" title="members of this bundle '
                    f'that are in the diff">{k} of {n} in diff</span>')
            bx = (f'<tr class="sxrow" id="{bxid}" hidden><td colspan=15>'
                  + _bundle_panel(i) + "</td></tr>")
        vcls = "us" if i["verdict"] == "us_inc_not_titan" else "ti"
        pairs = _sched_pairs(i)
        # One panel per RENDERED row. The same order is rendered once per
        # our-builder group (AMS_1 vs Titan, AMS_2 vs Titan), so the
        # builder has to be in the key or the ids collide and a click in
        # the second group opens the first group's panel.
        rid = f'sx{abs(hash((i["slot"], i["round"], i["order_index"], i["signature"], i["bundle_id"], i["verdict"], i.get("our_builder"), i.get("builder_id")))):x}'
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
        kind = i.get("kind") or "txn"
        battr += (f' data-v="{i["verdict"]}" data-k="{kind}" '
                  f'data-o="{html.escape(i.get("our_builder") or "", quote=True)}" '
                  f'data-s="{html.escape(_sched_key(i), quote=True)}" '
                  f'data-r="{html.escape(_trace_fkey(i), quote=True)}"')
        tid = "tr" + rid[2:]
        aid = "ap" + rid[2:]
        sid = "sm" + rid[2:]
        atag = _api_tag(i, aid)
        # the row number copies the whole row
        rowcp = (f'<span class="cpv" data-c="{html.escape(_row_copy(i, kind), quote=True)}" '
                 'role="button" tabindex="0" title="click to copy this row (type, '
                 "signature, sigprefix, bundle, landed, reward, recv_open, win_recv, "
                 f'wait_us, titan_off, scheduler, trace at our offer and path, slot, rn) as JSON">{i["order_index"]} &#x29C9;</span>')
        out.append(f'<tr{battr}{extra}><td class="n">{rowcp}</td>'
                   f'<td><span class="vd {vcls}">{i["verdict"]}</span></td>'
                   f'<td class="kd {kind}"'
                   + ('' if i.get("received_by_us") else
                      ' title="no scheduler/received on any of our builders"')
                   + f'>{kind}{"" if i.get("received_by_us") else " <i>unseen</i>"}'
                   + _alone_hint(i)
                   + atag + '</td>'
                   f'<td>{sigcell}</td>'
                   f'<td>{copy_val(i["sigprefix"])}</td>'
                   f"<td>{bid}</td>"
                   f'<td class="land {"y" if i["landed"] == "yes" else "n"}">'
                   f'{i["landed"] or "?"}</td>'
                   f'<td class="n">{_rw_cell(i)}</td>'
                   f'{_recv_cells(i, i.get("our_builder"))}'
                   f'{_toff_cell(i)}'
                   f'{_trace_cell(i, tid)}'
                   f'{_similar_cell(i, sid)}'
                   f"<td>{cell}</td></tr>")
        if bx:
            out.append(bx)
        if (i.get("trace") or {}).get("rows"):
            out.append(f'<tr class="sxrow" id="{tid}" hidden><td colspan=15>'
                       + _trace_panel(i) + "</td></tr>")
        if i.get("similar") or i.get("overlap"):
            out.append(f'<tr class="sxrow" id="{sid}" hidden><td colspan=15>'
                       + _similar_panel(i, sid) + "</td></tr>")
        if pairs:
            out.append(f'<tr class="sxrow" id="{rid}" hidden><td colspan=15>'
                       + _sched_panel(i) + "</td></tr>")
        if i.get("api_bundles") or i.get("api_txs"):
            out.append(f'<tr class="sxrow" id="{aid}" hidden><td colspan=15>'
                       + _api_panel(i) + "</td></tr>")
    out.append("</table>")
    if len(real) > len(shown):
        sm = sum(1 for i in shown if i["verdict"] == "us_inc_not_titan")
        st = len(shown) - sm
        tm = sum(1 for i in real if i["verdict"] == "us_inc_not_titan")
        out.append(f'<div class="more">showing {st} of {len(real) - tm:,} '
                   f"titan_inc_not_us and {sm} of {tm:,} us_inc_not_titan "
                   "rows (a bundle is one row) "
                   f'<button type="button" class="cpsm selmore">show all '
                   f"{len(real):,}</button></div>")
    return "".join(out)


def _alone_hint(i):
    """On a bundle row: how many of its members we had as plain txs. Their
    events are in the member panel, not on this row."""
    ms = i.get("members") or []
    if not i.get("bundle_id") or not ms:
        return ""
    k = sum(1 for m in ms if m.get("recv_alone"))
    if not k:
        return ""
    return (f'<div class="trthen" title="members that reached our builders '
            "as plain transactions (their own scheduler/received). The "
            "bundle row shows the bundle's own events only; open the "
            f'signature for each member\'s">{k} of {len(ms)} txs seen alone</div>')


def _bundle_panel(i):
    """Every member of the bundle this row came from, in bundle order.

    `in diff` marks the members that are rows of this table; a member
    without it was carried by BOTH sides and so is not a difference -- but
    it is still part of the bundle, and usually the one paying the tip."""
    rows = []
    mine = {r.get("sigprefix"): r for r in (i.get("_rows") or [i])}
    base = f'bm{abs(hash((i["slot"], i["round"], i["bundle_id"], i["verdict"], i.get("our_builder")))):x}'
    for n, m in enumerate(i["members"]):
        t = m.get("trace")
        mid = f"{base}m{n}"
        tc = _trace_summary(t)
        if t and t.get("rows"):
            tc = (f'<span class="scopen" data-sx="{mid}" role="button" '
                  f'tabindex="0">{tc}</span>')
        tag = ('<span class="vd us">in diff</span>' if m["in_diff"]
               else ('<span class="vd na">both sides</span>' if m["shared"]
                     else ""))
        rows.append(
            f'<tr><td class="n">{"" if m["bundle_tx_index"] is None else m["bundle_tx_index"]}</td>'
            f'<td>{copy_val(m["sig"]) if m["sig"] else "&mdash;"}</td>'
            f'<td>{copy_val(m["prefix"]) if m["prefix"] else "&mdash;"}</td>'
            f'<td class="land {"y" if m["landed"] == "yes" else "n"}">'
            f'{m["landed"] or "?"}</td>'
            f'<td class="n">{_rw_cell(m)}</td><td>{tag}</td>'
            + (_toff_cell(mine[m["prefix"]]) if m.get("prefix") in mine
               else '<td class="n dim">&mdash;</td>')
            + f"<td>{_alone_cell(m)}</td>"
            + f"<td>{_alone_sched_cell(m)}</td>"
            + f'<td class="trc" title="{html.escape(_trace_path(t), quote=True)}">'
            + tc + "</td></tr>")
        if t and t.get("rows"):
            rows.append(f'<tr class="sxrow" id="{mid}" hidden><td colspan=10>'
                        + _trace_panel(dict(i, bundle_id="", sigprefix=m["prefix"],
                                            trace=t)) + "</td></tr>")
    return ('<div class="sxbox"><div class="sxh">bundle '
            + copy_val(i["bundle_id"], i["bundle_id"][:16] + "\u2026")
            + f' &middot; {len(i["members"])} member'
            + ("" if len(i["members"]) == 1 else "s") + "</div>"
            '<table class="otab"><tr><th>#</th><th>signature</th>'
            "<th>sigprefix</th><th>landed</th>"
            '<th class="n">reward</th><th></th>'
            '<th class="n" title="ms from our admission to Titan&#39;s first '
            'offer carrying this member, relay clock (members in the diff '
            'only)">titan off</th>'
            '<th title="scheduler/received for the member\'s own SigPrefix: '
            "the transaction arriving on its own, outside this bundle -- "
            "per instance, ms after that instance's round_started, "
            "source/route and the relay's hold. Hover a line for the raw "
            'attrs and nums">received on its own</th>'
            '<th title="what the scheduler then did with that plain copy: '
            "every dropped / pin_skipped / check_dropped / ... row whose "
            "entity is the member\'s own SigPrefix with entity_kind = tx, "
            "per instance, ms after that instance\'s round_started. The "
            "bundle\'s own verdicts are on the row above, under its bundle "
            'id. Hover a line for the raw attrs and nums">'
            "scheduler events as a tx</th>"
            '<th title="builder_order_traces for the member as a plain tx on '
            "our builder: its state when the compared offer was dispatched, "
            'then what became of it. Click for every row">trace as a tx'
            "</th></tr>"
            + "".join(rows) + "</table></div>")


def _alone_cell(m):
    got = m.get("recv_alone")
    if got is None:
        return '<span class="dim">&mdash;</span>'
    if not got:
        return '<span class="dim">never on its own</span>'
    out = []
    for inst, after, ev in got:
        raw = html.escape(f"{ev['at']}\nattrs {ev['attrs']}\nnums  {ev['nums']}",
                          quote=True)
        when = (f"{after:+.1f} ms" if after is not None
                else html.escape(ev["at"][11:23]))
        hold = ("no wait_us" if ev["wait"] is None
                else f"hold {ev['wait'] / 1000:.1f} ms")
        out.append(f'<div class="alone" title="{raw}">'
                   f'{html.escape(inst.replace("ASTRALANE_", ""))} '
                   f'<span class="m">{when}</span> &middot; '
                   f'{html.escape(ev["source"] or "?")}/'
                   f'{html.escape(ev["route"] or "?")} &middot; '
                   f'<span class="{"held" if ev["wait"] else "dim"}">{hold}</span>'
                   "</div>")
    return "".join(out)


def _alone_sched_cell(m):
    got = m.get("sched_alone")
    if got is None:
        return '<span class="dim">&mdash;</span>'
    if not got:
        return '<span class="dim">nothing logged</span>'
    out = []
    for inst, after, n in got:
        raw = html.escape(f"{n['at']}\nattrs {n['attrs']}\nnums  {n['nums']}",
                          quote=True)
        when = (f"{after:+.1f} ms" if after is not None
                else html.escape(n["at"][11:23]))
        out.append(f'<div class="alone" title="{raw}">'
                   f'{html.escape(inst.replace("ASTRALANE_", ""))} '
                   f'<span class="m">{when}</span> &middot; '
                   f'<b>{html.escape(n["event"])}</b>'
                   + (f'/{html.escape(n["reason"])}' if n["reason"] else "")
                   + "</div>")
    return "".join(out)


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
    jr, bps = i.get("jito_raw") or 0, i.get("jito_weight_bps")
    tw = (f' title="Jito {jr:,} x {bps}/10000 = {jr * bps // 10000:,}, plus '
          f'{i["tip"] - jr * bps // 10000:,} to Titan/Bifrost"'
          if jr and bps is not None else "")
    t = (f' <span class="dim"{tw}>+{i["tip"]:,} tip</span>' if i.get("tip")
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
    """Per verdict: rows as shown (a bundle once), then signatures."""
    units = _units(group)
    c = collections.Counter(i["verdict"] for i in units)
    nsig = collections.Counter(i["verdict"] for i in group)
    land = collections.Counter(
        (i["verdict"], i["landed"]) for i in units if i["landed"])
    bits = []
    for v in ("titan_inc_not_us", "us_inc_not_titan",
              "no_accepted_offer", "no_content"):
        if not c[v]:
            continue
        diff = v in ("titan_inc_not_us", "us_inc_not_titan")
        cls = "us" if v == "us_inc_not_titan" else ("ti" if diff else "na")
        yes, und = land[(v, "yes")], c[v] - land[(v, "yes")] - land[(v, "no")]
        bits.append(f'<span class="vd {cls}">{v}</span> <b>{c[v]:,}</b>'
                    + (f' <span class="dim" title="signature-level diff rows '
                       f'(the CSV)">[{nsig[v]:,} sigs]</span>'
                       if diff and nsig[v] != c[v] else "")
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
           + copy_sel(rows, f"copy {len(rows):,} as CSV") + "</div>"
           + _sel_filters(rows)]
    for key in sorted(groups):
        g = groups[key]
        extra = ""
        if any(i["verdict"] == "no_content" for i in g):
            extra = ('<div class="selcnt dim">no stored content for '
                     "the losing side &mdash; our own store keeps the "
                     "winner announcement and our offers, never a "
                     "rival's losing rung</div>")
        out.append('<div class="selblk" data-o="'
                   + html.escape(g[0].get("our_builder") or "", quote=True)
                   + '">' + _sel_head(g[0])
                   + f'<div class="selcnt">{_sel_counts(g)}</div>'
                   + _kind_mix(g) + _trace_mix(g) + extra + _sel_table(g) + "</div>")
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


# ------------------------------------------------------------- offer diff
#
# The accuracy diff compares the offers the page chooses (the winner and
# our best as it stood). This one compares any two offers picked off a
# slot's race: two of our rungs, AMS_1 against AMS_2, ours against
# Titan's, or two of Titan's. Same machinery -- contents by uuid from the
# relay, a signature-level diff, one display row per bundle -- with each
# side labelled by its own builder instead of "us"/"titan".
#
# For every order one offer has and the other lacks, the "why" columns are
# read for the LACKING offer's builder, when that builder is ours: its
# builder_order_traces state when that offer was dispatched, what its
# scheduler said, when it admitted the order, and the bundles it received
# that carry the same transactions. A lacking Titan offer has no such
# view -- Titan's internals are not in any store of ours.

PAIR_SHOW = 150


def find_offers(relay_q, ch, slot, uuids, region="all"):
    """-> (timeline, {uuid: offer}) -- each picked offer as the race has
    it, plus its round, the round's anchor and winner."""
    win = slot_window(relay_q, slot)
    tl = slot_timeline(relay_q, slot, win, region=region)
    if not tl.get("rounds"):
        tl = slot_timeline_ours(ch, slot)
    want, out = set(uuids), {}
    for rnd in tl.get("rounds") or []:
        for o in rnd["offers"]:
            if o["uuid"] in want:
                out[o["uuid"]] = dict(
                    o, round=rnd["idx"], won_by=rnd.get("won_by") or "",
                    anchor=rnd.get("anchor") or "",
                    connector=o.get("connector") or tl.get("connector") or "")
    return tl, win, out


def _pair_rows(slot, a, b, contents, blk_sigs, rewards, bps):
    """The signature-level diff of two offers, both directions."""
    aset = {_okey(o): o for o in contents.get(a["uuid"], [])}
    bset = {_okey(o): o for o in contents.get(b["uuid"], [])}
    rows = []
    for verdict, own, lack, pool, other in ((PAIR_A, a, b, aset, bset),
                                            (PAIR_B, b, a, bset, aset)):
        src = pool.keys() - other.keys()
        mine = _is_ours(lack["builder"])
        base = {
            "slot": slot,
            # the LACKING offer's round: its marks and its trace boundary
            "round": lack["round"], "owner_round": own["round"],
            "connector": a.get("connector") or b.get("connector") or "",
            "won_by": lack.get("won_by") or "",
            "we_won": _is_ours(lack.get("won_by") or ""),
            "our_builder": lack["builder"] if mine else None,
            "our_uuid": lack["uuid"] if mine else "",
            "lack_builder": lack["builder"], "lack_uuid": lack["uuid"],
            "titan_builder": own["builder"], "owner_uuid": own["uuid"],
            "our_bid": lack["bid"], "titan_bid": own["bid"],
            "bid_gap": lack["bid"] - own["bid"],
            "is_last": own.get("is_last"), "jito_weight_bps": bps}
        for key in sorted(src, key=lambda k: (
                pool[k]["order_index"], pool[k]["bundle_tx_index"] or 0, k)):
            o = pool[key]
            members = None
            if o["bundle_id"]:
                members = sorted(
                    ({"sig": m["sig"], "prefix": m["prefix"],
                      "bundle_tx_index": m["bundle_tx_index"],
                      "landed": _landed_mark(m, blk_sigs),
                      "in_diff": _okey(m) in src,
                      "shared": _okey(m) in other,
                      **_reward_of(m, blk_sigs, rewards, bps)}
                     for m in pool.values()
                     if m["bundle_id"] == o["bundle_id"]),
                    key=lambda m: (m["bundle_tx_index"] is None,
                                   m["bundle_tx_index"] or 0))
            rows.append(dict(base, verdict=verdict, builder_id=own["builder"],
                             members=members, type=o["type"],
                             signature=o["sig"], sigprefix=o["prefix"],
                             bundle_id=o["bundle_id"],
                             bundle_tx_index=o["bundle_tx_index"],
                             order_index=o["order_index"],
                             landed=_landed_mark(o, blk_sigs),
                             **_reward_of(o, blk_sigs, rewards, bps)))
    shared = len(aset.keys() & bset.keys())
    return rows, shared, len(aset), len(bset)


def offer_pair(relay_q, ch, rpc_url, slot, ua, ub, region="all",
               relayer_q=None, quant_q=None):
    """Everything the offer diff page shows for one picked pair."""
    if not ua or not ub or ua == ub:
        raise ValueError("pick two different offers")
    tl, win, offs = find_offers(relay_q, ch, slot, (ua, ub), region)
    miss = [u for u in (ua, ub) if u not in offs]
    if miss:
        raise ValueError("offer not found in slot %d: %s"
                         % (slot, ", ".join(u[:8] for u in miss)))
    a, b = offs[ua], offs[ub]
    if not (win and win[0]):
        win = _chunk_window([a, b])
    lo, hi = win
    try:
        rew = block_rewards(rpc_url, slot)
        sigs = set(rew) or None
    except Exception:
        rew, sigs = None, None
    pmap = block_prefixes(sigs) if sigs else {}
    contents, src = order_contents(relay_q, ch, [ua, ub], win, pmap)
    bps = a.get("jito_w")
    if bps is None:
        try:
            bps = weight_at(jito_weights(relay_q), a["connector"], a["at"])
        except Exception:
            bps = None
    rows, shared, na, nb = _pair_rows(slot, a, b, contents,
                                      pmap if sigs else None, rew, bps)
    attach_notes(ch, rows, lo, hi, by_kind=True)
    marks = round_marks(ch, slot, lo, hi)
    attach_recv(ch, rows, slot, lo, hi, marks=marks, by_kind=True)
    attach_member_recv(ch, rows, marks, lo, hi)
    for step, why in ((attach_similar, "similar_why"),
                      (attach_traces, "trace_why")):
        try:
            step(ch, rows, lo, hi, {slot: marks})
        except Exception as exc:
            for x in rows:
                x[why] = f"could not be read: {str(exc)[:120]}"
    if relayer_q is not None:
        try:
            price_from_packets(relayer_q, rows, win)
        except Exception:
            pass
    if quant_q is not None:
        try:
            attach_api(quant_q, rows, win)
        except Exception:
            pass
    for x in rows:
        x["kind"] = ("bundle" if x.get("bundle_id") or x.get("type") == "bundle"
                     else "txn")
    return {"slot": slot, "a": a, "b": b, "rows": rows, "shared": shared,
            "na": na, "nb": nb, "src": src, "jito_weight_bps": bps,
            "chosen": next((o["uuid"] for r in tl.get("rounds") or []
                            for o in r["offers"] if o.get("chosen")
                            and r["idx"] in (a["round"], b["round"])), "")}


def pair_bar_html(slot, region="all"):
    """The picker under a slot's race, sticky so it stays in view down the
    race. Two modes (OFFDIFF_JS): "offer diff" picks A and B and lists the
    orders each has that the other lacks; "winner progression" picks the
    offers of ONE round's winning builder (the others are greyed out) and
    shows what each added to the one before it, over CU bins."""
    return (f'<div class="odbar" data-slot="{int(slot)}" '
            f'data-region="{html.escape(region, quote=True)}">'
            '<select class="odmode" aria-label="mode">'
            '<option value="diff">offer diff</option>'
            '<option value="prog">winner progression</option></select>'
            '<span class="odm-diff odpick odA"><b>A</b> <i>click an offer in '
            "the race or a row's pick</i></span>"
            '<span class="odm-diff odpick odB"><b>B</b> <i>then a second one</i></span>'
            '<button type="button" class="cpsm odm-diff odswap">swap</button>'
            '<span class="odm-prog odchips" hidden></span>'
            '<span class="odm-prog odauto" hidden>pick '
            '<select class="odpn" aria-label="how many"><option>3</option>'
            '<option selected>5</option><option>8</option>'
            '<option value="all">all</option></select> evenly from '
            '<select class="odpr" aria-label="round"></select>'
            '<button type="button" class="cpsm odeven">pick</button></span>'
            '<button type="button" class="cpsm odclr">clear</button>'
            '<span class="odst dim"></span></div>'
            f'<div class="odres" id="odres{int(slot)}"></div>')


def _offer_card(tag, o, chosen):
    mine = _is_ours(o["builder"])
    why = (f"protocol fees {o.get('fees', 0):,} + Jito {o.get('jito', 0):,}"
           f" x {o.get('jito_w', '?')}/10000 + builder payment "
           f"{o.get('pay', 0):,}")
    tms = "" if o.get("t_ms") is None else f" +{o['t_ms']:.1f} ms"
    return (f'<div class="odcard"><b class="odtag">{tag}</b> '
            f'<b class="{"ours" if mine else ""}">{html.escape(o["builder"])}</b>'
            f' &middot; r{o["round"]}{tms} &middot; score '
            f'<b title="{html.escape(why)}">{o["bid"]:,}</b>'
            f' <span class="dim">(fees {o.get("fees", 0):,} &middot; jito '
            f'{o.get("jito", 0):,} &middot; builder {o.get("pay", 0):,})</span>'
            f' &middot; {o["n"]:,} orders &middot; cost {o["cost"]:,} &middot; '
            f'graded {html.escape(o.get("reason") or "?")}'
            + (' &middot; <span class="tlstar">chosen</span>'
               if o["uuid"] == chosen else "")
            + f' &middot; {copy_val(o["uuid"], o["uuid"][:8] + "…")}</div>')


def _pair_table(rows, lack, mine):
    """One direction's rows (already one per bundle): the same cells as
    the accuracy diff, the "why" columns read for the lacking builder."""
    lk = html.escape(lack["builder"])
    out = ['<table class="otab"><tr><th>#</th><th>type</th><th>signature</th>'
           "<th>sigprefix</th><th>bundle</th><th>landed</th>"
           '<th class="n" title="what the order pays as the relay scores it: '
           "fee + tips to Titan/Bifrost wallets + Jito x this connector's "
           "Jito weight. landed: from the block; offered: from its bytes in "
           'our relayer\'s packet log; blank: no bytes anywhere">reward</th>'
           f'<th class="n" title="ms from {lk}\'s round_started to our builder '
           'admitting the order (scheduler/received); a bundle row is the '
           'bundle itself">recv +open</th>'
           '<th class="n" title="ms from admission to that builder\'s own '
           'winner row">win &minus;recv</th>'
           '<th class="n" title="the relay\'s hold (wait_us), ms">wait</th>'
           f'<th title="builder_order_traces on {lk}: the order\'s own state '
           f'when {lk}\'s picked offer was dispatched, then what became of it">'
           f"at {lk}'s offer</th>"
           '<th title="bundles we received under another id carrying every '
           'signature of this row">similar bundles</th>'
           f'<th title="scheduler events on our builders for this order">'
           "scheduler said</th></tr>"]
    for n, i in enumerate(rows):
        extra = ' class="selx" hidden' if n >= PAIR_SHOW else ""
        kind = i.get("kind") or "txn"
        key = (i["slot"], i["verdict"], i["order_index"], i["signature"],
               i["bundle_id"], i.get("owner_uuid"))
        rid = f'pd{abs(hash(key)):x}'
        tid, sid, aid = "t" + rid, "s" + rid, "a" + rid
        bid = (copy_val(i["bundle_id"], i["bundle_id"][:12] + "…")
               if i["bundle_id"] else '<span class="dim">&mdash;</span>')
        sigcell, bx = copy_val(i["signature"]), ""
        if i["bundle_id"] and i.get("members"):
            m_n, k = len(i["members"]), len(i.get("_rows") or [i])
            bxid = "b" + rid
            short = ((i["signature"] or "")[:24] + "…"
                     if i["signature"] else "&mdash;")
            sigcell = (f'<span class="scopen sigl" data-sx="{bxid}" '
                       'role="button" tabindex="0" title="click for every '
                       f'transaction in the bundle">{short} <span class="dim">'
                       f'{m_n} txn{"" if m_n == 1 else "s"}</span></span>')
            bid += (f' <span class="dim bxo">{k} of {m_n} in diff</span>')
            bx = (f'<tr class="sxrow" id="{bxid}" hidden><td colspan=13>'
                  + _bundle_panel(i) + "</td></tr>")
        pairs = _sched_pairs(i)
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
        if mine:
            tcell = _trace_cell(i, tid)
        else:
            tcell = (f'<td class="dim" title="{lk} is not one of our '
                     "builders: why it left the order out is not in any store "
                     'of ours">n/a</td>')
        rowcp = (f'<span class="cpv" data-c="{html.escape(_row_copy(i, kind), quote=True)}" '
                 'role="button" tabindex="0" title="the order\'s position in '
                 f'the offer that has it; click to copy the row as JSON">'
                 f'{i["order_index"]} &#x29C9;</span>')
        battr = (f' data-b="{html.escape(i["bundle_id"], quote=True)}"'
                 if i["bundle_id"] else "")
        out.append(
            f'<tr{battr}{extra}><td class="n">{rowcp}</td>'
            f'<td class="kd {kind}"'
            + ('' if i.get("received_by_us") else
               ' title="no scheduler/received on any of our builders"')
            + f'>{kind}{"" if i.get("received_by_us") else " <i>unseen</i>"}'
            + _alone_hint(i) + _api_tag(i, aid) + "</td>"
            f"<td>{sigcell}</td><td>{copy_val(i['sigprefix'])}</td>"
            f"<td>{bid}</td>"
            f'<td class="land {"y" if i["landed"] == "yes" else "n"}">'
            f'{i["landed"] or "?"}</td>'
            f'<td class="n">{_rw_cell(i)}</td>'
            f'{_recv_cells(i, i.get("our_builder"))}'
            f"{tcell}{_similar_cell(i, sid)}<td>{cell}</td></tr>")
        if bx:
            out.append(bx)
        if mine and (i.get("trace") or {}).get("rows"):
            out.append(f'<tr class="sxrow" id="{tid}" hidden><td colspan=13>'
                       + _trace_panel(i) + "</td></tr>")
        if i.get("similar") or i.get("overlap"):
            out.append(f'<tr class="sxrow" id="{sid}" hidden><td colspan=13>'
                       + _similar_panel(i, sid) + "</td></tr>")
        if pairs:
            out.append(f'<tr class="sxrow" id="{rid}" hidden><td colspan=13>'
                       + _sched_panel(i) + "</td></tr>")
        if i.get("api_bundles") or i.get("api_txs"):
            out.append(f'<tr class="sxrow" id="{aid}" hidden><td colspan=13>'
                       + _api_panel(i) + "</td></tr>")
    out.append("</table>")
    if len(rows) > PAIR_SHOW:
        out.append(f'<div class="more">showing {PAIR_SHOW} of {len(rows):,} '
                   '<button type="button" class="cpsm selmore">show all'
                   "</button></div>")
    return "".join(out)


def _pair_verdicts(units):
    """The lacking side's state at its offer, counted -- the line that
    says, at a glance, why most of these orders were left out."""
    c = collections.Counter(
        (i.get("trace") or {}).get("key") or "" for i in units)
    c.pop("", None)
    if not c:
        return ""
    return ('<div class="selcnt">state at that offer: ' + " &middot; ".join(
        f"{_trace_chip(k)} <b>{n:,}</b>" for k, n in c.most_common()) + "</div>")


def pair_html(d):
    a, b, rows = d["a"], d["b"], d["rows"]
    out = ['<div class="odout">',
           _offer_card("A", a, d["chosen"]), _offer_card("B", b, d["chosen"]),
           f'<div class="selcnt">A carries {d["na"]:,} order refs '
           f'(bundles expanded to members), B {d["nb"]:,}, '
           f'<b>{d["shared"]:,}</b> in both &middot; contents from the '
           f'{"relay" if d["src"] == RELAY_SRC else "our own store"}'
           + (f' &middot; Jito weight {d["jito_weight_bps"]:,} bps'
              if d.get("jito_weight_bps") is not None else "")
           + " &middot; " + copy_sel(rows, f"copy {len(rows):,} rows as CSV")
           + "</div>"]
    if a["round"] != b["round"]:
        out.append('<div class="selcnt aqcap">the two offers are in '
                   f'different rounds (r{a["round"]} and r{b["round"]}): an '
                   "order the earlier round's winner carried is gone from "
                   "the later round's pool, so part of this diff is the "
                   "round, not the builder</div>")
    for verdict, own, lack, tag, ltag in ((PAIR_A, a, b, "A", "B"),
                                          (PAIR_B, b, a, "B", "A")):
        group = [x for x in rows if x["verdict"] == verdict]
        units = sorted(_units(group), key=lambda i: i["order_index"])
        mine = _is_ours(lack["builder"])
        nb_ = sum(1 for i in units if i.get("bundle_id"))
        out.append(
            f'<div class="selblk"><div class="selhead">in <b>{tag}</b> '
            f'({html.escape(own["builder"])}), not in <b>{ltag}</b> '
            f'({html.escape(lack["builder"])}) &middot; <b>{len(units):,}</b> '
            f'orders ({nb_:,} bundles, {len(group):,} signatures)'
            + _rw_total(group, verdict) + "</div>")
        if not units:
            out.append('<div class="dmeta">nothing &mdash; every order of '
                       f"{tag} is also in {ltag}</div></div>")
            continue
        out.append(_kind_mix(group, verdict, f"{tag} only"))
        if mine:
            out.append(_pair_verdicts(units))
        else:
            out.append(f'<div class="selcnt dim">{html.escape(lack["builder"])}'
                       " is not ours: the why-columns show only what our "
                       "builders saw of these orders</div>")
        out.append(_pair_table(units, lack, mine) + "</div>")
    out.append("</div>")
    return "".join(out)


# ------------------------------------------------------ winner progression
#
# The offer diff page's second mode: several offers from one round's
# winning builder, side by side as distributions over compute units --
# how its offers grew from the first rung to the chosen one. Per
# transaction of each offer (bundles expanded): its declared CU (the
# compute-budget limit, or Agave's default when it sets none -- read off
# its bytes), the CU it actually consumed (meta.computeUnitsConsumed,
# only for transactions that landed in this slot), and its reward as the
# relay scores it (the block's figures when it landed here, else its
# bytes from our relayer's packet log). The page bins and draws them.

PROG_MAX = 16


def _tx_price(raw, got, bps):
    """(declared CU, reward) for one transaction's bytes, reward by the
    relay's formula: the fee less the burned 2,500 per signature, builder
    tips at face value, Jito at the connector's weight. `got` is the
    block's own figures when it landed in this slot."""
    pr = txprice.price(raw, TIP_ACCOUNTS, JITO_TIP_SET)
    if got:
        fee, jito, builder = got["fee"], got["jito"], got["tip"] - got["jito"]
    else:
        fee, jito, builder = pr["fee"], pr["jito"], pr["builder"]
    try:
        nsig = txprice.parse(raw)["n_sigs"]
    except Exception:
        nsig = 1
    _tip, reward = weigh(max(0, int(fee) - 2500 * nsig), jito, builder, bps)
    return int(pr["cu"]), int(reward)


def offer_progression(relay_q, ch, rpc_url, slot, uuids, region="all",
                      relayer_q=None):
    """One round's winning builder, offer after offer: every transaction
    of each picked offer (bundles expanded) with its declared CU, the CU
    it consumed if it landed in this slot, its reward, and whether it
    landed -- and, for the round's CHOSEN offer whether picked or not,
    every order that did not land in the slot, with where it landed
    instead and what the validator said."""
    uuids = list(dict.fromkeys(u for u in uuids if u))[:PROG_MAX]
    if not uuids:
        raise ValueError("pick at least one offer")
    tl, win, offs = find_offers(relay_q, ch, slot, uuids, region)
    miss = [u for u in uuids if u not in offs]
    if miss:
        raise ValueError("offer not found in slot %d: %s"
                         % (slot, ", ".join(u[:8] for u in miss)))
    rounds_ = {offs[u]["round"] for u in uuids}
    builders = {offs[u]["builder"] for u in uuids}
    if len(rounds_) > 1 or len(builders) > 1:
        raise ValueError("a progression is one builder in one round: these "
                         "offers span rounds %s and builders %s" % (
                             sorted(rounds_), sorted(builders)))
    rnd = next(r for r in tl["rounds"] if r["idx"] in rounds_)
    chosen = next((o["uuid"] for o in rnd["offers"] if o.get("chosen")), "")
    if chosen and chosen not in offs:
        co = next(o for o in rnd["offers"] if o["uuid"] == chosen)
        offs[chosen] = dict(co, round=rnd["idx"], won_by=rnd.get("won_by") or "",
                            anchor=rnd.get("anchor") or "",
                            connector=co.get("connector") or tl.get("connector") or "")
    if not (win and win[0]):
        win = _chunk_window([offs[u] for u in uuids])
    consumed, rew, blk = {}, {}, {}
    try:
        blk = block_tx_bytes(rpc_url, slot, consumed)
    except Exception:
        blk = {}
    try:
        rew = block_rewards(rpc_url, slot)
    except Exception:
        rew = {}
    in_block = (set(blk) | set(rew)) if (blk or rew) else None
    pmap = block_prefixes(in_block) if in_block else {}
    want = uuids + ([chosen] if chosen and chosen not in uuids else [])
    contents, src = order_contents(relay_q, ch, want, win, pmap)
    sigs = {o["sig"] for u in want for o in contents.get(u, []) if o.get("sig")}
    bodies = {}
    if relayer_q is not None:
        try:
            bodies = packet_bodies(relayer_q, sigs - set(blk), win)
        except Exception:
            bodies = {}
    memo = {}

    def tx_rec(item, bps):
        sig = item["sig"]
        key = item.get("prefix") or (sigprefix(sig) if sig else "")
        landed = (None if in_block is None or not sig else
                  (1 if sig in in_block else 0))
        cu = reward = None
        raw = (blk.get(sig) or bodies.get(sig)) if sig else None
        if raw:
            if (sig, bps) not in memo:
                try:
                    memo[(sig, bps)] = _tx_price(raw, rew.get(sig), bps)
                except Exception:
                    memo[(sig, bps)] = (None, None)
            cu, reward = memo[(sig, bps)]
        return [key or ("bundle:" + (item.get("bundle_id") or "")), cu,
                consumed.get(sig) if landed else None, reward, landed,
                item.get("bundle_id") or ""]

    def recs(u):
        o = offs[u]
        bps = DEFAULT_JITO_WEIGHT if o.get("jito_w") is None else o["jito_w"]
        seen, out = set(), []
        for it in contents.get(u, []):
            k = it.get("sig") or ("bundle:" + (it.get("bundle_id") or ""))
            if k in seen:
                continue
            seen.add(k)
            out.append(tx_rec(it, bps))
        return out

    offers = []
    for u in sorted(uuids, key=lambda u: offs[u]["at"]):
        o = offs[u]
        offers.append({
            "uuid": u, "builder": o["builder"], "round": o["round"],
            "t_ms": None if o.get("t_ms") is None else round(o["t_ms"], 1),
            "score": o["bid"], "fees": o.get("fees"), "jito": o.get("jito"),
            "pay": o.get("pay"), "orders": o["n"], "cost": o["cost"],
            "graded": o.get("reason") or "", "chosen": u == chosen,
            "txs": recs(u)})

    # the chosen offer: everything it carried that is not in this block
    missing = None
    if chosen:
        rows, items = [], contents.get(chosen, [])
        gone = [it for it in items if not it.get("sig")
                or (in_block is not None and it["sig"] not in in_block)]
        try:
            where = landed_elsewhere(rpc_url, [it["sig"] for it in gone
                                               if it.get("sig")])
        except Exception:
            where = {}
        lo, hi = win
        try:
            said = validator_results(ch, {_vkey(it) for it in gone}, lo, hi)
        except Exception:
            said = {}
        co = offs[chosen]
        bps = DEFAULT_JITO_WEIGHT if co.get("jito_w") is None else co["jito_w"]
        for it in gone:
            r = tx_rec(it, bps)
            w = where.get(it.get("sig")) if it.get("sig") else None
            v = said.get(_vkey(it)) or []
            rows.append({
                "sig": it.get("sig") or "", "prefix": it.get("prefix") or "",
                "bundle": it.get("bundle_id") or "", "type": it.get("type") or "",
                "cu": r[1], "reward": r[3],
                "elsewhere": ("?" if it.get("sig") not in where else
                              (None if w is None else w["slot"])),
                "failed": bool(w and w.get("err")),
                "reason": "; ".join(sorted({x["reason"] for x in v
                                            if x["reason"] and x["reason"] != "?"}))})
        missing = {"uuid": chosen, "picked": chosen in uuids,
                   "builder": co["builder"], "round": co["round"],
                   "t_ms": None if co.get("t_ms") is None else round(co["t_ms"], 1),
                   "score": co["bid"], "orders": co["n"],
                   "txs": sum(1 for it in items if it.get("sig")),
                   "block_known": in_block is not None, "rows": rows}
    return {"slot": slot, "bin": 50_000, "src": src, "block": in_block is not None,
            "builder": next(iter(builders)), "round": next(iter(rounds_)),
            "winner": rnd.get("won_by") or "", "chosen": chosen,
            "offers": offers, "missing": missing}


# -------------------------------------------- declared CU / reward profile
#
# Every transaction each side OFFERED in a round, not only the diff: what
# it declared it would burn (its SetComputeUnitLimit, or Agave's default
# when it sets none) and what it pays. Same offers the diff compares --
# the winner's chosen miniblock, our offer as it stood at the close.

PROFILE_METRICS = (
    ("cu", "declared CU", "CU"),
    ("reward", "reward", "lamports"),
    ("density", "reward per declared CU", "lamports/CU"),
)


def profile_sigs(keep):
    """Every signature in the offers `keep` holds."""
    contents = (keep or {}).get("contents") or {}
    out = set()
    for _rnd, _tb, tv, ours in (keep or {}).get("plan") or []:
        for v in [tv] + [v for _b, v in ours]:
            out.update(o["sig"] for o in contents.get(v.get("uuid"), [])
                       if o["sig"])
    return out


def offer_profile(keep, blk_bytes=None, rewards=None, bodies=None,
                  segs=None):
    """-> [{round, winner, sides: [{kind, builder, won, n, n_sig, recs}]}].

    A tx is profiled only if its bytes are somewhere -- the block (landed
    in this slot) or our relayer's packet log. Its reward is what the
    block says it paid when it landed here, else what its bytes would pay
    (txprice: fee + static transfers to a tip account). Gravity wallets
    are not tip accounts here, as everywhere on this page."""
    plan = (keep or {}).get("plan") or []
    contents = (keep or {}).get("contents") or {}
    blk_bytes, rewards, bodies = blk_bytes or {}, rewards or {}, bodies or {}
    memo = {}

    def rec(sig, bps):
        if (sig, bps) in memo:
            return memo[(sig, bps)]
        raw = blk_bytes.get(sig) or bodies.get(sig)
        out = None
        if raw:
            try:
                pr = txprice.price(raw, TIP_ACCOUNTS, JITO_TIP_SET)
                got = rewards.get(sig)
                if got:
                    jito = got.get("jito", 0)
                    fee, builder, src = got["fee"], got["tip"] - jito, "block"
                else:
                    fee, jito, builder, src = (pr["fee"], pr["jito"],
                                               pr["builder"], "bytes")
                _tip, reward = weigh(fee, jito, builder, bps)
                out = {"cu": pr["cu"], "explicit": pr["cu_explicit"],
                       "reward": reward, "src": src,
                       "density": reward / pr["cu"] if pr["cu"] else None}
            except Exception:
                out = None
        memo[(sig, bps)] = out
        return out

    prof = []
    for rnd, tb, tv, ours in plan:
        bps = round_weight(segs or {}, rnd)
        sides = []
        for kind, b, v in [("titan", tb, tv)] + [("ours", b, v)
                                                  for b, v in ours]:
            items = contents.get(v.get("uuid"), [])
            if not items:
                continue
            sigs = list(dict.fromkeys(o["sig"] for o in items if o["sig"]))
            sides.append({"kind": kind, "builder": b,
                          "won": b == rnd["winner"], "n": len(items),
                          "n_sig": len(sigs),
                          "recs": [r for r in (rec(x, bps) for x in sigs)
                                   if r]})
        if (any(x["kind"] == "titan" for x in sides)
                and any(x["kind"] == "ours" for x in sides)):
            prof.append({"round": rnd["idx"], "winner": rnd["winner"],
                         "jito_weight_bps": bps, "sides": sides})
    prof.sort(key=lambda r: r["round"])
    return prof


def _qtile(vals, q):
    """Nearest-rank quantile of a sorted list."""
    if not vals:
        return None
    return vals[max(0, min(len(vals) - 1,
                           int(round(q / 100 * (len(vals) - 1)))))]


def _num(v):
    if v is None:
        return "&mdash;"
    a = abs(v)
    if a >= 1e9:
        return f"{v / 1e9:.2f}B"
    if a >= 1e6:
        return f"{v / 1e6:.2f}M"
    if a >= 1e4:
        return f"{v / 1e3:.1f}k"
    if a >= 100:
        return f"{v:,.0f}"
    if a >= 1:
        return f"{v:.2f}"
    return f"{v:.3g}"


def _decade(e):
    """10**e as an axis label: 1k, 10M, 0.01."""
    for p, suf in ((9, "B"), (6, "M"), (3, "k")):
        if e >= p:
            return f"{10 ** (e - p)}{suf}"
    return f"{10 ** e}" if e >= 0 else f"{10.0 ** e:g}"


def _stats(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return None
    return {"n": len(v), "min": v[0], "max": v[-1], "sum": sum(v),
            "mean": sum(v) / len(v),
            **{f"p{q}": _qtile(v, q) for q in (10, 25, 50, 75, 90)}}


def _side_label(x):
    return x["builder"] + (" (won)" if x["won"] else "")


def _box_svg(sides, key, title, unit):
    """One metric, one row per side, on a shared log axis."""
    rows = [(x, _stats([r[key] for r in x["recs"]])) for x in sides]
    # the axis spans the whiskers, not min..max: one 1.4M-CU outlier would
    # otherwise squeeze every box into a sliver. Extremes are in the tooltip.
    vals = [st[k] for _x, st in rows if st for k in ("p10", "p90")]
    vals = [v for v in vals if v and v > 0]
    if not vals:
        return ""
    lo = math.floor(math.log10(min(vals)))
    hi = math.ceil(math.log10(max(vals)))
    if hi <= lo:
        hi = lo + 1
    lab_w, plot_w, row_h, top = 128, 230, 26, 6
    W = lab_w + plot_w + 24
    step = max(1, math.ceil(44 / (plot_w / (hi - lo))))
    H = top + row_h * len(rows) + 22

    def X(v):
        v = min(max(v, 10 ** lo), 10 ** hi)
        return lab_w + (math.log10(v) - lo) / (hi - lo) * plot_w

    g = [f'<svg class="pfsvg" viewBox="0 0 {W} {H}" width="{W}" '
         f'height="{H}" role="img" aria-label="{html.escape(title)}">']
    for e in range(lo, hi + 1):
        x = X(10 ** e)
        g.append(f'<line class="pfgrid" x1="{x:.1f}" x2="{x:.1f}" '
                 f'y1="{top}" y2="{H - 18}"/>')
        if (e - lo) % step == 0:
            g.append(f'<text class="pfax" x="{x:.1f}" y="{H - 5}" '
                     f'text-anchor="middle">{_decade(e)}</text>')
    for i, (x, st) in enumerate(rows):
        y = top + i * row_h
        cls = "ti" if x["kind"] == "titan" else "us"
        name = html.escape(_side_label(x))
        if not st:
            g.append(f'<text class="pflab" x="0" y="{y + 16}">{name}</text>'
                     f'<text class="pfax" x="{lab_w}" y="{y + 16}">'
                     "no priced txs</text>")
            continue
        tip = (f"{_side_label(x)} - {title}, {st['n']} txs: "
               f"p10 {_num(st['p10'])} · p25 {_num(st['p25'])} · "
               f"median {_num(st['p50'])} · p75 {_num(st['p75'])} · "
               f"p90 {_num(st['p90'])} · max {_num(st['max'])} · "
               f"mean {_num(st['mean'])} {unit}")
        x10, x25, x50, x75, x90 = (X(st[k]) for k in
                                   ("p10", "p25", "p50", "p75", "p90"))
        cy = y + 12
        g.append(f'<g class="pfrow" tabindex="0" data-tip="{html.escape(tip)}">'
                 f'<rect class="pfhit" x="0" y="{y}" width="{W}" '
                 f'height="{row_h}"/>'
                 f'<text class="pflab" x="0" y="{cy + 4}">{name}</text>'
                 f'<line class="pfwhisk {cls}" x1="{x10:.1f}" x2="{x90:.1f}" '
                 f'y1="{cy}" y2="{cy}"/>'
                 f'<rect class="pfbox {cls}" x="{x25:.1f}" y="{cy - 6}" '
                 f'width="{max(2.0, x75 - x25):.1f}" height="12" rx="3"/>'
                 f'<line class="pfmed" x1="{x50:.1f}" x2="{x50:.1f}" '
                 f'y1="{cy - 7}" y2="{cy + 7}"/></g>')
    g.append("</svg>")
    return (f'<div class="pfchart"><div class="pftitle">{html.escape(title)} '
            f'<span class="dim">{html.escape(unit)}, log scale</span></div>'
            + "".join(g) + "</div>")


def _profile_table(sides):
    head = ("<tr><th>side</th><th>txs offered</th><th>priced</th>"
            "<th>no explicit limit</th><th>&Sigma; declared CU</th>"
            "<th>&Sigma; reward</th><th>&Sigma;reward / &Sigma;CU</th>"
            "<th>median CU</th><th>median reward</th>"
            "<th>median reward/CU</th></tr>")
    body = []
    for x in sides:
        recs = x["recs"]
        cu = sum(r["cu"] for r in recs)
        rw = sum(r["reward"] for r in recs)
        nb = sum(1 for r in recs if r["src"] == "block")
        nd = sum(1 for r in recs if not r["explicit"])
        st = {k: _stats([r[k] for r in recs]) for k, _t, _u in PROFILE_METRICS}
        med = lambda k: _num(st[k]["p50"]) if st[k] else "&mdash;"
        cls = "ti" if x["kind"] == "titan" else "us"
        body.append(
            f'<tr><td><span class="vd {cls}">{html.escape(_side_label(x))}'
            f'</span></td><td class="n">{x["n"]:,}</td>'
            f'<td class="n" title="{nb} priced from the block (landed here), '
            f'{len(recs) - nb} from their bytes in our relayer\'s packet '
            f'log; {x["n_sig"] - len(recs)} have no bytes anywhere, '
            f'{x["n"] - x["n_sig"]} are bundle refs we could not expand">'
            f'{len(recs):,}</td><td class="n">{nd:,}</td>'
            f'<td class="n">{cu:,}</td><td class="n">{rw:,}</td>'
            f'<td class="n">{_num(rw / cu) if cu else "&mdash;"}</td>'
            f'<td class="n">{med("cu")}</td><td class="n">{med("reward")}</td>'
            f'<td class="n">{med("density")}</td></tr>')
    return f'<table class="otab pftab">{head}{"".join(body)}</table>'


def profile_html(prof):
    out = ['<div class="selsec"><div class="seltitle">declared CU / reward '
           '<span class="dim">&mdash; every transaction each side offered, '
           "Titan against us, per round</span></div>"
           '<div class="dmeta aqcap">Declared CU is the transaction\'s own '
           "SetComputeUnitLimit, or Agave's default when it sets none "
           "(3,000 per builtin instruction, 200,000 per other, capped at "
           "1.4M). Reward is scored the relay's way: fee + Titan/Bifrost tips "
           "+ Jito tips &times; the connector's Jito weight / 10000 &mdash; "
           "what the block paid when it landed in this slot, otherwise what "
           "its bytes would pay. Only transactions whose bytes we have "
           "count &mdash; the block or our relayer's packet log; most "
           "unlanded searcher bundle members have none. Boxes are p25&ndash;"
           "p75, whiskers p10&ndash;p90, the bar is the median.</div>"]
    if not prof:
        out.append('<div class="dmeta">no round in this slot has both a '
                   "Titan and an Astralane offer with stored contents</div>"
                   "</div>")
        return "".join(out)
    for r in prof:
        sides = r["sides"]
        out.append(f'<div class="selblk"><div class="selhead"><b>r{r["round"]}'
                   f'</b> &middot; won by {html.escape(r["winner"] or "?")} '
                   f'&middot; Jito weight {r["jito_weight_bps"]:,} bps '
                   '&middot; <span class="pfkey ti"></span>Titan '
                   '<span class="pfkey us"></span>Astralane</div>'
                   '<div class="pfcharts">'
                   + "".join(_box_svg(sides, k, t, u)
                             for k, t, u in PROFILE_METRICS)
                   + "</div>" + _profile_table(sides) + "</div>")
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
           "relay_score", "raw_sum", "jito_weight_bps",
           "builder_payment", "jito_tips", "protocol_fees",
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
    if r["slot"] is not None and r.get("win_lo") is None and ch is not None:
        # A slot only our own relay served has no rows in Gattaca's store,
        # so its window has to come from ours or the page stays empty.
        try:
            _, w = ch("SELECT toString(min(ts)), toString(max(ts)) "
                      "FROM bifrost_miniblocks WHERE ts > now() - INTERVAL "
                      f"14 DAY AND slot = {int(r['slot'])}")
            if w and w[0][0] and not w[0][0].startswith("1970"):
                pad = dt.timedelta(minutes=2)
                r["win_lo"] = dt.datetime.fromisoformat(w[0][0][:26]) - pad
                r["win_hi"] = dt.datetime.fromisoformat(w[0][1][:26]) + pad
        except Exception:
            pass
    if r["slot"] is not None and r.get("win_lo") is None:
        # nobody's store has it (no builder offered): the chain's block time
        try:
            _, w = relay_q("SELECT toString(estimated_slot_time) FROM "
                           "solana.block_summary WHERE estimated_slot_time > "
                           "now() - INTERVAL 14 DAY AND slot = "
                           f"{int(r['slot'])} LIMIT 1")
            if w and w[0][0]:
                t = dt.datetime.fromisoformat(w[0][0][:26])
                pad = dt.timedelta(minutes=2)
                r["win_lo"], r["win_hi"] = t - pad, t + pad
        except Exception:
            pass
    sub_w, _ = _bounds(r)[1], None
    _, rows = relay_q(
        "SELECT slot, index_in_slot AS idx, builder_id, count() AS offers, "
        f"  max({score_sql(relay_q)}) AS best_bid, max(order_count) AS n, "
        "  toString(min(timestamp)) AS first_t, "
        "  toString(max(timestamp)) AS last_t, "
        "  any(connector_identity) AS conn "
        "FROM relay.mini_block_events_v2 "
        f"WHERE {sub_w} AND event = 'submitted'{_region_sql(region)}"
        f"{_connector_sql(connector)} "
        "GROUP BY slot, idx, builder_id ORDER BY slot DESC, idx ASC")
    by = {}
    # the slot's leader: one connector per slot, whichever relay served it
    conn_of = {}
    for slot, idx, bld, offers, bid, n, ft, lt, conn in rows:
        if conn:
            conn_of.setdefault(int(slot), conn)
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
            # in slot mode only that slot -- the window is padded by two
            # minutes, which otherwise pulls in the validator's
            # neighbouring slots too (45 rows for one slot)
            _, blocks = relay_q(
                "SELECT slot FROM solana.block_summary "
                f"WHERE estimated_slot_time >= '{lo:{f}}' "
                f"  AND estimated_slot_time <= '{hi:{f}}' "
                f"  AND leader = '{connector}'"
                + (f" AND slot = {int(r['slot'])}"
                   if r["slot"] is not None else ""))
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
                    + (f"  AND slot = {int(r['slot'])} "
                       if r["slot"] is not None else "")
                    + "GROUP BY slot, idx, local_builder_id")
            except Exception:
                mine = []
            # Only slots the relay's store knows NOTHING about -- no offer,
            # no round opened. Merging per (round, builder) tagged slots
            # Gattaca did serve: a round where one of our builders logged
            # the winner but made no offer has no `submitted` row for it,
            # which read as "the relay never saw this round" (7 slots of
            # Ledger by Figment in 6 h, 2026-10-23). Slots past the
            # relay's newest are its ~12 s ingest lag, not another relay.
            relay_slots = ({sl for sl, rs in by.items() if rs}
                           | {sl for sl, v in led.items()
                              if v.get("rounds_opened")})
            relay_tip = max(relay_slots) if relay_slots else None
            for slot, idx, bld, offers, bid, n, ft, lt, won in mine:
                slot, idx = int(slot), int(idx)
                if slot in relay_slots or (relay_tip is not None
                                           and slot > relay_tip):
                    continue
                rounds_ = by.setdefault(slot, {})
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
    return ([{"slot": sl, "rounds": by[sl], "led": led.get(sl),
              "connector": conn_of.get(sl) or connector or ""}
             for sl in slots],
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
        "  toString(mini_block_uuid) AS uuid, is_last, connector_identity, "
        "  cost_budget, skipped_rounds, instance_id "
        f"FROM relay.mini_block_events_v2 WHERE {tb} AND slot = {int(slot)}"
        f"{_region_sql(region)} ORDER BY idx, t")
    # round_chosen carries no slot, so the winner is the chosen uuid
    # matched back to whichever submitted row owns it
    _, ch_rows = relay_q(
        "SELECT toString(mini_block_uuid) AS uuid, toString(timestamp) AS t "
        f"FROM relay.mini_block_events_v2 WHERE {tb} "
        f"  AND event = 'round_chosen'{_region_sql(region)}")
    chosen = {u: t for u, t in ch_rows if u}

    try:
        segs = jito_weights(relay_q)
    except Exception:
        segs = {}
    rounds_ = {}
    leader, leader_w = "", None
    for (idx, ev, bld, t, pay, jito, fees, cost, n, reason, uuid, is_last,
         conn, budget, skipped, inst) in rows:
        i = int(idx)
        if conn and not leader:
            leader = conn
            leader_w = weight_at(segs, conn, t)
        d = rounds_.setdefault(i, {"idx": i, "opened": "", "offers": [],
                                   "sealed": "", "tail": "",
                                   "is_last": False})
        if ev == "round_opened":
            d["opened"] = d["opened"] or t
            # the round's budget and skips live on its opening row; the
            # submitted rows carry 0
            d["budget"], d["skipped"] = int(budget or 0), int(skipped or 0)
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
        bps = weight_at(segs, conn, t)
        d["offers"].append({
            "at": t, "builder": bld or "", "uuid": uuid or "",
            "pay": int(pay or 0), "jito": int(jito or 0),
            "fees": int(fees or 0), "jito_w": bps,
            # what the relay ranks on; `raw` is the plain sum
            "bid": score_of(fees or 0, jito or 0, pay or 0, bps),
            "raw": int(pay or 0) + int(jito or 0) + int(fees or 0),
            "cost": int(cost or 0), "n": int(n or 0),
            "reason": reason or "", "relay": inst or "",
            "connector": conn or "",
            "is_last": str(is_last).lower() in ("1", "true")})
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
    return {"slot": slot, "rounds": out, "connector": leader,
            "jito_w": leader_w}


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
        "  won_by_us, connector_identity FROM bifrost_miniblocks "
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
    leader = connector
    for kind, idx, bld, t, uuid, reward, cost, n, won, conn in mb:
        leader = leader or conn
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
    return {"slot": slot, "rounds": out, "source": "ours",
            "connector": leader or ""}


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

RACE_HUES = ["var(--c0)", "var(--c6)", "var(--c2)", "var(--c4)"]
RACE_SHAPES = ["circle", "square", "diamond", "triangle", "plus", "bar"]
RACE_OTHER = "var(--text3)"


def _epoch_ms(t):
    try:
        return int(dt.datetime.fromisoformat(t[:26]).timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def offer_record(slot, rnd, o):
    """Everything the relay's `submitted` row says about one offer, plus
    what the page derives from it -- what a click on an offer copies."""
    rec = {"slot": int(slot), "rnd": rnd["idx"],
           "mini_block_uuid": o["uuid"], "builder_id": o["builder"],
           "submitted_at": o["at"],
           "ms_after_open": (None if o.get("t_ms") is None
                             else round(o["t_ms"], 3)),
           "round_opened_at": rnd.get("anchor") or "",
           "relay_score": o["bid"], "raw_reward": o.get("raw", o["bid"]),
           "protocol_fees": o.get("fees"), "jito_tips": o.get("jito"),
           "jito_weight_bps": o.get("jito_w"),
           "builder_payment": o.get("pay"),
           "execution_cost": o["cost"], "cost_budget": rnd.get("budget"),
           "order_count": o["n"], "graded": o["reason"],
           "chosen": bool(o.get("chosen")), "is_last": o.get("is_last"),
           "skipped_rounds": rnd.get("skipped"),
           "connector_identity": o.get("connector"),
           "relay_instance": o.get("relay")}
    return {k: v for k, v in rec.items() if v is not None}


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
                         "v": o["reason"].replace("_", " "),
                         "x": offer_record(tl["slot"], rnd, o)})
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


def race_html(tl, pick=False):
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
                       "ours": ours, "slot": tl["slot"], "pick": pick},
                      separators=(",", ":"))
    n = sum(len(r["bids"]) for r in data.values())
    return (
        '<div class="racewrap"><div class="dtlhead">the race'
        f'<span class="note">{n:,} offers across {len(data)} rounds '
        "&middot; time runs down, one column per builder, one bullet per "
        "offer &middot; hover a bullet for the bid and the order count "
        "&middot; every bid is the relay's score: Jito tips count at the "
        "connector's Jito weight, not at face value"
        "</span></div>"
        f'<div class="olegend">{legend}'
        '<span class="omark"><i class="omk ohalo2"></i>highest relay score</span>'
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
            "yes" if o["chosen"] else "no", o["bid"],
            o.get("raw", o["bid"]), o.get("jito_w", ""), o["pay"], o["jito"],
            o["fees"], o["cost"], o["n"], o["reason"], o["uuid"], o["at"])))
    return "\n".join(lines)


def _validator_cell(pubkey, name=""):
    """The slot's leader: its name where the Firedancer report knows it,
    then the pubkey, shortened but copied whole."""
    if not pubkey:
        return '<span class="dim">&mdash;</span>'
    lab = (f'<span class="vname">{html.escape(name)}</span> ' if name else "")
    return lab + copy_val(pubkey, pubkey[:8] + "\u2026")


def timeline_html(d, names=None, pick=False, region="all"):
    """The race first, then the same offers as numbers underneath.

    `pick`: the offer diff page -- every bullet and every row picks an
    offer instead of copying it, and the pair's diff loads under the race
    (OFFDIFF_JS)."""
    if not d["rounds"]:
        return ('<div class="dmeta">the relay recorded no offer for this '
                "slot in the window</div>")
    out = []
    conn = d.get("connector") or ""
    if conn:
        w = d.get("jito_w")
        out.append(
            '<div class="rhead">validator '
            + _validator_cell(conn, (names or {}).get(conn) or "")
            + stakingfac_link(conn)
            + (f' &middot; <span class="dim" title="the weight the relay '
               "scored this slot's Jito tips at, net of its 600 bps Jito "
               f'fee">Jito weight {w:,} bps</span>' if w is not None else "")
            + "</div>")
    out.append(race_html(d, pick))
    if pick:
        out.append(pair_bar_html(d["slot"], region))
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
                rowsh.append(f'<tr class="tlgap"><td colspan={9 if pick else 8}>'
                             "&middot;&middot;&middot;</td></tr>")
                continue
            pct = max(0.0, min(100.0, 100.0 * (o["t_ms"] or 0) / span))
            mine = _is_ours(o["builder"])
            raw = o.get("raw", o["bid"])
            why = (f"protocol fees {o.get('fees', 0):,} + Jito "
                   f"{o.get('jito', 0):,} x {o.get('jito_w', '?')}/10000 + "
                   f"builder payment {o.get('pay', 0):,}")
            tms = "&mdash;" if o["t_ms"] is None else f"+{o['t_ms']:.0f}"
            star = (' <span class="tlstar">chosen</span>' if o["chosen"]
                    else "")
            rec = json.dumps(offer_record(d["slot"], rnd, o),
                             separators=(",", ":"))
            pk = ""
            if pick:
                lab = (f'{o["builder"]} r{rnd["idx"]} {tms.replace("&mdash;", "")} ms'
                       f' \u00b7 {o["bid"]:,}')
                pk = (f'<td><span class="opk" role="button" tabindex="0" '
                      f'data-ou="{html.escape(o["uuid"], quote=True)}" '
                      f'data-slot="{d["slot"]}" '
                      f'data-l="{html.escape(lab, quote=True)}" '
                      'title="pick this offer for the diff">pick</span></td>')
            rowsh.append(
                f'<tr{" class=\"tlwin\"" if o["chosen"] else ""}>{pk}'
                f'<td class="n"><span class="cpv" data-c="{html.escape(rec, quote=True)}" '
                'role="button" tabindex="0" title="click to copy this offer '
                f'(uuid, rewards, slot, round, ...) as JSON">{tms}</span></td>'
                f'<td class="{"ours" if mine else ""}">'
                f'{html.escape(o["builder"])}{star}</td>'
                f'<td class="n" title="{html.escape(why)}">{o["bid"]:,}</td>'
                f'<td class="n dim">{raw:,}</td>'
                f'<td class="n">{o["cost"]:,}</td><td class="n">{o["n"]:,}</td>'
                f'<td>{html.escape(o["reason"])}</td>'
                f'<td class="tlbar"><i style="width:{pct:.1f}%"></i></td></tr>')
        out.append('<table class="otab tltab"><tr>'
                   + ("<th></th>" if pick else "") + '<th>t +ms</th>'
                   '<th>builder</th><th title="what the relay ranks on: '
                   "protocol fees + Jito tips x the connector's Jito weight "
                   '/ 10000 + builder payment">relay score</th>'
                   '<th title="protocol fees + Jito tips + builder payment, '
                   'Jito unweighted">raw</th><th>exec cost</th>'
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


def timeline_table_html(r, slots, shown, total, ours_re, slot_ref=None,
                        names=None, frag="/tech/offer-timelines/slot"):
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
                 f'<span class="m">{copy_val(r["connector"])}</span>'
                 f'{stakingfac_link(r["connector"])} only')
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
           '<table class="atab2"><tr><th>slot</th>'
           '<th title="the slot\'s leader: the connector the relay opened its rounds for">validator</th>'
           '<th class="n">rounds</th>'
           '<th class="n">offers</th><th>builders</th>'
           '<th class="n" title="the highest relay score offered: protocol fees + Jito tips x the connector\'s Jito weight / 10000 + builder payment">best score</th></tr>'
           '<tbody id="aqbody" data-metric="timeline" '
           f'data-frag="{html.escape(frag, quote=True)}" '
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
            f'<td class="m">{_validator_cell(srow.get("connector") or "", (names or {}).get(srow.get("connector") or "") or "")}</td>'
            f'<td class="n">{len(rs)}</td><td class="n">{offers:,}</td>'
            f"<td>{tags}</td><td class=\"n\">{best:,}</td></tr>"
            f'<tr class="drow" id="d{sl}" hidden><td colspan=6>'
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


def _export_tag(r):
    """File-name part: the range, plus the validator when the page is
    filtered to one (the export covers exactly what the page shows)."""
    tag = re.sub(r"[^0-9a-zA-Z]+", "-", r["label"]).strip("-")
    if r.get("connector"):
        tag += "_" + r["connector"][:8]
    elif r.get("region", "all") not in ("", "all"):
        tag += "_" + r["region"]
    return tag


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
                                    region=r.get("region", "all"),
                                    connector=r.get("connector", ""))
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
            % (_export_tag(r),
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
                                    region=r.get("region", "all"),
                                    connector=r.get("connector", ""))
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
            % (_export_tag(r),
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



/* winner progression: only the winner can be picked; the chosen offer's
   misses; the hatch key */
svg .om.opkdim{opacity:.16}
.opk.opkdim{visibility:hidden}
.odmiss{margin:10px 0 14px;padding:12px 16px;border-radius:10px;border:1px solid var(--line);
  border-left:3px solid var(--line3);background:var(--surface);font-size:13px}
.odmiss.bad{border-left-color:var(--bad-main);background:var(--bad-bg)}
.odmiss.good{border-left-color:var(--good-main)}
.odmiss.warn{border-left-color:var(--orange-main)}
.odmiss .odmh{margin-bottom:6px;color:var(--text)}
.odmiss .odmh b{color:var(--bad)}
.odmiss table{margin-top:4px}
.odmiss .odms{color:var(--text2);font-size:12.5px;margin:2px 0}
.odmiss .odms b{color:var(--text)}
.odmiss .odmall{margin-top:8px;background:var(--surface);color:var(--text);
  border:1px solid var(--line2);border-radius:999px;padding:4px 12px;font:inherit;font-size:12.5px;cursor:pointer}
.odpt td.odadd{color:var(--text)}
.odpt td.oddrop{color:var(--text2)}
.odpt td.odnl{color:var(--bad);font-weight:600}
.odkey{display:inline-flex;align-items:center;gap:6px;margin-left:auto;color:var(--text3);
  font-size:12px}
.odkey b{color:var(--bad)}
.odkey i{display:inline-block;width:14px;height:10px;border-radius:2px;margin-left:8px;
  border:1px solid var(--line3)}
.odkey i.below{background:radial-gradient(var(--accent-main) 1px,transparent 1.3px) 0 0/4px 4px;opacity:.7}
/* offer diff: winner progression */
.odbar select.odmode{font-weight:600}
.odbar [hidden]{display:none!important}
.odchips{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.odchip{display:inline-flex;align-items:center;gap:6px;padding:3px 4px 3px 9px;
  border:1px solid var(--line2);border-radius:999px;background:var(--surface);font-size:12px}
.odchip i{width:9px;height:9px;border-radius:50%;flex:none}
.odx{border:0;background:none;color:var(--text3);cursor:pointer;font-size:14px;
  line-height:1;padding:0 4px}
.odx:hover{color:var(--text)}
.odauto{display:inline-flex;align-items:center;gap:6px;color:var(--text2);font-size:12.5px}
.odauto input.odpn{width:52px;padding:4px 6px}
.odauto select{padding:4px 6px}
.opk.opkp::after{content:" ✓"}
.odprog{margin:6px 0 18px;color:var(--text)}
.odprog .dim,.odctl .dim{color:var(--text3)}
.odbar{color:var(--text)}
.odchip{color:var(--text)}
.odbar .cpsm{background:var(--surface);color:var(--text);border:1px solid var(--line2);
  border-radius:999px;padding:4px 12px;font:inherit;font-size:12.5px;cursor:pointer}
.odbar .cpsm:hover{border-color:var(--text3)}
.odbar .odeven{background:var(--text);color:var(--bg);border-color:var(--text)}
.odph b{font-weight:600}
.odph{margin:4px 0 8px;font-size:13px}
.odpt tr[data-u]{cursor:pointer}
.odpt tr.off td{opacity:.4}
.odsw{display:inline-block;width:12px;height:12px;border-radius:3px}
.odctl{display:flex;flex-wrap:wrap;gap:8px 12px;align-items:center;margin:14px 0 10px;
  font-size:12.5px}
.odtg{display:inline-flex;border:1px solid var(--line2);border-radius:999px;overflow:hidden}
.odtg button{border:0;background:transparent;color:var(--text2);padding:5px 12px;
  font:inherit;font-size:12.5px;cursor:pointer}
.odtg button[aria-pressed=true]{background:var(--text);color:var(--bg)}
.odcharts{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}
.odcard{background:var(--surface);border:1px solid var(--line);border-radius:10px;
  padding:14px 16px 6px}
.odcard.odwide{grid-column:1/-1}
.odch{font-size:12.5px;font-weight:600;color:var(--text2);text-transform:uppercase;
  letter-spacing:.06em;margin-bottom:4px}
.odch .dim{text-transform:none;letter-spacing:0;font-weight:400;margin-left:6px}
@media (max-width:600px){.odcharts{grid-template-columns:minmax(0,1fr)}}
/* offer diff: the picker bar and the two picks */
.odbar{position:sticky;bottom:0;z-index:5;display:flex;flex-wrap:wrap;gap:8px;
  align-items:center;background:var(--bg2,var(--bg));border:1px solid var(--line);
  border-radius:6px;padding:7px 10px;margin:8px 0;font-size:12px}
.odlab{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--text3)}
.odpick{padding:2px 8px;border-radius:4px;border:1px dashed var(--line2)}
.odpick i{color:var(--text3);font-style:normal}
.odpick.set{border-style:solid}
.odA.set{border-color:var(--accent)}
.odB.set{border-color:var(--orange-main,#d97706)}
.odres:empty{display:none}
.odres{margin:6px 0 14px}
.odcard{margin:4px 0;font-size:12px}
.odtag{display:inline-block;min-width:16px;text-align:center;border-radius:3px;
  padding:0 4px;color:var(--bg);background:var(--accent)}
.odcard + .odcard .odtag{background:var(--orange-main,#d97706)}
.opk{cursor:pointer;font-size:10px;padding:1px 6px;border:1px solid var(--line2);
  border-radius:3px;color:var(--text3)}
.opk:hover,.opk:focus{color:var(--accent);border-color:var(--accent);outline:none}
.opk.opka{background:var(--accent);color:var(--bg);border-color:var(--accent)}
.opk.opkb{background:var(--orange-main,#d97706);color:var(--bg);
  border-color:var(--orange-main,#d97706)}
.opk.opka::after{content:" A"}
.opk.opkb::after{content:" B"}
svg .om.opka{stroke:var(--accent);stroke-width:3.2}
svg .om.opkb{stroke:var(--orange-main,#d97706);stroke-width:3.2}
.cpcsv{margin-left:10px;padding:1px 8px;border:1px solid var(--line2);
  border-radius:5px;font-size:10px;color:var(--text3);cursor:pointer;
  text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.cpcsv:hover,.cpcsv:focus{color:var(--accent);border-color:var(--accent-bg);outline:none}
.aqform{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap;
  padding:16px 28px;border-bottom:1px solid var(--line)}
.aqform label[hidden]{display:none}
.aqform label{display:flex;flex-direction:column;gap:4px;color:var(--text3);
  font-size:10.5px;text-transform:uppercase;letter-spacing:.04em}
.aqform input,.aqform select{background:var(--bg);border:1px solid var(--line2);
  border-radius:6px;color:var(--accent);padding:6px 10px;
  font:12px ui-monospace,Menlo,monospace}
.aqform input:focus,.aqform select:focus{outline:none;border-color:var(--accent)}
.aqform button{background:var(--primary);border:1px solid var(--primary);color:#fff;
  border-radius:6px;padding:7px 16px;font-size:12px;cursor:pointer}
.aqform button:hover{filter:brightness(.92)}
.aqform .ts-wrapper{min-width:360px;text-transform:none;letter-spacing:0}
.aqform .ts-control{background:var(--bg);border:1px solid var(--line2);
  border-radius:6px;color:var(--accent);padding:6px 10px;min-height:0;
  box-shadow:none;font:12px ui-monospace,Menlo,monospace}
.aqform .ts-wrapper.focus .ts-control{border-color:var(--accent);box-shadow:none}
.aqform .ts-control>input{background:none;border:0;padding:0;
  color:var(--accent);font:inherit}
.aqform .ts-dropdown{background:var(--bg);border:1px solid var(--line2);
  color:var(--accent);font:12px ui-monospace,Menlo,monospace;
  text-transform:none;letter-spacing:0}
.aqform .ts-dropdown-content{max-height:380px}
.aqform .ts-dropdown .active{background:var(--accent-bg);color:var(--accent)}
.aqform .ts-dropdown .highlight{background:var(--accent-bg);color:inherit}
.aqform .ts-dropdown .create,.aqform .ts-dropdown .no-results{color:var(--accent)}
.aqform .ts-dropdown .dim{color:var(--text3);font-size:10.5px}
.aqform .ts-dropdown .dropdown-input-wrap{padding:6px}
.aqform .ts-dropdown .dropdown-input{background:var(--bg);border:1px solid var(--line2);
  border-radius:4px;color:var(--accent);padding:5px 8px;font:inherit;width:100%}
.aqform .ts-dropdown .dropdown-input:focus{outline:none;border-color:var(--accent)}
.aqhead{padding:14px 28px 0;color:var(--text2);font-size:12.5px}
.aqhead b{color:var(--text)}
.aqcap{color:var(--warn)}
.atab2{margin:12px 28px 40px;border-collapse:collapse;width:calc(100% - 56px);
  max-width:1400px;font-size:12.5px}
.atab2 th{text-align:left;color:var(--text3);font-size:10.5px;font-weight:600;
  text-transform:uppercase;letter-spacing:.04em;padding:0 12px 7px 0;
  border-bottom:1px solid var(--line)}
.atab2 th.n,.atab2 td.n{text-align:right}
.atab2 td{padding:7px 12px 7px 0;border-bottom:1px solid var(--line);color:var(--accent)}
.atab2 td.m{font:11.5px ui-monospace,Menlo,monospace}
.srow{cursor:pointer}
.srow:hover{background:var(--bg)}
.srow td:first-child::before{content:"\\25B8 ";color:var(--accent)}
.srow.open td:first-child::before{content:"\\25BE "}
/* a slot the picked validator led that no builder at all offered into */
.srow.noours td{background:var(--bad-bg)}
.srow.noours td:first-child{color:var(--bad)}
.srow.noours td:first-child::before{color:var(--bad)}
.srow.noours:hover td{background:var(--bad-bg)}
.redn{color:var(--bad)}
.otab td.neg{color:var(--warn)}
.otab td.held{color:var(--violet)}
.kd{font:11px ui-monospace,Menlo,monospace}
.kd.bundle{color:var(--violet)}.kd.txn{color:var(--text)}
td.kd i{font-style:normal;color:var(--bad);font-size:10px;margin-left:4px}
.apio{font-size:10px;color:var(--accent);cursor:pointer;white-space:nowrap}
.apibox .otab{margin:4px 0 10px}
.apibox .apimatch{color:var(--warn)}
.selfilt{display:flex;flex-wrap:wrap;gap:10px 14px;align-items:center;
  margin:6px 0 10px;padding:6px 10px;border:1px solid var(--line);
  border-radius:6px;font-size:12px}
.selfilt select{margin-left:4px;max-width:340px}
.sfk{cursor:pointer;white-space:nowrap}
.alone{white-space:nowrap;font-size:11.5px}
.gfilt{margin:10px 28px 0}
.gfilt .gfnote{font-size:11px}
.sf-use{font-size:11px;color:var(--accent);cursor:pointer}
.pfcharts{display:flex;flex-wrap:wrap;gap:18px;margin:6px 0 8px}
.pfchart{min-width:0}
.pftitle{font-size:12px;color:var(--text);margin-bottom:2px}
.pfsvg{display:block;max-width:100%;height:auto;overflow:visible}
.pfgrid{stroke:var(--line);stroke-width:1}
.pfax{fill:var(--text3);font:10px ui-monospace,Menlo,monospace}
.pflab{fill:var(--text2);font:11px ui-monospace,Menlo,monospace}
.pfhit{fill:transparent}
.pfrow:hover .pfhit,.pfrow:focus .pfhit{fill:var(--line)}
.pfrow:focus{outline:none}
.pfbox.ti,.pfkey.ti{fill:var(--orange-main);background:var(--orange-main)}
.pfbox.us,.pfkey.us{fill:var(--accent-main);background:var(--accent-main)}
.pfbox{stroke:var(--line);stroke-width:2}
.pfwhisk{stroke-width:2;stroke-linecap:round}
.pfwhisk.ti{stroke:var(--orange-main)}.pfwhisk.us{stroke:var(--accent-main)}
.pfmed{stroke:var(--text2);stroke-width:2;stroke-linecap:round}
.pfkey{display:inline-block;width:10px;height:10px;border-radius:2px;
  margin:0 4px 0 8px;vertical-align:-1px}
.pftab td.n{text-align:right}
.extbtn{display:inline-block;padding:1px 8px;margin-left:4px;border:1px solid var(--line3);border-radius:4px;font-size:11px;color:var(--accent);text-decoration:none;white-space:nowrap}
.extbtn:hover{background:var(--bg2);border-color:var(--accent-main)}
.sage{margin-left:8px;font-size:10.5px;font-variant-numeric:tabular-nums}
.wtag.own{color:var(--warn);border-color:var(--warn-bg)}
.nextlead{margin:10px 28px 0;color:var(--text2);font-size:12.5px}
.nextlead b{color:var(--accent)}
.nextlead .nl-eta{color:var(--accent);font:600 13px ui-monospace,Menlo,monospace}
.nextlead .nl-eta.past{color:var(--warn)}
.ours{color:var(--accent);font-weight:600}
.wtag{font:10.5px ui-monospace,Menlo,monospace;padding:1px 7px;
  border-radius:4px;border:1px solid var(--line2);color:var(--text2)}
.vname{font:12px system-ui,sans-serif;color:var(--text)}
.wtag.us{border-color:var(--accent-main);color:var(--accent);background:var(--accent-bg)}
.drow>td{padding:0 0 0 0;background:var(--bg)}
.dbox{padding:12px 28px 16px 20px}
.dmeta{color:var(--text3);font-size:11.5px;margin-bottom:9px}
.otab{border-collapse:collapse;width:100%;font-size:11.5px}
.otab th{text-align:left;color:var(--text3);font-size:10px;text-transform:uppercase;
  padding:0 10px 5px 0;border-bottom:1px solid var(--line)}
.otab td{padding:4px 10px 4px 0;border-bottom:1px solid var(--line);
  font:11px ui-monospace,Menlo,monospace;color:var(--text)}
.land{font-weight:650}
.land.y{color:var(--accent)}
.land.n{color:var(--bad)}
.land.u{color:var(--text3)}
.aqexp{display:flex;align-items:center;gap:12px;margin-left:auto}
.aqexp button{background:var(--bg);border:1px solid var(--line2);color:var(--text2);
  border-radius:6px;padding:7px 14px;font-size:12px;cursor:pointer}
.aqexp button:hover:not(:disabled){border-color:var(--accent-main);color:var(--accent)}
.aqexp button:disabled{opacity:.55;cursor:default}
.aqexpmsg{font-size:11.5px;color:var(--text3);max-width:460px}
.aqexpmsg a{color:var(--accent);font-weight:650}
.aqexpmsg b{color:var(--text)}
.aqexpmsg.bad{color:var(--bad)}
.selsec{margin-top:18px;border-top:1px solid var(--line);padding-top:12px}
.seltitle{color:var(--text);font-size:12px;font-weight:650;margin-bottom:8px;
  text-transform:lowercase;letter-spacing:.02em}
.selblk{margin:10px 0 14px;padding-left:10px;border-left:2px solid var(--line)}
.selhead{color:var(--text2);font-size:11.5px;font-weight:600}
.selbid{color:var(--text3);font-size:11px;margin:2px 0 4px;
  font:11px ui-monospace,Menlo,monospace}
.selcnt{color:var(--text2);font-size:11px;margin:3px 0 6px}
.vd{font:10px ui-monospace,Menlo,monospace;padding:1px 6px;border-radius:4px;
  border:1px solid var(--line2)}
.vd.us{color:var(--accent);border-color:var(--accent-bg)}
.vd.ti{color:var(--warn);border-color:var(--warn-bg)}
.vd.na{color:var(--text3)}
.rwt{color:var(--text)}
.tltab td{vertical-align:middle}
.tltab .tlbar{width:28%;padding-right:0}
.tltab .tlbar i{display:block;height:5px;border-radius:3px;
  background:var(--bg3);min-width:2px}
.racewrap{margin:4px 0 18px}
.tlwin{background:var(--accent-bg)}
.tlgap td{color:var(--text3);text-align:center;letter-spacing:4px;padding:2px 0}
.tlwin .tlbar i{background:var(--accent-main)}
.tlstar{font:9px ui-monospace,Menlo,monospace;color:var(--accent);
  border:1px solid var(--accent-bg);border-radius:4px;padding:0 5px;margin-left:6px;
  text-transform:uppercase;letter-spacing:.05em}
.sc{font:10px ui-monospace,Menlo,monospace;color:var(--bad);margin-right:6px;
  white-space:nowrap}
.sc i{color:var(--text2);font-style:normal;margin-left:4px}
.scopen{cursor:pointer;border-bottom:1px dotted var(--line2)}
.tr{font:10px ui-monospace,Menlo,monospace;white-space:nowrap}
.tr.pend{color:var(--accent)}.tr.hold{color:var(--violet)}
.tr.bad{color:var(--bad)}.tr.inc{color:var(--good)}.tr.na{color:var(--text3)}
.trthen{font-size:10px;color:var(--text3);white-space:nowrap}
td.trc{min-width:120px}
.trtab tr.trmark td{background:var(--accent-bg);color:var(--text2);font-size:10.5px}
.trtab tr.trat td{box-shadow:inset 0 -1px 0 var(--accent)}
.scopen:hover,.scopen:focus{border-bottom-color:var(--accent);outline:none}
/* A hovered value GLOWS, not just recolours: these are 88-char
   signatures in an 11.5px table, and the eye needs help finding the one
   under the pointer. Hovering any value in a bundle row gives the same
   glow to every value in every row of that bundle, so it reads as one
   thing (the class is set by the hover handler in ACCURACY_JS). */
.cpv:hover,.cpv:focus,.cpv.glow{color:var(--accent);background:var(--accent-bg);
  text-shadow:0 0 6px var(--accent-line),0 0 14px var(--accent-line)}
.scopen::after{content:" \\25B8";color:var(--accent);font-size:9px}
.scopen.open::after{content:" \\25BE"}
.sxrow>td{background:var(--bg);padding:0}
.sxbox{padding:8px 10px 10px;border-left:2px solid var(--line2);margin:4px 0 8px}
.sxh{color:var(--text2);font-size:11px;margin-bottom:6px}
.sxev{margin-bottom:7px}
.sxmeta{color:var(--text3);font-size:10.5px}
.sxmeta b{color:var(--bad)}
.sxpre{margin:2px 0 0;padding:6px 8px;background:var(--bg);
  border:1px solid var(--line);border-radius:5px;color:var(--text);
  font:10.5px ui-monospace,Menlo,monospace;white-space:pre-wrap;
  word-break:break-all}
.nowhere{color:var(--bad)}
.vr{font-weight:650}
.vr.y{color:var(--accent)}
.vr.n{color:var(--warn)}
.rhead{margin:10px 0 5px;color:var(--text2);font-size:11.5px;font-weight:600}
.more{color:var(--text3);font-size:11px;font-style:italic;margin-top:6px}
.more .cpsm{font-style:normal;margin-left:8px}
.bxo{margin-left:6px;font-size:10px;color:var(--text2)}
"""

# The offer diff page: pick two offers (a bullet in the race, or a row's
# "pick"), and the diff of the pair loads under the race. OFFERS_JS calls
# window.__offerPick on a bullet click when its data says pick mode, and
# window.__offerPaint after every redraw so the picks survive a zoom.
OFFDIFF_JS = r'''
(function(){
  // Two modes, one picker.
  //   diff: A then B -- the order diff of the pair (/tech/offer-diff/pair).
  //   prog: one round's WINNING builder, offer after offer
  //         (/tech/offer-diff/progression): what each offer added to the
  //         one before it, stacked over 50k-CU bins, and whatever the
  //         chosen offer carried that never landed.
  // OFFERS_JS calls __offerPick on a bullet click when the race is in pick
  // mode, and __offerPaint after every redraw so the picks survive a zoom.
  var KEY = 'offdiff_mode_v1', MAX = 16, BIN = 50000;
  var mode = 'diff';
  try { if (localStorage.getItem(KEY) === 'prog') mode = 'prog'; } catch (e) {}
  var sel = [];                                   // diff: [{u, slot, label}]
  var prog = {slot: '', round: null, builder: '', picks: []};   // picks: [{u, t, label}]
  var hidden = {}, view = {v: 'steps', rw: 'sum', basis: 'decl'}, lastData = null, timer = null;

  function tok(n){ return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
  function rgb(c){
    c = (c || '').trim();
    var m = c.match(/^#([0-9a-f]{6})$/i);
    if (m) { var v = parseInt(m[1], 16); return [v >> 16 & 255, v >> 8 & 255, v & 255]; }
    m = c.match(/rgba?\(([^)]+)\)/);
    if (m) { var p = m[1].split(',').map(parseFloat); return [p[0], p[1], p[2]]; }
    return [128, 128, 128];
  }
  // one distinct colour per offer: the site's categorical chart tokens, in
  // the order that keeps neighbours furthest apart, then generated hues
  var CAT = ['--c0', '--c6', '--c4', '--c8', '--c2', '--c7', '--c1', '--c5', '--c3', '--c9'];
  function ink(col){
    // dark or light label text for a segment of this colour
    var m = col.match(/hsl\((\d+),\s*\d+%,\s*(\d+)%/);
    if (m) return +m[2] > 55 ? '#141413' : '#ffffff';
    var c = rgb(col), l = (0.299 * c[0] + 0.587 * c[1] + 0.114 * c[2]) / 255;
    return l > 0.62 ? '#141413' : '#ffffff';
  }
  function palette(n){
    var out = [];
    for (var k = 0; k < n; k++) {
      if (k < CAT.length) { out.push('rgb(' + rgb(tok(CAT[k])).join(',') + ')'); continue; }
      var h = Math.round((k - CAT.length) * 137.5 + 20) % 360;
      out.push('hsl(' + h + ',62%,' + (document.documentElement.getAttribute('data-theme') === 'dark'
        || (!document.documentElement.getAttribute('data-theme') && matchMedia('(prefers-color-scheme: dark)').matches)
        ? '64%' : '46%') + ')');
    }
    return out;
  }
  function esc(s){ var d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML; }
  function bar(slot){ return document.querySelector('.odbar[data-slot="' + slot + '"]'); }
  function out(slot){ return document.getElementById('odres' + slot); }
  function index(slot){
    var idx = {};
    document.querySelectorAll('script[id="offdata"]').forEach(function(s){
      var D; try { D = JSON.parse(s.textContent); } catch (e) { return; }
      if (String(D.slot) !== String(slot)) return;
      D.rounds.forEach(function(rd){
        var w = rd.chosen ? rd.chosen.b : '';
        rd.bids.forEach(function(b){
          idx[b.u] = {b: b.b, round: rd.round, winner: w, t: b.t, r: b.r,
                      chosen: !!(rd.chosen && rd.chosen.u === b.u),
                      base: rd.opened || (rd.bids[0] || {}).t || b.t};
        });
      });
    });
    return idx;
  }
  function label(info){
    return info.b + ' r' + info.round + ' +' + Math.round(info.t - info.base)
      + ' ms · ' + info.r.toLocaleString() + (info.chosen ? ' · chosen' : '');
  }
  function colors(){
    var c = palette(prog.picks.length), m = {};
    prog.picks.forEach(function(p, k){ m[p.u] = c[k]; });
    return m;
  }

  function paint(){
    var pc = mode === 'prog' ? colors() : {};
    document.querySelectorAll('.odbar').forEach(function(b){
      var slot = b.getAttribute('data-slot'), idx = mode === 'prog' ? index(slot) : {};
      b.querySelector('.odmode').value = mode;
      b.querySelectorAll('.odm-diff').forEach(function(x){ x.hidden = mode !== 'diff'; });
      b.querySelectorAll('.odm-prog').forEach(function(x){ x.hidden = mode !== 'prog'; });
      var mine = sel.length && sel[0].slot === slot;
      [['.odA', 0, 'click an offer in the race or a row’s pick'],
       ['.odB', 1, 'then a second one']].forEach(function(z){
        var el = b.querySelector(z[0]), s = mine ? sel[z[1]] : null;
        el.classList.toggle('set', !!s);
        el.innerHTML = '<b>' + (z[1] ? 'B' : 'A') + '</b> ';
        var t = document.createElement(s ? 'span' : 'i');
        t.textContent = s ? s.label : z[2];
        el.appendChild(t);
      });
      var chips = b.querySelector('.odchips');
      var here = prog.slot === slot ? prog.picks : [];
      chips.innerHTML = here.length ? here.map(function(p, k){
        return '<span class="odchip" data-u="' + esc(p.u) + '"><i style="background:'
          + pc[p.u] + '"></i>#' + (k + 1) + ' ' + esc(p.label)
          + '<button type="button" class="odx" title="remove">×</button></span>';
      }).join('') : '<i class="dim">click the winning builder’s offers in one round '
        + '(the others are greyed out), or pick them evenly →</i>';
      var rs = b.querySelector('.odpr');
      if (rs && !rs.options.length) {
        var all = index(slot), seen = {};
        Object.keys(all).forEach(function(u){
          var x = all[u];
          if (x.winner && x.b === x.winner) seen[x.round] = (seen[x.round] || 0) + 1;
        });
        Object.keys(seen).sort(function(a, c){ return a - c; }).forEach(function(r){
          var any = Object.keys(all).find(function(u){ return String(all[u].round) === r; });
          var o = document.createElement('option');
          o.value = r; o.textContent = 'r' + r + ' · ' + all[any].winner + ' (' + seen[r] + ' offers)';
          rs.appendChild(o);
        });
      }
      // in progression mode only the winner's offers can be picked: in the
      // picked round once one is picked, else any round's winner
      document.querySelectorAll('[data-ou]').forEach(function(e){
        var u = e.getAttribute('data-ou'), x = idx[u];
        if (!x && mode === 'prog') return;
        var ok = mode !== 'prog' || (x.winner && x.b === x.winner
          && (prog.round === null || prog.slot !== slot || x.round === prog.round));
        e.classList.toggle('opkdim', !ok);
      });
    });
    document.querySelectorAll('[data-ou]').forEach(function(e){
      var u = e.getAttribute('data-ou'), i = -1;
      if (mode === 'diff') sel.forEach(function(s, k){ if (s.u === u) i = k; });
      e.classList.toggle('opka', i === 0);
      e.classList.toggle('opkb', i === 1);
      var on = mode === 'prog' && pc[u];
      if (mode !== 'prog') e.classList.remove('opkdim');
      e.classList.toggle('opkp', !!on);
      if (e.tagName.toLowerCase() === 'span') {
        e.style.background = on ? pc[u] : '';
        e.style.borderColor = on ? pc[u] : '';
        e.style.color = on ? 'var(--text)' : '';
      } else {
        e.style.stroke = on ? pc[u] : '';
        e.style.strokeWidth = on ? '3.4px' : '';
      }
    });
  }
  window.__offerPaint = paint;
  function status(slot, text){ var b = bar(slot); if (b) b.querySelector('.odst').textContent = text || ''; }

  // ---------------------------------------------------------------- diff
  function loadDiff(){
    if (sel.length !== 2) return;
    var slot = sel[0].slot, b = bar(slot), o = out(slot);
    if (!b || !o) return;
    status(slot, 'diffing…');
    o.innerHTML = '<div class="dmeta">reading both offers, the block, '
      + 'the scheduler events and the order traces…</div>';
    var want = sel[0].u + '|' + sel[1].u;
    fetch('/tech/offer-diff/pair?slot=' + encodeURIComponent(slot)
          + '&a=' + encodeURIComponent(sel[0].u) + '&b=' + encodeURIComponent(sel[1].u)
          + '&region=' + encodeURIComponent(b.getAttribute('data-region') || 'all'))
      .then(function(r){ return r.text(); })
      .then(function(t){
        if (mode !== 'diff' || sel.length !== 2 || sel[0].u + '|' + sel[1].u !== want) return;
        o.innerHTML = t; status(slot, '');
        o.scrollIntoView({behavior: 'smooth', block: 'start'});
      })
      .catch(function(e){ status(slot, ''); o.innerHTML = '<div class="dmeta">could not load: ' + esc(e) + '</div>'; });
  }

  // ---------------------------------------------------------------- progression
  function loadProg(){
    clearTimeout(timer);
    timer = setTimeout(function(){
      var slot = prog.slot, b = bar(slot), o = out(slot);
      if (!b || !o) return;
      if (!prog.picks.length) { o.innerHTML = ''; return; }
      status(slot, 'reading ' + prog.picks.length + ' offer' + (prog.picks.length > 1 ? 's' : '') + '…');
      var want = prog.picks.map(function(p){ return p.u; }).join(',');
      fetch('/tech/offer-diff/progression?slot=' + encodeURIComponent(slot)
            + '&u=' + encodeURIComponent(want)
            + '&region=' + encodeURIComponent(b.getAttribute('data-region') || 'all'))
        .then(function(r){ return r.json(); })
        .then(function(d){
          if (mode !== 'prog' || prog.picks.map(function(p){ return p.u; }).join(',') !== want) return;
          status(slot, '');
          if (d.error || d.state === 'error') {
            o.innerHTML = '<div class="dmeta aqcap">' + esc(d.error) + '</div>'; return;
          }
          lastData = d; renderProg(o);
        })
        .catch(function(e){ status(slot, ''); o.innerHTML = '<div class="dmeta">could not load: ' + esc(e) + '</div>'; });
    }, 250);
  }
  function seg(name, opts, cur){
    return '<span class="odtg" data-k="' + name + '">' + opts.map(function(o){
      return '<button type="button" data-v="' + o[0] + '" aria-pressed="' + (o[0] === cur) + '">' + o[1] + '</button>';
    }).join('') + '</span>';
  }
  function n0(v){ return v == null ? '—' : Math.round(v).toLocaleString(); }
  function steps(d){
    // per offer, in time order: what it added to the previous one and what it dropped
    var prev = null;
    return d.offers.map(function(o){
      var m = {};
      o.txs.forEach(function(t){ m[t[0]] = t; });
      var add = [], drop = [];
      o.txs.forEach(function(t){ if (!prev || !prev.m[t[0]]) add.push(t); });
      if (prev) prev.o.txs.forEach(function(t){ if (!m[t[0]]) drop.push(t); });
      var st = {o: o, m: m, add: add, drop: drop, prevScore: prev ? prev.o.score : null};
      prev = st;
      return st;
    });
  }
  function sum(ts, f){ return ts.reduce(function(a, t){ return a + (f(t) || 0); }, 0); }
  function missingHtml(d){
    var m = d.missing;
    if (!m) return '<div class="odmiss dim">this round has no chosen offer</div>';
    var who = esc(m.builder) + '’s chosen offer (r' + m.round + ' +' + (m.t_ms == null ? '?' : m.t_ms)
      + ' ms, score ' + m.score.toLocaleString() + (m.picked ? '' : ', not among your picks') + ')';
    if (!m.block_known) return '<div class="odmiss warn">The block for slot ' + d.slot
      + ' could not be read, so whether ' + who + ' landed is unknown.</div>';
    if (!m.rows.length) return '<div class="odmiss good">Every transaction of ' + who
      + ' landed in slot ' + d.slot + ' (' + m.txs + ' of ' + m.txs + ').</div>';
    var where = {earlier: 0, later: 0, nowhere: 0, unknown: 0, failed: 0}, why = {};
    m.rows.forEach(function(r){
      if (r.elsewhere === '?') where.unknown++;
      else if (r.elsewhere == null) where.nowhere++;
      else if (r.elsewhere < d.slot) where.earlier++;
      else where.later++;
      if (r.failed) where.failed++;
      (r.reason || 'no reason logged').split('; ').forEach(function(x){
        if (x !== 'included') why[x] = (why[x] || 0) + 1; });
    });
    var bits = [['landed in an earlier slot', where.earlier], ['landed in a later slot', where.later],
                ['nowhere on chain', where.nowhere], ['unknown', where.unknown],
                ['of those landed, failed', where.failed]]
      .filter(function(z){ return z[1]; })
      .map(function(z){ return z[0] + ' <b>' + z[1] + '</b>'; }).join(' \u00b7 ');
    var reasons = Object.keys(why).sort(function(a, c){ return why[c] - why[a]; })
      .map(function(k){ return esc(k) + ' <b>' + why[k] + '</b>'; }).join(' \u00b7 ');
    var SHOW = 8;
    var rows = m.rows.map(function(r, k){
      var w = r.elsewhere === '?' ? '<span class="dim">unknown</span>'
        : r.elsewhere == null ? '<b>nowhere on chain</b>'
        : 'slot ' + r.elsewhere + (r.failed ? ' (failed)' : '');
      return '<tr' + (k >= SHOW ? ' class="odmx" hidden' : '') + '><td>' + (r.bundle ? 'bundle member' : 'txn') + '</td>'
        + '<td class="m">' + (r.sig ? '<span class="cpv" data-c="' + esc(r.sig) + '" role="button" tabindex="0">'
                              + esc(r.sig.slice(0, 28)) + '…</span>' : '<span class="dim">no signature</span>') + '</td>'
        + '<td class="m">' + esc(r.prefix) + '</td>'
        + '<td class="m">' + (r.bundle ? '<span class="cpv" data-c="' + esc(r.bundle) + '" role="button" tabindex="0">'
                              + esc(r.bundle.slice(0, 12)) + '…</span>' : '—') + '</td>'
        + '<td class="n">' + n0(r.cu) + '</td><td class="n">' + n0(r.reward) + '</td>'
        + '<td>' + w + '</td><td>' + (r.reason ? esc(r.reason) : '<span class="dim">—</span>') + '</td></tr>';
    }).join('');
    return '<div class="odmiss bad"><div class="odmh"><b>' + m.rows.length + ' of ' + m.txs
      + ' transactions</b> of ' + who + ' did not land in slot ' + d.slot + '</div>'
      + '<div class="odms">' + bits + '</div>'
      + (reasons ? '<div class="odms">validator said: ' + reasons + '</div>' : '')
      + '<table class="otab"><tr><th>type</th><th>signature</th><th>sigprefix</th><th>bundle</th>'
      + '<th class="n">declared CU</th><th class="n">reward</th><th>landed instead</th><th>validator said</th></tr>'
      + rows + '</table>'
      + (m.rows.length > SHOW ? '<button type="button" class="cpsm odmall">show all ' + m.rows.length + '</button>' : '')
      + '</div>';
  }
  function renderProg(o){
    var d = lastData, st = steps(d), cols = colors();
    var rows = st.map(function(s, k){
      var x = s.o, notl = s.o.txs.filter(function(t){ return t[4] === 0; }).length;
      var cons = s.o.txs.filter(function(t){ return t[2] != null; }).length;
      var priced = s.o.txs.filter(function(t){ return t[3] != null; }).length;
      var dscore = s.prevScore == null ? '' : (x.score - s.prevScore);
      return '<tr class="' + (hidden[x.uuid] ? 'off' : '') + '" data-u="' + esc(x.uuid) + '">'
        + '<td><span class="odsw" style="background:' + cols[x.uuid] + '"></span></td>'
        + '<td class="n">#' + (k + 1) + '</td>'
        + '<td class="n">' + (x.t_ms == null ? '—' : '+' + x.t_ms.toFixed(1)) + '</td>'
        + '<td class="n">' + x.score.toLocaleString() + (x.chosen ? ' <span class="tlstar">chosen</span>' : '') + '</td>'
        + '<td class="n">' + (dscore === '' ? '' : (dscore >= 0 ? '+' : '') + dscore.toLocaleString()) + '</td>'
        + '<td class="n">' + x.orders.toLocaleString() + '</td>'
        + '<td class="n">' + s.o.txs.length.toLocaleString() + '</td>'
        + '<td class="n odadd">+' + s.add.length + ' <span class="dim">' + n0(sum(s.add, function(t){ return t[3]; })) + '</span></td>'
        + '<td class="n oddrop">' + (s.drop.length ? '−' + s.drop.length + ' <span class="dim">' + n0(sum(s.drop, function(t){ return t[3]; })) + '</span>' : '<span class="dim">—</span>') + '</td>'
        + '<td class="n' + (notl ? ' odnl' : '') + '">' + (notl ? notl : '<span class="dim">0</span>') + '</td>'
        + '<td class="n">' + cons + ' <span class="dim">/ ' + s.o.txs.length + '</span></td>'
        + '<td class="n">' + priced + ' <span class="dim">/ ' + s.o.txs.length + '</span></td></tr>';
    }).join('');
    var first = d.offers[0], last = d.offers[d.offers.length - 1], gone = {};
    d.offers.forEach(function(x){ x.txs.forEach(function(t){ if (t[4] === 0) gone[t[0]] = 1; }); });
    var nl = Object.keys(gone).length;
    o.innerHTML = '<div class="odprog">'
      + '<div class="odph"><b>' + esc(d.builder) + '</b> · r' + d.round + ' of slot ' + d.slot
      + ' · ' + d.offers.length + ' offer' + (d.offers.length > 1 ? 's' : '')
      + (first.t_ms != null ? ', +' + first.t_ms + ' → +' + last.t_ms + ' ms after the round opened' : '')
      + ' <span class="dim">· reward as the relay scores it: fee less the burned 2,500 per signature, + Titan/Bifrost tips, + Jito at the connector’s weight</span></div>'
      + missingHtml(d)
      + '<table class="otab odpt"><tr><th></th><th class="n">step</th><th class="n">ms after open</th>'
      + '<th class="n">relay score</th><th class="n">Δ score</th><th class="n">orders</th><th class="n">txs</th>'
      + '<th class="n" title="transactions in this offer that the previous one did not carry: count, reward">added</th>'
      + '<th class="n" title="transactions the previous offer carried that this one dropped: count, reward">dropped</th>'
      + '<th class="n" title="transactions of this offer that are not in the slot’s block">not landed</th>'
      + '<th class="n" title="landed in this slot, so the CU it burned is known">consumed CU known</th>'
      + '<th class="n" title="bytes found in the block or our relayer’s packet log, so declared CU and reward are known">priced</th></tr>'
      + rows + '</table>'
      + '<div class="odctl">' + seg('v', [['steps', 'what each offer added'], ['whole', 'each offer whole']], view.v)
      + '<span class="odm-whole"' + (view.v === 'whole' ? '' : ' hidden') + '><span class="dim">reward per bin</span> '
      + seg('rw', [['sum', 'total'], ['mean', 'per tx']], view.rw) + '</span>'
      + '<span class="dim">count by</span>' + seg('basis', [['decl', 'declared CU'], ['used', 'consumed CU']], view.basis)
      + '<span class="odkey">' + (nl ? 'charts leave out the <b>' + nl + '</b> transaction' + (nl > 1 ? 's' : '')
        + ' that did not land in slot ' + d.slot + ' · ' : 'every transaction drawn landed in slot ' + d.slot + ' · ')
      + '<i class="below"></i>below zero: dropped since the previous offer · click a step row to hide it</span></div>'
      + '<div class="odcharts">'
      + '<div class="odcard"><div class="odch">reward by declared CU</div><div class="odplot" data-c="decl"></div></div>'
      + '<div class="odcard"><div class="odch">reward by consumed CU</div><div class="odplot" data-c="used"></div></div>'
      + '<div class="odcard odwide"><div class="odch">transactions by CU <span class="dim odcb"></span></div><div class="odplot" data-c="count"></div></div>'
      + '</div></div>';
    draw(o);
    o.scrollIntoView({behavior: 'smooth', block: 'start'});
  }
  // the charts draw only transactions that landed in this slot: one that
  // did not never executed in this block, so its CU and reward describe
  // nothing that happened (they are listed in the callout instead)
  function landed(ts){ return ts.filter(function(t){ return t[4] !== 0; }); }
  function binsOf(ts, which, val){
    var b = {}, n = {};
    ts.forEach(function(t){
      var cu = t[which]; if (cu == null) return;
      var v = val(t); if (v == null) return;
      var i = Math.floor(cu / BIN);
      b[i] = (b[i] || 0) + v; n[i] = (n[i] || 0) + 1;
    });
    return {b: b, n: n};
  }
  function draw(o){
    if (!window.Plotly) {
      if (!window.__odPlotly) {
        window.__odPlotly = true;
        var sc = document.createElement('script');
        sc.src = '__PLOTLY_CDN__'; sc.charset = 'utf-8';
        sc.onload = function(){ draw(o); };
        document.head.appendChild(sc);
      }
      return;
    }
    var d = lastData, st = steps(d), cols = colors(), surf = 'rgb(' + rgb(tok('--surface')).join(',') + ')';
    var cb = o.querySelector('.odcb');
    if (cb) cb.textContent = view.basis === 'decl' ? 'declared CU' : 'consumed CU';
    o.querySelectorAll('.odm-whole').forEach(function(x){ x.hidden = view.v !== 'whole'; });
    o.querySelectorAll('.odplot').forEach(function(el){
      var c = el.getAttribute('data-c');
      var which = c === 'decl' ? 1 : c === 'used' ? 2 : (view.basis === 'decl' ? 1 : 2);
      var val = c === 'count' ? function(){ return 1; } : function(t){ return t[3]; };
      var top = 0, traces = [];
      function grow(bk){ Object.keys(bk.b).forEach(function(i){ top = Math.max(top, +i); }); }
      st.forEach(function(s){ grow(binsOf(landed(s.o.txs), which, val)); });
      var nb = top + 1, xs = [];
      for (var i = 0; i < nb; i++) xs.push(i * BIN + BIN / 2);
      var unit = c === 'count' ? ' txs' : ' lamports';
      if (view.v === 'steps') {
        st.forEach(function(s, k){
          if (hidden[s.o.uuid]) return;
          var col = cols[s.o.uuid], nm = '#' + (k + 1) + ' +' + s.o.t_ms + ' ms';
          [[landed(s.add), 'added', false, 1],
           [landed(s.drop), 'dropped', false, -1]].forEach(function(part){
            if (!part[0].length) return;
            var bk = binsOf(part[0], which, val), ys = [], cd = [], any = false;
            for (var i = 0; i < nb; i++) {
              var v = bk.b[i]; cd.push(bk.n[i] || 0);
              if (v) { ys.push(part[3] * v); any = true; } else ys.push(null);
            }
            if (!any) return;
            var mk = {color: col, line: {color: surf, width: 0.8}};
            if (part[3] < 0) { mk.opacity = 0.55; mk.pattern = {shape: '.', fillmode: 'replace', fgcolor: col, bgcolor: surf, size: 5, solidity: 0.5}; }
            traces.push({type: 'bar', x: xs, y: ys, customdata: cd, width: BIN * 0.86, marker: mk,
                         text: ys.map(function(v){ return v == null ? '' : '#' + (k + 1); }),
                         textposition: 'inside', insidetextanchor: 'middle',
                         textfont: {size: 10, color: part[3] < 0 ? 'var(--text2)' : ink(col)},
                         textangle: 0, cliponaxis: false,
                         name: nm + ' ' + part[1],
                         hovertemplate: '%{y:,.0f}' + unit + (c === 'count' ? '' : ' · %{customdata} txs')
                           + '<extra>' + esc(nm + ' ' + part[1]) + '</extra>'});
          });
        });
      } else {
        st.forEach(function(s){
          if (hidden[s.o.uuid]) return;
          var col = cols[s.o.uuid], bk = binsOf(landed(s.o.txs), which, val), ys = [], cd = [];
          for (var i = 0; i < nb; i++) {
            var cnt = bk.n[i] || 0, v = bk.b[i] || 0; cd.push(cnt);
            ys.push(c === 'count' ? cnt : view.rw === 'mean' ? (cnt ? v / cnt : null) : v);
          }
          traces.push({type: 'scatter', mode: 'lines+markers', x: xs, y: ys, customdata: cd,
                       connectgaps: true, line: {color: col, width: 2}, marker: {size: 5, color: col},
                       name: '#' + (st.indexOf(s) + 1),
                       hovertemplate: '%{y:,.0f}' + unit + (c === 'count' ? '' : ' · %{customdata} txs')
                         + '<extra>#' + (st.indexOf(s) + 1) + ' +' + s.o.t_ms + ' ms</extra>'});
        });
      }
      var layout = {
        height: c === 'count' ? 320 : 300, margin: {l: 58, r: 14, t: 8, b: 46},
        paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
        font: {size: 11, color: 'var(--text2)'}, showlegend: false,
        barmode: 'relative', bargap: 0.1, hovermode: 'x unified',
        uniformtext: {minsize: 9, mode: 'hide'},
        hoverlabel: {bgcolor: 'var(--surface)', bordercolor: 'var(--line2)', font: {color: 'var(--text)', size: 11}},
        xaxis: {title: {text: (which === 2 ? 'consumed' : 'declared') + ' CU, 50k bins',
                        font: {size: 11, color: 'var(--text3)'}, standoff: 8},
                range: [0, nb * BIN], tickformat: '~s', showgrid: false, zeroline: false,
                showline: true, linecolor: 'var(--line2)', ticks: 'outside', tickcolor: 'var(--line2)',
                ticklen: 4, hoverformat: '~s'},
        yaxis: {title: {text: c === 'count' ? 'transactions'
                         : (view.v === 'whole' && view.rw === 'mean' ? 'reward per tx (lamports)' : 'reward (lamports)'),
                        font: {size: 11, color: 'var(--text3)'}, standoff: 6},
                tickformat: '~s', gridcolor: 'var(--line)', zeroline: true, zerolinecolor: 'var(--line2)',
                zerolinewidth: 1}};
      bbTheme.plot(el, traces, layout, {displayModeBar: false, responsive: true});
    });
  }

  // ---------------------------------------------------------------- picking
  function setPicks(slot, round, builder, list){
    prog = {slot: slot, round: round, builder: builder, picks: list};
    hidden = {};
  }
  window.__offerPick = function(u, slot){
    slot = String(slot);
    var idx = index(slot), info = idx[u];
    if (mode === 'diff') {
      if (sel.length && sel[0].slot !== slot) sel = [];
      var i = -1;
      sel.forEach(function(s, k){ if (s.u === u) i = k; });
      if (i >= 0) sel.splice(i, 1);
      else { sel.push({u: u, slot: slot, label: info ? label(info) : u.slice(0, 8)}); if (sel.length > 2) sel.shift(); }
      paint();
      if (sel.length === 2) loadDiff();
      return;
    }
    if (!info) return;
    if (!info.winner) { status(slot, 'r' + info.round + ' has no winner'); return; }
    if (info.b !== info.winner) {
      status(slot, 'only the winner’s offers: r' + info.round + ' was won by ' + info.winner); return;
    }
    var j = prog.slot === slot ? prog.picks.findIndex(function(p){ return p.u === u; }) : -1;
    if (j >= 0) {
      prog.picks.splice(j, 1);
      if (!prog.picks.length) prog.round = null;
    } else {
      if (prog.slot !== slot || prog.round === null || prog.round !== info.round) {
        if (prog.picks.length && prog.slot === slot) status(slot, 'started over in r' + info.round + ': a progression is one round');
        else status(slot, '');
        setPicks(slot, info.round, info.b, []);
      } else status(slot, '');
      if (prog.picks.length >= MAX) { status(slot, 'at most ' + MAX + ' offers'); return; }
      prog.picks.push({u: u, t: info.t, label: label(info)});
      prog.picks.sort(function(a, c){ return a.t - c.t; });
    }
    paint(); loadProg();
  };
  function pickEvenly(b){
    var slot = b.getAttribute('data-slot'), r = b.querySelector('.odpr').value;
    var nv = b.querySelector('.odpn').value, idx = index(slot), all = [];
    Object.keys(idx).forEach(function(u){
      var x = idx[u];
      if (String(x.round) === String(r) && x.winner && x.b === x.winner) all.push([u, x]);
    });
    all.sort(function(a, c){ return a[1].t - c[1].t; });
    var ch = all.findIndex(function(z){ return z[1].chosen; });
    if (ch >= 0) all = all.slice(0, ch + 1);       // rungs after the chosen one did not count
    if (!all.length) { status(slot, 'no offers from that round’s winner'); return; }
    var n = nv === 'all' ? Math.min(MAX, all.length) : Math.max(1, Math.min(MAX, parseInt(nv, 10) || 5));
    n = Math.min(n, all.length);
    var at = [];
    for (var k = 0; k < n; k++) {
      var p = n === 1 ? all.length - 1 : Math.round(k * (all.length - 1) / (n - 1));
      if (at.indexOf(p) < 0) at.push(p);
    }
    setPicks(slot, all[0][1].round, all[0][1].b, at.map(function(p){
      return {u: all[p][0], t: all[p][1].t, label: label(all[p][1])}; }));
    status(slot, all.length > MAX && nv === 'all' ? 'the first ' + MAX + ' of ' + all.length + ' offers' : '');
    paint(); loadProg();
  }

  document.addEventListener('click', function(ev){
    var t = ev.target;
    var p = t.closest && t.closest('.opk');
    if (p) {
      ev.preventDefault(); ev.stopPropagation();
      if (p.classList.contains('opkdim')) return;
      window.__offerPick(p.getAttribute('data-ou'), p.getAttribute('data-slot'));
      return;
    }
    var b = t.closest && t.closest('.odbar');
    if (b && t.closest('.odswap') && sel.length === 2) { sel.reverse(); paint(); loadDiff(); return; }
    if (b && t.closest('.odclr')) {
      var o = out(b.getAttribute('data-slot'));
      if (mode === 'diff') sel = []; else setPicks('', null, '', []);
      paint(); if (o) o.innerHTML = ''; return;
    }
    if (b && t.closest('.odeven')) { pickEvenly(b); return; }
    var ma = t.closest && t.closest('.odmall');
    if (ma) { ma.parentNode.querySelectorAll('tr.odmx').forEach(function(r){ r.hidden = false; }); ma.remove(); return; }
    var x = t.closest && t.closest('.odx');
    if (x) { window.__offerPick(x.closest('.odchip').getAttribute('data-u'), prog.slot); return; }
    var tg = t.closest && t.closest('.odtg button');
    if (tg) {
      view[tg.parentNode.getAttribute('data-k')] = tg.getAttribute('data-v');
      tg.parentNode.querySelectorAll('button').forEach(function(z){ z.setAttribute('aria-pressed', String(z === tg)); });
      var o2 = tg.closest('.odres'); if (o2 && lastData) draw(o2);
      return;
    }
    var tr = t.closest && t.closest('.odpt tr[data-u]');
    if (tr) {
      var uu = tr.getAttribute('data-u');
      hidden[uu] = !hidden[uu]; tr.classList.toggle('off', !!hidden[uu]);
      var o3 = tr.closest('.odres'); if (o3 && lastData) draw(o3);
    }
  });
  document.addEventListener('change', function(ev){
    var m = ev.target.closest && ev.target.closest('.odmode');
    if (!m) return;
    mode = m.value === 'prog' ? 'prog' : 'diff';
    try { localStorage.setItem(KEY, mode); } catch (e) {}
    document.querySelectorAll('.odres').forEach(function(o){ o.innerHTML = ''; });
    paint();
    if (mode === 'diff' && sel.length === 2) loadDiff();
    if (mode === 'prog' && prog.picks.length) loadProg();
  });
  document.addEventListener('keydown', function(ev){
    if (ev.key !== 'Enter' && ev.key !== ' ') return;
    var p = ev.target.closest && ev.target.closest('.opk');
    if (p) { ev.preventDefault(); p.click(); }
  });
})();

'''


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
        // the page-wide diff filter, if this page has one
        if (window.__selGlobal) window.__selGlobal(box);
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
  if (reg && con) reg.addEventListener('change', function(){
    // silent: a region change must not submit the form by itself
    if (con.tomselect) con.tomselect.clear(true); else con.value = '';
  });
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


(function(){
  // Per-slot diff filters (verdict, kind, what the scheduler said). The
  // diff ships with a 40-row cap; while any filter is set the cap is
  // lifted and rows are shown by the filter alone. Back at the defaults,
  // the cap comes back unless "show all" was already pressed.
  function apply(bar){
    var sec = bar.closest('.selsec');
    if (!sec) return;
    var v = bar.querySelector('.sf-v').value;
    var s = bar.querySelector('.sf-s').value;
    var rb = bar.querySelector('.sf-r');
    var r = rb ? rb.value : '';
    var ob = bar.querySelector('.sf-o');
    var o = ob ? ob.value : '';
    sec.querySelectorAll('.selblk[data-o]').forEach(function(blk){
      blk.hidden = !!o && blk.getAttribute('data-o') !== o;
    });
    var boxes = bar.querySelectorAll('.sf-t');
    var kinds = [].filter.call(boxes, function(b){ return b.checked; })
                  .map(function(b){ return b.value; });
    var active = !!(v || s || r || o || kinds.length !== boxes.length);
    var shown = 0, total = 0;
    sec.querySelectorAll('table.otab').forEach(function(tab){
      var note = tab.nextElementSibling;
      var capped = !!(note && note.classList.contains('more'));
      if (capped) note.hidden = active;
      var parentHidden = false;
      tab.querySelectorAll('tr').forEach(function(tr){
        if (tr.hasAttribute('data-v')) {
          total++;
          var ok = (!v || tr.getAttribute('data-v') === v)
            && (!o || tr.getAttribute('data-o') === o)
            && kinds.indexOf(tr.getAttribute('data-k')) >= 0
            && (!s || ('|' + tr.getAttribute('data-s') + '|')
                        .indexOf('|' + s + '|') >= 0)
            && (!r || tr.getAttribute('data-r') === r);
          if (active) tr.hidden = !ok;
          else tr.hidden = capped && tr.classList.contains('selx');
          parentHidden = tr.hidden;
          if (!tr.hidden && (!active || ok)) shown++;
        } else if (tr.classList.contains('sxrow') && parentHidden) {
          tr.hidden = true;       // an open panel goes with its row
        }
      });
    });
    var n = bar.querySelector('.sf-n');
    if (n) n.textContent = active ? (shown + ' of ' + total + ' rows match') : '';
    recount(bar, sec, v, o);
  }
  // Verdict and our builder are the population: type and scheduler counts
  // are taken over those rows alone, not narrowed by each other. The
  // builder counts follow the verdict.
  function recount(bar, sec, v, o){
    var kc = {}, sc = {}, oc = {}, rc = {};
    sec.querySelectorAll('tr[data-v]').forEach(function(tr){
      if (v && tr.getAttribute('data-v') !== v) return;
      var ob = tr.getAttribute('data-o');
      oc[ob] = (oc[ob] || 0) + 1;
      if (o && ob !== o) return;
      var k = tr.getAttribute('data-k');
      kc[k] = (kc[k] || 0) + 1;
      tr.getAttribute('data-s').split('|').forEach(function(x){
        sc[x] = (sc[x] || 0) + 1;
      });
      var rk = tr.getAttribute('data-r');
      rc[rk] = (rc[rk] || 0) + 1;
    });
    bar.querySelectorAll('.sf-r option[data-l]').forEach(function(op){
      var c = rc[op.value] || 0;
      op.textContent = op.getAttribute('data-l') + ' (' + c.toLocaleString() + ')';
      op.disabled = c === 0 && !op.selected;
    });
    bar.querySelectorAll('.sf-t').forEach(function(b){
      var c = b.parentNode.querySelector('.sf-c');
      if (c) c.textContent = (kc[b.value] || 0).toLocaleString();
    });
    bar.querySelectorAll('.sf-s option[data-l]').forEach(function(op){
      var c = sc[op.value] || 0;
      op.textContent = op.getAttribute('data-l') + ' (' + c.toLocaleString() + ')';
      op.disabled = c === 0 && !op.selected;
    });
    bar.querySelectorAll('.sf-o option[data-l]').forEach(function(op){
      var c = oc[op.value] || 0;
      op.textContent = op.getAttribute('data-l') + ' (' + c.toLocaleString() + ')';
      op.disabled = c === 0 && !op.selected;
    });
  }
  window.__selApply = apply;
  document.addEventListener('change', function(ev){
    var bar = ev.target.closest && ev.target.closest('.selfilt');
    if (!bar || bar.classList.contains('gfilt')) return;
    // changed by hand: this slot stops following the page filter
    bar.setAttribute('data-local', '1');
    var u = bar.querySelector('.sf-use');
    if (u) u.hidden = false;
    apply(bar);
  });
})();

(function(){
  // The page-wide filter. Pushed into each slot's bar when the slot
  // loads, and into every loaded bar not changed by hand when it changes.
  var g = document.querySelector('.gfilt');
  if (!g) return;
  var KEY = 'accuracy.pagefilter.v1';
  var gv = g.querySelector('.gf-v'), go = g.querySelector('.gf-o'),
      gs = g.querySelector('.gf-s'), gr = g.querySelector('.gf-r');
  function state(){
    var so = gs.selectedOptions[0], ro = gr.selectedOptions[0];
    return {v: gv.value, o: go.value, s: gs.value, r: gr.value,
            sl: (so && so.getAttribute('data-l')) || gs.value,
            rl: (ro && ro.getAttribute('data-l')) || gr.value,
            k: [].filter.call(g.querySelectorAll('.gf-t'),
                              function(b){ return b.checked; })
                 .map(function(b){ return b.value; })};
  }
  // a value the select does not list yet gets an option, so a filter for
  // a builder or verdict absent from this slot shows 0 rows, not all
  function ensure(sel, val, label){
    if (!sel || !val) return;
    var has = [].some.call(sel.options, function(o){ return o.value === val; });
    if (!has){
      var op = document.createElement('option');
      op.value = val;
      op.setAttribute('data-l', label || val);
      // a slot's own select shows counts (recount fills them in)
      op.textContent = (label || val) + (sel.closest('.gfilt') ? '' : ' (0)');
      sel.appendChild(op);
    }
  }
  function push(bar){
    if (bar.getAttribute('data-local') === '1') return;
    var st = state();
    var v = bar.querySelector('.sf-v'), o = bar.querySelector('.sf-o'),
        s = bar.querySelector('.sf-s'), rr = bar.querySelector('.sf-r');
    ensure(rr, st.r, st.rl); if (rr) rr.value = st.r || '';
    ensure(v, st.v); if (v) v.value = st.v;
    ensure(o, st.o); if (o) o.value = st.o;
    ensure(s, st.s, st.sl); if (s) s.value = st.s;
    bar.querySelectorAll('.sf-t').forEach(function(b){
      b.checked = st.k.indexOf(b.value) >= 0;
    });
    if (window.__selApply) window.__selApply(bar);
  }
  // the scheduler verdicts are per slot: collect them as slots load
  function harvest(bar){
    bar.querySelectorAll('.sf-s option[data-l]').forEach(function(op){
      ensure(gs, op.value, op.getAttribute('data-l'));
    });
    bar.querySelectorAll('.sf-o option[data-l]').forEach(function(op){
      ensure(go, op.value, op.getAttribute('data-l'));
    });
    bar.querySelectorAll('.sf-r option[data-l]').forEach(function(op){
      ensure(gr, op.value, op.getAttribute('data-l'));
    });
  }
  function save(){
    try { localStorage.setItem(KEY, JSON.stringify(state())); } catch(e) {}
  }
  function all(){ document.querySelectorAll('.selfilt:not(.gfilt)').forEach(push); }
  try {
    var st = JSON.parse(localStorage.getItem(KEY) || 'null');
    if (st){
      ensure(gv, st.v); gv.value = st.v || '';
      ensure(go, st.o); go.value = st.o || '';
      ensure(gs, st.s, st.sl || st.s); gs.value = st.s || '';
      ensure(gr, st.r, st.rl || st.r); gr.value = st.r || '';
      g.querySelectorAll('.gf-t').forEach(function(b){
        b.checked = !st.k || st.k.indexOf(b.value) >= 0;
      });
    }
  } catch(e) {}
  g.addEventListener('change', function(){ save(); all(); });
  window.__selGlobal = function(root){
    (root || document).querySelectorAll('.selfilt:not(.gfilt)')
      .forEach(function(bar){ harvest(bar); push(bar); });
  };
  document.addEventListener('click', function(ev){
    var u = ev.target.closest && ev.target.closest('.sf-use');
    if (!u) return;
    var bar = u.closest('.selfilt');
    bar.removeAttribute('data-local');
    u.hidden = true;
    push(bar);
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
            + CONNECTOR_PICKER if connectors is not None else ""),
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
                 f'<span class="m">{copy_val(r["connector"])}</span>'
                 f'{stakingfac_link(r["connector"])} only')
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
           '<th class="n" title="ms from the builder admitting the order to that builder&#39;s own winner row (bifrost_miniblocks kind = winner) for this slot and round, same clock">win &minus;recv</th>'
           '<th class="n" title="the relay&#39;s hold on the order (scheduler/received nums wait_us), ms: the builder may not extend it before admission + wait. 0 = no hold (votes, non-nonce bundles, searcher flow); blank = not logged">wait</th>'
                   "<th>validator said</th></tr>"
                   + "".join(rowsh) + "</table>")
        if len(miss) > SIG_CAP:
            out.append(f'<div class="more">{len(miss) - SIG_CAP:,} more '
                       "misses not shown</div>")
    return "".join(out)
