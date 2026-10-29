"""Order lookup -- everything every store we read says about one transaction
or bundle, on one page.

Give it a full signature, a SigPrefix, a builder bundle id (64 hex) or an
API bundle_submission_id (uuid). It finds when the order was around, widens
the identity once (a transaction pulls in the bundles that carried it, a
bundle pulls in its members and the ids other systems gave it), then reads:

  chain        getTransaction per signature: slot, error, fee, tips
  builders     block_builder.bifrost_events -- every event any of our
               builders logged for any of the ids
  relay        Gattaca's relay: ingestion from connectors, every builder's
               offers that carried it (and whether that offer won), what the
               validator did with it
  our API      db_sync.inbound_transactions_enriched / _mev_bundles_enriched
               (Arnav, 2026-10-27: look up `signature`, then follow
               bundle_submission_id and gravity_blocks_bundle_id)
  our relayer  relayer.validator_packet_events / validator_bundle_events and
               the searcher egress / backrun logs

Every relayer table is sorted on created_at alone, so nothing here scans
without a window: the window comes from the chain when the order landed,
from our builders' or the relay's own record when it did not, or from the
form. API keys and searcher tokens are credentials -- only their first 8
characters are ever shown.
"""

import base64
import concurrent.futures as cf
import datetime as dt
import html
import json
import re
import urllib.request

LOOKUP_PATH = "/tech/order-lookup"

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
SIG_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{80,90}$")
PFX_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{18,24}$")
HEX_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# our API's bundle_submission_id is a ULID (26 chars, Crockford base32)
ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?Z?$")

# How far back an id with no time is searched for in the stores that are
# sorted by it (bifrost_events on entity, connector_ingested_orders on
# signature, the API tables on created_at + id).
FIND_DAYS = 14
MAX_SIGS = 40          # members + carriers, after one widening
MAX_CHAIN = 12         # getTransaction calls per lookup
MAX_EVENTS = 600
MAX_FOCAL = 8          # transactions the summary is about
F = "%Y-%m-%d %H:%M:%S"
# a receipt this close after a round started is "during" that round; later
# than this the builder was between windows, and the next round is shown
IN_ROUND_MS = 250


# ------------------------------------------------------------------ ids

def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + B58.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + raw


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58[r] + out
    pad = len(b) - len(b.lstrip(b"\x00"))
    return "1" * pad + out


def sigprefix(sig):
    return b58encode(b58decode(sig)[:16])


def classify(q):
    q = (q or "").strip()
    if SIG_RE.match(q):
        return "signature", q
    if HEX_RE.match(q.lower()):
        return "bundle", q.lower()
    if UUID_RE.match(q.lower()):
        return "uuid", q.lower()
    if ULID_RE.match(q.upper()) and len(q) == 26:
        return "uuid", q.upper()
    if PFX_RE.match(q):
        return "prefix", q
    return "", q


def _key8(k):
    k = k or ""
    return (k[:8] + "…") if len(k) > 8 else k


def _q(s):
    return str(s).replace("\\", "").replace("'", "")


def _lst(xs):
    return ", ".join("'%s'" % _q(x) for x in sorted(xs))


def _ms_str(v):
    """recv_ts / sent_at / ts columns: integer ms (or us/ns) since epoch."""
    try:
        v = int(v)
    except (TypeError, ValueError):
        return ""
    if v <= 0:
        return ""
    while v > 10 ** 14:              # us or ns -> ms
        v //= 1000
    return dt.datetime.fromtimestamp(v / 1000, dt.UTC).strftime(F + ".%f")[:-3]


def _rows(res):
    return res[1] if isinstance(res, tuple) else res


# ------------------------------------------------------------------ chain

def _rpc(rpc_url, method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    req = urllib.request.Request(rpc_url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "curl/8"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.loads(r.read())
    if d.get("error"):
        raise RuntimeError(str(d["error"].get("message", ""))[:160])
    return d.get("result")


def chain_tx(rpc_url, sig, tip_sets):
    """-> {slot, time, err, fee, jito, builder, signer} or None (not found)."""
    t = _rpc(rpc_url, "getTransaction",
             [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1,
                    "commitment": "confirmed"}])
    if not t:
        return None
    meta = t.get("meta") or {}
    keys = (t.get("transaction") or {}).get("message", {}).get("accountKeys") or []
    keys = keys + (meta.get("loadedAddresses") or {}).get("writable", []) \
        + (meta.get("loadedAddresses") or {}).get("readonly", [])
    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
    jito_set, all_tips = tip_sets
    jito = builder = 0
    for i, k in enumerate(keys):
        if k in all_tips and i < len(pre) and i < len(post):
            got = max(0, int(post[i]) - int(pre[i]))
            if k in jito_set:
                jito += got
            else:
                builder += got
    bt = t.get("blockTime")
    return {"slot": t.get("slot"),
            "time": (dt.datetime.fromtimestamp(bt, dt.UTC).strftime(F)
                     if bt else ""),
            "err": json.dumps(meta.get("err")) if meta.get("err") else "",
            "fee": int(meta.get("fee") or 0), "jito": jito,
            "builder": builder, "signer": keys[0] if keys else ""}


# ------------------------------------------------------------------ window

def _parse_time(s):
    return dt.datetime.fromisoformat(s.strip().replace("T", " ").rstrip("Z")[:26])


def find_anchor(src, kind, ident):
    """-> (datetime, how) for an id with no time given, or (None, why)."""
    ch, relay, quant, rpc = src["ch"], src["relay"], src["quant"], src["rpc"]
    if kind == "signature":
        try:
            ct = chain_tx(rpc, ident, src["tips"])
            if ct and ct["time"]:
                return _parse_time(ct["time"]), "block time on chain"
        except Exception:
            pass
        try:
            r = _rows(relay(
                "SELECT toString(min(relay_sent_at)) FROM "
                "relay.connector_ingested_orders WHERE relay_sent_at > now() - "
                f"INTERVAL {FIND_DAYS} DAY AND signature = "
                f"base58Decode('{_q(ident)}')"))
            if r and r[0][0] and not r[0][0].startswith("1970"):
                return _parse_time(r[0][0]), "the relay's ingestion record"
        except Exception:
            pass
        try:
            # a tx that never landed and never reached the relay (a bundle
            # member sent through our router) is still in our API's log
            r = _rows(quant(
                "SELECT toString(min(created_at)) FROM "
                "db_sync.inbound_transactions_enriched WHERE created_at > "
                f"now() - INTERVAL {FIND_DAYS} DAY AND signature = '{_q(ident)}'"))
            if r and r[0][0] and not r[0][0].startswith("1970"):
                return _parse_time(r[0][0]), "our API's record"
        except Exception:
            pass
        ident = sigprefix(ident)
    if kind in ("signature", "prefix", "bundle"):
        try:
            r = _rows(ch(
                "SELECT toString(min(ts)) FROM bifrost_events WHERE ts > now() "
                f"- INTERVAL {FIND_DAYS} DAY AND entity = '{_q(ident)}'"))
            if r and r[0][0] and not r[0][0].startswith("1970"):
                return _parse_time(r[0][0]), "our builders' first event"
        except Exception:
            pass
    if kind in ("uuid", "bundle"):
        col = "bundle_submission_id" if kind == "uuid" else "gravity_blocks_bundle_id"
        try:
            r = _rows(quant(
                "SELECT toString(min(created_at)) FROM "
                "db_sync.inbound_mev_bundles_enriched WHERE created_at > now() - "
                f"INTERVAL 30 DAY AND {col} = '{_q(ident)}'"))
            if r and r[0][0] and not r[0][0].startswith("1970"):
                return _parse_time(r[0][0]), "our API's record"
        except Exception:
            pass
    return None, f"nothing found in the last {FIND_DAYS} days"


# ------------------------------------------------------------------ widen

def widen(src, ids, lo, hi):
    """One round of widening: the bundles that carried each transaction and
    the members of each bundle, plus the ids our API gave them. In place."""
    ch, relay, quant = src["ch"], src["relay"], src["quant"]
    w = f"ts >= '{lo:{F}}' AND ts <= '{hi:{F}}'"
    notes = []
    why = ids["why"]

    def tag(x, reason):
        why.setdefault(x, reason)
    if ids["sigs"]:
        try:
            for ent, sigs in _rows(ch(
                    "SELECT entity, any(signatures) FROM bifrost_events "
                    f"WHERE {w} AND stage = 'scheduler' AND event = 'received' "
                    f"AND entity_kind = 'bundle' AND hasAny(signatures, "
                    f"[{_lst(ids['sigs'])}]) GROUP BY entity LIMIT 50")):
                ids["bundles"].add(ent)
                tag(ent, "a bundle our builders received carrying it")
                for m in _arr(sigs):
                    ids["sigs"].add(m)
                    tag(m, f"member of bundle {ent[:10]}\u2026")
        except Exception as exc:
            notes.append(f"builder bundle search: {str(exc)[:120]}")
    if ids["bundles"]:
        try:
            for ent, sigs in _rows(ch(
                    "SELECT entity, any(signatures) FROM bifrost_events "
                    f"WHERE {w} AND stage = 'scheduler' AND event = 'received' "
                    f"AND entity IN ({_lst(ids['bundles'])}) GROUP BY entity")):
                for m in _arr(sigs):
                    ids["sigs"].add(m)
                    tag(m, f"member of bundle {ent[:10]}\u2026")
        except Exception as exc:
            notes.append(f"builder bundle members: {str(exc)[:120]}")
    seen_bundles = set(ids["bundles"])
    # the relay's offers name both, tx by tx
    rw = f"ingested_at >= '{lo:{F}}' AND ingested_at <= '{hi:{F}}'"
    conds = []
    if ids["sigs"]:
        conds.append("signature IN (" + ", ".join(
            f"base58Decode('{_q(s)}')" for s in sorted(ids["sigs"])) + ")")
    if ids["bundles"]:
        conds.append(f"bundle_id IN ({_lst(ids['bundles'])})")
    if ids["prefixes"] and not ids["sigs"]:
        conds.append("base58Encode(substring(signature, 1, 16)) IN "
                     f"({_lst(ids['prefixes'])})")
    if conds:
        try:
            for sig, bid in _rows(relay(
                    "SELECT DISTINCT base58Encode(signature), ifNull(bundle_id, '') "
                    f"FROM relay.mini_block_transactions_v2 WHERE {rw} AND "
                    f"({' OR '.join(conds)}) LIMIT 400")):
                ids["sigs"].add(sig)
                tag(sig, f"in relay offers, member of bundle {bid[:10]}\u2026"
                    if bid else "in relay offers")
                if bid:
                    ids["bundles"].add(bid)
                    tag(bid, "a bundle in relay offers carrying it")
        except Exception as exc:
            notes.append(f"relay offers: {str(exc)[:120]}")
    # bundles only the relay named: their members from our builders' log
    new = ids["bundles"] - seen_bundles
    if new:
        try:
            for ent, sigs in _rows(ch(
                    "SELECT entity, any(signatures) FROM bifrost_events "
                    f"WHERE {w} AND stage = 'scheduler' AND event = 'received' "
                    f"AND entity IN ({_lst(new)}) GROUP BY entity")):
                for m in _arr(sigs):
                    ids["sigs"].add(m)
                    tag(m, f"member of bundle {ent[:10]}\u2026")
        except Exception as exc:
            notes.append(f"builder bundle members: {str(exc)[:120]}")
    # our API's own ids
    aw = f"created_at >= '{lo:{F}}' AND created_at <= '{hi:{F}}'"
    try:
        cond = []
        if ids["sigs"]:
            cond.append(f"hasAny(signatures, [{_lst(ids['sigs'])}])")
        if ids["uuids"]:
            cond.append(f"bundle_submission_id IN ({_lst(ids['uuids'])})")
        if ids["bundles"]:
            cond.append(f"gravity_blocks_bundle_id IN ({_lst(ids['bundles'])})")
        if cond:
            for bsid, sigs, gb in _rows(quant(
                    "SELECT bundle_submission_id, any(signatures), "
                    "any(ifNull(toString(gravity_blocks_bundle_id), '')) FROM "
                    f"db_sync.inbound_mev_bundles_enriched WHERE {aw} AND "
                    f"({' OR '.join(cond)}) GROUP BY bundle_submission_id "
                    "LIMIT 50")):
                ids["uuids"].add(bsid)
                tag(bsid, "an API bundle carrying one of these transactions")
                for m in _arr(sigs):
                    ids["sigs"].add(m)
                    tag(m, f"member of API bundle {bsid[:8]}\u2026")
                if gb:
                    ids["gravity"].add(gb)
                    tag(gb, f"Gravity bundle id of API bundle {bsid[:8]}\u2026")
    except Exception as exc:
        notes.append(f"API bundles: {str(exc)[:120]}")
    if len(ids["sigs"]) > MAX_SIGS:
        notes.append(f"{len(ids['sigs'])} signatures found; only the first "
                     f"{MAX_SIGS} are followed")
        # the input's own transactions are never the ones cut
        ids["sigs"] = set(sorted(ids["sigs"], key=lambda s: (
            s not in focal_sigs(ids), s))[:MAX_SIGS])
    ids["prefixes"].update(sigprefix(s) for s in ids["sigs"])
    for s_ in ids["sigs"]:
        if why.get(sigprefix(s_)) == "your input":
            why[s_] = "your input"
    return notes


def focal_sigs(ids):
    """The transactions the summary is about: the input itself, the
    transactions of a SigPrefix, or the members of an input bundle -- not
    everything the widening pulled in."""
    ident, why = ids["input"], ids["why"]
    tags = {"your input", f"member of bundle {ident[:10]}…",
            f"in relay offers, member of bundle {ident[:10]}…",
            f"member of API bundle {ident[:8]}…"}
    return sorted(s for s in ids["sigs"]
                  if why.get(s) in tags or sigprefix(s) == ident)[:MAX_FOCAL]


def _arr(v):
    """A ClickHouse Array(String) as either a list or its text form."""
    if isinstance(v, list):
        return [x for x in v if x]
    v = (v or "").strip()
    if not v.startswith("["):
        return [v] if v else []
    return [x.strip().strip("'\"") for x in v[1:-1].split(",") if x.strip().strip("'\"")]


# ------------------------------------------------------------------ summary

def summary_jobs(src, focal, lo, hi):
    """The reads behind the summary, searcher and arb sections -- all about
    the focal transactions only, not every id the widening pulled in."""
    if not focal:
        return {}
    ch, relay, quant = src["ch"], src["relay"], src["quant"]
    fl = _lst(focal)
    fp = _lst({sigprefix(s) for s in focal})
    dec = ", ".join(f"base58Decode('{_q(s)}')" for s in focal)
    w_ts = f"ts >= '{lo:{F}}' AND ts <= '{hi:{F}}'"
    w_ca = f"created_at >= '{lo:{F}}' AND created_at <= '{hi:{F}}'"
    # every entity our builders received the transactions as: the tx itself
    # (its SigPrefix) and each bundle carrying it
    recv = (f"{w_ts} AND stage = 'scheduler' AND event = 'received' AND "
            f"(entity IN ({fp}) OR (entity_kind = 'bundle' AND "
            f"hasAny(signatures, [{fl}])))")
    ents = f"(SELECT groupUniqArray(entity) FROM bifrost_events WHERE {recv})"
    rs_lo = (lo - dt.timedelta(minutes=2)).strftime(F)
    rs_hi = (hi + dt.timedelta(minutes=2)).strftime(F)
    rounds = ("SELECT ts, instance_id, nums, attrs FROM bifrost_events WHERE "
              f"ts >= '{rs_lo}' AND ts <= '{rs_hi}' AND stage = 'scheduler' "
              "AND event = 'round_started'")
    jobs = {}
    # each receipt with the round that was running then (p) and the next
    # one to start (n): the leader slot and identity it was received for
    jobs["s_recv"] = lambda: _rows(ch(
        "SELECT toString(r.ts), r.instance_id, r.entity_kind, r.entity, "
        "r.attrs['source'], r.attrs['route'], r.nums['wait_us'], "
        "toString(p.ts), p.nums['slot'], p.nums['index'], p.attrs['identity'], "
        "toString(n.ts), n.nums['slot'], n.nums['index'], n.attrs['identity'] "
        "FROM (SELECT ts, instance_id, entity_kind, entity, attrs, nums FROM "
        f"bifrost_events WHERE {recv}) r ASOF LEFT JOIN ({rounds}) p ON "
        "r.instance_id = p.instance_id AND r.ts >= p.ts ASOF LEFT JOIN "
        f"({rounds}) n ON r.instance_id = n.instance_id AND r.ts <= n.ts "
        "ORDER BY r.ts LIMIT 1 BY r.instance_id, r.entity LIMIT 600"))
    jobs["s_events"] = lambda: _rows(ch(
        "SELECT instance_id, entity, toString(ts), stage, event, reason, "
        "attrs['result'], attrs['error'] FROM bifrost_events WHERE "
        f"{w_ts} AND has({ents}, entity) AND event != 'received' "
        "ORDER BY ts LIMIT 3000"))
    jobs["s_ours"] = lambda: _rows(ch(
        "SELECT instance_id, kind, slot, index_in_slot, max(won_by_us), "
        "toString(min(ts)), arrayDistinct(arrayFlatten(groupArray("
        f"arrayFilter(b -> has({ents}, b), order_bundle_ids)))), "
        f"max(hasAny(order_tx_prefixes, [{fp}])) FROM bifrost_miniblocks "
        f"WHERE {w_ts} AND (hasAny(order_bundle_ids, {ents}) OR "
        f"hasAny(order_tx_prefixes, [{fp}])) GROUP BY 1, 2, 3, 4 "
        "ORDER BY 3, 4 LIMIT 500"))

    def _relay():
        offers = _rows(relay(
            "SELECT builder_id, slot, index_in_slot, connector_identity, "
            "ifNull(bundle_id, ''), groupUniqArray(toString(mini_block_uuid)) "
            "FROM relay.mini_block_transactions_v2 WHERE ingested_at >= "
            f"'{lo:{F}}' AND ingested_at <= '{hi:{F}}' AND signature IN "
            f"({dec}) GROUP BY 1, 2, 3, 4, 5 ORDER BY 2, 3, 1 LIMIT 600"))
        slots = {int(r[1]) for r in offers}
        winners = []
        if slots:
            # who won every round of those slots, carrying it or not
            winners = _rows(relay(
                "SELECT slot, index_in_slot, builder_id, "
                "toString(mini_block_uuid), connector_identity, protocol_fees, "
                "jito_tips, builder_payment FROM relay.mini_block_events_v2 "
                f"WHERE timestamp >= '{lo:{F}}' AND timestamp <= '{hi:{F}}' "
                "AND event = 'submitted' AND slot IN ("
                + ", ".join(str(x) for x in sorted(slots)) + ") AND "
                "mini_block_uuid IN (SELECT mini_block_uuid FROM "
                f"relay.mini_block_events_v2 WHERE timestamp >= '{lo:{F}}' "
                f"AND timestamp <= '{hi:{F}}' AND event = 'round_chosen') "
                "LIMIT 1 BY mini_block_uuid"))
        return offers, winners
    jobs["s_relay"] = _relay
    # every submission of it to our API, with the searcher behind the key
    # and, for a bundle, its size, tip, dropping indices and targets
    jobs["s_api"] = lambda: _rows(quant(
        "WITH (SELECT max(snapshot_ts) FROM db_sync.api_keys_snapshot) AS kt, "
        "(SELECT max(snapshot_ts) FROM db_sync.users_snapshot) AS ut "
        "SELECT toString(t.received_at), t.signature, t.transaction_type, "
        "ifNull(u.name, ''), ifNull(k.name, ''), ifNull(t.api_key, ''), "
        "t.bundle_submission_id, ifNull(t.tip, 0), t.status, "
        "ifNull(toString(t.slot_landed), ''), ifNull(a.n, 0), ifNull(a.tips, 0), "
        "ifNull(b.num_transactions, 0), indexOf(b.signatures, t.signature), "
        "b.dropping_indices, b.target_accounts, "
        "ifNull(toString(b.target_slot), ''), "
        "ifNull(toString(b.total_wire_tip_lamports), ''), "
        "ifNull(toString(b.bundle_landing_status), ''), "
        "ifNull(toString(t.gravity_blocks_landing_status), ''), "
        "ifNull(toString(t.gravity_blocks_bundle_id), '') "
        "FROM (SELECT * FROM db_sync.inbound_transactions_enriched WHERE "
        f"{w_ca} AND signature IN ({fl}) ORDER BY updated_at DESC LIMIT 1 BY "
        "id) t LEFT JOIN (SELECT key_value, name, user_id FROM "
        "db_sync.api_keys_snapshot WHERE snapshot_ts = kt) k ON k.key_value "
        "= t.api_key LEFT JOIN (SELECT id, name FROM db_sync.users_snapshot "
        "WHERE snapshot_ts = ut) u ON u.id = k.user_id LEFT JOIN (SELECT "
        "bundle_submission_id, uniqExact(signature) n, sum(t2) tips FROM "
        "(SELECT bundle_submission_id, signature, any(ifNull(tip, 0)) t2 FROM "
        f"db_sync.inbound_transactions_enriched WHERE {w_ca} AND "
        "bundle_submission_id IN (SELECT bundle_submission_id FROM "
        f"db_sync.inbound_transactions_enriched WHERE {w_ca} AND signature "
        f"IN ({fl})) GROUP BY 1, 2) GROUP BY 1) a ON a.bundle_submission_id "
        "= t.bundle_submission_id LEFT JOIN (SELECT * FROM "
        f"db_sync.inbound_mev_bundles_enriched WHERE {w_ca} AND "
        f"hasAny(signatures, [{fl}]) ORDER BY updated_at DESC LIMIT 1 BY "
        "bundle_submission_id) b ON b.bundle_submission_id = "
        "t.bundle_submission_id ORDER BY t.received_at LIMIT 300"))

    def _arbs():
        arbs = _rows(quant(
            "SELECT arb_sig, slot, position_in_block, signer, economic_actor, "
            "profit_lamports, profit_mint, ifNull(profit_decimals, 9), "
            "dex_route, leg_count, source_sig, source_slot, match_arm, "
            "is_strong, toString(block_time) FROM "
            "backrun_intel.all_circular_arbs WHERE block_time >= "
            f"'{lo:{F}}' AND block_time <= '{hi:{F}}' AND (source_sig IN "
            f"({fl}) OR arb_sig IN ({fl})) ORDER BY parser_version DESC "
            "LIMIT 1 BY arb_sig, source_sig LIMIT 300"))
        # ours: every backrun our searchers landed on it, circular or not
        mine = _rows(quant(
            "SELECT backrun_signature, source_signature, "
            "ifNull(toString(delivery_searcher_token), ''), backrun_position "
            "FROM backrun_intel.backrun_source FINAL WHERE source_signature "
            f"IN ({fl}) OR backrun_signature IN ({fl}) LIMIT 100"))
        sigs = {r[0] for r in arbs} | {r[0] for r in mine}
        found = {}
        if sigs:
            found = {r[0]: r[1:] for r in _rows(quant(
                "SELECT signature, searcher_name, slot, "
                "revenue_share_amount_lamports, revenue_share_token FROM "
                "backrun_intel.backrun_discovery FINAL WHERE signature IN "
                f"({_lst(sigs)})"))}
        return arbs, mine, found
    jobs["s_arbs"] = _arbs
    return jobs


# ------------------------------------------------------------------ gather

def gather(src, kind, ident, lo, hi):
    """Every section, read in parallel. -> {name: (cols, rows) or error str}"""
    ids = {"sigs": set(), "prefixes": set(), "bundles": set(),
           "uuids": set(), "gravity": set(), "why": {ident: "your input"},
           "input": ident}
    {"signature": ids["sigs"], "prefix": ids["prefixes"],
     "bundle": ids["bundles"], "uuid": ids["uuids"]}[kind].add(ident)
    notes = widen(src, ids, lo, hi)
    ids["bundles"] |= ids["gravity"]
    ch, relay, quant, relayer = src["ch"], src["relay"], src["quant"], src["relayer"]
    w_ts = f"ts >= '{lo:{F}}' AND ts <= '{hi:{F}}'"
    w_ca = f"created_at >= '{lo:{F}}' AND created_at <= '{hi:{F}}'"
    w_ing = f"ingested_at >= '{lo:{F}}' AND ingested_at <= '{hi:{F}}'"
    sigs, pfx, bun, uu = ids["sigs"], ids["prefixes"], ids["bundles"], ids["uuids"]
    dec = ", ".join(f"base58Decode('{_q(s)}')" for s in sorted(sigs))
    focal = focal_sigs(ids)
    ids["focal"] = focal
    jobs = summary_jobs(src, focal, lo, hi)

    if sigs:
        def _chain():
            out = []
            for s in sorted(sigs, key=lambda x: (x not in focal, x))[:MAX_CHAIN]:
                try:
                    c = chain_tx(src["rpc"], s, src["tips"])
                except Exception as exc:
                    c = {"err": f"lookup failed: {str(exc)[:80]}"}
                out.append((s, sigprefix(s), c))
            return out
        jobs["chain"] = _chain
    ents = pfx | bun
    if ents:
        jobs["builders"] = lambda: _rows(ch(
            "SELECT toString(ts), instance_id, stage, event, reason, "
            "entity_kind, entity, toJSONString(attrs), toJSONString(nums) "
            f"FROM bifrost_events WHERE {w_ts} AND entity IN ({_lst(ents)}) "
            f"ORDER BY ts LIMIT {MAX_EVENTS}"))
    if sigs:
        jobs["ingested"] = lambda: _rows(relay(
            "SELECT toString(relay_sent_at), instance_id, connector_identity, "
            "source_uri, order_type, ifNull(bundle_id, ''), bundle_tx_index, "
            "base58Encode(signature) FROM relay.connector_ingested_orders "
            f"WHERE relay_sent_at >= '{lo:{F}}' AND relay_sent_at <= "
            f"'{hi:{F}}' AND signature IN ({dec}) ORDER BY relay_sent_at "
            "LIMIT 300"))
        jobs["executed"] = lambda: _rows(relay(
            "SELECT toString(relay_received_at), instance_id, "
            "connector_identity, slot, toString(mini_block_uuid), order_type, "
            "ifNull(bundle_id, ''), execution_slot, not_included_reason, "
            "base58Encode(signature) FROM relay.connector_execution_results "
            f"WHERE relay_received_at >= '{lo:{F}}' AND relay_received_at <= "
            f"'{hi:{F}}' AND signature IN ({dec}) ORDER BY relay_received_at "
            "LIMIT 300"))
    if sigs or bun:
        cond = []
        if sigs:
            cond.append(f"signature IN ({dec})")
        if bun:
            cond.append(f"bundle_id IN ({_lst(bun)})")

        def _offers(cond=list(cond)):
            rows = _rows(relay(
                "SELECT builder_id, slot, index_in_slot, "
                "uniqExact(mini_block_uuid), toString(min(ingested_at)), "
                "toString(max(ingested_at)), any(ifNull(bundle_id, '')), "
                "min(order_index), groupUniqArray(toString(mini_block_uuid)) "
                f"FROM relay.mini_block_transactions_v2 WHERE {w_ing} AND "
                f"({' OR '.join(cond)}) GROUP BY builder_id, slot, "
                "index_in_slot ORDER BY slot, index_in_slot, builder_id "
                "LIMIT 200"))
            uuids = {u for r in rows for u in _arr(r[8])}
            won, val = set(), {}
            if uuids:
                # what each offer declared (protocol fees, Jito tips, builder
                # payment) and whether it was chosen -- "what did Titan value
                # the round at with this order in it"
                for u, ev, fees, jito, pay, rsn in _rows(relay(
                        "SELECT toString(mini_block_uuid), event, protocol_fees, "
                        "jito_tips, builder_payment, reason FROM "
                        "relay.mini_block_events_v2 WHERE timestamp >= "
                        f"'{lo:{F}}' AND timestamp <= '{hi:{F}}' AND event IN "
                        "('round_chosen', 'submitted') AND "
                        f"toString(mini_block_uuid) IN ({_lst(list(uuids)[:600])})")):
                    if ev == "round_chosen":
                        won.add(u)
                    else:
                        val[u] = (int(fees or 0), int(jito or 0), int(pay or 0),
                                  rsn or "")
            out = []
            for r in rows:
                us = _arr(r[8])
                best = max((val[u] for u in us if u in val),
                           key=lambda v: v[0] + v[1] + v[2], default=None)
                out.append(list(r[:8]) + [[u for u in us if u in won], best])
            return out
        jobs["offers"] = _offers
    if sigs or uu:
        cond = []
        if sigs:
            cond.append(f"signature IN ({_lst(sigs)})")
        if uu:
            cond.append(f"bundle_submission_id IN ({_lst(uu)})")
        jobs["api_txs"] = lambda cond=list(cond): _rows(quant(
            "SELECT signature, transaction_type, ifNull(toString(api_key), ''), "
            "ifNull(toString(bundle_submission_id), ''), toString(created_at), "
            "ifNull(toString(slot_received), ''), status, "
            "ifNull(toString(slot_landed), ''), ifNull(toString(leader), ''), "
            "ifNull(toString(source_ip), ''), "
            "ifNull(toString(gravity_blocks_landing_status), ''), "
            "ifNull(toString(gravity_blocks_slot_landed), ''), "
            "ifNull(toString(gravity_blocks_egress_tip), ''), "
            "ifNull(toString(gravity_blocks_egress_tip_signature), ''), "
            "ifNull(toString(gravity_blocks_bundle_id), ''), "
            "ifNull(toString(jito_landing_status), ''), "
            "ifNull(toString(harmonic_landing_status), ''), "
            "ifNull(toString(bifrost_landing_status), ''), "
            "ifNull(toString(rakurai_landing_status), '') "
            f"FROM db_sync.inbound_transactions_enriched WHERE {w_ca} AND "
            f"({' OR '.join(cond)}) ORDER BY updated_at DESC LIMIT 1 BY id "
            "LIMIT 100"))
    if sigs or uu or bun:
        cond = []
        if sigs:
            cond.append(f"hasAny(signatures, [{_lst(sigs)}])")
        if uu:
            cond.append(f"bundle_submission_id IN ({_lst(uu)})")
        if bun:
            cond.append(f"gravity_blocks_bundle_id IN ({_lst(bun)})")
        jobs["api_bundles"] = lambda cond=list(cond): _rows(quant(
            "SELECT bundle_submission_id, ifNull(toString(api_key), ''), "
            "toString(created_at), mev_variant, num_transactions, "
            "ifNull(toString(target_slot), ''), "
            "ifNull(toString(target_bundle_id), ''), "
            "ifNull(toString(total_wire_tip_lamports), ''), "
            "ifNull(toString(provider_direct_tip_lamports), ''), "
            "ifNull(toString(estimated_revenue_amount), ''), "
            "ifNull(toString(tip_wallet), ''), tip_wallet_type, "
            "bundle_landing_status, ifNull(toString(slot_landed), ''), "
            "ifNull(toString(gravity_blocks_landing_status), ''), "
            "ifNull(toString(gravity_blocks_slot_landed), ''), "
            "ifNull(toString(gravity_blocks_bundle_id), ''), "
            "ifNull(toString(source_ip), ''), arrayStringConcat(signatures, ' ') "
            f"FROM db_sync.inbound_mev_bundles_enriched WHERE {w_ca} AND "
            f"({' OR '.join(cond)}) ORDER BY updated_at DESC LIMIT 1 BY "
            "bundle_submission_id LIMIT 100"))
    if relayer is not None and sigs:
        jobs["packets"] = lambda: _rows(relayer(
            "SELECT signature, ifNull(toString(recv_ts), ''), "
            "ifNull(toString(recv_path), ''), relay_location, "
            "ifNull(toString(instance_id), ''), "
            "ifNull(toString(validator_pubkey), ''), "
            "ifNull(toString(approx_slot), ''), ifNull(from_addr, ''), "
            "ifNull(toString(from_ts), ''), ifNull(toString(source_ip), '') "
            f"FROM validator_packet_events WHERE {w_ca} AND signature IN "
            f"({_lst(sigs)}) ORDER BY recv_ts LIMIT 300"))
        jobs["searcher_tx"] = lambda: _rows(relayer(
            "SELECT ifNull(toString(searcher_token), ''), relay_location, "
            "ifNull(toString(sent_at), ''), ifNull(toString(relay_latency_us), ''), "
            "ifNull(toString(instance_id), ''), arrayStringConcat(signatures, ' ') "
            f"FROM searcher_egress_events WHERE {w_ca} AND hasAny(signatures, "
            f"[{_lst(sigs)}]) ORDER BY sent_at LIMIT 300"))
        jobs["backruns"] = lambda: _rows(relayer(
            "SELECT ifNull(toString(searcher_token), ''), relay_location, "
            "ifNull(toString(searcher_ip), ''), source_signature, "
            "backrun_signature, toString(ts), ifNull(toString(instance_id), '') "
            f"FROM searcher_backrun_events WHERE {w_ca} AND (source_signature "
            f"IN ({_lst(sigs)}) OR backrun_signature IN ({_lst(sigs)})) "
            "ORDER BY ts LIMIT 300"))
    if relayer is not None and (sigs or bun or uu):
        cond = []
        if sigs:
            cond.append(f"hasAny(signatures, [{_lst(sigs)}])")
        if bun or uu:
            cond.append(f"bundle_uuid IN ({_lst(bun | uu)})")
        jobs["val_bundles"] = lambda cond=list(cond): _rows(relayer(
            "SELECT bundle_uuid, ifNull(toString(recv_ts), ''), "
            "ifNull(toString(recv_path), ''), relay_location, "
            "ifNull(toString(instance_id), ''), "
            "ifNull(toString(validator_pubkey), ''), num_transactions, "
            "ifNull(toString(approx_slot), ''), arrayStringConcat(signatures, ' ') "
            f"FROM validator_bundle_events WHERE {w_ca} AND "
            f"({' OR '.join(cond)}) ORDER BY recv_ts LIMIT 300"))
    if relayer is not None and (bun or uu):
        jobs["searcher_bundles"] = lambda: _rows(relayer(
            "SELECT ifNull(toString(searcher_token), ''), relay_location, "
            "ifNull(toString(sent_at), ''), ifNull(toString(relay_latency_us), ''), "
            "ifNull(toString(instance_id), ''), arrayStringConcat(bundle_uuids, ' ') "
            f"FROM searcher_bundle_egress_events WHERE {w_ca} AND "
            f"hasAny(bundle_uuids, [{_lst(bun | uu)}]) ORDER BY sent_at "
            "LIMIT 300"))

    if sigs:
        jobs["bk_landed"] = lambda: _rows(quant(
            "SELECT signature, searcher_name, searcher_signer, slot, "
            "toString(block_time), revenue_share_wallet, "
            "revenue_share_amount_lamports, revenue_share_token, "
            "is_circular_arb, arrayStringConcat(programs_involved, ' '), "
            "bloom_fee_lamports FROM backrun_intel.backrun_discovery FINAL "
            f"WHERE signature IN ({_lst(sigs)}) LIMIT 50"))
        jobs["bk_source"] = lambda: _rows(quant(
            "SELECT backrun_signature, source_signature, source_provider, "
            "source_slug, attribution_tier, source_slot, source_position, "
            "backrun_position, source_api_key, delivery_searcher_token, "
            "delivery_relay_location, swap_size_lamports FROM "
            "backrun_intel.backrun_source FINAL WHERE backrun_signature IN "
            f"({_lst(sigs)}) OR source_signature IN ({_lst(sigs)}) "
            "ORDER BY parser_version DESC LIMIT 1 BY backrun_signature, "
            "source_signature LIMIT 100"))
        jobs["bk_external"] = lambda: _rows(quant(
            "SELECT arb_sig, source_sig, source_slot, toString(block_time), "
            "source_provider, source_slug, toString(attribution_tier), "
            "slot_leader, toString(match_arm), is_strong FROM "
            "backrun_intel.external_backrun_source WHERE block_time >= "
            f"'{lo:{F}}' AND block_time <= '{hi:{F}}' AND (arb_sig IN "
            f"({_lst(sigs)}) OR source_sig IN ({_lst(sigs)})) ORDER BY "
            "parser_version DESC LIMIT 1 BY arb_sig, source_sig LIMIT 100"))
        # the raw API request: the only place an unlanded bundle's bytes
        # (rev-share instructions, CU limits) survive. 6.3e9 rows, sorted
        # on created_at -- the window is what keeps this cheap.
        jobs["requests"] = lambda: _rows(quant(
            "SELECT toString(created_at), location, ifNull(toString(api_key), ''), "
            "ifNull(toString(user_ip), ''), ifNull(toString(method), ''), "
            "ifNull(toString(path), ''), status_code, response_error, "
            "ifNull(toString(latency), ''), ifNull(signature, ''), "
            "substring(ifNull(request, ''), 1, 20000), "
            "substring(ifNull(json_response, ''), 1, 2000) FROM "
            f"db_sync.request_log_record_backup WHERE {w_ca} AND signature "
            f"IN ({_lst(sigs)}) ORDER BY created_at LIMIT 50"))
    # searcher tokens -> names, for every relayer/backrun table below
    def _names():
        return {t: n for n, t in _rows(quant(
            "SELECT searcher_name, searcher_token FROM "
            "backrun_intel.searcher_tokens FINAL"))}
    jobs["searchers"] = _names

    out = {}
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(fn): name for name, fn in jobs.items()}
        for f in cf.as_completed(futs):
            try:
                out[futs[f]] = f.result()
            except Exception as exc:
                out[futs[f]] = f"could not be read: {str(exc)[:200]}"
    return ids, notes, out


# ------------------------------------------------------------------ render

def _cv(copy_val, v, short=None):
    v = "" if v is None else str(v)
    if not v:
        return '<span class="dim">&mdash;</span>'
    if short and len(v) > short:
        return copy_val(v, v[:short] + "…")
    return copy_val(v)


def _table(cols, rows, empty):
    if isinstance(rows, str):
        return f'<div class="herr lkerr">{html.escape(rows)}</div>'
    if not rows:
        return f'<div class="dim lknone">{empty}</div>'
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>"
                   for r in rows)
    return (f'<div class="lkwrap"><table class="otab lktab"><tr>{head}</tr>'
            f"{body}</table></div>")


def _sec(title, sub, inner, n=None, cap=None):
    cnt = "" if n is None else f' <span class="dim">({n:,})</span>'
    if n is not None and cap and n >= cap:
        cnt = (f' <span class="aqcap">(first {n:,} only &mdash; the query '
               "stops there; narrow the window to see the rest)</span>")
    return (f'<details class="lksec" open><summary><b>{title}</b>{cnt} '
            f'<span class="dim">{sub}</span></summary>{inner}</details>')


# the LIMIT on each section's query; a table that reaches it was cut
CAPS = {"builders": MAX_EVENTS, "offers": 200, "ingested": 300,
        "executed": 300, "api_txs": 100, "api_bundles": 100, "packets": 300,
        "val_bundles": 300, "searcher_tx": 300, "searcher_bundles": 300,
        "backruns": 300, "bk_landed": 50, "bk_source": 100, "bk_external": 100,
        "requests": 50}


def _count(v):
    return len(v) if isinstance(v, list) else None


def render(kind, ident, lo, hi, how, ids, notes, data, copy_val, names=None):
    names = names or {}
    snames = data.get("searchers") if isinstance(data.get("searchers"), dict) else {}
    e = html.escape
    out = []
    kinds = {"signature": "transaction", "prefix": "transaction (SigPrefix)",
             "bundle": "builder bundle id", "uuid": "API bundle_submission_id"}
    out.append(
        f'<div class="aqhead">{e(kinds.get(kind, kind))} '
        f'<span class="m">{copy_val(ident)}</span> &middot; window '
        f'<b>{lo:%Y-%m-%d %H:%M:%S}</b> &rarr; <b>{hi:%H:%M:%S}</b> UTC '
        f'<span class="dim">({e(how)})</span><br><span class="dim">'
        f'followed {len(ids["sigs"])} signature(s), {len(ids["bundles"])} '
        f'bundle id(s), {len(ids["uuids"])} API bundle id(s)</span></div>')
    for n in notes:
        out.append(f'<div class="aqcap lknote">{e(n)}</div>')
    out.append(render_summary(ids, data, copy_val, names))
    out.append(render_searchers(ids, data, copy_val))
    out.append(render_arbs(ids, data, copy_val, snames))
    out.append('<div class="lkhead">Details &mdash; every store, as read '
               '<span class="dim">(click a section to open it)</span></div>')
    top = len(out)
    if ids["sigs"] or ids["bundles"] or ids["uuids"]:
        why = ids.get("why") or {}

        def found(x):
            r = why.get(x, "")
            return ('<b class="land y">your input</b>' if r == "your input"
                    else e(r or "found"))
        items = ([("transaction", s, _cv(copy_val, s), _cv(copy_val, sigprefix(s)))
                  for s in ids["sigs"]]
                 + [("bundle", b, _cv(copy_val, b, 16), "") for b in ids["bundles"]]
                 + [("API bundle", u, _cv(copy_val, u), "") for u in ids["uuids"]])
        items.sort(key=lambda t: (why.get(t[1]) != "your input", t[0], t[1]))
        idrows = [[k, c, p, found(x)] for k, x, c, p in items]
        pulled = sum(1 for _k, x, _c, _p in items if why.get(x) != "your input")
        out.append(_sec("ids", "your input first; every other id was pulled "
                        "in by one widening step, and says how. A shared "
                        "source tx pulls in every bundle that carried it, "
                        "i.e. the competing backruns",
                        _table(["kind", "id", "SigPrefix", "how it was found"],
                               idrows, ""), len(idrows)))
        if pulled > 10:
            out.append(f'<div class="aqcap lknote">{pulled} ids were pulled in '
                       "beyond your input, so the tables below cover all of "
                       "them, not just yours.</div>")

    # chain
    ch = data.get("chain")
    if ch is not None:
        rows = []
        for s, p, c in (ch if isinstance(ch, list) else []):
            if not c:
                rows.append([_cv(copy_val, s, 16), _cv(copy_val, p),
                             '<span class="dim">not on chain</span>'] + [""] * 6)
                continue
            if "slot" not in c:
                rows.append([_cv(copy_val, s, 16), _cv(copy_val, p),
                             f'<span class="aqcap">{e(c.get("err", ""))}</span>']
                            + [""] * 6)
                continue
            rows.append([_cv(copy_val, s, 16), _cv(copy_val, p),
                         _cv(copy_val, c["slot"]), e(c["time"]),
                         (f'<span class="redn">{e(c["err"])}</span>' if c["err"]
                          else '<span class="land y">ok</span>'),
                         f'{c["fee"]:,}', f'{c["jito"]:,}', f'{c["builder"]:,}',
                         _cv(copy_val, c["signer"], 10)])
        out.append(_sec("chain", "getTransaction, per signature (tips = "
                        "positive deltas on the Jito / Titan / Bifrost tip "
                        "accounts)", _table(
            ["signature", "SigPrefix", "slot", "block time", "result", "fee",
             "jito tip", "builder tip", "fee payer"], rows, "")
            if isinstance(ch, list) else _table([], ch, ""), _count(ch)))

    # builders
    b = data.get("builders")
    if b is not None:
        rows = [] if isinstance(b, str) else [
            [e(r[0][:23]), e(r[1]), e(r[2]), f"<b>{e(r[3])}</b>", e(r[4] or ""),
             e(r[5]), _cv(copy_val, r[6], 16),
             f'<span class="lkjs" title="{e(r[7])}">{e(r[7][:90])}</span>',
             f'<span class="lkjs" title="{e(r[8])}">{e(r[8][:90])}</span>']
            for r in b]
        out.append(_sec("our builders", "bifrost_events for every id above, in "
                        "time order; each builder's own clock",
                        _table(["ts", "builder", "stage", "event", "reason",
                                "kind", "entity", "attrs", "nums"],
                               b if isinstance(b, str) else rows,
                               "none of our builders logged any of these ids"),
                        _count(b), CAPS["builders"]))

    # relay
    o = data.get("offers")
    if o is not None:
        rows = [] if isinstance(o, str) else [
            [_cv(copy_val, r[1]), e(str(r[2])), e(r[0]),
             f"{int(r[3]):,}", e(r[4][11:23]), e(r[5][11:23]),
             _cv(copy_val, r[6], 12), e(str(r[7])),
             ('<b class="land y">won</b> ' + " ".join(
                 _cv(copy_val, u, 8) for u in r[8]) if r[8] else
              '<span class="dim">no</span>'),
             _val(r[9])]
            for r in o]
        out.append(_sec("relay &middot; offers that carried it",
                        "mini_block_transactions_v2, every builder, grouped per "
                        "round; won = one of those offers was chosen",
                        _table(["slot", "round", "builder", "offers", "first",
                                "last", "bundle id", "first index", "won",
                                "best offer: fees + jito + builder payment"],
                               o if isinstance(o, str) else rows,
                               "no builder's offer carried it in this window"),
                        _count(o), CAPS["offers"]))
    for key, title, sub, cols, fmt in (
            ("ingested", "relay &middot; ingested from connectors",
             "connector_ingested_orders: how the relay got it (source_uri = "
             "the Jito block engine it came through; empty = TPU)",
             ["relay sent", "relay", "connector", "source", "type", "bundle id",
              "#", "signature"],
             lambda r: [e(r[0][:23]), e(r[1]), _conn(copy_val, r[2], names),
                        e(r[3] or "TPU"), e(r[4]), _cv(copy_val, r[5], 12),
                        e("" if r[6] is None else str(r[6])),
                        _cv(copy_val, r[7], 16)]),
            ("executed", "relay &middot; execution results",
             "connector_execution_results: what the validator did with it",
             ["received", "relay", "connector", "slot", "mini-block", "type",
              "bundle id", "executed in", "not included because", "signature"],
             lambda r: [e(r[0][:23]), e(r[1]), _conn(copy_val, r[2], names),
                        _cv(copy_val, r[3]), _cv(copy_val, r[4], 8), e(r[5]),
                        _cv(copy_val, r[6], 12), e(str(r[7] or "")),
                        (f'<span class="redn">{e(r[8])}</span>' if r[8] else
                         '<span class="land y">included</span>'),
                        _cv(copy_val, r[9], 16)])):
        v = data.get(key)
        if v is None:
            continue
        out.append(_sec(title, sub, _table(
            cols, v if isinstance(v, str) else [fmt(r) for r in v],
            "nothing in this window"), _count(v), CAPS.get(key)))

    # our API
    for key, title, sub, cols, fmt in (
            ("api_txs", "our API &middot; transactions",
             "db_sync.inbound_transactions_enriched by signature / "
             "bundle_submission_id (api key: first 8 characters only)",
             ["signature", "type", "api key", "bundle_submission_id", "created",
              "slot recv", "status", "slot landed", "leader", "source ip",
              "gravity status", "gravity slot", "gravity egress tip",
              "egress tip sig", "gravity bundle id", "jito", "harmonic",
              "bifrost", "rakurai"],
             lambda r: [_cv(copy_val, r[0], 16), e(r[1]), e(_key8(r[2])),
                        _cv(copy_val, r[3], 10), e(r[4][:23]), e(r[5]), e(r[6]),
                        e(r[7]), _conn(copy_val, r[8], names), e(r[9]),
                        e(r[10]), e(r[11]), e(r[12]), _cv(copy_val, r[13], 12),
                        _cv(copy_val, r[14], 12), e(r[15]), e(r[16]), e(r[17]),
                        e(r[18])]),
            ("api_bundles", "our API &middot; MEV bundles",
             "db_sync.inbound_mev_bundles_enriched (api key: first 8 "
             "characters only)",
             ["bundle_submission_id", "api key", "created", "variant", "txs",
              "target slot", "target bundle", "wire tip", "direct tip",
              "est. revenue", "tip wallet", "wallet type", "status",
              "slot landed", "gravity status", "gravity slot",
              "gravity bundle id", "source ip", "signatures"],
             lambda r: [_cv(copy_val, r[0], 10), e(_key8(r[1])), e(r[2][:23]),
                        e(r[3]), e(str(r[4])), e(r[5]), _cv(copy_val, r[6], 10),
                        e(r[7]), e(r[8]), e(r[9]), _cv(copy_val, r[10], 10),
                        e(r[11]), e(r[12]), e(r[13]), e(r[14]), e(r[15]),
                        _cv(copy_val, r[16], 12), e(r[17]),
                        " ".join(_cv(copy_val, s, 10)
                                 for s in (r[18] or "").split())])):
        v = data.get(key)
        if v is None:
            continue
        out.append(_sec(title, sub, _table(
            cols, v if isinstance(v, str) else [fmt(r) for r in v],
            "not in our API in this window"), _count(v), CAPS.get(key)))

    # our relayer
    for key, title, sub, cols, fmt in (
            ("packets", "our relayer &middot; packets",
             "relayer.validator_packet_events: every path the tx reached our "
             "relayer by (gravity_relay = the Gattaca relay's mempool feed, "
             "builder = re-published by our builder, json_rpc = our "
             "sendTransaction)",
             ["signature", "received", "path", "relayer host", "instance",
              "validator", "approx slot", "from", "sender ts", "source ip"],
             lambda r: [_cv(copy_val, r[0], 16), e(_ms_str(r[1])), e(r[2]),
                        e(r[3]), e(r[4]), _conn(copy_val, r[5], names), e(r[6]),
                        e(r[7]), e(_ms_str(r[8])), e(r[9])]),
            ("val_bundles", "our relayer &middot; bundles to validators",
             "relayer.validator_bundle_events",
             ["bundle", "received", "path", "relayer host", "instance",
              "validator", "txs", "approx slot", "signatures"],
             lambda r: [_cv(copy_val, r[0], 12), e(str(r[1])[:23]), e(r[2]),
                        e(r[3]), e(r[4]), _conn(copy_val, r[5], names),
                        e(str(r[6])), e(r[7]),
                        " ".join(_cv(copy_val, s, 10) for s in (r[8] or "").split())]),
            ("searcher_tx", "our relayer &middot; streamed to searchers",
             "relayer.searcher_egress_events: which searchers we sent the tx "
             "to (token: first 8 characters only)",
             ["searcher", "relayer host", "sent", "relay latency (us)",
              "instance", "signatures in message"],
             lambda r: [_who(r[0], snames), e(r[1]), e(_ms_str(r[2])), e(r[3]),
                        e(r[4]), e(str(len((r[5] or "").split())))]),
            ("searcher_bundles", "our relayer &middot; bundles streamed to searchers",
             "relayer.searcher_bundle_egress_events (token: first 8 characters only)",
             ["searcher", "relayer host", "sent", "relay latency (us)",
              "instance", "bundles in message"],
             lambda r: [_who(r[0], snames), e(r[1]), e(str(r[2])[:23]), e(r[3]),
                        e(r[4]), e(str(len((r[5] or "").split())))]),
            ("backruns", "our relayer &middot; searcher backruns",
             "relayer.searcher_backrun_events: backruns searchers sent us on "
             "this tx, or this tx as a backrun (token: first 8 characters only)",
             ["searcher", "relayer host", "searcher ip", "source", "backrun",
              "at", "instance"],
             lambda r: [_who(r[0], snames), e(r[1]), e(r[2]), _cv(copy_val, r[3], 12),
                        _cv(copy_val, r[4], 12), e(_ms_str(r[5])), e(r[6])])):
        v = data.get(key)
        if v is None:
            continue
        out.append(_sec(title, sub, _table(
            cols, v if isinstance(v, str) else [fmt(r) for r in v],
            "nothing in this window"), _count(v), CAPS.get(key)))

    for key, title, sub, cols, fmt in (
            ("bk_landed", "backrun intel &middot; landed backruns",
             "backrun_intel.backrun_discovery: the landed truth for rev share",
             ["signature", "searcher", "signer", "slot", "block time",
              "rev-share wallet", "rev share", "token", "circular",
              "programs", "bloom fee"],
             lambda r: [_cv(copy_val, r[0], 16), f"<b>{e(r[1])}</b>",
                        _cv(copy_val, r[2], 8), _cv(copy_val, r[3]), e(r[4]),
                        _cv(copy_val, r[5], 8), f"{int(r[6] or 0):,}",
                        _cv(copy_val, r[7], 8), "yes" if str(r[8]) in ("1", "true") else "",
                        e(" ".join(p[:6] for p in (r[9] or "").split())),
                        f"{int(r[10] or 0):,}"]),
            ("bk_source", "backrun intel &middot; source attribution (ours)",
             "backrun_intel.backrun_source: which source tx a backrun was on, "
             "and how it was delivered (api key / token: first 8 characters)",
             ["backrun", "source", "provider", "slug", "tier", "source slot",
              "source pos", "backrun pos", "source api key", "delivered to",
              "via relayer", "swap size"],
             lambda r: [_cv(copy_val, r[0], 12), _cv(copy_val, r[1], 12), e(r[2]),
                        e(r[3]), e(r[4]), _cv(copy_val, r[5]), e(str(r[6])),
                        e(str(r[7])), e(_key8(r[8])), _who(r[9], snames),
                        e(r[10]), f"{int(r[11] or 0):,}"]),
            ("bk_external", "backrun intel &middot; competitors' backruns",
             "backrun_intel.external_backrun_source: other searchers' backruns "
             "on the same source",
             ["backrun", "source", "source slot", "block time", "provider",
              "slug", "tier", "leader", "match", "strong"],
             lambda r: [_cv(copy_val, r[0], 12), _cv(copy_val, r[1], 12),
                        _cv(copy_val, r[2]), e(r[3]), e(r[4]), e(r[5]), e(r[6]),
                        _conn(copy_val, r[7], names), e(r[8]),
                        "yes" if str(r[9]) in ("1", "true") else ""]),
            ("requests", "our API &middot; raw requests",
             "db_sync.request_log_record_backup: the request body as sent -- "
             "the only copy of an unlanded bundle's bytes (api key: first 8 "
             "characters; click a body to copy all of it)",
             ["at", "location", "api key", "user ip", "method", "status",
              "error", "latency", "signature", "request", "response"],
             lambda r: [e(r[0]), e(r[1]), e(_key8(r[2])), e(r[3]),
                        e(r[4] or r[5]), e(str(r[6])),
                        "yes" if str(r[7]) in ("1", "true") else "", e(r[8]),
                        _cv(copy_val, r[9], 12), _cv(copy_val, r[10], 60),
                        _cv(copy_val, r[11], 60)])):
        v = data.get(key)
        if v is None:
            continue
        out.append(_sec(title, sub, _table(
            cols, v if isinstance(v, str) else [fmt(r) for r in v],
            "nothing for these ids"), _count(v), CAPS.get(key)))
    # the summary sections stay open; the raw ones start closed
    return "".join(out[:top]) + "".join(out[top:]).replace(
        '<details class="lksec" open>', '<details class="lksec">')


# ------------------------------------------------------------------ summary render

def _bshort(b):
    """ASTRALANE_AMS_1 -> AMS_1; other builders keep their name."""
    return b[len("ASTRALANE_"):] if (b or "").startswith("ASTRALANE_") else (b or "")


def _family(b):
    b = b or ""
    return ("us" if b.startswith("ASTRALANE") else
            "titan" if b.startswith("TITAN") else "other")


def _ok(v):
    return isinstance(v, (list, tuple))


def _verdicts(data, prefixes):
    """(builder, entity) -> our scheduler's verdict, as short html."""
    e = html.escape
    out, cls = {}, {}
    offered = {}
    ours = data.get("s_ours")
    for r in (ours if isinstance(ours, list) else []):
        inst, kind, slot, rnd, won = r[0], r[1], r[2], r[3], str(r[4]) == "true"
        txs = list(prefixes) if str(r[7]) in ("1", "true") else []
        for ent in _arr(r[6]) + txs:
            d = offered.setdefault((inst, ent), {"rounds": set(), "won": False})
            if kind == "selected":
                d["rounds"].add((int(slot), int(rnd)))
            elif kind == "winner" and won:
                d["won"] = True
    last = {}
    ev = data.get("s_events")
    for r in (ev if isinstance(ev, list) else []):
        key = (r[0], r[1])
        if r[4] in ("dropped", "check_dropped", "entry_dropped",
                    "winner_order_refused", "pin_skipped", "bundle_selection"):
            last[key] = r
        else:
            last.setdefault(key, r)
    for key in set(offered) | set(last):
        parts = []
        o = offered.get(key)
        if o and o["rounds"]:
            parts.append('<b class="land y">offered</b> ' + ", ".join(
                f"{s}/{i}" for s, i in sorted(o["rounds"]))
                + (' <b class="land y">won</b>' if o["won"] else ""))
        r = last.get(key)
        if r:
            what = " ".join(x for x in (r[5], r[6]) if x) or r[4]
            err = (r[7] or "").replace("Error processing ", "")
            parts.append(f'<span class="redn">{e(r[4])}</span> {e(what)}'
                         + (f' <span class="dim">{e(err[:70])}</span>'
                            if err and err != what else ""))
        out[key] = " &rarr; ".join(parts)
        # the one-word class, for the summary's tally
        cls[key] = ("offered" if o and o["rounds"] else
                    (" ".join(x for x in (r[5], r[6]) if x) or r[4]) if r else
                    "no event after receipt")
    return out, cls


def _leader(copy_val, names, slot, ident):
    if not slot or slot == "0":
        return '<span class="dim">&mdash;</span>'
    return f"{_cv(copy_val, slot)} {_conn(copy_val, ident, names)}"


def render_summary(ids, data, copy_val, names):
    e = html.escape
    focal = ids.get("focal") or []
    if not focal:
        return ""
    fset = set(focal)
    bullets, out = [], []
    who = (f"transaction {copy_val(focal[0], focal[0][:16] + '…')} "
           f"(SigPrefix {copy_val(sigprefix(focal[0]))})" if len(focal) == 1
           else f"the {len(focal)} transactions of your input")
    # chain
    landed = {}
    chn = data.get("chain")
    for s, _p, c in (chn if isinstance(chn, list) else []):
        if s in fset:
            landed[s] = c
    for s in focal:
        c = landed.get(s)
        lab = "" if len(focal) == 1 else f"{copy_val(s, s[:10] + '…')}: "
        if c is None and s not in landed:
            continue
        if not c:
            bullets.append(f"{lab}<b>not on chain</b>")
        elif "slot" in c:
            bullets.append(
                f"{lab}landed in slot {_cv(copy_val, c['slot'])} at "
                f"{e(c['time'])} &mdash; " +
                (f'<span class="redn">failed {e(c["err"])}</span>' if c["err"]
                 else '<span class="land y">ok</span>'))
    land_slots = {int(c["slot"]) for c in landed.values() if c and c.get("slot")}

    # received by our builders
    rv = data.get("s_recv")
    verd, vcls = _verdicts(data, {sigprefix(s) for s in focal})
    recv_rows, bundles = [], {}
    if isinstance(rv, str):
        bullets.append(f'our builders: <span class="aqcap">{e(rv)}</span>')
    elif rv is not None:
        firsts, standalone = {}, set()
        for r in rv:
            ts, inst, ek, ent = r[0], r[1], r[2], r[3]
            firsts.setdefault(inst, ts)
            if ek == "bundle":
                bundles.setdefault(ent, {"recv": {}, "us": set(), "titan": set(),
                                         "other": set(), "won": set()})
                bundles[ent]["recv"].setdefault(inst, ts)
            else:
                standalone.add(inst)
            try:
                rt = dt.datetime.fromisoformat(r[0][:26])
                pt = dt.datetime.fromisoformat(r[7][:26])
                nt = dt.datetime.fromisoformat(r[11][:26])
            except ValueError:
                rt = pt = nt = None
            if pt and pt.year > 1970 and (rt - pt).total_seconds() * 1000 <= IN_ROUND_MS:
                lead = f"{_leader(copy_val, names, r[8], r[10])} " \
                       f'<span class="dim">round {e(r[9])}, running</span>'
            elif nt and nt.year > 1970:
                gap = (nt - rt).total_seconds()
                lead = (f"{_leader(copy_val, names, r[12], r[14])} "
                        f'<span class="dim">round {e(r[13])}, next to open, '
                        f"+{gap * 1000:,.0f} ms</span>" if gap < 1 else
                        f"{_leader(copy_val, names, r[12], r[14])} "
                        f'<span class="dim">next window, +{gap:,.0f} s</span>')
            else:
                lead = '<span class="dim">no round in the window</span>'
            wait = int(r[6] or 0)
            recv_rows.append([
                e(ts[11:26]), e(_bshort(inst)),
                ("standalone tx" if ek != "bundle" else
                 "bundle " + _cv(copy_val, ent, 12)),
                e(f"{r[4]}/{r[5]}".strip("/")),
                (f"{wait / 1000:,.1f} ms" if wait else
                 '<span class="dim">0</span>'), lead,
                verd.get((inst, ent))
                or '<span class="dim">no event after receipt</span>'])
        if rv:
            first = min(firsts.items(), key=lambda kv: kv[1])
            bullets.append(
                f"<b>received by our builders</b>: {len(firsts)} "
                f"({', '.join(e(_bshort(b)) for b in sorted(firsts))}), first "
                f"{e(_bshort(first[0]))} at {e(first[1][11:23])}; "
                f"in {len(bundles)} distinct bundle(s)"
                + (f"; standalone from the relay on "
                   f"{', '.join(e(_bshort(b)) for b in sorted(standalone))}"
                   if standalone else "; never standalone"))
        else:
            bullets.append("<b>none of our builders received it</b> in this "
                           "window")
        if rv:
            tally = {}
            for r in rv:
                c = vcls.get((r[1], r[3]), "no event after receipt")
                tally[c] = tally.get(c, 0) + 1
            bullets.append(
                "<b>our scheduler's verdicts</b> (per builder &times; "
                "entity): " + " &middot; ".join(
                    f"{e(c)} <b>{n}</b>" for c, n in
                    sorted(tally.items(), key=lambda kv: (kv[0] != "offered",
                                                          -kv[1]))))

    # offers, ours and Titan's
    rel = data.get("s_relay")
    rounds = {}
    if isinstance(rel, str):
        bullets.append(f'relay offers: <span class="aqcap">{e(rel)}</span>')
    elif rel is not None:
        offers, winners = rel
        win = {(int(w[0]), int(w[1])): w for w in winners}
        for b, slot, rnd, conn, bid, uuids in offers:
            key = (int(slot), int(rnd))
            d = rounds.setdefault(key, {"conn": conn, "us": {}, "titan": {},
                                        "other": {}, "carried": None})
            d[_family(b)].setdefault(b, set()).add(bid)
            w = win.get(key)
            if w and w[3] in set(_arr(uuids)):
                d["carried"] = (b, bid)
            if bid:
                bd = bundles.setdefault(bid, {"recv": {}, "us": set(),
                                              "titan": set(), "other": set(),
                                              "won": set()})
                bd[_family(b)].add(f"{_bshort(b)} {slot}/{rnd}")
                if w and w[3] in set(_arr(uuids)):
                    bd["won"].add(_bshort(b))
        for fam, label in (("us", "us"), ("titan", "Titan")):
            hit = sorted(k for k, d in rounds.items() if d[fam])
            bids = {x for d in rounds.values() for v in d[fam].values()
                    for x in v if x}
            plain = any("" in v for d in rounds.values() for v in d[fam].values())
            if hit:
                by_slot = {}
                for s, i in hit:
                    by_slot.setdefault(s, []).append(str(i))
                bullets.append(
                    f"<b>offered by {label}</b>: yes &mdash; " + "; ".join(
                        f"slot {s} round {', '.join(r)}"
                        for s, r in sorted(by_slot.items()))
                    + f" ({', '.join(e(_bshort(b)) for b in sorted({b for k in hit for b in rounds[k][fam]}))})"
                    f" &middot; <b>{len(bids)}</b> distinct bundle(s)"
                    + (" + as a plain transaction" if plain else ""))
            else:
                where = (f" in slot {', '.join(str(s) for s in sorted(land_slots))}"
                         " (where it landed)" if land_slots else "")
                bullets.append(f"<b>offered by {label}</b>: no &mdash; none of "
                               f"{'our' if fam == 'us' else 'its'} offers "
                               f"carried it{where}")
        carried = [(k, d["carried"]) for k, d in sorted(rounds.items())
                   if d["carried"]]
        if carried:
            bullets.append("<b>won</b>: " + "; ".join(
                f"slot {s} round {i} by {e(_bshort(b))}"
                + (f" (bundle {copy_val(bid, bid[:10] + '…')})" if bid else
                   " (as a plain transaction)")
                for (s, i), (b, bid) in carried))
        elif rounds:
            bullets.append("<b>won</b>: no offer carrying it won a round")
        # per round: who carried it, who won
        rrows = []
        for (s, i) in sorted(set(rounds) | set(win)):
            d = rounds.get((s, i)) or {"us": {}, "titan": {}, "other": {},
                                        "carried": None, "conn": ""}
            w = win.get((s, i))

            def fam_cell(f):
                if not d[f]:
                    return '<span class="dim">no</span>'
                return "<br>".join(
                    f"<b>{e(_bshort(b))}</b> " + (", ".join(
                        _cv(copy_val, x, 10) if x else "plain tx"
                        for x in sorted(v)))
                    for b, v in sorted(d[f].items()))
            winner = '<span class="dim">&mdash;</span>'
            if w:
                winner = (f"{e(_bshort(w[2]))} " + (
                    '<b class="land y">with it</b>' if d["carried"] else
                    '<span class="dim">without it</span>'))
            rrows.append([_cv(copy_val, s) + (" <b>landed</b>" if s in land_slots
                                               else ""),
                          e(str(i)), _conn(copy_val, d["conn"] or (w[4] if w else ""),
                                           names),
                          fam_cell("us"), fam_cell("titan"), fam_cell("other"),
                          winner])
        if rrows:
            out.append(_sec("rounds", "every round of the slots it was offered "
                            "in: which of our / Titan's offers carried it, in "
                            "which bundle, and who won the round",
                            _table(["slot", "round", "leader", "us", "Titan",
                                    "other", "winner"], rrows, ""),
                            len(rrows)))

    # bundles
    if bundles:
        brows = []
        for bid, d in sorted(bundles.items(),
                             key=lambda kv: (-len(kv[1]["us"]), -len(kv[1]["titan"]),
                                             min(kv[1]["recv"].values() or ["~"]))):
            vs = [f"<b>{e(_bshort(b))}</b> " + (verd.get((b, bid)) or
                  '<span class="dim">no event after receipt</span>')
                  for b in sorted(d["recv"])]
            brows.append([
                _cv(copy_val, bid, 12),
                (e(min(d["recv"].values())[11:23]) if d["recv"] else
                 '<span class="dim">never received by us</span>'),
                "<br>".join(vs) or '<span class="dim">&mdash;</span>',
                "<br>".join(e(x) for x in sorted(d["us"])) or
                '<span class="dim">no</span>',
                "<br>".join(e(x) for x in sorted(d["titan"])) or
                '<span class="dim">no</span>',
                ('<b class="land y">' + e(", ".join(sorted(d["won"]))) + "</b>"
                 if d["won"] else "")])
        nus = sum(1 for d in bundles.values() if d["us"])
        nti = sum(1 for d in bundles.values() if d["titan"])
        both = sum(1 for d in bundles.values() if d["us"] and d["titan"])
        out.append(_sec(
            "bundles that carried it",
            f"{len(bundles)} distinct: {nus} offered by us, {nti} by Titan, "
            f"{both} by both. Our verdict is per builder: offered (slot/round), "
            "or the last drop with its reason. A bundle offered under dropping "
            "indices goes out under a derived id, so it can read as not "
            "offered here",
            _table(["bundle", "first received by us", "our scheduler's verdict",
                    "offered by us", "offered by Titan", "won"], brows, ""),
            len(brows)))
    if recv_rows:
        out.append(_sec("received by our builders",
                           "first receipt per builder and per entity; leader = "
                           "the round running at receipt (or the next to open "
                           "when the builder was between windows)",
                           _table(["received", "builder", "as", "source/route",
                                   "wait", "leader slot", "verdict"],
                                  recv_rows, ""), len(recv_rows), 600))
    head = (f'<div class="lkhead">Summary &mdash; {who}</div>'
            '<ul class="lksum">' + "".join(f"<li>{b}</li>" for b in bullets)
            + "</ul>")
    return head + "".join(out)


def render_searchers(ids, data, copy_val):
    e = html.escape
    v = data.get("s_api")
    if v is None:
        return ""
    focal = ids.get("focal") or []
    if isinstance(v, str):
        return _sec("searchers &middot; received", "", _table([], v, ""))
    rows = []
    for r in v:
        (at, sig, method, user, kname, key, bsid, tip, status, slot, n, tips,
         nmev, pos, drop, targets, tslot, wire, bstatus, gstatus, gbid) = r
        mev = method == "send_mev_bundle"
        drop, targets = _arr(drop), _arr(targets)
        size = int(nmev or 0) or int(n or 0)
        rows.append([
            e(at[11:23]),
            (_cv(copy_val, sig, 12) + " " + _cv(copy_val, sigprefix(sig))
             if len(focal) > 1 else ""),
            f"<b>{e(user or kname or '?')}</b> "
            f'<span class="dim">{e(_key8(key))}</span>',
            e(method),
            _cv(copy_val, bsid) if bsid and size > 1 else
            ('<span class="dim">single tx</span>' if size <= 1 else ""),
            (f"{int(pos)} of {size}" if int(pos or 0) else
             f"{size}" if size else ""),
            f"{int(tip or 0):,}",
            (f"{int(wire):,}" if wire else f"{int(tips or 0):,}"),
            (e(", ".join(str(x) for x in drop)) if drop else
             '<span class="dim">none</span>' if mev else ""),
            ((" ".join(_cv(copy_val, a, 8) for a in targets) if targets else
              '<span class="dim">none</span>') if mev else ""),
            e(tslot) if mev else "",
            e(bstatus or status) + (f" {e(slot)}" if slot else ""),
            _cv(copy_val, gbid, 10)])
    users = {(r[3] or r[4] or "?") for r in v}
    methods = {}
    for r in v:
        methods[r[2]] = methods.get(r[2], 0) + 1
    sub = (f"{len(v)} submission(s) to our API by {len(users)} searcher(s): "
           + ", ".join(f"{m} &times;{c}" for m, c in sorted(methods.items()))
           + ". Tip = this transaction's own tip; bundle tip = the bundle's "
           "wire tip. Dropping indices and target accounts exist only on "
           "send_mev_bundle (api key: first 8 characters only)") if v else ""
    cols = ["received", "transaction" if len(focal) > 1 else "",
            "searcher", "method", "bundle_submission_id", "position",
            "tx tip", "bundle tip", "dropping indices", "target accounts",
            "target slot", "status", "gravity bundle"]
    if len(focal) <= 1:
        cols.pop(1)
        rows = [r[:1] + r[2:] for r in rows]
    return _sec("searchers &middot; received",
                sub or "db_sync.inbound_transactions_enriched",
                _table(cols, rows, "no searcher sent it to our API in this "
                       "window"), len(rows), 300)


def _profit(lamports, mint, dec):
    try:
        x = int(lamports) / 10 ** int(dec or 9)
    except (TypeError, ValueError):
        return ""
    unit = "SOL" if mint.startswith("So1111111111") else mint[:4] + "…"
    return f"{x:,.6f} {html.escape(unit)}"


def render_arbs(ids, data, copy_val, snames):
    e = html.escape
    v = data.get("s_arbs")
    if v is None:
        return ""
    if isinstance(v, str):
        return _sec("searcher arbs", "", _table([], v, ""))
    arbs, mine, found = v
    fset = set(ids.get("focal") or [])
    delivered = {r[0]: r for r in mine}
    rows, seen = [], set()
    for r in sorted(arbs, key=lambda r: (int(r[1]), int(r[2]))):
        (sig, slot, pos, signer, actor, profit, mint, dec, route, legs,
         source, sslot, arm, strong, bt) = r
        seen.add(sig)
        d = found.get(sig)
        dl = delivered.get(sig)
        ours = ""
        if d or dl:
            ours = ('<b class="land y">ours</b> ' + (f"<b>{e(d[0])}</b> " if d else "")
                    + (f'<span class="dim">rev share {int(d[2] or 0):,} '
                       f"{e(d[3] or '')}</span>" if d else "")
                    + (f" via {_who(dl[2], snames)}" if dl and dl[2] else ""))
        rows.append([
            _cv(copy_val, sig, 12), _cv(copy_val, sigprefix(sig)),
            ("this is the arb" if sig in fset else
             "backrun of it" if source in fset else ""),
            _cv(copy_val, slot), e(str(pos)), _cv(copy_val, actor or signer, 8),
            _profit(profit, mint, dec), e(route.replace(";", " → ")),
            e(str(legs)), e(arm), "yes" if str(strong) in ("1", "true") else "",
            ours or '<span class="dim">no</span>'])
    for sig, _src, tok, bpos in mine:
        if sig in seen:
            continue
        d = found.get(sig)
        rows.append([
            _cv(copy_val, sig, 12), _cv(copy_val, sigprefix(sig)),
            "backrun of it (not a circular arb)", _cv(copy_val, d[1] if d else ""),
            e(str(bpos)), "", "", "", "", "", "",
            '<b class="land y">ours</b> ' + (f"<b>{e(d[0])}</b> " if d else "")
            + (f"via {_who(tok, snames)}" if tok else "")])
    nours = sum(1 for r in rows if "ours" in r[-1])
    sub = (f"{len(rows)} backrun(s), {nours} by our searchers. "
           "backrun_intel.all_circular_arbs on the source tx; ours = landed "
           "through us (backrun_discovery / backrun_source)")
    return _sec("searcher arbs", sub, _table(
        ["arb", "SigPrefix", "", "slot", "position", "actor", "profit",
         "route", "legs", "match", "strong", "ours?"], rows,
        "no circular arb backran it (or it is not in backrun_intel yet: "
        "that pipeline runs a few minutes behind the chain)"), len(rows), 300)


def _val(v):
    if not v:
        return '<span class="dim">&mdash;</span>'
    fees, jito, pay, _r = v
    return (f'<span title="protocol fees {fees:,} + Jito tips {jito:,} (raw) '
            f'+ builder payment {pay:,}">{fees + jito + pay:,} '
            f'<span class="dim">= {fees:,} + {jito:,} + {pay:,}</span></span>')


def _who(tok, snames):
    """A searcher token: its name where backrun_intel knows it, and only the
    first 8 characters of the token itself -- it is a credential."""
    if not tok:
        return '<span class="dim">&mdash;</span>'
    n = snames.get(tok)
    return (f"<b>{html.escape(n)}</b> " if n else "") + html.escape(_key8(tok))


def _conn(copy_val, ident, names):
    if not ident:
        return '<span class="dim">&mdash;</span>'
    n = names.get(ident) or ""
    return ((f'<span class="vname">{html.escape(n)}</span> ' if n else "")
            + copy_val(ident, ident[:8] + "…"))


# ------------------------------------------------------------------ page

def form_html(q, at, span):
    e = html.escape
    return (f'<form class="aqform" method="get" action="{LOOKUP_PATH}">'
            '<label>transaction or bundle<input name="q" size="70" '
            f'value="{e(q)}" placeholder="signature, SigPrefix, bundle id '
            '(64 hex) or API bundle_submission_id (ULID)" autocomplete="off"></label>'
            '<label>around (UTC, optional)<input name="at" size="19" '
            f'value="{e(at)}" placeholder="2026-10-24 07:38:10"></label>'
            '<label>&plusmn; minutes<input name="span" size="4" '
            f'value="{e(span)}"></label>'
            '<button type="submit">look up</button></form>')


def page_body(qs, src, copy_val, names=None):
    one = lambda k: (qs.get(k, [""])[0] or "").strip()
    q, at, span = one("q"), one("at"), one("span") or "10"
    out = [form_html(q, at, span)]
    if not q:
        out.append('<div class="aqhead">Paste a full signature, a SigPrefix, '
                   "a builder bundle id or an API bundle_submission_id. The "
                   "lookup finds when it was around (the chain if it landed, "
                   "otherwise our builders, the relay or our API), follows the "
                   "bundles that carried it and the members of a bundle, and "
                   "reads every store we have for all of them.</div>")
        return "".join(out)
    kind, ident = classify(q)
    if not kind:
        out.append('<div class="herr" style="margin:16px 28px">not a '
                   "signature, SigPrefix, 64-hex bundle id or API "
                   "bundle_submission_id</div>")
        return "".join(out)
    try:
        sp = max(1, min(180, int(span)))
    except ValueError:
        sp = 10
    if at:
        if not TIME_RE.match(at):
            out.append('<div class="herr" style="margin:16px 28px">the time '
                       "must look like 2026-10-24 07:38:10 (UTC)</div>")
            return "".join(out)
        t, how = _parse_time(at), "the time you gave"
    else:
        t, how = find_anchor(src, kind, ident)
        if t is None:
            out.append('<div class="herr" style="margin:16px 28px">'
                       f"{html.escape(how)}: give a time to search around"
                       "</div>")
            return "".join(out)
    lo, hi = t - dt.timedelta(minutes=sp), t + dt.timedelta(minutes=sp)
    ids, notes, data = gather(src, kind, ident, lo, hi)
    out.append(render(kind, ident, lo, hi, f"{how}, ±{sp} min", ids,
                      notes, data, copy_val, names))
    return "".join(out)


CSS = """
.lksec{margin:10px 28px;border:1px solid #1e2937;border-radius:8px;
  padding:8px 12px;background:#0b1016}
.lksec>summary{cursor:pointer;color:#dbe4ee;font-size:13px}
.lksec>summary .dim{font-size:11.5px;margin-left:6px}
.lkwrap{overflow-x:auto;margin-top:8px}
.lktab{font-size:11.5px}
.lktab td{white-space:nowrap}
.lkjs{font:10.5px ui-monospace,Menlo,monospace;color:#8fa6bf}
.lknone{padding:8px 0;font-size:12px}
.lkerr{margin:8px 0}
.lknote{padding:4px 28px;font-size:12px}
.vname{font:12px system-ui,sans-serif;color:#cbd5e1}
.lkhead{margin:18px 28px 4px;font-size:14px;font-weight:600;color:#dbe4ee}
.lksum{margin:4px 28px 8px;padding-left:18px;font-size:12.5px;line-height:1.7;
  color:#cbd5e1}
.lktab td{vertical-align:top}
"""
