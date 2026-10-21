"""GBX tips per leader slot, from Dune.

What this is for: `avg_reward` in the Firedancer report is priority + base
fees, and it is the common baseline both validators are measured on. GBX
builder tips are paid into tip wallets, not to the leader in-block, so they
appear nowhere in that number. This module measures them, per leader slot,
so they can be added on top of our validator's normalised on-chain figure.

The query is two steps: the slots a validator led (solana.rewards, where a
'Fee' reward is written to the leader once per slot it produced), then the
inflows to the builder tip wallets in exactly those slots.

Dune runs take ~60s, so nothing here is called from a request thread --
valcomp.py drives it from a background job.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request

BASE = "https://api.dune.com/api/v1"
# This account is at its private-query cap, so a new query cannot be created:
# the documented pattern (gbx_daily/dune.py) is to PATCH one scratch query and
# run it. That query is SHARED -- anything else using it is overwritten.
REUSE_QID = int(os.environ.get("DUNE_QUERY_ID", "8609558"))
POLL_S = 5
TIMEOUT_S = 600

# bifrost-builder/src/policy.rs and txn-router common/src/constants.rs, via the
# gravity-rev-compare skill. Builder tips only: backrun and non-backrun wallets
# are a different stream and are not slot-bound in the same way.
TITAN_TIP_ACCOUNTS = [
    "xk95T6h7nFSRZepEf6JGdv9BCvZLER6dDGXg2KmJ1t1",
    "AEg8jYwFpEYXaVd3jMpxPu8V1HXvSvmZr9ms8wm4ub1a",
    "HiLntniupDE3Y18j8BtoMtE6MxQdXRfJw3xYCKfBt3t5",
    "8bXKHaeUzcYGr8ag8WJVdg2Etiy8vw3TCcb5f3dQ4xaq",
    "HtmPJ2k1SzFzLE6Zdu7RXqGxgDPKx2C5Hkdu189B7TXe",
    "5yjjgHRL8q4uuWXsg3CAvBJdLQ3NUDwJK17EjHHeCorX",
    "DDceWhu2X1x38FWGzjPXnNGnKqJwPYHqTX4Xr2gd6xbW",
    "2bERRYFDaZfMNN4TwiFLgm5wntZ457YnmpRBrpSUw9Aj",
]
BIFROST_TIP_ACCOUNTS = [
    "BifrU9xXTEebSVftkHTSLyLztJR4yJyHF3kaZ8aEAytZ",
    "BiFr9udkWn26s4RLjMZ2iwugibPzRV6ZrXYsHFfHxByJ",
    "BifrnpS6g6NUHWcwvTtLMV1D2rKPy28c3JuKdTf2ttEa",
    "BifrDg5pq1Jfh3MiGA2YxmvmkpCL1PR4CMCgdaAERaqo",
    "BifRXtNG6F5QN1sYDgKTgChd2dPc8okKVbUyp1pyi6cy",
    "BifrXhZTj6iwBRVxtcanqf83zZiTDr9QCBoZ2nP6jC2T",
    "BiFrewNddv4Ai4kDcDYUWfpZ5qxAfHUZvCDRLL3LsoTo",
    "BifrDVX2ZZS3E5uFoqgc22zsdwY6LwrSjverK7xmqetw",
]

_lock = threading.Lock()   # the scratch query is one shared slot


def _key():
    k = os.environ.get("DUNE_API_KEY", "")
    if not k:
        raise RuntimeError("DUNE_API_KEY is not set")
    return k


def _call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{BASE}{path}", data=data, method=method,
        headers={"X-DUNE-API-KEY": _key(), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode()[:400]}


def _sql(validator, lo, hi):
    wallets = ",".join(
        "'" + w.replace("'", "") + "'"
        for w in TITAN_TIP_ACCOUNTS + BIFROST_TIP_ACCOUNTS)
    v = validator.replace("'", "")
    # An explicit date range, not a lookback: the cache fills gaps, and a
    # gap is a set of specific days rather than "the last N".
    lo = str(lo)[:10].replace("'", "")
    hi = str(hi)[:10].replace("'", "")
    return f"""
WITH ls AS (
  SELECT block_date, block_slot
  FROM solana.rewards
  WHERE reward_type = 'Fee' AND recipient = '{v}'
    AND block_date BETWEEN DATE '{lo}' AND DATE '{hi}'
),
tips AS (
  SELECT a.block_slot,
         SUM(CAST(a.balance_change AS DOUBLE))/1e9 AS tip_sol
  FROM solana.account_activity a
  WHERE a.block_date BETWEEN DATE '{lo}' AND DATE '{hi}'
    AND a.address IN ({wallets})
    AND a.balance_change > 0
    AND a.token_mint_address IS NULL
    AND a.tx_success = true
    AND a.block_slot IN (SELECT block_slot FROM ls)
  GROUP BY a.block_slot
)
SELECT ls.block_date AS day,
       COUNT(*) AS leader_slots,
       COALESCE(SUM(t.tip_sol), 0) AS gbx_tips_sol,
       COUNT(t.block_slot) AS slots_with_tips
FROM ls LEFT JOIN tips t ON t.block_slot = ls.block_slot
GROUP BY ls.block_date
ORDER BY ls.block_date
"""


def _run(sql, on_step=lambda _m: None):
    """PATCH the scratch query, execute, poll, return rows.

    Serialised: the query id is a single shared slot, so two runs at once
    would overwrite each other's SQL mid-flight."""
    with _lock:
        on_step("patching the Dune query")
        s, d = _call("PATCH", f"/query/{REUSE_QID}", {"query_sql": sql})
        if s != 200:
            raise RuntimeError(f"Dune PATCH {s}: {str(d)[:160]}")
        on_step("starting the run")
        s, d = _call("POST", f"/query/{REUSE_QID}/execute",
                     {"performance": "medium"})
        eid = d.get("execution_id")
        if not eid:
            raise RuntimeError(f"Dune execute {s}: {str(d)[:160]}")
        t0 = time.time()
        while True:
            _, st = _call("GET", f"/execution/{eid}/status")
            state = st.get("state", "")
            if state == "QUERY_STATE_COMPLETED":
                break
            if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                raise RuntimeError(f"Dune run {state}: "
                                   f"{str(st.get('error'))[:160]}")
            if time.time() - t0 > TIMEOUT_S:
                raise RuntimeError(f"Dune run still {state} after "
                                   f"{TIMEOUT_S}s")
            on_step(f"Dune running, {int(time.time() - t0)}s")
            time.sleep(POLL_S)
        _, d = _call("GET", f"/execution/{eid}/results?limit=1000")
        return (d.get("result") or {}).get("rows") or []


def tips_per_day(validator, lo, hi, on_step=lambda _m: None):
    """[{day, leader_slots, gbx_tips_sol, slots_with_tips}] over [lo, hi]."""
    rows = _run(_sql(validator, lo, hi), on_step)
    return [{"day": str(r.get("day"))[:10],
             "leader_slots": int(r.get("leader_slots") or 0),
             "gbx_tips_sol": float(r.get("gbx_tips_sol") or 0.0),
             "slots_with_tips": int(r.get("slots_with_tips") or 0)}
            for r in rows]


def _fees_sql(validator, day):
    """Top priority-fee payers in one validator's leader slots on one day.

    Solana does not store a priority fee: a transaction's `fee` is the base
    fee plus the priority fee, and the base is 5,000 lamports per required
    signature. So the priority part is `fee - 5000 * required_signatures`,
    and anything at or below that threshold paid no priority fee at all.

    The day total rides along on every row via a CROSS JOIN so the share
    each transaction accounts for is computed against the same scan -- a
    second query could land on a different slot set as the day advances."""
    v = validator.replace("'", "")
    d = str(day)[:10].replace("'", "")
    return f"""
WITH ls AS (
  SELECT block_slot FROM solana.rewards
  WHERE reward_type = 'Fee' AND recipient = '{v}' AND block_date = DATE '{d}'
),
tx AS (
  SELECT t.id AS signature, t.signer, t.block_slot,
         t.fee - 5000 * t.required_signatures AS prio,
         t.compute_units_consumed AS cu, t.success
  FROM solana.transactions t
  WHERE t.block_date = DATE '{d}'
    AND t.block_slot IN (SELECT block_slot FROM ls)
    AND t.fee > 5000 * t.required_signatures
),
agg AS (SELECT SUM(prio) AS total_prio, COUNT(*) AS n FROM tx)
SELECT tx.signature, tx.signer, tx.block_slot, tx.prio, tx.cu, tx.success,
       agg.total_prio, agg.n
FROM tx CROSS JOIN agg ORDER BY tx.prio DESC LIMIT 5
"""


def top_priority_fees(validator, day, on_step=lambda _m: None):
    """-> {"total_prio", "n", "rows": [...]} for one validator-day."""
    rows = _run(_fees_sql(validator, day), on_step)
    if not rows:
        return {"total_prio": 0, "n": 0, "rows": []}
    total = float(rows[0].get("total_prio") or 0)
    return {
        "total_prio": total, "n": int(rows[0].get("n") or 0),
        "rows": [{"signature": r.get("signature") or "",
                  "signer": r.get("signer") or "",
                  "slot": int(r.get("block_slot") or 0),
                  "prio": float(r.get("prio") or 0),
                  "cu": int(r.get("cu") or 0),
                  "success": bool(r.get("success")),
                  "share": (float(r.get("prio") or 0) / total
                            if total else None)} for r in rows]}
