"""Price a Solana transaction from its raw bytes: the fee its payer pays and
the lamports it moves to a tip account. Standard library only.

This exists for orders that did NOT land. A landed order is priced from the
block (`accuracy.block_rewards`); one that never landed has no block record,
and our own store keeps only 16-byte SigPrefixes of what we offered -- but
the relay's packet log (`relayer.validator_packet_events.tx_body`) keeps the
bytes of everything that reached a served validator's TPU, which covers a
good share of the orders in the selection diff. Same arithmetic the builder
scores with (`bifrost-builder/src/order/packet.rs`: explicit_compute_budget,
static_tips), so the number here is what the builder saw, not an estimate
of what the chain would have charged.

Three wire formats, and they do not share a first byte:

  legacy  [compact-u16 n_sigs][n_sigs x 64B][header 3B][compact keys x 32B]
          [blockhash 32B][compact instructions]
  v0      same, but the message opens with 0x80 and ends with address-table
          lookups. Those keys are NOT in the packet, so a tip paid to a
          table-resolved destination is invisible here; the result says so.
  v1      starts with 0x81 (SIMD-0385): the compute budget is a config block,
          not instructions, and the priority fee is a plain lamport count.
          Layout from agave transaction-view/src/transaction_frame.rs and
          solana-message 4.2.3 versions/v1/config.rs:
            0x81 | header 3B | config mask u32 | blockhash 32B | n_ix u8 |
            n_addr u8 | addresses | config values | ix headers (4B each:
            program_id_index u8, num_accounts u8, data_len u16) | payloads
            (accounts then data, per instruction, contiguous) | signatures
          Config values follow the mask in bit order: priority fee (bits 0-1,
          u64), compute unit limit (bit 2, u32), loaded-accounts data size
          (bit 3, u32), heap size (bit 4, u32).
"""

import struct

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

SYSTEM_PROGRAM = "11111111111111111111111111111111"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"
SYSTEM_TRANSFER_TAG = 2            # u32 LE instruction discriminator
SET_COMPUTE_UNIT_LIMIT_TAG = 2     # u8
SET_COMPUTE_UNIT_PRICE_TAG = 3     # u8
LAMPORTS_PER_SIGNATURE = 5000
# Agave's default limit for a tx that sets none
# (compute-budget-instruction compute_budget_instruction_details.rs
# calculate_default_compute_unit_limit): 3,000 per instruction to a builtin
# program, 200,000 per any other instruction, capped at 1.4M. The builtins
# are builtins-default-costs NON_MIGRATING_BUILTINS_COSTS plus Vote, which
# only leaves the list under SIMD-0387 and has no bearing on fee-paying
# flow either way.
DEFAULT_INSTRUCTION_CU = 200_000
BUILTIN_INSTRUCTION_CU = 3_000
MAX_CU_LIMIT = 1_400_000
BUILTIN_PROGRAMS = frozenset((
    SYSTEM_PROGRAM, COMPUTE_BUDGET_PROGRAM,
    "BPFLoaderUpgradeab1e11111111111111111111111",
    "BPFLoader1111111111111111111111111111111111",
    "BPFLoader2111111111111111111111111111111111",
    "LoaderV411111111111111111111111111111111111",
    "KeccakSecp256k11111111111111111111111111111",
    "Ed25519SigVerify111111111111111111111111111",
    "Vote111111111111111111111111111111111111111"))


def default_cu_limit(instructions):
    """What Agave charges a tx that declares no compute-unit limit."""
    n_builtin = sum(1 for prog, _a, _d in instructions
                    if prog in BUILTIN_PROGRAMS)
    n_other = len(instructions) - n_builtin
    return min(MAX_CU_LIMIT, n_builtin * BUILTIN_INSTRUCTION_CU
               + n_other * DEFAULT_INSTRUCTION_CU)


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(b) - len(b.lstrip(b"\0"))
    return "1" * pad + out


class ParseError(ValueError):
    pass


class _R:
    """A cursor over the bytes; every read is bounds-checked."""

    def __init__(self, b):
        self.b, self.i = b, 0

    def take(self, n):
        if self.i + n > len(self.b):
            raise ParseError(f"short read at {self.i}+{n} of {len(self.b)}")
        out = self.b[self.i:self.i + n]
        self.i += n
        return out

    def u8(self):
        return self.take(1)[0]

    def u16(self):
        return struct.unpack("<H", self.take(2))[0]

    def u32(self):
        return struct.unpack("<I", self.take(4))[0]

    def u64(self):
        return struct.unpack("<Q", self.take(8))[0]

    def compact_u16(self):
        v, shift = 0, 0
        for _ in range(3):
            c = self.u8()
            v |= (c & 0x7f) << shift
            if c & 0x80 == 0:
                return v
            shift += 7
        raise ParseError("bad compact-u16")


def parse(raw):
    """-> dict(version, n_sigs, sig, keys, instructions, priority_fee,
    cu_limit, declared_cu, has_lookups). `instructions` is
    [(program_key, [account_idx], data)], account indexes referring to
    `keys` (static keys only). `sig` is the first signature -- the tx id.
    `cu_limit` is the explicit limit or None; `declared_cu` is that, or
    Agave's default when there is none."""
    if not raw:
        raise ParseError("empty")
    t = _parse_v1(raw) if raw[0] & 0x80 else _parse_legacy_v0(raw)
    t["declared_cu"] = (min(MAX_CU_LIMIT, t["cu_limit"])
                        if t["cu_limit"] is not None
                        else default_cu_limit(t["instructions"]))
    return t


def _parse_v1(raw):
    r = _R(raw)
    version = r.u8()
    if version != 0x81:
        raise ParseError(f"unknown transaction version byte {version:#x}")
    n_sigs, _ro_signed, _ro_unsigned = r.u8(), r.u8(), r.u8()
    mask = r.u32()
    r.take(32)                                   # lifetime specifier
    n_ix, n_addr = r.u8(), r.u8()
    keys = [b58encode(r.take(32)) for _ in range(n_addr)]
    priority_fee, cu_limit = 0, None
    if mask & 0b11 == 0b11:
        priority_fee = r.u64()
    elif mask & 0b11:
        raise ParseError("half-set priority fee bits")
    if mask & 0b100:
        cu_limit = r.u32()
    if mask & 0b1000:
        r.u32()                                  # loaded accounts data size
    if mask & 0b10000:
        r.u32()                                  # heap size
    if mask & ~0b11111:
        raise ParseError(f"unknown config bits {mask:#x}")
    heads = [(r.u8(), r.u8(), r.u16()) for _ in range(n_ix)]
    ixs = []
    for pidx, n_acc, dlen in heads:
        accts = list(r.take(n_acc))
        data = r.take(dlen)
        if pidx >= len(keys):
            raise ParseError("program index out of range")
        ixs.append((keys[pidx], accts, data))
    sigs = r.take(64 * n_sigs)
    if r.i != len(raw):
        raise ParseError(f"{len(raw) - r.i} trailing bytes")
    return {"version": "v1", "n_sigs": n_sigs,
            "sig": b58encode(sigs[:64]) if n_sigs else "", "keys": keys,
            "instructions": ixs, "priority_fee": priority_fee,
            "cu_limit": cu_limit, "has_lookups": False}


def _parse_legacy_v0(raw):
    r = _R(raw)
    n_sigs = r.compact_u16()
    sigs = r.take(64 * n_sigs)
    first = r.u8()
    if first & 0x80:
        if first & 0x7f != 0:
            raise ParseError(f"unknown message version {first & 0x7f}")
        version, n_req = "v0", r.u8()
    else:
        version, n_req = "legacy", first
    r.u8(), r.u8()                               # readonly counts
    n_keys = r.compact_u16()
    keys = [b58encode(r.take(32)) for _ in range(n_keys)]
    r.take(32)                                   # recent blockhash
    n_ix = r.compact_u16()
    ixs = []
    for _ in range(n_ix):
        pidx = r.u8()
        n_acc = r.compact_u16()
        accts = list(r.take(n_acc))
        dlen = r.compact_u16()
        data = r.take(dlen)
        if pidx >= len(keys):
            raise ParseError("program index out of range")
        ixs.append((keys[pidx], accts, data))
    has_lookups = False
    if version == "v0":
        n_lk = r.compact_u16()
        has_lookups = n_lk > 0
        for _ in range(n_lk):
            r.take(32)
            r.take(r.compact_u16())
            r.take(r.compact_u16())
    if r.i != len(raw):
        raise ParseError(f"{len(raw) - r.i} trailing bytes")
    # compute budget: the builder's explicit_compute_budget, plus Agave's
    # default limit so a price without a limit still prices
    cu_limit, cu_price = None, None
    for prog, _accts, data in ixs:
        if prog != COMPUTE_BUDGET_PROGRAM:
            continue
        if not data:
            continue
        if data[0] == SET_COMPUTE_UNIT_LIMIT_TAG and len(data) >= 5:
            cu_limit = struct.unpack("<I", data[1:5])[0]
        elif data[0] == SET_COMPUTE_UNIT_PRICE_TAG and len(data) >= 9:
            cu_price = struct.unpack("<Q", data[1:9])[0]
    priority_fee = 0
    if cu_price:
        limit = (min(MAX_CU_LIMIT, cu_limit) if cu_limit is not None
                 else default_cu_limit(ixs))
        priority_fee = -(-(limit * cu_price) // 1_000_000)     # ceil
    return {"version": version, "n_sigs": n_sigs,
            "sig": b58encode(sigs[:64]) if n_sigs else "", "keys": keys,
            "instructions": ixs, "priority_fee": priority_fee,
            "cu_limit": cu_limit, "has_lookups": has_lookups}


def price(raw, tip_accounts, jito_accounts=frozenset()):
    """-> {"fee", "tip", "jito", "builder", "version", "lookups", "cu",
    "cu_explicit", "sig"}. `jito`/`builder` split `tip` by whether the
    destination is in `jito_accounts` -- the relay weighs only the Jito
    part (by the connector's Jito weight).

    `fee` is what the payer pays: base fee per signature plus the priority
    fee -- the same quantity `getBlock` reports as meta.fee for a landed
    transaction, so the two columns are comparable. `tip` is the sum of
    System transfers to a tip account; `lookups` is true when the message
    resolves accounts through address tables, in which case a tip to one of
    those cannot be seen from the packet and `tip` is a lower bound."""
    t = parse(raw)
    tip = jito = 0
    for prog, accts, data in t["instructions"]:
        if prog != SYSTEM_PROGRAM or len(data) < 12 or len(accts) < 2:
            continue
        if struct.unpack("<I", data[:4])[0] != SYSTEM_TRANSFER_TAG:
            continue
        dest = accts[1]
        if dest < len(t["keys"]) and t["keys"][dest] in tip_accounts:
            amount = struct.unpack("<Q", data[4:12])[0]
            tip += amount
            if t["keys"][dest] in jito_accounts:
                jito += amount
    return {"fee": LAMPORTS_PER_SIGNATURE * t["n_sigs"] + t["priority_fee"],
            "tip": tip, "jito": jito, "builder": tip - jito,
            "version": t["version"], "lookups": t["has_lookups"],
            "cu": t["declared_cu"], "cu_explicit": t["cu_limit"] is not None,
            "sig": t["sig"]}
