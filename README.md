# bb-metrics-dashboard

A slot explorer for a Solana block builder. Start from the leader windows you
competed in, drill into a slot's auction rounds, and see what the simulator's
mutation lane and the validator's shred path were doing underneath.

Standard library only. No `pip install`, no CDN, no build step, no framework.

```bash
cp .env.example .env         # fill in hosts + credentials
./run.sh                     # binds 0.0.0.0:8899 - see Network below
./run.sh --port 9000
BIND_HOST=127.0.0.1 ./run.sh # loopback only
```

## What it shows

### 1. Leader windows

A horizontally scrolling strip, newest on the left. A Solana leader owns four
consecutive slots, so slots are folded into their window and keyed by the
window's first slot — four rows in the strip were always four views of one
assignment.

Each chip carries the window's first slot, a **live-ticking age**, the leader
identity, and `N/4 won`. Scope is every slot you *competed* in — at least one
`kind='selected'` offer — not only the ones you produced. That distinction
matters: winning tends to be concentrated on a small number of connector
identities, so a won-only list can show a single leader and hide every other
one you ever bid into.

Ages keep counting after render. The page carries the server's clock and the
browser holds the difference as a fixed offset, so a machine whose clock is off
still shows ages consistent with the server rather than its own drift.

### 2. Builder run

`run_id` is one builder **process lifetime**, so it changes on every restart. It
is constant within a (slot, instance) pair, but a slot can carry two runs when
two instances were live at once — so the panel renders a row per run.

The column that earns its place is **how far into the run this slot fell**. A
slot served shortly after a restart ran against a cold program cache and a cold
account overlay; its extend timings are not comparable to one served hours in,
and without this the panels below would look anomalous for no visible reason.
Under ten minutes raises a `cold start` pill.

Also carried: the run's instance, start time, total slots and wins, its slot
span, and this slot's row count and `seq_id` range (`seq_id` is global across
the run, not per slot, so gaps are visible).

### 3. Auction rounds

Click a window to expand its slots; click a slot for its rounds. Each round
collapses to one line — offer count, won / not ours / no winner echo, `is_last`,
and an extend badge — and expands to the detail.

### 4. Mutation lane, per round

`sim-extend` points bucketed by round. Attribution is exact rather than a
timestamp join: the simulator emits `("index", round.index_in_slot)` on every
point.

Three different clocks are in play, so all three are reported:

| | |
|---|---|
| **body** | the extend itself — sanitize, check, DAG execute, overlay commit |
| **queue** | wait before that, on the single mutation lane shared with commit |
| **wall** | the round's span, `last.time − (first.time − first.body_us)` |

A point is written *after* the body finishes, so its timestamp is the extend's
**completion, not its start** — hence the subtraction. `busy%` is body ÷ wall:
how much of the round the lane was actually working. Body and queue are
per-call sums; wall is measured once and is deliberately not their sum, because
gaps where the relay is thinking or a commit holds the lane fall inside wall
but inside no call.

**Refused extends emit no datapoint at all.** A throttled request is rejected
before the worker runs, so the count is *accepted* calls only and a round's
true offered load is not visible here. A round with zero accepted extends is
rendered in red and says so explicitly, rather than showing a blank.

### 5. Program cache, per round

The seven `program_cache_*` fields ride on the same `sim-extend` point as
`index`, so they are round-attributed structurally — no timestamp matching.

Two kinds of number, deliberately not one row of sums:

- **Costs** — `program_cache_us`, `compile_us`, `clone_us` — are per extend and
  add up across the round. Like the other stage timings they accumulate across
  replay workers, so they are CPU time and can exceed wall clock. Read them
  against `exec sum`; never subtract them from `body`.
- **Sizes** — `entries`, `entries_cloned` — are snapshots taken *inside* one
  extend (cache length at fork time, and at end of batch), so summing them
  would be meaningless. Reported as maxima.

The cache fork is copy-on-write and fires only when an admitted order actually
**modifies** a program — a deploy or upgrade landing in the batch. So `forks`
and `clone us` are 0 on almost every round; a non-zero value is the signal, not
the baseline. Compiles and forks raise a pill in the header and turn their
cells amber.

### 6. Shred path — leader vs our simulator

Per slot, when each side saw it: slot complete, bank frozen, optimistic
confirmed, with the sim-minus-leader delta. Positive (we were later) is amber,
negative teal. `total_time_ms`, recovered and repaired shred counts ride along.

Requires `SHRED_LEADER_ID` and `SHRED_SIM_ID`. A stage row appears only if at
least one side reported it. When the leader has no rows the column shows `—`
with a note that it is a **reporting gap, not a slow node** — a leader's metric
submission can be intermittent while peers report continuously, and an empty
cell must not read as a measurement.

## Things that will bite you

- **Join on `slot`, never on time.** ClickHouse and InfluxDB normally agree to
  within tens of milliseconds, but an 11.5 s divergence has been observed, with
  three independent Influx hosts agreeing against ClickHouse. Every cross-store
  query here pins the slot; time is only ever used to bound a scan, with a
  150 s margin so a skew cannot silently produce "0 extends".
- **`host_id` is the validator identity, and many nodes write the same
  measurement names.** Anything unbound blends machines. `SHRED_EXCLUDE_IDS`
  exists because a node on a different chain will happily write `sim-*` at a
  slot height millions away.
- **`slot` and `index` are fields, not tags**, so InfluxQL cannot `GROUP BY`
  them and an unbounded predicate walks the entire retention window.
- **Some measurements are flushed long after the event.** Their row timestamp
  is meaningless; use the embedded epoch field instead.

## Performance

A slot costs three queries — two ClickHouse, one InfluxDB, the slow pair run
concurrently — and they are
cached for five minutes (64 slots, oldest evicted). Neither query was ever
per-round; both return the whole slot and are bucketed client-side. Clicking
between rounds of a cached slot is ~5 ms and issues no query at all. A failed
fetch is not cached, so a transient outage retries instead of pinning an error.

## Configuration

Everything is environment-driven; there are no credentials, hostnames, or
validator identities in the source. See `.env.example`.

## Network

The datastores are typically reachable on a private network only. The dashboard
must run somewhere that can reach them — it does not proxy or tunnel on your
behalf.

`run.sh` binds **`0.0.0.0`**, so the page is reachable from other machines. The
dashboard has **no authentication** and will serve anything the configured
credentials can read, so expose it on a private interface (a tailnet, a VPN),
not a public one. To narrow it:

```bash
BIND_HOST=127.0.0.1 ./run.sh     # loopback only
./run.sh --host 100.x.y.z        # a specific interface; --host wins over BIND_HOST
```

Running `python3 app.py` directly still defaults to `127.0.0.1`.
