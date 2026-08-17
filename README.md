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

### 0. Health header

Six independent cells across the top. The load-bearing idea: **a process being
up is not the same as it building.** A status heartbeat keeps ticking on a
stalled builder, so liveness is judged on the **work stream** — `sim-extend`,
`sim-commit`, `sim-context`, `sim-probe` counts over 20 minutes — not on any
status message.

| cell | source | reads |
|---|---|---|
| building | InfluxDB | work-stream counts; idle when extend+commit are 0 |
| connector feed | otel | `live_slot > 0`, not `active > 0` |
| topology | otel | connectors/relays, amber if it flaps in 10m |
| scheduler | otel | pool **peak** over 15m, plus the latest sample |
| event ingest | ClickHouse | this instance's lag, against the best peer |
| last restart | ClickHouse | most recent local-sim (127.0.0.1) reconnect |

Three of these are subtle enough to be worth spelling out:

- **`active > 0` is not the health signal.** An idle connector still reports the
  live chain-head slot, and `active` only rises when a served validator is
  actually leading — so `active = 0` is normal off-window. `live_slot = 0`
  across identities is the real "feed dead" signal.
- **The connector query must pin `Body='connector status'`.** The
  `builder network status` rows carry no `leader_state`/`slot` attributes, so
  mixing them in counts absent data as inactive and makes a healthy feed look
  dead.
- **Scheduler pool is judged on its peak, not its latest sample.** It oscillates
  on the ~10s log cadence (1,376 → 0 within a minute is normal), so a single
  sample flaps between "busy" and "nothing to build". Only a peak of zero across
  the whole window means there is genuinely nothing to build.

Each cell degrades on its own: a dead otel cluster leaves the InfluxDB and
ClickHouse cells intact, and vice versa. Cached 30s.

### 1. Leader windows

A horizontally scrolling strip, newest on the left. A Solana leader owns four
consecutive slots, so slots are folded into their window and keyed by the
window's first slot — four rows in the strip were always four views of one
assignment.

Each chip carries the window's first slot, a **live-ticking age**, the leader
identity, the **`run_id`** that served it, and `N/4 won`.

`run_id` is one builder **process lifetime**, so it changes on every restart —
putting it on the strip makes deploy boundaries visible while scanning, rather
than only after opening a slot. A window served within ten minutes of a restart
gets a `cold` pill, which explains itself on hover or keyboard focus with that
window's own numbers — it ran against a cold program cache and a cold account
overlay, so its timings are not comparable to a window served hours into the
same run. Hovering gives the full UUID, when the run started, how many slots it
covers, and how far into it this window fell. A window spanning a restart is
marked with a warning rather than silently showing one of its two runs. Scope is every slot you *competed* in — at least one
`kind='selected'` offer — not only the ones you produced. That distinction
matters: winning tends to be concentrated on a small number of connector
identities, so a won-only list can show a single leader and hide every other
one you ever bid into.

Ages keep counting after render. The page carries the server's clock and the
browser holds the difference as a fixed offset, so a machine whose clock is off
still shows ages consistent with the server rather than its own drift.

### 2. Auction rounds

Click a window to expand its slots; click a slot for its rounds. Each round
collapses to one line — offer count, won / not ours / no winner echo, `is_last`,
and an extend badge — and expands to the detail.

The winner miniblock's last column is **won by**, carrying the actual builder
id rather than a bare YES/-. Knowing it was not us is rarely the question; who
took it is. The name comes from the relay when configured, and falls back to
"not ours". The uuid is shown in full and is copyable, since it joins onward to
`bifrost_events` for the orders inside that block.

### 3. Timeline tab

The whole causal chain for a slot, in order — starting on the **parent slot**,
because that is what actually gates us: a context cannot install until the
parent's bank is frozen.

```
+0.0 ms     parent · slot complete every shred in blockstore insert took 342 ms
+4.5 ms     parent · bank frozen
+22.2 ms    context installed      the parent is frozen      built on parent 439391880
+1.0 ms     round 0 extends        20 accepted over 61 ms    p50 1.1 ms, 185/186 applied
+100.4 ms   round 0 commit         replay, 30.6 ms           replay 183 orders
+101.5 ms   round 1 extends        8 accepted over 27 ms     p50 1.8 ms
...
+392.5 ms   slot complete          every shred in blockstore insert took 343 ms
+397.0 ms   bank frozen
```

**Builder events are aligned, not assumed.** `bifrost_events` is ClickHouse, a
different clock from InfluxDB — measured 899 ms apart on one slot and within
5 ms on another. But `round_committed` is recorded on *both* sides, so the shift
is measured per slot from those pairs and applied. It is a genuine clock offset
rather than latency: across a slot's rounds the per-round differences agree to
**0.1 ms** while sitting 899 ms from zero — parallel, just displaced. The header
states the offset, how many pairs it came from, and the spread, so the alignment
is auditable. With no anchor the builder events are dropped rather than guessed.

Relay timestamps stay out entirely — no shared event to anchor them.

Extends are collapsed per round: twenty extends in a round is one burst, not
twenty things to read. The shape it exposes is the round cadence — extend
burst, commit, extend burst, commit — with the auction finishing well before
the slot completes.

### 4. Comparison tab


A per-slot tab beside the rounds view: what won each round, what we offered,
and what the lane spent.

| column | meaning |
|---|---|
| winner | from the relay's `round_chosen`, which names the actual builder, plus the winning **miniblock uuid** |
| their reward | **their accepted block when they won; their best losing offer when we won** |
| ours | **the final cumulative offer**, not a sum — see below |
| margin | ours vs theirs, in percent |
| orders / CU (them/us) | winner row vs our final offer |
| commit replay | `sim-commit body_us` — on a lost round this is the cost of replaying the foreign winner before we can build on it |
| extends n / p50 / max | accepted extends for that round |

**Ours is the final offer, not the sum of the selected rows.** Those rows are
cumulative prefixes — each is a superset of the last — so summing double-counts
by 2–6×. Verified on rounds we won, where the winner row *is* our own
composition: `winner.order_count` equals the max of selected (128=128, 208=208,
278=278, 360=360) and never the sum (309, 569, 1246, 1990). Reward behaves the
same way.

The uuid under each winner is the winning miniblock's own id. It is on our
winner row regardless of whether the relay is configured, and it is the *same*
uuid the relay puts on its `round_winner` event — so it joins the two stores
for a single round, and joins onward to `bifrost_events` for the orders that
made up that block.

**Their side comes from the relay** when `RELAY_URL` is configured. The relay
sees every builder's submissions and its `round_chosen` event names the winner
— something our own data structurally cannot do, since `won_by_us=false` only
ever means "not us". Which of their numbers is shown depends on the result:
their accepted block when they won, their best `submitted` offer when they
lost. The relay also reports its **verdict on our offers**, so a round where
ours were rejected says so and why.

Without the relay it falls back to our own winner rows and the generic
`OPPONENT_NAME` label, which is named by exclusion rather than identified.
`commit replay` and the extend columns are ours only; there is no counterpart
on their side.

### 5. Mutation lane, per round

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

### 6. Program cache, per round

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

### 7. Shred path — leader vs our simulator

Per slot, when each side saw it: first shred received, slot complete and
bank frozen, with the sim-minus-leader delta.

**`first shred received` is derived, not read from a column.**
`retransmit-first-shred` looks like the obvious source, but our simulator host
never writes it — it does not run the retransmit path — so that row was
permanently blank on the host we most want to measure. Instead:

```
first_shred = shred_insert_is_full.time − total_time_ms
```

because `total_time_ms` is the completion time measured from that node's *own*
first shred. Checked against the real retransmit stamp where both exist: within
0.2–9.5 ms on a 4.2.0 host, but ~55 ms early on a 4.1.0 one — so they are **not
the same instant**, and the raw retransmit stamp is shown as its own separate
row rather than conflated. Using the derived value on every host at least keeps
the comparison on one basis. Positive (we were later) is amber,
negative teal. `total_time_ms`, recovered and repaired shred counts ride along.

Requires `SHRED_LEADER_ID` and `SHRED_SIM_ID`. `SHRED_LEADER_ID` is
comma-separated in preference order, because leader identities can be
**complementary rather than redundant**: measured over 30 days, one covers
~96% of contested slots but none of the 20 we won, while the other covers only
the ~800-slot stretches around our own leader windows. The panel picks whichever
reported the slot and names the identity it used. A stage row appears only if at
least one side reported it. When the leader has no rows the column shows `—`
with a note that it is a **reporting gap, not a slow node** — a leader's metric
submission can be intermittent while peers report continuously, and an empty
cell must not read as a measurement.

## Metrics reference — `/reference`

A page documenting every metric: what it measures, its source field, the amber
and red cutoffs, and **the basis for each cutoff**, alongside this deployment's
own p50/p90/p99 over the last 24 hours so a threshold can be judged against the
distribution it is supposed to describe.

Every health cell in the header also carries an **i** affordance, opening on
hover or keyboard focus, with the same meaning / healthy / amber / red / gotcha
text. Both are rendered from one `METRIC_DOCS` table, so the header and this
page cannot drift apart.

Cutoffs are tagged by provenance, because they are not equally trustworthy:

| tag | meaning |
|---|---|
| `from code` | forced by the source — a slot is 400 ms, the replay pool is 8 |
| `measured` | taken from this deployment's own distribution |
| `PROVISIONAL` | an inference, **not** validated against an SLO or an incident |

Most are provisional. The page says so at the top rather than presenting
inferred numbers as agreed limits.

The one most in need of a second opinion is **per-order commit cost**. The ops
runbook quotes 111–149 µs/order as healthy; on this deployment p50 sits at
146 µs — right on the upper edge — and only **32%** of commits fall inside the
band at all. Either it is a p50 target we barely meet, or it is stale.

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

Opening a slot, or merely expanding a leader window, warms the window's other
slots in the background. A window is the unit people actually inspect — you
open one slot and then walk the other three — so those clicks become cache hits
(measured: 0.91 s cold, then 4–7 ms for each sibling).

Fetches are **single-flight**: the first caller does the work and the rest wait
on it. Without that the ordinary path double-fetches, because expanding a
window starts a prefetch and clicking a slot a moment later starts a second
fetch for the same slot. Measured with four concurrent requests for one cold
slot: 2 ClickHouse + 3 InfluxDB queries, not 8 + 12.

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
