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

Chips are labelled with the window's **slot range** (`440042240–43`), not just
its first slot, and a **go to slot** box in the header resolves any slot to its
window and selects it — a window chip labelled only `440027604` is unfindable
when the number you are holding is `440027606`.

Each chip carries the window's first slot, a **live-ticking age**, the leader
identity, the **`run_id`** that served it, and **how many rounds we won**.

**Rounds, not slots.** A slot is several auction rounds and each is won or lost
on its own, so `N/4 won` — slots we produced — hid the interesting half:
measured over seven days, the slots we won at all ranged from one round of eight
to all eight. Window 440354548 read as `2/4 won` when it was really **4 of 32
rounds**, one slot taking three and another taking one. So a window chip now
reads `4/32 rounds`, a slot chip `3/8 rounds`, and the slot count survives in
the chip's hover title and beside the window bar.

The denominator is the rounds the relay **echoed a winner for**, which is what
there was to win. It is scoped to slots we competed in, the same scope as the
rest of the strip — over 30 days that is 25,414 rounds, against 25,520 across
every slot seen; the 106-round difference is 18 slots we never offered into at
all. A slot that was contested but where the relay echoed no winner for any
round reads `no rounds echoed` rather than `0/0`.

A **run bar** above the strip lists every builder run in view, newest first,
and jumps to the window where it began. Each of those windows is marked with an
amber rule and a `run starts here` label. `run_id` changes on every restart, so
these are the deploy boundaries — with 25 runs across ~700 windows they are
otherwise invisible scrolling, and they are the first thing to check when a
metric shifts.

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

Each round row also carries a **reward badge**: our best offer against the
block that actually won, with the margin. Ours is the final cumulative offer,
never a sum — selected rows are prefixes of one another. A won round reads
`won with X · offered up to Y` instead, because there the second number is our
*own* accepted block and a gap means we kept bidding after the round was
already decided.

Each round also shows **applied / dropped** — orders the round's extends
applied against what they were handed, both summed from `sim-extend`, so the
attribution is exact. Neither number sees extends that were *refused*: a
throttled request never reaches the worker and emits no datapoint, so the real
offered load was higher.

When a round had non-SUCCESS extends, a red **error dropdown** appears between
the row and its detail, listing each status with a count. Per-order drop
reasons are not available per round — `check_dropped` and `dropped` in
`bifrost_events` carry `index = 0` on every row, so they are slot-scoped — and
only the per-call status can be attributed to a round.

Rounds that had to replay also carry a **replay badge** — `replay 34.7 ms ·
203 orders`. It is commit **N−1** on row N, the same attribution the comparison
tab uses: commit N applies round N's winner and round N+1 builds on it, so the
cost gating a round is the previous round's commit, and round 0 has none. It
appears only when that commit was a real replay, which is exactly when the
previous round was lost — a promote is a pointer move and says nothing.

That means the badge can sit on a round marked `won` (because the round before
it was lost) and be absent from one marked `lost` (because the round before it
was won). It answers "what did this round have to replay before it could
start", not "what did losing this round cost".

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

### 4. Logs tab

warn / error / fatal from ClickStack `otel_logs` for this slot, in order, amber
for warn and red for error, with fatal darker still. Info-level status
heartbeats are excluded — they are ~99% of the volume.

Attribution is exact where it can be: most error rows carry
`LogAttributes['slot']`, so they are matched on the slot itself with no clock
involved. Lines without it — ALT refresh failures, teardowns — are picked up by
time window and **marked as time-matched**, because they are often the ones
that explain the round and dropping them would hide the cause. otel's clock
agrees with InfluxDB (checked on slot 440027607: tagged errors 09:00:30.087–.400
against sim-extend 09:00:30.075–.400), so the window is taken from InfluxDB.

Runs of the same message are collapsed with a count and a span: one ALT failure
repeated 146 times in a third of a second is one fact, and listing it 146 times
buries the line that matters.

A slot with no miniblock rows says so rather than reporting "not configured" —
that case means the builder was down for it, which is when you want logs most.

### 5. Comparison tab


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

A strip above the table carries what the slot was built on: parent slot,
its completion and freeze, when our context installed, and the gap between the
two — dead time before we can offer anything (17.7 ms on the slot above).

Vote and non-vote are counted **from the payload**, not the chain. A `selected`
payload carries whole transactions, so the vote program id appears verbatim in
the account keys of each vote and `countSubstrings` classifies every offered
transaction — including ones that never landed, which a chain lookup cannot
classify at all. Votes are a real and dominant part of what we bid: 382 of 569
transactions in one round's final offer.

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

**Builder names are never configured.** The relay's `round_chosen` names
whoever actually won — us or anyone else — so a builder added later appears on
its own with no change here. Without a relay the winner can only be reported as
ours / not ours, and the cell says which of "no relay configured" or "no winner
echo from relay" applies rather than inventing a label.

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

### 6. Mutation lane, per round

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

### 7. Program cache, per round

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

### 8. Shred path — leader vs our simulator

The **parent** slot's completion and bank freeze, with the sim-minus-leader
delta. The parent, not this slot: a context cannot install until the parent's
bank is frozen, so that is what gates the auction. This slot's own completion
lands *after* its auction has finished — measured on 439391881 the last commit
was at +368.6 ms and the slot did not complete until +414.8 ms — so it cannot
have gated anything.

The grid carries four milestones, ordered by the clock rather than by which
store they came from, so the row order is the sequence:

| milestone | source | writers |
|---|---|---|
| `warmup` | `bifrost_events` | builder only |
| `sequencing` | `bifrost_events` | builder only |
| `bank ready` | `bifrost_events` | builder only |
| `slot complete` | InfluxDB `shred_insert_is_full` | every node that saw the slot |
| `bank frozen` | InfluxDB `bank_frozen` | every node that saw the slot |

The bottom two are the comparable ones: the leader and our simulator are two
independent observations of the same event, so the delta between them means
something. The top three come from our own builder and have **exactly one
writer** — no validator emits them — so the leader column is `—` and there is no
delta. The detail cell says "builder only" on each, because an empty cell must
not read as a gap in someone else's reporting.

`warmup` and `sequencing` are the connector's `leader_state`, which walks
Inactive → Warmup → Sequencing → Cooldown(slot) with one row per slot per
identity. Which of the two appears on the parent depends on where the parent
falls: at a window start the parent is outside the window and only reached
Warmup, mid-window it is being sequenced and has a `bank ready` of its own.

**The two stores are on different clocks.** `bifrost_events` is ClickHouse and
the shred stages are InfluxDB, and the two have been measured 899 ms apart on
one slot and within 5 ms on another. The builder rows are therefore shifted onto
the InfluxDB clock using the offset measured on `round_committed`, which both
stores record, and the header states the shift and its spread. If no round of
the slot was committed in both stores the offset cannot be measured, and the
header says so rather than silently ordering rows that cannot be ordered.

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

### The `dag` tab

The conflict graph the simulator executes, and how it spreads over the worker
lanes. Pick a round, then whether to graph **our offer** or **the winner**.

**Our own offer is read, not rebuilt.** A `selected` payload is a wincode
`WireMiniblock`, and it carries the builder's dependency graph in CSR form:
`row_offsets`/`col_indices` for the edges, and a node list with each node's
remaining-path `depth`. So for our own offers nothing is inferred — the panel
decodes the payload and draws what the builder planned.

This replaced a rebuild that was **wrong**. The old version took its order from
the row's `signatures` array, which is *not* wire order: `miniblock_recording.rs`
builds it by walking `orders.txs` and then every bundle's txs, i.e. the order the
BYTES were supplied in, with all bundle members appended at the end. A conflict
DAG is sequence-dependent — last-writer and readers-since-write both are — so
the wrong sequence produced the wrong edges. Only `graph.nodes` has the real
order. On slot 440266475 round 0 the difference is not subtle: the rebuild
claimed 74 orders with a critical path of 47, the recorded graph has **453 nodes,
441 edges and a critical path of 52**.

The decode is checked against every `selected` row of a slot: node count equals
the `order_count` column, `row_offsets` is nodes+1 long, uuid/slot/index/reward
match their columns, tx and bundle counts match theirs, every edge points
forward, `depth` reproduces as the remaining-path height, and the payload is
consumed to the last byte. Only `payload_version` 0 is decoded; anything else is
refused rather than guessed at.

Two things fall out of reading the payload instead of the block. Bundles are no
longer opaque — their member transactions are in the payload — and no RPC call
is needed at all, so orders that never landed on chain are still in the graph.

Votes stay in as nodes, because dropping one would renumber the CSR. They are
counted above the graph, and since a vote touches only its own vote account
every one of them starts unblocked with no edges — **only orders with a
dependency** hides them (372 of 453 on that round).

### A winner row's own counts are not usable

`transaction_count` and `bundle_count` are written from `BuilderOriginatedOrders`,
which holds only the orders the **sending** builder originated. A foreign winner
attaches none, so both columns read 0 — or, when it attaches some, a number with
no relation to the block it won. Measured on one slot the columns said 3 txs and
2 bundles for a round whose payload carries 228 and 33, and 0/0 for the five
rounds after it.

So for winner rows the columns are ignored and the counts come from the payload's
order refs, which sum to `order_count` exactly. Votes cannot come from a winner
payload at all — a tx ref is a bare SigPrefix with no bytes, so there is nothing
to find the vote program id in — and are classified against the block instead,
which is why a winner's vote split reads as a dash in the offers table and is
resolved in the comparison tab.

### The winner's graph is rebuilt

A `WireChosenMiniblock` carries a flat `Vec<NodeOrderRef>` and no CSR, so the
winner's edges have to be recomputed. That is where the reconstruction, and the
`sim-commit` scoring below, still apply.

The graph is REBUILT, not fetched. `sim-extend` and `sim-commit` record only the
DAG's shape — `critical_path`, `initial_width`, `exec_pool` — never its nodes or
edges, so the panel ports `build_stream` out of
`simulation-service/src/replay.rs`: read-after-write and write-after-write
edges through the last writer of each account, write-after-read edges through
the readers since that write. Account keys come from the block with the
address-lookup tables already resolved, so the read and write sets are the ones
the runtime used rather than a reconstruction.

The two sources use the two merge modes the simulator itself uses. Winner replay
coalesces linear runs into one chain, capped at 64 transactions; the extend path
keeps every order its own chain, because the round-budget verdict can refuse an
order that a merged chain's worker would already have carried forward.

**The rebuild is scored, not asserted.** On a lost round, `sim-commit` holds the
critical path and initial width the simulator measured replaying that same
winning miniblock, and the panel prints them next to the rebuilt figures. On the
cleanest round measured — slot 440267808 round 0, two refs unresolved — the
rebuild produced a critical path of 55 against a recorded 55, and an initial
width of 32 against 33. Where the two diverge, the unresolved count above the
graph is the reason. On a round we WON there is no comparison at all: the commit
promotes our own block instead of replaying, and the promote path never builds a
DAG.

What the graph cannot show, and says so on the page:

- **Foreign bundles.** A bundle arrives as an opaque 32-byte id; its members are
  only knowable if we received the bundle too, and the builder that won a round
  usually has bundles we never saw. In the winner graph those orders are absent
  entirely. In our own offer they are not lost — the transactions are in the
  row's `signatures` either way — but they appear as separate orders instead of
  one, so the grouping is wrong even though the edges are real.
- **Votes** are excluded. On the replay path that matches the simulator exactly:
  `replayed` is the winner's order count minus the votes billed to `votes_cu`,
  which is how the two numbers reconcile. On the extend path it is a choice — a
  vote touches only its own vote account, so it adds width and no depth.
- **The x axis is model time in compute units, not wall time.** A chain costs
  the sum of its orders' measured CU, from our own `executed` events, and the
  lanes run the simulator's dispatch order: deepest remaining path first, which
  is what `replay.rs` pops off its ready heap. Orders with no `executed` event
  fall back to a flat 5,000 CU and are counted on the page.

### Per extend

Below the graph, one row per accepted extend with the DAG shape the simulator
measured for that call — critical path, how many orders started unblocked, and
the running prefix.

This is a table rather than a graph on purpose. The simulator records the shape
of each call but never which orders were in it, batches are capped at sixteen,
and nothing in ClickHouse or InfluxDB ties those sixteen back to identities, so
a per-extend graph would have to invent its own membership. The round figures
above and these rows are NOT comparable: one is the round's whole order list at
once, the other is the same work cut into batches. Refused extends are missing
entirely — a throttled call never reaches the worker and emits no point.

## Range analysis — `/analysis`

The slot explorer answers "what happened in this slot". This answers "what has
been happening", over a date range, for one connector identity. It is a port of
`bcj_report.py`, which wanted its own credentials and printed to a terminal.

Inputs: a range (`-24h`, `-7d`, `2026-08-21`, or a full timestamp), a reward-gap
threshold, and a connector — the dropdown lists the identities that actually
appear in the range, and `ANALYSIS_IDENTITY` sets which one it opens on.

1. **Winner landed it, we never offered it.** Winner refs minus every ref we
   offered in that round, at any rung — the union over the whole ladder, since a
   later rung can drop an order an earlier one carried. Expand a slot for its
   rounds, expand a round for the actual signatures and bundle ids.
2. **Commit-time replay in the simulator.** Percentiles for every stage of the
   commit, plus the shape of the replayed batch. Each **max** links to the slot
   and round it came from.
3. **Rounds at least N% below the winner**, with the whole-range gap
   distribution beside the flagged rows.
4. **Leader Sequencing → simulator context install**, raw and clock-corrected.
5. **Reward per miniblock** — our best offer against the winning bid, one point
   per round across every slot in the range.

The chart's y axis is **logarithmic**: rewards span four orders of magnitude in a
day and a linear axis flattens nine rounds in ten against the floor. A round we
never offered into **breaks** our line rather than plotting a zero — nothing was
bid, and a dot on the floor reads as a near miss; those rounds get a tick on the
axis and a count instead. The band between the lines is the gap. Hovering gives
the slot, round, both rewards and the gap, and the same numbers are in a table
below the chart, so no value is reachable only by hover.

The two hues are slots 1 and 2 of the categorical order, validated against this
page's own surface rather than eyeballed:
`validate_palette.js "#3987e5,#d95926" --mode dark --surface #0c141b --pairs all`
— lightness band, chroma, CVD ΔE 26.8, normal-vision ΔE 31.8 and contrast all
pass.

**The two stores are on different clocks**, so report 4 corrects for it. The
skew is bounded per leader window as the minimum of `round_committed` minus
`sim-commit` across that window's rounds: that difference is return latency plus
skew, latency is never negative, and some round always returns fast, so the
minimum bounds the skew tightly from above. Per window and not once for the
range — the hosts drift tens of milliseconds over a day and get pulled back by
NTP.

Two things about cost, because this page reads a lot more than a slot page does:

- The range is capped at 7 days, and every query is bounded by it.
- A run is cached for 15 minutes, keyed on the range and the connector but
  **not** the threshold — only report 3 depends on that, and it costs half a
  second against ten for the rest, so moving the threshold re-runs only what it
  affects. An open-ended range is quantised to the minute, or the key would
  never repeat.

Report 1's vote classification is the expensive part and is bounded per leader
window rather than across the range: a winner tx ref carries no bytes, so a vote
is only identifiable by having reached our own ingest with `route=vote`, and
asking that of a whole day scans every receive in it. Per window it is about a
second, and the windows go out together — measured 12s down to about 4s.

### The `offers` tab

Every builder's bidding for one slot, as the relay saw it. Our own tables record
what *we* sent and nothing else, so the relay is the only place the shape of an
auction exists.

One timeline per round, because a round is the auction. **Time runs down** the
spine in milliseconds since the relay opened the round, each builder has its own
column beside it — two builders bidding in the same millisecond sit side by side
rather than on top of each other — and every bullet is one offer. Hovering gives
the builder, the reward and the order count; the whole thing is also a table
below.

`round opened` and the `deadline` are flags across the full width, because they
are moments in the round rather than one builder's doing. The winner is chosen
**at** the deadline — measured across every round of 441348747, `chosen` minus
`deadline` was 0 ms every time — so those two share one rule, labelled with the
outcome and the winning miniblock's uuid.

**The timelines zoom.** At the fitted height a round is about 7 px per
millisecond, so an offer that missed the deadline by 1 ms sits 7 px from it —
visible, but not readable. `fit / 2× / 4× / 8×` stretches the *time* axis only,
leaving the builder columns where they are; at 8× that same gap is 56 px. There
is a control per round and one at the top that drives all of them, because
chasing the same button down eight rounds is the annoying part.

A bullet is sized by the reward it carried relative to the best bid of that
round, so a ladder climbing toward the deadline is visible without reading a
number. The ringed bullet is the offer the relay chose; a faded one was
rejected; **an offer that arrived after the deadline is ringed amber rather than
faded** — it is work that was done and then missed the window, which is worth
seeing rather than hiding. On that slot two of them were ours, 1 ms late,
rejected `index_mismatch`, one of them carrying 8,388,721 against a winning
8,490,273.

Colour is four hues and no more:
`validate_palette.js "#3987e5,#c98500,#d55181,#008300" --mode dark --surface #0c141b --pairs all`
— the all-pairs list rather than the adjacent one, because any two markers can
end up side by side in a 45 ms column. That passes with CVD ΔE 6.9, which the
validator allows **only** with secondary encoding, so **marker shape is a full
identity channel**: every builder has its own, whether or not it got a hue. Five
hues do not pass at all, and a slot routinely carries six builders — measured on
441348747, six, of which the one that won every round would otherwise have
folded into the same grey circle as a fallback nobody cares about.

## More than one builder

We run more than one — Amsterdam and Tokyo — and they are separate in every
store: different `local_builder_id` in `bifrost_miniblocks`, different
`instance_id` in `bifrost_events`, a different simulator `host_id` in InfluxDB,
and they never serve the same validator, so they never compete for a slot.

A **deployment** is that whole set under one name, and a selector in the header
switches between them. The choice is remembered in a cookie so ordinary links
need not carry it; `?dep=<name>` overrides the cookie, so a link stays
shareable. With `DEPLOYMENTS` unset the previous single-deployment variables are
used as one unnamed deployment and no selector appears.

Two things this had to get right:

- **The active deployment is per request, not per process.** The server is
  threaded, so a module-level "current builder" would be read by one request
  while another wrote it. It lives in a `threading.local`, set once at the top
  of the handler. Background work — the window prefetch, the six-way per-slot
  fan-out, the analysis vote lookup — runs on pool threads that do **not**
  inherit it, so each captures the name and re-sets it inside the worker.
  Without that the prefetch would fetch Tokyo's slots against Amsterdam's
  builder id and cache the empty result under Tokyo's key.
- **Every cache is keyed by deployment.** Slot data, windows, health, the
  reference page and analysis runs are all per builder; a bare slot number is
  not unique across them.

### Which connectors actually grade us

A validator either considers our offers or rejects every one with
`builder_not_eligible`, and **that verdict exists only in the relay's table** —
`bifrost_miniblocks` records what we sent, never what was made of it. So a slot
on an ineligible connector looks entirely normal in our own data right up to the
point where it is never won.

The strip therefore carries an **all connectors / grading us** filter, and a
window whose connector rejects everything is marked so an unwinnable slot is not
read as one we lost narrowly. Unknown connectors are kept: absence of evidence
is not evidence of ineligibility. Without relay credentials the filter says
`connector grading unknown` rather than silently showing everything as fine.

**This is not the same as a mock builder.** A shadow builder id submits
alongside ours on every validator and is rejected on all of them, so it cannot
be used to tell mock validators from real ones — and it writes nothing to
`bifrost_miniblocks` or `bifrost_events` at all, so there is no dashboard view
"of" it to switch to. What *is* selectable is whether the connector grades us,
which is the question the mock/non-mock distinction usually stands in for.

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

### Moving inside a window costs nothing

Within one leader window the header, the 30-day strip and the window bar are
identical for all four slots and all five tabs — only the panel below them
changes. That panel is the small half of the page:

| tab | whole page | panel alone |
|---|---|---|
| rounds | 782 KB | 41 KB |
| comparison | 748 KB | 7 KB |
| timeline | 756 KB | 15 KB |
| dag | 772 KB | 32 KB |
| logs | 742 KB | 0.8 KB |

So a click on a slot chip or a tab fetches `?partial=1`, which renders the panel
and nothing else, and swaps it into `#view`. Once the page is idle the remaining
nineteen combinations — every tab of every slot in the window — are pulled two
at a time in the background. After that, moving anywhere inside the window
issues **zero** network requests and does not reload.

Three details that make it behave rather than merely work:

- **Scripts are re-run on swap.** `innerHTML` never executes a `<script>`, and
  the dag panel ships one, so the swapped subtree's scripts are re-created as
  fresh elements. Most other behaviour — tooltips, copy buttons, expanding a
  round — is on delegated document-level listeners and survives untouched. The
  live age ticker re-scans instead of holding a captured node list, which would
  otherwise keep ticking elements no longer on screen.
- **Slot identity comes from `data-slot`, not the href.** Chip hrefs are
  rendered server-side and the selected chip's own href deliberately omits
  `slot`; after a swap that stale href would navigate to "no slot selected".
- **The current tab carries across a slot switch.** A slot chip is rendered
  without a tab, so following it verbatim would drop you from `dag` back to
  `rounds` on every step. The point is moving freely in both directions.

The links stay real hrefs and the server still returns the whole page for any of
them, so with JavaScript off — or if a fetch fails, where the handler falls back
to a normal navigation — this degrades to exactly what it did before.

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
