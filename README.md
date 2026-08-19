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
identity, the **`run_id`** that served it, and `N/4 won`.

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
