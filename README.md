# bb-metrics-dashboard

A single-file dashboard for the stateful simulator's mutation lane — the
`sim-extend` and `sim-commit` measurements — viewed through their **slot** column.

Standard library only. No `pip install`, no CDN, no build step, no framework.

```bash
cp .env.example .env      # fill in hosts + credentials
./run.sh                  # http://127.0.0.1:8899
./run.sh --port 9000
```

## What it shows

19 metric cards, each with count / mean / p50 / p90 / p95 / p99 / max, a
distribution histogram, and a plot against **slot number** rather than wall clock.

| group | fields |
|---|---|
| lane timing | `body_us` (extend, commit), `queue_us`, `age_us` |
| execution phase | `exec_wall_us` (extend, commit), `execute_us`, `load_us`, `exec_pool` |
| DAG shape | `layer_count`, `max_layer_width` (extend, commit) |
| overlay copies | `account_cache_clone_us`, `account_cache_entries_cloned` |
| program cache | `program_cache_us` (extend, commit), `program_cache_clone_us` |

Two controls: a **time range** (1h → 30d) and **last N slots**. Slot mode resolves
the window from `sim-extend` and applies it to every measurement, so the cards
stay comparable.

### Field names that no longer mean what they say

After the DAG-stream migration the wire names were kept but the semantics moved:

- **`layer_count`** carries `stream.critical_path` — the *longest dependency
  chain*, i.e. the batch's serial floor.
- **`max_layer_width`** carries `stream.initial_width` — orders with in-degree
  zero, i.e. how many could dispatch immediately. It is **not** peak concurrency;
  orders unblock as verdicts land, so real peak can exceed it.

Both read `0` when nothing executed — a refusal or a promote leaves
`BatchStats::default()`. `exec_pool` (0 vs 8) is the cleanest "did this batch
execute" predicate.

### Per-slot drill-down

Any slot in an amber or red position is a link. It opens a page carrying the
metric, band and value you clicked from, plus everything four sources know about
that slot: `sim-extend`, `sim-commit`, and — when ClickHouse is configured —
`bifrost_miniblocks` and `bifrost_events`.

## Configuration

Everything is environment-driven; there are no credentials in the source.
See `.env.example`.

- **InfluxDB is required.** `INFLUX_HOSTS` is comma-separated and tried in
  order, so you can list a DNS name and an IP fallback.
- **ClickHouse is optional.** It powers only the drill-down's miniblock and
  event panels. Leave `CH_URL` / `CH_PASS` blank and those panels degrade with a
  message; every other page works.

`host_id` is bound automatically to whichever node wrote most recently. That tag
is the validator identity and several nodes write the same measurement names, so
unbound percentiles would silently blend machines.

## Network

The datastores are typically reachable on a private network only. The dashboard
must run somewhere that can reach them — it does not proxy or tunnel on your
behalf. Bind to `127.0.0.1` (the default) unless you have a reason not to.

## Notes

- Every point is one **accepted** call. Lane-busy requests are refused before the
  worker and emit nothing, so this data never shows contention directly.
- Stage timings (`execute_us`, `load_us`, `program_cache_us`) are accumulated CPU
  time across replay workers and routinely exceed wall clock. Compare them to
  `exec_wall_us` as a *ratio* — never subtract them from `body_us`.
- Thresholds: amber > 25 ms (`STALL_WARN_US`), red > 47 ms (one slot).
  `queue_us` is judged on its own scale (1 ms / 5 ms), as is layer depth (16 / 48)
  and width (8 / 24, the replay pool size).
