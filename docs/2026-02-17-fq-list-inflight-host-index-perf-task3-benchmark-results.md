# Task 3 Benchmark Results Note

Date: 2026-02-17
Worktree: `/root/simple-alist-cf-proxy/.worktrees/fq-host-index-perf`
Module root: `/root/simple-alist-cf-proxy/.worktrees/fq-host-index-perf/slot-handler`

## Reproducible Command

Run from repository root:

```bash
go test -C ./slot-handler ./internal/slothandler -run '^$' -bench 'BenchmarkListInFlightByHost' -benchmem -count=3
```

Equivalent command from module root (`slot-handler/`):

```bash
go test ./internal/slothandler -run '^$' -bench 'BenchmarkListInFlightByHost' -benchmem -count=3
```

## Benchmark Profiles (Routine vs Evidence)

- Fast routine validation profile (quick regression signal during development):

```bash
go test -C ./slot-handler ./internal/slothandler -run '^$' -bench 'BenchmarkListInFlightByHost' -benchmem -benchtime=100ms -count=1
```

- Evidence profile used by this Task 3 note (more stable means and threshold judgments):

```bash
go test -C ./slot-handler ./internal/slothandler -run '^$' -bench 'BenchmarkListInFlightByHost' -benchmem -count=3
```

- Expected wall-clock ranges on the reference host in this note (`linux/amd64`, `Intel(R) Xeon(R) E-2288G CPU @ 3.70GHz`):
  - Fast routine profile: usually ~20-35 seconds.
  - One-pass default profile (`-count=1`, no `-benchtime` override): usually ~2.5-4 minutes.
  - Evidence profile in this note (`-count=3`): usually ~7-12 minutes and may exceed constrained shell timeouts.

Decision rule for threshold judgments in this note:
- Use arithmetic mean (`ns/op`) across the three runs from the exact command output above.
- For `hot_host`, compute relative delta as `(indexed_mean - legacy_mean) / legacy_mean`.
- Benchmarks build a fresh dataset per benchmark iteration with timer paused (`b.StopTimer` before setup, `b.StartTimer` before lookup), so setup is excluded from timing.
- `sparse_hosts` and `hot_host` run multiple lookups per iteration on mutation-free fixtures (no expired/stale entries), so repeated lookups see equivalent state.
- `mixed_expired` is mutation-prone by design (expired entries), so it runs exactly one timed lookup per fresh store (`lookupsPerIteration=1`) to eliminate intra-iteration drift.

## Raw Key Results (ns/op)

Environment context from the run host:
- `goos=linux`, `goarch=amd64`
- CPU: `Intel(R) Xeon(R) E-2288G CPU @ 3.70GHz`

- Indexed `sparse_hosts`: 162528, 162634, 168332
- Legacy `sparse_hosts`: 20295553, 20181029, 20127003
- Indexed `hot_host`: 50279370, 61840949, 59081679
- Legacy `hot_host`: 58095429, 60038545, 59660033
- Indexed `mixed_expired`: 1396, 1389, 1387
- Legacy `mixed_expired`: 19239, 18636, 19193

## Threshold Evaluation (Task 3 Step 3)

- `sparse_hosts` threshold: indexed >= 5x faster than legacy.
  - Mean indexed: 164498.0 ns/op
  - Mean legacy: 20201195.0 ns/op
  - Speedup: 122.81x
  - Outcome: PASS

- `hot_host` threshold: indexed no worse than legacy by more than 10%.
  - Mean indexed: 57067332.7 ns/op
  - Mean legacy: 59264669.0 ns/op
  - Relative delta: -3.71%
  - Outcome: PASS
  - Stability note: run-to-run spread remains present (`50279370` to `61840949` ns/op for indexed in this count=3 sample), so `hot_host` can fluctuate between runs.

- Allocation threshold: indexed allocations should not regress materially.
  - `sparse_hosts`: indexed 147456 B/op, 512 allocs/op vs legacy 221184 B/op, 1024 allocs/op
  - `hot_host`: indexed 37224928-37225222 B/op, 83-84 allocs/op vs legacy 157946865-157946949 B/op, 1107-1109 allocs/op
  - `mixed_expired`: indexed 2688 B/op, 1 alloc/op vs legacy 7200 B/op, 85 allocs/op
  - Outcome: PASS

Notes:
- PASS/FAIL labels are derived from the same raw values recorded in this note using the decision rule above.
- `ns/op` values are per benchmark iteration and compared indexed-vs-legacy within each scenario using the same fixture shape and lookup-call pattern.
- This note is a Task 3 artifact only; no production-path behavior was changed.

## Task 4 Verification Alignment

- Focused fairqueue tests (`TestAcquire|TestProbe|TestScheduler|TestFlow|TestOverload|TestHostInFlightIndex`) and package race run are expected to pass after Task 2.
- Final benchmark replay in Task 4 uses `-count=5` for stability checks. On the same reference host class this is typically a double-digit-minute run and can exceed default command timeouts in constrained CI/agent shells even when benchmarks are healthy.
- If timeout occurs at `-count=5`, treat it as an execution-time budget caveat first; rerun with a longer command timeout or run from a less constrained shell before judging regression.
