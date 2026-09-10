# Telemetry-only calibration status

**Status: stopped early; retained for frequency diagnostics, invalid for final coverage.**

Update, 2026-09-10: the last snapshot was written at 2026-09-09 07:53:26
UTC, leaving a gap far above the locked 300-second limit. A live source
preflight then showed HTTP 451 from the primary Binance stream on this host.
The saved interval remains useful for descriptive frequency and engineering
diagnostics; the outage does not erase it. Do not silently merge source/code
versions or label interrupted coverage continuous. Whether to restart a full
14-day run or approve a documented segmented calibration is a separate operator
decision, not an automatic consequence of the outage. No new calibration or
strategy cohort was started by the quality-fix work.

Update, quality policy v2 (2026-09-10):

Later update, transport v3: public time requests now use pre-established,
verified TLS and retain the full HTTP round-trip uncertainty. The 90-second
smoke recovered fresh quotes and reconnect (age upper bound p95 400.61ms), but
did not pass full acceptance. A separate 30-minute run began at 09:26:35 MSK;
read `reports/source_acceptance_v3_30m_2026-09-10.json` after completion before
claiming acceptance. This does not restart the 14-day calibration.

- Every new collection attempt is journalled, including failed/exceptional
  attempts. Older databases without this journal have an UNKNOWN denominator,
  not a fabricated 100% availability. The read-only quality checker does not
  migrate or rewrite those databases.
- Freshness rejects negative lag; Binance's 750ms check includes the measured
  clock uncertainty. A healthy connection is not evidence of a fresh quote.
- Source acceptance requires 1800 observed seconds, >=95% valid attempts,
  <=10 seconds between valid observations (including startup/tail), multiple
  monotonic update IDs and recovery after an injected socket disconnect.
  These are conservative engineering acceptance criteria, not trading rules.
  Short smoke runs always fail the duration gate by design.
- Final quality includes leading/trailing gaps up to the earlier of report
  time and planned end, successful scheduled polling slots, and successful
  attempts. An independent offline audit is explicitly a separate read and
  cannot influence the snapshot-consistent quality gates.
- The older `source_acceptance_2026-09-10.json` used policy v1. Its `passed`
  flag cannot certify policy v2 or approve a cohort. Use the v2-labelled reports.
- The v2 60-second smoke observed 0/240 valid failover samples, with 208 stale
  quotes and 32 startup/no-quote samples. Gamma feeSchedule was present. This
  blocks long-run acceptance; it is not evidence against the trading hypothesis.

Update, 2026-09-05: the shared Python dependency disappeared again. A project
`.venv` now holds `websocket-client==1.9.2`, and the existing launcher uses it
on subsequent starts. The running collector was not interrupted. All 24
tests pass in this environment. See `BOT_LEARNING.md` and `decision_audit.py`
for corrections to earlier candidate counts and measurement claims; in
particular, Polymarket stale books are stored and screened by offline audit,
not rejected inside the current collector.

- Run ID: `aee5c46a-b1a5-47ba-8eec-fef8ecc70e93`
- Started: 2026-09-02 23:14:30 UTC (2026-09-03 02:14:30 MSK)
- Scheduled end: 2026-09-16 23:14:30 UTC (2026-09-17 02:14:30 MSK)
- Database: `data/telemetry_calibration.sqlite3`
- Scheduler task: `PolymarketTelemetry14Day`; no execution-time limit,
  bounded restart policy, and same-run resume with exact config matching
- Runtime dependency check: `websocket-client==1.9.2` was restored and
  reverified on 2026-09-04; the launcher interpreter can import it, generate
  the report, and resume this run after a process restart

The local Windows clock was measured about 1.1 seconds ahead of Binance at
launch. The collector now estimates this offset from multiple Binance
`serverTime` samples, retains the lowest-uncertainty sample, and refreshes it
every five minutes. The fixed freshness threshold remains 750 ms and is
applied after clock-offset correction. The first corrected smoke test observed
Binance event lag p50 195.984 ms and p95 205.555 ms, with 32/32 rows passing.

An invalid launch attempt (`92dad498-9baf-4f41-b39b-de169bc554a2`) wrote zero
snapshots before clock-offset calibration was added. Its separate database
`data/telemetry_main.sqlite3` is excluded.

The previous 14-day process (`b8bef436-6991-4c15-b0a3-29408077fe5d`) was
stopped before it could contribute data. Its collector lacked source-side
timestamps, cycle timing, retry diagnostics, and midpoint-missing reasons.
Its data is excluded from calibration.

The final pre-calibration acceptance command is:

```bash
python telemetry.py run --db data/telemetry_ws_acceptance.sqlite3 --duration-seconds 900
```

Completed acceptance run: `6a7f9d36-b145-4930-a021-f90094de6ce5`, from
2026-09-01 09:51:35 UTC through 2026-09-01 10:06:35 UTC. Its data is
review-only and cannot be merged into the 14-day calibration.

Acceptance evidence:

- 845 valid snapshots; 12 telemetry/unit tests passed.
- Full cycle: p50 284.293 ms, p95 453.413 ms. Snapshot cadence: p50 1.000 s,
  p95 1.001 s.
- Binance source timestamps: 845/845 `payload_E_ms`; lag p50 565.847 ms,
  p95 596.295 ms, maximum 633.626 ms; 100% met the fixed 750 ms gate.
- Polymarket timestamp coverage was 100%; 99.527% of UP and 99.763% of DOWN
  observations met the 750 ms gate.
- Three post-connect WebSocket gaps were observed. Cached quotes were rejected
  during every gap as `ws_disconnect` or `ws_stale_quote`; reconnect windows
  were stored separately. There was no silent carry-forward.
- Missing Polymarket midpoint occurred in 177/845 rows (20.947%), always as the
  paired reasons UP `no_bid` and DOWN `no_ask`, in four clusters. Only 18 of
  these rows were within 10 seconds after a collector error, so the dominant
  cause is the returned book shape/liquidity rather than an unclassified fetch
  failure.

The earlier REST acceptance (`ba0bb020-c327-482f-8977-3c7159d2e543`) is also
excluded. It proved that REST `bookTicker` has no usable price event timestamp.

Its report was reviewed against the acceptance criteria before the approved
14-day run was manually started. The accepted final database is
`data/telemetry_calibration.sqlite3` (gitignored runtime data).

- Poll cadence: one second; Polymarket books are fetched in parallel while the
  latest synchronized Binance diff-depth quote is read from memory
- Inputs: public Polymarket CLOB top-three levels and Binance BTCUSDT
  `depth@100ms`, synchronized by update ID with source time `E`
- Diagnostics: source timestamp and received timestamp per source, source lag,
  p50/p95 cycle-stage timing, retry attempts/error classifications, and the
  explicit reason for every missing midpoint

The process writes no orders, wallet data, PnL, outcome, resolution, or live
strategy cohort. Its results are frequency and feed-health telemetry only.

An earlier preflight run (`21492221-98a3-4ab2-9880-a25382a2ae1b`) stopped after
1 hour 33 minutes because its background host ended. It is retained only for
feed diagnostics and is not part of any calibration run.

After an explicitly approved 14-day run ends, run:

```bash
python telemetry.py report --db data/telemetry_calibration.sqlite3 --run-id aee5c46a-b1a5-47ba-8eec-fef8ecc70e93
```

Use the report to approve one immutable parameter set for a separate forward
cohort, or to declare this configuration operationally infeasible. Do not
start a strategy cohort automatically.
