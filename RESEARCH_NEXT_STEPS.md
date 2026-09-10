# Research readiness — 10 September 2026

Status at 09:26 MSK: infrastructure fixes tested; clock blocker addressed in
short smoke, full source acceptance RUNNING (not passed yet). No new telemetry
calibration, strategy cohort, outcome collection or orders launched. Separate
disposable source/telemetry tests are not part of calibration.
The original eight trading filters, formulas, 750ms threshold, fixed earliest
representative rule and LOCKED_PARAMETERS are unchanged.

## Implemented before collecting outcome data

- Source acceptance v2: monotonic elapsed time; all polling attempts;
  fresh-quote outage including startup/tail; clock uncertainty; deliberate
  disconnect/recovery; short smoke explicitly cannot pass 30-minute acceptance.
- Telemetry attempts recorded on success, failure and exceptions. Legacy
  attempt coverage is unknown. Negative lags are no longer counted as fresh.
- Final telemetry quality: consistent read transaction, explicit connection
  closure, leading/tail outages, successful attempt and scheduled-slot fractions.
- Shadow coverage checks both heartbeat and complete-input gaps plus a minimum
  complete-attempt fraction (engineering default 0.95, printed in report).
- Complete public input payload and result are recorded separately for valid,
  invalid and unevaluable input attempts. Unevaluable inputs are not trades.
  Failures before source inputs exist remain classified heartbeats/events;
  there is no invented book or hypothetical filter result for missing data.
- Append-only resolver state history supports candidate and outcome cutoffs
  at as_of_ts, including retry exhaustion/requeue. Pre-migration rows lacking
  state history are AS_OF_UNKNOWN and cannot certify an historical result.
- Reconnect backoff no longer inherits a previous successful session flag
  across failed handshakes.
- Whole-UTC-day cluster bootstrap added as DESCRIPTIVE sensitivity in report.
  It keeps each day's representatives together, but assumes independent days;
  it is not a substitute for an approved inference plan. A positive IID estimate
  is now explicitly labelled as still requiring cluster/execution validation.

## Immediate source blocker

The public Gamma response included feesEnabled and feeSchedule (rate .07,
exponent 1). The primary Binance host returned HTTP 451. The documented public
market-data endpoint connected, but its quotes failed the 750ms freshness check
after including clock uncertainty in the 60-second v2 smoke (0/240 valid).
No regional bypass, alternate asset/venue, timestamp substitution or relaxed
freshness threshold is authorized by this result. Diagnose transport and clock
bounds first. Do not spend 14 days collecting data known to fail the gate.

The follow-up 20-second diagnostic measured 48 rejected synchronized quotes:
age p50 633.55ms, clock uncertainty 581.63ms, conservative age bound p50
1215.18ms (p95 1258.52ms). These are not valid samples. The uncertainty comes
from the time-sync round trip; it is not evidence that the physical quote is
exactly 1.2s old. Next isolate time-request setup/RTT from actual quote delay,
then rerun acceptance without reducing the uncertainty bound by assumption.
The injected reconnect could not be exercised because no fresh baseline was
established; it is explicitly unverified, not passed.

### Later transport-v3 smoke (90 seconds)

The corrected time transport produced 307 valid samples / 358 attempts. Age
upper bound p50 353.65ms, p95 400.61ms, maximum 486.12ms; selected clock
uncertainty about 149.44ms. The longest gap including startup/injected outage
was 7.157s. Reconnection was observed after the injected disconnect. The run
still correctly FAILS full acceptance (short duration and only 85.75% valid
attempts over this short startup/recovery-heavy window). Do not advertise it
as a completed 30-minute pass.

At 09:26:35 MSK a separate 1800-second acceptance process was started (PID 1852),
with output `reports/source_acceptance_v3_30m_2026-09-10.json` and an incremental
`.progress.json` alongside it. Current code additionally requires a newer
connection generation for injected-recovery evidence, not just a newer quote.
The result must be read after completion. No scheduled automation was created.

A 60-second end-to-end telemetry test uses only
`data/telemetry_transport_v3_smoke_20260910.sqlite3`, run
`0c092503-e9b3-4129-b953-d597c6a97982`. It must never be merged into calibration.

End-to-end smoke completed: all 60 attempts were persisted (55 successes and
5 initial ws_no_quote failures); 55 snapshots, zero missing midpoints. Successful
cycle p50 219.585ms / p95 569.371ms; clock uncertainty 152.1663ms. Source lag at
receipt ranged 150.591–197.656ms. These small-sample diagnostics do not establish
long-run availability. The test database contains telemetry/health tables only.

## Decisions required before the next phase (not auto-approved)

1. Operational recovery: demonstrate source acceptance v2 and a bounded
   end-to-end telemetry-only run. Keep new source/code versions separate.
2. Calibration protocol: retain the old 6.36-day interval descriptively; decide
   explicitly whether completion uses a new uninterrupted interval or documented
   segments. The approved 14-day protocol is not silently shortened.
3. Economic target: intended position size and a minimum useful net profit per
   share after actual fee, execution delay, depth and partial-fill constraints.
   The current one-share VWAP is a proxy, not proof of an executable order.
4. Inference plan: define blocks, minimum number of informative blocks, effect
   size/power assumptions, fixed evaluation times and maximum horizon. Do not
   pick the first significant daily report. Thirty hourly representatives alone
   are not a guarantee of adequate sample size or independent evidence.
5. Execution study: predeclare latency/price/depth stress scenarios and exact
   per-fill fee rounding. Do not tune them against profitable outcomes.
6. Separately approve one immutable parameter set and cohort identity. Only then
   start forward outcome collection. A positive shadow result needs confirmation
   on a new time interval; it never grants live-order permission.

## Verification

Clock-transport correction prepared after the diagnostic: the public time
request is now sent only after verified TLS setup, and the connection is reused
within the sampling batch. Setup duration is reported separately; the entire
HTTP request/response RTT plus 1ms timestamp quantization remains in the clock
bound. Configured proxies retain the old conservative urllib transport (no
proxy bypass). Local wall-clock steps during a sample invalidate it. Source,
750ms gate and trading formulas remain unchanged. Network acceptance of this
correction must be evaluated separately from the earlier v2 failures.

Regression coverage includes future data/outcome exclusion, retry-state time
travel, failed heartbeats, invalid raw inputs, failure-tail accounting, short
smoke rejection, one-quote-then-outage, clock uncertainty, negative source lag,
and day-block resampling. Existing filter-boundary tests remain unchanged except
fixtures that had relied on future resolutions or ignored clock uncertainty.

All 68 unit/regression tests pass; Python compilation passes.

### Sticky fallback validation

The first v3 30-minute acceptance failed only `maximum_10_seconds_without_valid_quote`
(23.531s) despite 96.66% valid attempts and a fresh upper-bound p95 of 428.12ms.
The primary endpoint's HTTP 451 was deterministic and endpoint rotation retried it
after a fallback interruption. It is now quarantined for the lifetime of the
process only after that specific 451 policy error; generic disconnects are not
quarantined. In the disposable five-minute v4 test, max time without a valid
quote was 6.265s, valid attempts were 96.24%, quote-age upper-bound p95 was
405.50ms, and injected reconnect recovered. All acceptance criteria except the
deliberately unmet 1800-second duration passed. A final separate 30-minute v4
acceptance began at 10:58:14 MSK; report:
`reports/source_acceptance_v4_30m_2026-09-10.json`. No calibration began.

The acceptance figures measure source quality only. No edge claim or estimated
profitability can be derived from these telemetry runs.
