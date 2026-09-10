# Draft: BTC 15m Polymarket Microstructure-Reversion Shadow Study (v1)

**Status: TELEMETRY APPROVED; STRATEGY COHORT DRAFT — DO NOT TRADE.** The
telemetry-only calibration is authorised. The strategy cohort still requires a
separate parameter approval after telemetry; its version and every numeric
value become immutable only then.

## Why this is a new hypothesis

The closed strategy tested **directional BTC momentum** confirmed by Chainlink
and Binance. Its shadow opportunity stream was negative after the
market-implied entry price. Repeating that idea with new thresholds would be
retrospective tuning, so this study does not use Chainlink, and it rejects
Binance-confirmed moves.

**Hypothesis:** a short-lived, contract-specific Polymarket price shock with
no matching BTC move on Binance and immediate opposite-side order-book pressure
occasionally overstates the probability of the shocked outcome. Buying the
opposite outcome at an executable price may have positive resolution EV.

This is a contrarian market-microstructure hypothesis, not a BTC-momentum
hypothesis. It is initially shadow-only.

## Universe and decision timing

- Market: active Polymarket BTC Up/Down contracts with a 15-minute resolution
  window and exactly one `UP` and one `DOWN` outcome.
- Evaluation cadence: one decision snapshot per second, plus an immediate
  evaluation on a Polymarket book update. Duplicate evaluations within one
  second use the newest complete snapshot only.
- No live orders, approvals, wallet actions, or CLOB order submission.

For each complete snapshot, compute the midpoint of each outcome:

```text
mid(side, t) = (best_bid(side, t) + best_ask(side, t)) / 2
delta_10s(side) = mid(side, t) - mid(side, t - 10 seconds)
btc_return_10s = ln(binance_mid(t) / binance_mid(t - 10 seconds))
```

`t - 10 seconds` means the newest complete baseline whose observation time is
at or before `t - 10s`; it may be at most one second older. The baseline,
its observation time, and all three baseline midpoints are stored with the
decision. A future observation, a missing midpoint, or a wider gap fails its
own filter and cannot silently become a candidate.

Candidate direction is derived, not chosen independently:

```text
if delta_10s(UP) >= +0.04:   candidate_side = DOWN
if delta_10s(DOWN) >= +0.04: candidate_side = UP
otherwise:                   no directional candidate
```

If both shocks occur in one snapshot, the snapshot is invalid. The shadow
order has a fixed size of one share. `entry_price` is its estimated VWAP across
displayed asks no more than one cent above the best ask, not a midpoint. The
raw book, best ask, VWAP, and fee input are retained. This is an estimate of a
taker fill, not evidence that a real order would fill.

## Fixed FilterRegistry proposal

Every item below is a pure `Callable[[Snapshot], Verdict]`, registered under
the shown name. `evaluate_all()` runs every item on the same snapshot; a live
decision would be `would_enter(verdicts)`, but this version only records it.

| Registry name | Inputs | Pass formula | Proposed constants |
| --- | --- | --- | --- |
| `market_integrity` | market status, outcome labels, token ids, open/close times, full books, fee in bps | Market is active; has exactly the mutually exclusive `UP`/`DOWN` pair; both token ids exist; books are valid, ordered and neither crossed nor locked; the taker fee is known; `open_ts < ts < close_ts`; one and only one directional candidate exists. | none |
| `evaluation_window` | `ts`, `market.open_ts`, `market.close_ts` | `90 <= ts - open_ts <= 720` and `close_ts - ts >= 180`. | 90 s after open; 180 s before close |
| `book_freshness` | timestamps of Polymarket UP/DOWN books and Binance midpoint | The adjusted source lag of every input is at most 0.750 seconds. Binance uses diff-depth WebSocket event time `E` and the feed's measured local-minus-Binance clock offset plus uncertainty; the same threshold defines `ws_stale_quote`. | 750 ms |
| `polymarket_microshock` | two Polymarket midpoints, candidate side | The *shocked*, opposite outcome rose by at least 4 cents over 10 s: `delta_10s(opposite(candidate_side)) >= 0.04`. | 4 cents, 10 s |
| `binance_neutrality` | Binance midpoints | `abs(btc_return_10s) <= 0.0004`; a shock that follows an underlying BTC move is not this hypothesis. | 4 bps, 10 s |
| `reversal_orderbook` | top 3 bid/ask levels of shocked outcome | Let `I = (bid_notional_3 - ask_notional_3) / (bid_notional_3 + ask_notional_3)`. Pass when `I <= -0.20`: after its price jump, the shocked outcome has net ask pressure. | 3 levels, -0.20 |
| `executable_contrarian_entry` | candidate best bid/ask and asks | `best_ask(candidate) <= 0.48`; `best_ask - best_bid <= 0.015`; cumulative ask notional at prices `<= best_ask + 0.01` is at least 20 USDC; one share is fillable in that range. | 48 cents, 1.5 cents, 20 USDC, 1 share |
| `market_cooldown` | prior accepted candidates for same market | No earlier `would_enter=True` snapshot for this market in the previous 60 seconds. | 60 s |

Definitions:

```text
opposite(UP) = DOWN
opposite(DOWN) = UP
bid_notional_3 = sum(price_i * size_i for first 3 bids)
ask_notional_3 = sum(price_i * size_i for first 3 asks)
```

Missing input fails its owning filter with an explicit reason such as
`missing_binance_mid_10s`; it never passes by default. The `Snapshot` stored
with each verdict must retain all raw input values, source and decision times,
the baseline evidence, fee in basis points, estimated VWAP, constants, and the
immutable `strategy_version = "microstructure-reversion-v1"`.

## Threshold provenance and calibration gate

The numeric constants above are **design hypotheses, not estimates from
historical ticks**. The sanitized repository has no tick history, and no claim
is made that 4 cents, 4 bps, -0.20, or 48 cents are optimal or even frequent.
They must not be presented as data-derived values.

Before choosing the final version, run a separate 14-calendar-day
**telemetry-only calibration**. It records raw books and feeds but does not
write a strategy cohort, fetch outcomes, calculate PnL, or inspect wins/losses.
It may be used only to report feed completeness and the frequency of the
predeclared raw conditions (2/3/4-cent shocks, 2/4/6-bps Binance moves, and
the stated book-depth thresholds). It cannot be used to select a threshold by
outcome or PnL.

After telemetry, the operator chooses one complete parameter set, gives it a
new immutable strategy version, and approves it in writing. Only then does a
fresh 30-day forward cohort begin. Changing a rare threshold after a cohort
starts is a new strategy, not a continuation.

## Shadow data and resolution

- Every complete **directional candidate** snapshot is persisted through
  `record_shadow`, whether `would_enter` is true or false. A directional
  candidate exists only after the 10-second microshock rule selects `UP` or
  `DOWN`; snapshots with no shock do not represent a potential entry and are
  counted separately as feed-health telemetry.
- Every evaluation attempt writes a separate `record_heartbeat` row, including
  no-candidate, invalid-input and failed-source attempts. Candidate rows alone
  cannot establish how long the collector was actually observing the market.
  A cohort report is invalid if any heartbeat gap from cohort start through the
  report time exceeds 300 seconds. The fraction of `complete` heartbeats and
  their reason codes are reported for data-quality review; an incomplete
  attempt remains evidence of collection, not an invented clean observation.
- The database is separate from all execution state, for example
  `data/shadow_microstructure_reversion_v1.sqlite3`.
- The cohort name is chosen before first launch in the fixed form
  `attribution-forward-YYYYMMDD-reversion-v1`; it is stored on every row and
  never renamed.
- Every row also stores a deterministic `episode_id` for dependence-aware
  analysis: `btc-episode-<floor(ts / 3600)>`. All candidates in the same UTC
  hour belong to one episode, including candidates from adjacent 15-minute
  markets. The raw Binance and Polymarket timestamps remain stored so this
  conservative clustering can be audited.
- If several eligible rows occur in one episode, the single inferential
  representative is selected before outcomes are known: the smallest
  `(ts, snapshot_id)` tuple. It is never replaced by a later, cheaper,
  higher-PnL, or otherwise more favourable row.
- `resolve_due_batches` runs offline at most once per hour. One run has an
  explicit batch limit and maximum number of batches. It caches an outcome per
  market inside each batch, records final `UP`/`DOWN` only with terminal
  evidence, and preserves raw API evidence. Disputed or unknown status remains
  pending; void/refund is a separate non-PnL state.
- Virtual PnL uses the estimated one-share VWAP and subtracts the observed
  taker fee `fee_rate × price × (1 - price)`. It still does not simulate a
  fill, exit, extra slippage, or partial execution.

## Pre-registered evaluation and stopping rules

Collection lasts **at least 30 calendar days**. The first report is produced
only after all conditions are met:

1. 30 calendar days have elapsed; and
2. heartbeat coverage has no gap longer than 300 seconds through the report
   time; and
3. at least 100 resolved candidate snapshots exist; and
4. at least 30 resolved first-accepted `episode_id` representatives exist.
   This is also the meaning of `min_n=30` throughout filter attribution; raw
   snapshot rows never count toward the inferential minimum.

The 100-row / 30-episode figures are planning targets, not a frequency estimate.
Their feasibility is checked by the telemetry-only calibration. If either
target is not met after 60 calendar days, the study is labelled **infeasible
at this frequency and configuration**. This says nothing about whether the
hypothesis has edge; it only says the approved configuration cannot collect an
adequate sample at the intended cadence. It is not retuned or extended silently.

The study is not approved for live BUY unless all of the following hold:

1. **Strategy-level result:** the single primary strategy hypothesis uses an
   ordinary two-sided 95% Wilson interval (`family_size=1`), calculated on the
   first accepted candidate in each `episode_id`. It is reported alongside
   a deterministic bootstrap interval for net per-episode virtual PnL,
   equal-weighted per-episode mean virtual PnL, drop-best-episode, and
   leave-one-episode-out. Row-level figures are
   descriptive only and are never treated as independent evidence.
2. **Filter diagnostics:** `report()` uses the fixed registry, solo-block
   attribution, drop-best-episode stability, leave-one-episode-out stability,
   and Bonferroni correction with
   `family_size = len(FilterRegistry)` (eight for the approved table). These
   diagnostics do not convert multiple filters into multiple strategy tests.
3. filter conclusions are not `UNSTABLE` or `LOW_CONFIDENCE` where a filter is
   being used to justify a decision;
4. the accepted (`would_enter=True`) subset has positive mean virtual PnL both
   row-weighted and after first averaging PnL within each `episode_id`; and
5. a separate, pre-registered execution-and-cost study validates that the
   executable shadow price remains achievable after fees, slippage, and fills.

The Wilson interval is a supporting summary, not the final go/no-go gate. A
narrow interval cannot override insufficient episode count, a non-positive
equal-weighted per-episode PnL, or a failed leave-one-episode-out check. The
episode-level stability result is the primary safeguard against pseudo-
replication in this version.

During the cohort there is no parameter tuning, no filter addition/removal, no
cohort renaming, and no live BUY. Any approved change creates a new strategy
version and a new cohort.

## Approval required

The operator must explicitly approve or edit every proposed constant before
implementation begins:

- microshock: 4 cents over 10 seconds;
- feature baseline: latest complete observation at or before 10 seconds, no
  more than one second older;
- Binance neutrality: 4 bps over 10 seconds;
- book imbalance: -0.20 across three levels;
- entry price cap: 48 cents;
- maximum spread: 1.5 cents;
- minimum near-touch depth: 20 USDC;
- shadow fill: one share at displayed VWAP no more than one cent above best
  ask; taker fee stored in basis points from the market response;
- decision window: 90–720 seconds after open and at least 180 seconds before
  resolution;
- cooldown: 60 seconds;
- collection heartbeat: every evaluation attempt, maximum evidence gap 300
  seconds;
- 14-day telemetry-only calibration before the final parameter set is chosen;
- minimum evaluation sample: 100 resolved candidates across at least 30
  resolved episodes, with a 60-day feasibility stop.
