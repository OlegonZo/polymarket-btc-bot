# Polymarket BTC 15M Momentum Bot - Edge Measurement Framework

An automated trading research system for the **BTC Up/Down 15-minute** prediction market on Polymarket, built primarily as an apparatus for measuring whether short-horizon BTC momentum retains edge after market-implied pricing.

The headline result of this project is a **negative one**, and it is stated up front by design. The value of the repo is the measurement infrastructure and the discipline used to reach that conclusion, not a green PnL curve.

This public version is sanitized. Full trading code, credentials, raw logs, wallet details, and private execution parameters are not published.

---

## Key Finding

After the resolver was fixed and the shadow dataset was recomputed:

- **Directional hit rate was high but not high enough.** The clean resolved shadow/counterfactual sample showed a `69.43%` row-level winrate.
- **The average entry price was higher than the hit rate.** The average entry price was `70.38%`.
- **Net edge was negative.** In a binary market, breakeven is approximately the entry price. The measured spread was:

```text
winrate 69.43% - average entry price 70.38% = -0.95% per share
```

Final clean sample:

```text
total rows: 1496
resolved rows: 1485
pending rows: 11
unique resolved markets: 181
resolver errors: 0
average virtual PnL: -0.00956
total virtual PnL: -14.20
```

**Population definition:** the 1485 rows are logged shadow/counterfactual entry observations from the candidate stream, mostly blocked by the filter stack. They are not real fills and should not be read as 1485 independent live trades.

**Pseudo-replication note:** multiple rows can belong to the same 15-minute market, and each market resolves once. Outcome confidence should therefore be clustered closer to the 181 unique markets than the 1485 row count. The row-level EV accounting is still useful for measuring the logged opportunity stream, but it is not an iid binomial sample of 1485 independent outcomes.

A simple market-clustered check also stayed negative:

```text
unique markets: 181
mean per-market average PnL: -0.03848
```

The row-weighted and market-weighted means answer different questions. Row-level PnL is closer to the economics of fixed-size trading on every logged signal, while market-level PnL is closer to an independent-market edge estimate. The gap between `-0.00956` and `-0.03848` means signal frequency was correlated with market outcome; frequently logged markets were less negative, so row-weighting pulled the result toward zero. Both views remain negative.

**Conclusion: not deployable.** The logged directional opportunity stream did not beat the market-implied entry price, and no approved live-entry edge was validated.

This is treated as a valid result, not a failure to be hidden. The purpose of the build was to determine whether the edge exists with enough rigor to defend the answer either way. The answer for this version is no.

> Methodology note: the decisive conclusion is based on the corrected resolver output and the breakeven relationship between winrate and entry price. Small retrospective filter buckets were not used to move the goalposts toward a favorable read.

---

## Core Idea

The hypothesis under test was that BTC impulse moves can be detected early and confirmed across data sources fast enough to enter the 15-minute market with positive net expectancy:

- detect BTC impulse moves early;
- confirm direction through a second exchange feed instead of trusting a single source;
- avoid entering into local pumps and tops;
- exit automatically when the market turns.

The architecture below was built to give this hypothesis a fair test, and the tested logged opportunity stream still came out negative.

## Multi-Layer Entry System

Entry decisions ran through a stacked filter pipeline, not a single signal:

1. **Chainlink Signal** - primary BTC/USD price direction via `move_pct`
2. **Binance Confirmation** - cross-checks direction using 5s/10s/30s moves, volume spikes, and acceleration
3. **Entry Score V1** - rates each signal using Chainlink strength, multi-window moves, acceleration, volume spike, orderbook imbalance, spread, and entry price
4. **Forecast Score 15M** - secondary forecast model, stricter on expensive entries
5. **Binance Entry Quality Guard** - blocks weak confirmation entries
6. **Cheap Entry Reversal Guard** - estimates reversal risk to avoid buying tops
7. **Anti Fake Pump Filter** - blocks entries into local pumps
8. **Orderbook Guards** - validate bid/ask depth, imbalance, spread, and fill context
9. **Binance Armed Mode** - prioritizes signals during confirmed market acceleration
10. **Fast Entry Window** - entries only allowed in a defined window after a new market opens

## Exit System

A dedicated exit monitor was implemented to test whether active exits could improve the strategy:

- **Take Profit** - locks in gains
- **Trailing Exit** - exits on pullback from a local high
- **Predictive Exit** - early exit when conditions deteriorate
- **Defensive / Hard Defensive Exit** - protective and emergency exits
- **Binance Reversal Exit** - exits when the confirming feed turns against the position
- **Ladder Sell** - scaled exit logic

The exit sample was too small to rescue the strategy conclusion:

```text
simulated exits: 4
average exit PnL: -0.0529
total exit PnL: -0.2117
```

The remaining exit question is bounded: it would require a pre-defined rule and a 30-50+ exit sample. It is not a reason to continue open-ended filter tuning.

## Shadow Logging & Analytics System

This is the part of the project I would point a reviewer to first.

A custom analytics layer logged and evaluated blocked or hypothetical trades to measure whether the filter stack actually added edge rather than assuming it did:

- **Shadow Logger** - records blocked entries with full context and blocking filters
- **Resolver** - replays logged signals against final market outcomes
- **Analyzer** - surfaces which filters block most often and what the result would have been without them

### Resolver Bug - Found and Fixed

During analysis, the resolver was found to be capable of misreading unresolved or not-final markets. The issue was isolated and corrected, then the affected results were recomputed.

Catching and fixing errors in the measurement layer is part of the point: a strategy can only be trusted as far as the instrument that evaluates it.

### Independent Filter Attribution

The pipeline used short-circuit evaluation: the first failing filter stopped execution. That makes attribution difficult because later filters are not always evaluated independently.

The attribution roadmap was:

1. **Independent Filter Attribution** - evaluate every filter regardless of earlier failures
2. **Resolver against real market outcomes**
3. **Filter Impact Analysis**
4. `simulate_remove(filter)` - estimate each filter's marginal contribution
5. **Wilson Confidence Interval** for statistical significance
6. **LOW_CONFIDENCE** flag when sample size is too small

Scope discipline matters here. Once the corrected aggregate EV was negative, additional instrumentation became useful only for a bounded research question, not as a justification for endless optimization.

## Priced-In Signal vs Overfitting

The main result is a priced-in signal problem: the public momentum information was real enough to produce a high raw hit rate, but not strong enough to beat the price paid.

The overfitting risk is a separate issue. It appears when small retrospective buckets make one filter look guilty, then the next sample flips and another filter becomes the new target. This repo treats that as data-snooping risk, not as evidence for another round of tuning.

## Tech Stack

- **Language:** Python
- **Data Sources:** Chainlink BTC/USD, Binance BTCUSDT, Polymarket order book
- **Execution Venue:** Polymarket CLOB API
- **Analytics:** Custom JSONL shadow logging, resolver, and attribution scripts
- **Statistics:** Breakeven-by-entry-price EV accounting, bucket analysis, cluster-aware sample-size caveats, Wilson confidence intervals for appropriately scoped buckets

## Status

- **UP strategy:** forward/shadow measurement complete; classified negative-edge for the tested hypothesis
- **DOWN variant:** experimental mirrored logic existed, but is not presented as a deployable result
- **Analytics:** shadow logging + resolver fix + attribution foundation in place
- **Remaining open angle:** exit-path simulation only, bounded and pre-defined
- **Live BUY:** not approved

---

Strategy parameters, signal logic, credentials, raw logs, and execution details are kept private. Architecture, measurement methodology, and the reasoning behind the negative result are available for discussion in interviews.
