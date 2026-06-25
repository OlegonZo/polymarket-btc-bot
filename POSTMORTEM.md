# Polymarket BTC Up/Down 15m Momentum Bot - Postmortem

Date: 2026-06-25
Status: CLOSED AS NEGATIVE EDGE

## Decision

This bot is not approved for live BUY execution.

The strategy is frozen. Do not tune entry filters, thresholds, guards, or exit logic based on the current shadow results.

Allowed:
- keep shadow collection running only if the goal is to finish the exit sample;
- run resolver/analyzers for reporting;
- use this project as a portfolio/research case.

Not allowed:
- enable real BUY;
- loosen filters because a small bucket looks positive;
- add more guards to chase recent losses;
- treat `n < 50` buckets as evidence of edge;
- restart the optimization loop without a new hypothesis and a pre-registered test.

## Hypothesis

The original hypothesis was:

BTC short-term momentum, Chainlink/Polymarket price context, Binance tape, and order book filters can identify positive expected value UP entries in 15-minute Polymarket BTC Up/Down markets.

The tested execution shape was directional:

Buy UP when the signal stack says the probability of UP is higher than the market-implied price.

## Final Core Result

Latest resolved shadow sample:

- total rows: 1496
- resolved: 1485
- pending: 11
- errors: 0
- unique resolved markets: 181
- winrate: 69.43%
- average entry price: 70.38%
- average virtual PnL: -0.00956
- total virtual PnL: -14.20

Population definition:

The 1485 rows are logged shadow/counterfactual entry observations from the candidate stream, mostly blocked by the filter stack. They are not real fills and not 1485 independent live trades.

Core formula:

```text
expected PnL per share = winrate - average entry price
```

Observed:

```text
0.6943 - 0.7038 = -0.0095
```

The logged candidate stream is correct often, but not often enough for the prices it would have paid.

This is the main result. The market price already captures the public directional information better than this signal stack.

Pseudo-replication caveat:

The 1485 row count is not an iid outcome sample. The rows are clustered across 181 unique 15-minute markets, and each market resolves once. Effective outcome confidence is therefore closer to the unique-market count than the raw row count. A market-clustered check was still negative, with mean per-market average PnL of about -0.03848.

Row-weighted and market-weighted means answer different questions. Row-level PnL is closer to fixed-size execution on every logged signal; market-level PnL is closer to an independent-market edge estimate. The gap between row-level -0.00956 and market-level -0.03848 suggests signal frequency was correlated with market outcome, but both weighting schemes remain negative.

## Why 69% Winrate Is Still Negative

In a Polymarket binary market, buying at 0.70 means:

- win: +0.30
- loss: -0.70

So a 70 cent entry needs more than 70% true win probability before fees/slippage.

The bot achieved about 69.43% while paying about 70.38% on average. That is a negative spread between true hit rate and market-implied probability.

## Resolver Fix

The important data-quality issue was resolution correctness.

Before the resolver fix, unresolved or not-final markets could distort the analysis. After the strict resolver path, the test used closed/final outcomes and produced a clean resolved dataset.

Final resolver state:

- input: `logs/shadow_log.jsonl`
- output: `logs/shadow_log_resolved_v3.jsonl`
- resolved now: 1485
- still pending: 11
- errors: 0
- unique markets checked: 181

This means the negative conclusion is not based on the old broken resolver state.

## Filter Tuning Trap

The current filters should not be interpreted as optimization targets.

Examples from the latest run:

- a cheap-entry reversal guard had a positive-looking blocked bucket, but small `n`
- a forecast-style block had conflicting all-time and recent-period behavior
- a Binance-quality guard had conflicting all-time and recent-period behavior

This is not a signal to tune filters. It is the overfit loop:

1. A small bucket looks positive or negative.
2. A filter gets changed.
3. The next sample flips.
4. Another filter becomes the new suspect.
5. Total PnL stays around zero/minus while work continues indefinitely.

Any future change must start from a new hypothesis and a pre-registered test rule. No more tuning from small retrospective buckets.

## Exit Test

Exit logic is not proven.

Current exit-log sample:

- simulated exits: 4
- average exit PnL: -0.0529
- total exit PnL: -0.2117

This sample is too small to answer whether exit logic can help.

If shadow collection continues, the only valid remaining bot question is:

Can exit logic turn an otherwise negative directional entry into positive EV?

Minimum useful sample:

- weak read: 30 exits
- better read: 50+ exits

No strategy changes should be made before that sample exists.

## Final Engineering Conclusion

The logged directional opportunity stream did not show positive EV after market-implied entry price.

The bot should not be improved by more filter work. The project should be treated as a completed negative-edge research case unless a materially different hypothesis is introduced.

## Next Useful Output

Turn this into a portfolio case:

Title:

`Testing a Polymarket BTC 15m Momentum Edge and Killing It With Data`

Useful sections:

- hypothesis
- architecture
- data pipeline
- resolver bug and fix
- final EV math
- overfit loop
- decision to stop
- what would be required for a new test

The valuable result is not a profitable bot. The valuable result is proving that this version should not trade.
