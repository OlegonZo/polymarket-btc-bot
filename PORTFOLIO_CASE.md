# Testing a Polymarket BTC 15m Momentum Edge and Killing It With Data

## One-line summary

I built and shadow-tested a Polymarket BTC Up/Down 15-minute momentum bot, fixed the data-resolution pipeline, and rejected the strategy after the clean sample showed negative expected value.

## Context

The project tested whether short-term BTC momentum could produce a tradeable edge in Polymarket's 15-minute BTC Up/Down markets.

The bot used:

- Polymarket order book state;
- Chainlink BTC/USD reference movement;
- Binance BTCUSDT tape/momentum;
- entry price gates;
- order book and forecast-style filters;
- shadow logging and post-resolution analysis.

The goal was not just to produce a high winrate. The goal was to beat the market-implied probability embedded in the entry price.

## Hypothesis

The tested hypothesis:

> BTC short-term momentum plus market microstructure filters can identify UP entries where true win probability is higher than the Polymarket price.

In binary markets, that means:

```text
expected PnL per share = true winrate - average entry price
```

So buying at 0.70 requires more than 70% true win probability before fees and slippage.

## System design

The bot was structured as a live-style shadow system:

- discover/current-market loop for BTC 15m Up/Down markets;
- data feeds from Polymarket, Chainlink, and Binance;
- entry decision engine with price, momentum, book, and forecast filters;
- JSONL shadow logger for every blocked or hypothetical entry;
- resolver that matched logged markets to final outcomes;
- analysis scripts for filter buckets, EV, exit simulation, and edge reports.

This public repository intentionally does not include the private live-trading implementation, credentials, raw logs, or account-specific details.

## Data quality issue and fix

The most important engineering issue was resolver correctness.

Early analysis risked mixing unresolved or not-final markets into the result. That made the statistics unreliable.

The resolver was tightened to use final closed outcomes and produce a clean resolved dataset:

```text
input: shadow_log.jsonl
output: shadow_log_resolved_v3.jsonl
total rows: 1496
resolved: 1485
pending: 11
errors: 0
unique markets checked: 181
```

This mattered because a trading conclusion is only as good as the settlement data behind it.

## Final result

Clean resolved shadow sample:

```text
resolved rows: 1485
winrate: 69.43%
average entry price: 70.38%
average virtual PnL: -0.00956
total virtual PnL: -14.20
```

Core EV check:

```text
0.6943 - 0.7038 = -0.0095
```

The bot was directionally right often, but it paid too much for those entries.

The market price already reflected the public momentum information better than the strategy did.

## Why the high winrate was not enough

A 69% winrate sounds strong in isolation. In a priced binary market, it is incomplete.

If the bot buys UP at 0.70:

- win: +0.30
- loss: -0.70

The breakeven winrate is therefore roughly 70%.

The strategy's realized hit rate was lower than its average entry price. That makes the system negative EV despite a high raw winrate.

## What almost caused overfitting

Some small filter buckets looked attractive after the fact.

Examples:

- a cheap-entry reversal guard appeared to block profitable trades in a small sample;
- forecast and Binance quality filters had conflicting all-time and recent-period behavior;
- some microstructure buckets looked positive with low sample sizes.

This is the classic tuning trap:

1. A small bucket looks good or bad.
2. A threshold gets changed.
3. The next sample flips.
4. Another filter becomes the new suspect.
5. The strategy keeps consuming time while total EV stays near zero or negative.

The decision was to stop tuning because the main EV equation had already failed.

## Exit logic

Exit monitoring was added, but the sample was too small to validate:

```text
simulated exits: 4
average exit PnL: -0.0529
total exit PnL: -0.2117
```

This does not prove exit logic cannot help. It proves there was not enough evidence to use exit logic as a reason to continue optimizing the same entry strategy.

A valid future exit test would need a pre-defined rule and at least 30-50 completed exit samples.

## Decision

The strategy was closed as negative edge.

Locked decisions:

- do not enable live BUY;
- do not tune filters from small retrospective buckets;
- do not treat log growth as progress unless it answers a predefined question;
- archive the project as a completed research case.

## Lessons

High winrate is not edge. Winrate only matters relative to the price paid.

Resolver correctness is not a detail. Bad settlement data can make a losing system look promising.

Retrospective filter tuning is dangerous. A strategy can always produce small buckets that look good by chance.

Stopping is part of the engineering process. The useful output of this project was not a profitable bot, but a defensible decision not to trade it.

## What I would test next

I would not continue with the same directional momentum hypothesis.

A valid next test would need to be materially different, for example:

- a market-making or spread-capture hypothesis;
- a latency-specific hypothesis with measurable queue/fill advantage;
- a cross-market information edge that is not already visible in the Polymarket price;
- a pre-registered exit-only experiment with a fixed entry baseline.

Any new test should define before running:

- exact entry rule;
- exact exit rule;
- minimum sample size;
- success metric;
- stop condition.

No retrospective filter search.
