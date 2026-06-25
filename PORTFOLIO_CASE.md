# Testing a Polymarket BTC 15m Momentum Edge and Killing It With Data

## One-line summary

I built and shadow-tested a Polymarket BTC Up/Down 15-minute momentum bot, fixed the data-resolution pipeline, and rejected live deployment after the clean shadow/counterfactual sample showed negative expected value against the market-implied entry price.

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
input: logs/shadow_log.jsonl
output: logs/shadow_log_resolved_v3.jsonl
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
unique resolved markets: 181
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

## Population and independence

The 1485 rows are logged shadow/counterfactual entry observations from the candidate stream, mostly blocked by the filter stack. They are not real fills and should not be described as 1485 independent live trades.

There is also pseudo-replication in the row count. The observations are distributed across 181 unique 15-minute markets, and each market has one final outcome. Multiple observations inside the same market therefore share the same resolution.

For outcome confidence, the effective sample size is closer to the number of unique markets than the raw row count. The row-level EV is still useful as decision-stream accounting, but Wilson-style precision should not be claimed as if all 1485 rows were iid.

A simple market-clustered check was still negative:

```text
unique markets: 181
mean per-market average PnL: -0.03848
```

This does not weaken the conclusion. It makes the uncertainty accounting explicit.

## Why the high winrate was not enough

A 69% winrate sounds strong in isolation. In a priced binary market, it is incomplete.

If the bot buys UP at 0.70:

- win: +0.30
- loss: -0.70

The breakeven winrate is therefore roughly 70%.

The logged candidate stream's realized hit rate was lower than its average entry price. That makes the tested opportunity stream negative EV despite a high raw winrate.

## Priced-in signal vs overfitting

The primary diagnosis is not overfitting. The primary diagnosis is priced-in public information: the momentum signal was directionally useful, but the Polymarket price already charged more than that signal was worth.

Overfitting is the separate trap that appears after the main EV check fails.

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

The decision was to stop tuning because the main EV equation had already failed. Changing filters after seeing small retrospective buckets would have turned the project from measurement into data-snooping.

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

The strategy was closed as not deployable from the tested evidence.

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
