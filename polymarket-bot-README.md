# Polymarket BTC 15M Trading Bot

Automated trading bot for the **BTC Up/Down 15-minute** prediction market on Polymarket. Designed to catch BTC momentum before the crowd, confirm it across data sources, avoid local pumps, and exit automatically when conditions deteriorate.

## Core Idea

- Catch BTC impulse moves early
- Confirm direction through a second exchange feed (Binance) rather than trusting a single source
- Avoid entering into local pumps and tops
- Exit automatically when the market turns
- Maximize win rate without collapsing trade frequency

## Multi-Layer Entry System

The bot makes entry decisions through a stacked filter pipeline, not a single signal:

1. **Chainlink Signal** — primary BTC/USD price direction via `move_pct`
2. **Binance Confirmation** — cross-checks direction using 5s/10s/30s moves, volume spikes, and acceleration
3. **Entry Score V1** — rates each signal up to 12 points (Chainlink strength, multi-window moves, acceleration, volume spike, orderbook imbalance, spread, entry price)
4. **Forecast Score 15M** — secondary forecast model, stricter on expensive entries
5. **Binance Entry Quality Guard** — one of the strictest filters; blocks low volume-ratio entries
6. **Cheap Entry Reversal Guard** — estimates reversal probability to avoid buying tops
7. **Anti Fake Pump Filter** — blocks entries into local pumps
8. **Orderbook Guards (V2/V3)** — validate bid/ask depth, imbalance, spread, fill ratio
9. **Binance Armed Mode** — prioritizes signals during confirmed market acceleration
10. **Fast Entry Window** — entries only allowed in a defined window after a new market opens

## Exit System

A dedicated exit monitor implements multiple exit strategies:

- **Take Profit** — locks in gains
- **Trailing Exit** — exits on pullback from local high
- **Predictive Exit** — early exit when the picture deteriorates
- **Defensive / Hard Defensive Exit** — protective and emergency exits
- **Binance Reversal Exit** — exits when the confirming feed turns against the position
- **Ladder Sell** — scaled exit

Includes both UP and DOWN bot variants with mirrored exit logic.

## Shadow Logging & Analytics System

A custom analytics layer logs and evaluates every **blocked** trade to measure whether each filter actually adds edge:

- **Shadow Logger** — records every blocked entry with full context and which filters blocked it
- **Resolver** — replays blocked signals against actual market outcomes (virtual win/PnL)
- **Analyzer** — surfaces which filters block most often and what the result would have been without them

### Next Engineering Phase: Independent Filter Attribution

The current pipeline uses short-circuit evaluation (first failing filter stops execution), which makes it impossible to know which filters are genuinely useful vs. which cut good trades. The roadmap:

1. Independent Filter Attribution — evaluate every filter regardless of earlier failures
2. Resolver against real market outcomes
3. Filter Impact Analysis
4. `simulate_remove(filter)` to measure each filter's true contribution
5. Wilson Confidence Interval for statistical significance
6. LOW_CONFIDENCE flag when sample size < 30

## Tech Stack

- **Language:** Python
- **Data Sources:** Chainlink BTC/USD, Binance (price, volume, orderbook)
- **Execution:** Polymarket CLOB API
- **Analytics:** Custom JSON shadow-logging and attribution system

## Status

- **UP bot:** live, trading real funds, full exit system, shadow logging active
- **DOWN bot:** running with mirrored exit monitor; entry filters being refined
- **Analytics:** shadow logging + resolver + attribution foundation in place

---

*Strategy parameters and signal logic kept private. Architecture and methodology available for discussion in interviews.*
