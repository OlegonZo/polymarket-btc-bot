# Polymarket BTC 15M Momentum Research

Public research case for a Polymarket BTC Up/Down 15-minute momentum bot.

The full trading bot code, credentials, raw logs, and execution details are not published in this public repository. This repo is a sanitized portfolio version focused on methodology, data quality, and the final research conclusion.

## Status

Closed as a negative-edge strategy.

The bot is not approved for live BUY execution.

Final clean shadow result:

```text
resolved rows: 1485
winrate: 69.43%
average entry price: 70.38%
average virtual PnL: -0.00956
total virtual PnL: -14.20
```

Core result:

```text
expected PnL per share = winrate - average entry price
0.6943 - 0.7038 = -0.0095
```

The bot was directionally right often, but not often enough for the prices it paid. The market-implied probability was stronger than the tested public momentum signal stack.

## What This Repository Contains

- `PORTFOLIO_CASE.md` - full English case study
- `PORTFOLIO_POST_RU.md` - shorter Russian write-up
- `POSTMORTEM.md` - internal-style technical postmortem
- `DECISION_LOCK.md` - explicit stop/tuning lock

## What Is Not Published

- private keys or API credentials
- `.env` files
- full live trading code
- raw JSONL logs
- wallet/account details
- executable trading thresholds that are not needed for the public research conclusion

## Research Question

Can short-term BTC momentum, confirmed across Chainlink, Binance, and Polymarket order-book context, produce positive expected value in 15-minute BTC Up/Down markets?

Answer from the tested sample: no.

## Main Lesson

High winrate is not edge.

In priced binary markets, winrate only matters relative to the entry price. A 69% winrate can still be negative if the average entry price is above 69%.

## Decision

No live BUY.

No retrospective filter tuning.

The project is treated as a completed negative-edge research case unless a materially different hypothesis is defined before testing.
