# Decision Lock - Polymarket BTC 15m Momentum Bot

Date: 2026-06-25

## Locked Decision

Do not enable live BUY for this bot.

Do not tune filters in this strategy.

The tested directional edge is negative:

```text
winrate 69.43% - avg_entry 70.38% = -0.95% per share
```

This is the controlling fact.

## Only Allowed Bot Work

- keep shadow mode running to collect exit statistics;
- run reports;
- write the research/portfolio case;
- archive the project.

## Blocked Work

- changing thresholds because a small bucket looks good;
- weakening `CHEAP_ENTRY_REVERSAL_GUARD_V1`;
- changing `FORECAST_SCORE_15M_BLOCK` from recent small samples;
- changing `BINANCE_ENTRY_QUALITY_GUARD_V1` from recent small samples;
- enabling real BUY;
- calling more log growth "progress" unless it tests a pre-defined question.

## Reopen Criteria

This project can only be reopened for active strategy work if there is a new hypothesis that is materially different from the tested directional momentum idea.

A valid new test must define before running:

- exact entry rule;
- exact exit rule;
- minimum sample size;
- success metric;
- stop condition.

No retrospective filter search.
