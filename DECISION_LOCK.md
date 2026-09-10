# Decision Lock - Polymarket BTC 15m Momentum Bot

Date: 2026-06-25

## Locked Decision

Do not enable live BUY for this bot.

Do not tune filters in this strategy.

The logged directional opportunity stream is negative:

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
- weakening cheap-entry reversal logic from a small positive-looking blocked bucket;
- changing forecast-style blocks from recent small samples;
- changing Binance-quality guards from recent small samples;
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

## Rebuild Protocol: Shadow-Only Strategy

The original private implementation cannot be reconstructed from this
sanitized repository. Any replacement is a **new research strategy**, not a
continuation of the previous one.

Before the first shadow record is collected:

1. The complete entry-filter registry, formulas, thresholds, input fields, and
   version identifier must be written down and explicitly approved by the
   operator.
2. Every filter must run through independent, non-short-circuit evaluation
   (`evaluate_all`) against the same immutable snapshot.
3. The proposed filter set and thresholds must remain unchanged for at least
   30 calendar days of a named cohort. Any change starts a new cohort.

During collection:

4. Shadow logging records every candidate snapshot, including entries that
   would pass and entries blocked by one or more filters.
5. The cohort name is fixed before launch and is never renamed. The default
   form is `attribution-forward-YYYYMMDD`.
6. Live BUY remains disabled.
7. The unit of statistical inference is a pre-registered `episode_id`, not an
   individual snapshot row. When several solo-blocked rows occur in one
   episode, the sole representative is the one with the smallest `(ts,
   snapshot_id)` tuple. This selection rule is immutable for the cohort.
8. Any `min_n` threshold for attribution counts independent episode
   representatives. In particular, `min_n=30` means 30 distinct episodes, not
   30 rows.
9. Every future cohort evaluation attempt must write a collection heartbeat,
   including no-candidate and failed-input attempts. A report is invalid if
   there is an evidence gap greater than the pre-registered 300 seconds.

After collection:

10. No live-entry decision may be considered before `report()` is run using
   Wilson intervals with Bonferroni correction, solo-block attribution,
   drop-best-episode stability, leave-one-episode-out stability, and
   leave-one-UTC-day-out stability checks.
