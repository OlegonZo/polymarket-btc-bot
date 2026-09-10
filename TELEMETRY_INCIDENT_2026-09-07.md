# WebSocket evidence — 2026-09-07

This note covers the active telemetry-only run
`aee5c46a-b1a5-47ba-8eec-fef8ecc70e93`. It is an operational observation, not
an edge result and not a reason to change the approved strategy parameters.

On 2026-09-10 the run was reclassified as incomplete: its final snapshot was
2026-09-09 07:53:26 UTC and the resulting collection gap exceeds 300 seconds.
It remains valid for descriptive frequency work but cannot pass the final
coverage gate or serve as the forward strategy cohort.

## What the recorded errors show

At the time of review, the dominant Binance feed failures were:

- 504 `WebSocketConnectionClosedException`: remote host connection lost;
- 161 `WebSocketTimeoutException`: no message before the receive timeout;
- 37 DNS resolution failures (`getaddrinfo`);
- 32 TLS handshake timeouts.

The collector rejected the cached Binance quote during each gap and stored the
gap plus its cause. This is why these events reduce completeness rather than
quietly manufacturing a fresh-looking price. The observed snapshot-cycle p50
and p95 were well below one second, so the main problem is transport recovery,
not ordinary cycle speed or SQLite throughput.

The current 14-day run remains unchanged. Mixing a new reconnect policy into
its middle would make its calibration population ambiguous.

## Repair implemented for the next run

The future-only `FailoverBinanceDepthFeed` now rotates through an explicit
ordered endpoint list and writes endpoint switches and disconnect causes to
`shadow_transport_events`. It keeps the ordinary sequence and freshness checks:
a reconnect cannot reuse the cached quote before a newly synchronized depth
stream publishes one.

Backoff now grows from the configured initial delay to
`ws_max_backoff_seconds`. `ws_max_reconnect_attempts` is retained only as a
logged threshold; it no longer silently caps the delay. The default primary is
the documented Spot endpoint. The secondary market-data endpoint is a fallback
only and remains subject to acceptance below.

## Acceptance blocker before any new strategy cohort

Run the future collector alone for 30 minutes, without registering a strategy
cohort or recording PnL. Accept it only if both endpoints, when used, provide
valid diff-depth `E` event times and update-ID sequencing, and the resulting
transport evidence reports reconnect count, endpoint switches, DNS/TLS/remote
close breakdown, gap p50/p95 and maximum gap. Also report the fraction of
attempts rejected for missing or stale Binance quotes. Any endpoint that cannot
meet this contract must be removed before the separate cohort approval.

Live preflight on 2026-09-10 confirmed that Gamma exposes the current BTC
market fee configuration as `feesEnabled=true` and `feeSchedule` with rate
`0.07`, exponent `1`, and `takerOnly=true`. The primary Binance stream returned
HTTP 451 from this host. The market-data fallback therefore pairs
`wss://data-stream.binance.vision` with `https://data-api.binance.vision` for
the depth snapshot and server-time synchronization. Mixing the fallback stream
with the restricted primary REST domain does not produce a valid source. The
30-minute result is written to `reports/source_acceptance_2026-09-10.json`.

The official stream specification confirms that diff-depth provides event time
`E` and supports 100 ms updates. It remains the correct source type for the
750 ms freshness rule; a REST book-ticker substitute would not supply the same
verifiable timestamp. See [Binance Spot WebSocket market streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).
