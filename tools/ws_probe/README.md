# ws_probe — standalone AX WebSocket order-entry probe

Empirically verifies, **before** any engine change is built on it, the proposed
order-flow design:

> On feed price change: cancel immediately → cancel ack arrives on the WS (not
> REST) → send both new orders immediately after the ack.

It produces measured evidence for four claims:

| Claim | What it checks |
|-------|----------------|
| **C1** | WS cancel ack (`t=x` → `c`) is fast (vs the ~210 ms REST round-trip). |
| **C2** | WS place (`t=p` → `n`) is fast (REST baseline ~60–69 ms). |
| **C3** | Terminal-event ordering is trustworthy, so a `c` ack can gate the re-add. |
| **C4** | `t=r` atomic replace inherits the `cid` with no window of zero/two live orders. |

This tool is **completely standalone**. It does not include, link, or modify
`MakeMarketStrategy`, `MmOrderMover`, `OrderManager`, `StartupSequence`, or any
`src/` module. It reuses only the vendored networking libraries (websocketpp +
standalone Asio + OpenSSL, libcurl, nlohmann/json) that the top-level build
already fetches.

## Build

```bash
cd bin/make && cmake ../.. -DBUILD_TESTS=ON && make ws_probe -j
# binary: bin/ax_bin/ws_probe
```

## Safety model (non-negotiable — funded account)

- Endpoint and credentials come **only** from env vars / flags. The probe never
  reads anything under `config/`.
- One instrument, `qty = 1` (minimum lot) by default.
- **All** orders are priced far from market: bids at `best_bid × (1 − far_offset)`
  (default `far_offset = 20%`), asks symmetric. The probe **refuses to start** if
  the computed price is within **5%** of the touch.
- `cancel_on_disconnect=true` is requested at login. A `t=X` cancel-all for the
  probe's own orders runs at normal exit and on `SIGINT`/`SIGTERM`. Because
  network I/O is not async-signal-safe, the signal handler flips a stop flag and
  the cancel-all runs on the main thread as soon as the current (bounded) wait
  returns.
- Every OID the probe creates is tracked; at exit it verifies via REST
  open-orders that none remain and prints a **red warning with the OIDs** if any do.
- Hard caps: `--iterations` ≤ 100 (default 50); ≤ 1 working order at a time in
  modes A/B, ≤ 2 in mode C; global runtime cap 10 minutes.
- A banner prints the endpoint, symbol, far offset and iteration count with
  `*** THIS WILL PLACE REAL ORDERS ***`, and the probe refuses to proceed
  without `--yes`.

## Live invocation (operator sandbox only)

```bash
export AX_WS_URL="wss://gateway.architect.exchange/orders/ws"
export AX_REST_URL="https://gateway.architect.exchange/api"
export AX_API_TOKEN="<sandbox token>"
export AX_PROBE_SYMBOL="<one instrument>"

./bin/ax_bin/ws_probe --yes --iterations 50 --far-offset 0.20 --qty 1
```

Useful flags: `--modes ABC` (subset), `--symbol/--ws-url/--rest-url/--token`
(override env), `--runtime-cap-sec`. Run `ws_probe --help` for the full list.

## Modes

- **A** — WS place-ack (C2) + cancel-ack (C1) + the "place immediately on the
  cancel ack" gate (`ack→next place`, should be ~0).
- **B** — REST baseline (place + cancel) on the same network path, for an
  apples-to-apples comparison table.
- **C** — ordering evidence (place then cancel; assert exactly one terminal
  event, no post-terminal events, monotonic sequence numbers) and atomic replace
  (`t=r` P1→P2; assert exactly one live order at P2 and that the `cid` inherits).
  The cancel-vs-fill race is **not** tested far from market; C3's fill-side is
  reported as pending the first live fill observation.

## Output

- `ws_probe_results.csv` — one row per measured event
  (`iter,mode,metric,transport,oid,cid,latency_ms`).
- `ws_probe_frames.jsonl` — **every** tx and rx frame with a monotonic
  `steady_clock` timestamp (`{"dir","ts_ns","payload"}`), captured before parsing.
  The wire protocol is treated as a hypothesis; if the venue differs, the raw
  frames here are the source of truth and the parser adapts tolerantly.
- A printed summary table (min/p50/p90/p99/max per metric) plus C1–C4 verdicts
  and a one-paragraph implication of the measured cancel→re-add gap.

## Dry-run / tests

`--dry-run` runs the full state machine against an in-process mock venue (no
network), asserting the measurement plumbing, the far-price refusal guard, and
the exit-cleanup path. This is the ctest entry point:

```bash
./bin/ax_bin/ws_probe --dry-run     # or: ctest -R ws_probe_dry_run
```

The live modes are never invoked from ctest.

## Protocol reference (hypothesis — verify on the wire)

`wss://gateway.architect.exchange/orders/ws`: `t=p` place, `t=x` cancel,
`t=r` atomic replace (inherits `cid`), `t=X` cancel-all; events `n` (new/ack),
`f` (fill), `c` (cancelled), `j` (rejected), `x`, `r`, `e`; the login response is
an open-orders snapshot. If reality differs, the frames log records it verbatim
and the tolerant parser adapts.
