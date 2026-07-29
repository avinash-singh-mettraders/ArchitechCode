# Market maker, external feed, and hedging

## 1. Market maker strategy – **in place**

- **Where:** `MakeMarketStrategy` in `include/strategy/MakeMarketStrategy.h` and `src/strategy/MakeMarketStrategy.cpp`.
- **Logic:**
  - Gets **theo** from the external feed (Neon FIX or REST mid) via `theo_provider_`.
  - Computes **bid** = mid − mid · `bid_width_bps` / 10000 − skew, **ask** = mid + mid · `ask_width_bps` / 10000 − skew (per-side bps offset on the pricing mid; with `bid_width_bps == ask_width_bps == W` the placed pair is `2·W` bps apart). Each side is then rounded to `price_tick` (bid floors, ask ceilings). When the desk pricer-snapshot transform is active (`quote_snapshot > 0 && pricer_snapshot > 0`), `mid` is `newMidpoint = qs + qs·slope·((theo − ps)/ps)`; otherwise the raw theo is passed through with an optional legacy `theo_scale` multiplier. `invert_theo` was removed 2026-05-12 — use the pricer-snapshot transform for any cross-scale legs.
  - Submits **limit buy** and **limit sell** to **Architect** via `submit_order()` → `ORDER_SUBMITTED` → REST `place_order`.
  - Tracks `bid_order_id_` / `ask_order_id_`; on accept/fill/cancel/reject updates or clears them.
  - When theo moves by more than one tick, **cancels** both quotes and resubmits on the next timer.
- **Driver:** In `examples/main.cpp`, the strategy is registered, `theo_provider_` is set to `main.externalFeed()->getTheoPrice(sym)`, and `fireTimer()` is called every `market_maker.update_interval_sec` (e.g. 2s).

---

## 2. External feed (Neon FIX or REST bookTicker) – **implemented**

- **Purpose:** External source used **only to price the two orders** (theo). Enables stress-testing the full flow (theo → quotes → place_order → fills → PnL). This feed will be **replaced per client** later with their own pricing source.
- **Where:** `ExternalFeedManager` in `include/marketdata/ExternalFeedManager.h` and `src/marketdata/ExternalFeedManager.cpp`.
- **Behaviour:**
  - If `external_feed.provider` is a Neon/Integral FIX variant, a background thread runs the FIX quote session (see `NeonFixFeed`).
  - Otherwise, if `external_feed.rest_url` is set, a thread **polls** REST bookTicker:
  - Spot-style path: `GET {rest_url}/api/v3/ticker/bookTicker?symbol={symbol}` when `rest_uses_spot_ticker_path` is true.
  - Futures-style path: `GET {rest_url}/fapi/v1/ticker/bookTicker?symbol={symbol}` when false.
  - Parses bid/ask, computes **mid**, stores last quote. `getTheoPrice(display_symbol)` returns that mid (used by the market maker).
- **Config:** `external_feed.provider`, `external_feed.rest_url`, `external_feed.rest_uses_spot_ticker_path`, `external_feed.symbol`, `external_feed.display_symbol`. Auth is not sent to the external API (`skip_auth` for external requests).

---

## 3. Hedging – **generic interface, empty default**

- **Design:** Hedging logic is **generic** and has **empty logic by default**. It is intended to be **connected to a hedging exchange per client** (e.g. another venue). This allows stress-testing the platform end-to-end without implementing a specific hedge execution path.
- **Where:**
  - **Interface:** `include/hedging/HedgeProvider.h` – `IHedgeProvider::onFill(const HedgeFillInfo&)` and `NoOpHedgeProvider` (default, does nothing).
  - **Wiring:** On every Architect fill, when `hedge.enabled` is true, the platform calls `hedge_provider_->onFill(info)`. The default provider is `NoOpHedgeProvider`; clients call `Main().setHedgeProvider(std::make_unique<MyHedgeProvider>())` to plug their own implementation that sends offsetting orders to their hedging venue.
- **Config:** `hedge.enabled` turns the hook on/off. When enabled, the generic provider is always invoked; the optional **webhook** (see below) is an additional path.
- **Optional webhook:** If `hedge.webhook_url` is set, the platform also **POSTs** the fill to that URL (async, no auth). Body is JSON: `symbol`, `side`, `quantity`, `price`, `order_id`. Useful for a simple HTTP receiver that places the hedge elsewhere; the **main extension point** for per-client hedging is the **HedgeProvider** implementation.

**Summary:** Use the default (no-op) to stress-test without sending hedges; replace with a custom `IHedgeProvider` implementation to connect to the client’s hedging exchange.
