#pragma once

/**
 * @file HedgeProvider.h
 * @brief Generic hedging interface – empty default logic, plug per-client hedging exchange
 *
 * Used to stress-test the platform: on every Architect fill the platform calls
 * the hedge provider. Default implementation is no-op; clients replace it with
 * their own implementation that sends offsetting orders to their hedging venue
 * (e.g. another exchange or broker).
 */

#include "core/Types.h"

#include <string>
#include <memory>

namespace architect {
namespace hedging {

using namespace core;

/**
 * @brief Fill information passed to the hedge provider when an order fills on the primary venue (e.g. Architect).
 */
struct HedgeFillInfo {
    std::string symbol;
    Side        side         = Side::BUY;
    Quantity    quantity     = 0.0;
    Price       price        = 0.0;
    OrderId     order_id     = 0;
};

/**
 * @brief Generic hedging interface.
 *
 * Implement this and set it on the platform (e.g. Main().setHedgeProvider(...))
 * to send hedge orders to your exchange. Default is NoOpHedgeProvider (no action).
 */
class IHedgeProvider {
public:
    virtual ~IHedgeProvider() = default;

    /**
     * @brief Called on every fill on the primary venue when hedge.enabled is true.
     * Override to send an offsetting order to your hedging exchange.
     */
    virtual void onFill(const HedgeFillInfo& fill) = 0;
};

/**
 * @brief No-op hedge provider (default). Does nothing; used when no per-client hedging is connected.
 */
class NoOpHedgeProvider : public IHedgeProvider {
public:
    void onFill(const HedgeFillInfo& /* fill */) override {}
};

} // namespace hedging
} // namespace architect
