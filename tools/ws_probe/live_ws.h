// =============================================================================
// ws_probe/live_ws.h — live AX order-entry WebSocket transport (TLS).
// Reuses the repo's vendored websocketpp + standalone Asio + OpenSSL stack
// (same one src/marketdata uses). All websocketpp/asio machinery is hidden
// behind a pimpl so its heavy templates only compile in live_ws.cpp.
// =============================================================================
#pragma once

#include <memory>
#include <string>

#include "transport.h"

namespace wsprobe {

class LiveWsTransport : public WsTransport {
public:
    // `url` is wss://... ; `token` is the bearer credential; if
    // `cancel_on_disconnect` is true the login frame requests venue-side
    // auto-cancel of this session's orders on disconnect.
    LiveWsTransport(std::string url, std::string token, bool cancel_on_disconnect);
    ~LiveWsTransport() override;

    bool connect(std::string& err, std::string& login_snapshot) override;
    long long send(const json& msg) override;
    void close() override;
    bool connected() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace wsprobe
