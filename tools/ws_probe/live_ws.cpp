// =============================================================================
// ws_probe/live_ws.cpp — websocketpp(asio_tls_client) implementation.
// =============================================================================
#include "live_ws.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>

namespace wsprobe {

using WsClient = websocketpp::client<websocketpp::config::asio_tls_client>;

struct LiveWsTransport::Impl {
    std::string url;
    std::string token;
    bool cancel_on_disconnect;

    WsClient ws;
    websocketpp::connection_hdl hdl;
    std::thread run_thread;

    std::atomic<bool> open{false};
    std::atomic<bool> failed{false};
    std::string fail_reason;

    std::mutex snap_mu;
    std::condition_variable snap_cv;
    bool snap_captured = false;
    std::string snapshot;

    LiveWsTransport* owner = nullptr;
};

LiveWsTransport::LiveWsTransport(std::string url, std::string token, bool cancel_on_disconnect)
    : impl_(std::make_unique<Impl>()) {
    impl_->url = std::move(url);
    impl_->token = std::move(token);
    impl_->cancel_on_disconnect = cancel_on_disconnect;
    impl_->owner = this;
}

LiveWsTransport::~LiveWsTransport() { close(); }

bool LiveWsTransport::connect(std::string& err, std::string& login_snapshot) {
    auto& I = *impl_;
    I.ws.clear_access_channels(websocketpp::log::alevel::all);
    I.ws.clear_error_channels(websocketpp::log::elevel::all);
    I.ws.init_asio();
    I.ws.start_perpetual();

    I.ws.set_tls_init_handler([](websocketpp::connection_hdl) {
        auto ctx = websocketpp::lib::make_shared<websocketpp::lib::asio::ssl::context>(
            websocketpp::lib::asio::ssl::context::tls_client);
        try {
            ctx->set_default_verify_paths();
            ctx->set_verify_mode(websocketpp::lib::asio::ssl::verify_peer);
        } catch (...) {
            ctx->set_verify_mode(websocketpp::lib::asio::ssl::verify_none);
        }
        return ctx;
    });

    I.ws.set_open_handler([&I](websocketpp::connection_hdl h) {
        I.hdl = h;
        I.open.store(true);
    });

    I.ws.set_message_handler([&I](websocketpp::connection_hdl, WsClient::message_ptr msg) {
        // Capture receive timestamp IMMEDIATELY, before any parsing.
        const long long rx = nowNs();
        if (!msg) return;
        std::string payload = msg->get_payload();
        {
            std::lock_guard<std::mutex> lk(I.snap_mu);
            if (!I.snap_captured) {
                I.snap_captured = true;
                I.snapshot = payload;
                I.snap_cv.notify_all();
            }
        }
        if (I.owner && I.owner->on_frame) I.owner->on_frame(RxFrame{rx, std::move(payload)});
    });

    I.ws.set_fail_handler([&I](websocketpp::connection_hdl h) {
        I.failed.store(true);
        websocketpp::lib::error_code ec;
        auto con = I.ws.get_con_from_hdl(h, ec);
        I.fail_reason = con ? con->get_ec().message() : "ws fail";
        std::lock_guard<std::mutex> lk(I.snap_mu);
        I.snap_captured = true;
        I.snap_cv.notify_all();
    });

    I.ws.set_close_handler([&I](websocketpp::connection_hdl) { I.open.store(false); });

    websocketpp::lib::error_code ec;
    auto con = I.ws.get_connection(I.url, ec);
    if (ec || !con) {
        err = ec ? ec.message() : "failed to create ws connection";
        return false;
    }
    // Bearer credential on the upgrade request (venues commonly authenticate
    // the WS at the HTTP handshake; a login frame is also sent post-open).
    con->append_header("Authorization", "Bearer " + I.token);
    I.ws.connect(con);

    I.run_thread = std::thread([&I] { I.ws.run(); });

    // Wait for transport to come up (or fail) with a bounded timeout.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!I.open.load() && !I.failed.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (I.failed.load()) {
        err = I.fail_reason.empty() ? "ws handshake failed" : I.fail_reason;
        return false;
    }
    if (!I.open.load()) {
        err = "ws connect timed out";
        return false;
    }

    // Login frame (hypothesis form). If the venue authenticates purely via the
    // header above, this is harmless; the response is logged verbatim.
    json login{{"t", "login"}, {"token", I.token}};
    if (I.cancel_on_disconnect) login["cancel_on_disconnect"] = true;
    send(login);

    // The login response == open-orders snapshot: capture the first inbound
    // frame within a short window (may legitimately be empty).
    {
        std::unique_lock<std::mutex> lk(I.snap_mu);
        I.snap_cv.wait_for(lk, std::chrono::seconds(3), [&I] { return I.snap_captured; });
        login_snapshot = I.snapshot;
    }
    return true;
}

long long LiveWsTransport::send(const json& msg) {
    auto& I = *impl_;
    if (!I.open.load()) return 0;
    const std::string payload = msg.dump();
    websocketpp::lib::error_code ec;
    // Capture tx timestamp at the socket write, before checking the result.
    const long long tx = nowNs();
    I.ws.send(I.hdl, payload, websocketpp::frame::opcode::text, ec);
    if (ec) return 0;
    return tx;
}

void LiveWsTransport::close() {
    auto& I = *impl_;
    if (I.open.exchange(false)) {
        websocketpp::lib::error_code ec;
        I.ws.close(I.hdl, websocketpp::close::status::normal, "probe shutdown", ec);
    }
    I.ws.stop_perpetual();
    if (I.run_thread.joinable()) I.run_thread.join();
}

bool LiveWsTransport::connected() const { return impl_->open.load(); }

}  // namespace wsprobe
