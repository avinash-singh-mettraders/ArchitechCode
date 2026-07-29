#include "marketdata/ExternalFeedManager.h"
#include "marketdata/NeonFixFeed.h"
#include "config/Config.h"
#include "api/RestClient.h"
#include "utils/Logger.h"
#include <nlohmann/json.hpp>
#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <optional>
#include <unordered_map>

#ifndef _WIN32
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace architect {
namespace marketdata {

namespace fs = std::filesystem;

#ifndef _WIN32

namespace {

bool tcp_port_accepting(const std::string& host, int port, int timeout_ms) {
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    const std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) {
        return false;
    }
    bool connected = false;
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        const int fd = static_cast<int>(socket(p->ai_family, p->ai_socktype, p->ai_protocol));
        if (fd < 0) {
            continue;
        }
        const int flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) {
            fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        const int cr = connect(fd, p->ai_addr, p->ai_addrlen);
        if (cr == 0) {
            connected = true;
            close(fd);
            break;
        }
        if (errno != EINPROGRESS) {
            close(fd);
            continue;
        }
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(fd, &wfds);
        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        const int sel = select(fd + 1, nullptr, &wfds, nullptr, &tv);
        if (sel > 0) {
            int so_err = 0;
            socklen_t len = sizeof(so_err);
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
            connected = (so_err == 0);
        }
        close(fd);
        if (connected) {
            break;
        }
    }
    freeaddrinfo(res);
    return connected;
}

} // namespace

#endif // !_WIN32

using json = nlohmann::json;
using MettradersWsClient = websocketpp::client<websocketpp::config::asio_tls_client>;

namespace {

std::string mettradersHandshakeFailureDetail(MettradersWsClient& ws,
                                             websocketpp::connection_hdl hdl,
                                             const std::string& url) {
    std::ostringstream o;
    o << "url=" << url;
    try {
        const auto c = ws.get_con_from_hdl(hdl);
        if (!c) {
            o << " connection=null";
            return o.str();
        }
        const auto ec = c->get_ec();
        o << " ec=" << ec.message() << "(" << ec.value() << ")";
        const int http_status = static_cast<int>(c->get_response_code());
        o << " http_status=" << http_status;
        o << " status_text=\"" << c->get_response_msg() << "\"";
        o << " headers={";
        static const char* kHdr[] = {"Server",
                                     "Date",
                                     "Content-Type",
                                     "Content-Length",
                                     "WWW-Authenticate",
                                     "Strict-Transport-Security",
                                     "CF-Ray",
                                     "Via",
                                     "X-Request-Id",
                                     "Connection",
                                     "Upgrade",
                                     "Sec-WebSocket-Accept"};
        bool first = true;
        for (const char* k : kHdr) {
            const std::string& v = c->get_response_header(k);
            if (v.empty()) {
                continue;
            }
            if (!first) {
                o << " | ";
            }
            first = false;
            o << k << "=" << v;
        }
        o << "}";
        std::string raw_preview;
        try {
            raw_preview = c->get_response().raw();
        } catch (...) {
        }
        if (raw_preview.size() > 900) {
            raw_preview.resize(900);
            raw_preview += "...";
        }
        for (char& ch : raw_preview) {
            if (ch == '\r' || ch == '\n' || ch == '\t') {
                ch = ' ';
            }
        }
        o << " body_preview=\"" << raw_preview << "\"";
    } catch (const std::exception& ex) {
        o << " detail_exception=" << ex.what();
    } catch (...) {
        o << " detail_exception=unknown";
    }
    return o.str();
}

std::string toUpperAlnumKey(std::string_view s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char ch : s) {
        if (ch == '/' || ch == '-' || ch == ' ' || ch == '_') {
            continue;
        }
        o.push_back(static_cast<char>(std::toupper(ch)));
    }
    return o;
}

void splitComma(const std::string& s, std::vector<std::string>& out) {
    std::string cur;
    for (char ch : s) {
        if (ch == ',') {
            if (!cur.empty()) {
                out.push_back(cur);
            }
            cur.clear();
        } else {
            cur += ch;
        }
    }
    if (!cur.empty()) {
        out.push_back(cur);
    }
}

void addUnique(std::vector<std::string>& v, const std::string& x) {
    const std::string u = toUpperAlnumKey(x);
    if (u.empty()) {
        return;
    }
    for (const auto& a : v) {
        if (a == u) {
            return;
        }
    }
    v.push_back(u);
}

/** Case-fold Hyperliquid allMids keys; preserve '-' (e.g. silver-usdc). */
void addHlMidsKey(std::vector<std::string>& v, const std::string& raw) {
    std::string t;
    for (const unsigned char ch : raw) {
        if (ch == ' ' || ch == '\t') {
            continue;
        }
        t.push_back(static_cast<char>(std::toupper(ch)));
    }
    if (t.empty()) {
        return;
    }
    for (const auto& a : v) {
        if (a == t) {
            return;
        }
    }
    v.push_back(std::move(t));
}

/**
 * Map reference_fix_symbol to Hyperliquid allMids key candidates (in order).
 * theo_venue_symbol (e.g. "@279" or "silver-usdc") is probed first when set.
 *
 * IMPORTANT: when `theo_venue_symbol` is set, ONLY those candidates are used —
 * we skip the auto-derived reference_fix_symbol fallbacks. Otherwise short
 * fallback tokens like "SP" / "SPX" silently match unrelated HL altcoins
 * (e.g. SPX coin ≈ $0.38, NOT the S&P 500). The user's `theo_venue_symbol` is
 * the authoritative override; treat it as such.
 */
void appendHyperliquidMidsKeyCandidates(
    const architect::config::MarketMakerInstrumentLeg& leg, std::vector<std::string>& cands) {
    if (!leg.theo_venue_symbol.empty()) {
        std::vector<std::string> parts;
        splitComma(leg.theo_venue_symbol, parts);
        for (const auto& p : parts) {
            if (!p.empty()) {
                addHlMidsKey(cands, p);
            }
        }
        // Explicit override — do NOT add reference_fix_symbol auto-fallbacks
        // that could shadow the user's intent with a wrong-asset match.
        return;
    }
    const std::string r = toUpperAlnumKey(leg.reference_fix_symbol);
    if (r.empty() && cands.empty()) {
        return;
    }
    if (r == "USDJPY" || r == "JPY") {
        addHlMidsKey(cands, "xyz:JPY");
        addUnique(cands, "JPY");
        addUnique(cands, "USDJPY");
    } else if (r == "XAU" || r == "GOLD") {
        // HL HIP-3 "xyz" sub-DEX (real-world commodity perps)
        addHlMidsKey(cands, "xyz:GOLD");
        addUnique(cands, "GOLD");
        addUnique(cands, "XAU");
    } else if (r == "XAG" || r == "SILVER") {
        addHlMidsKey(cands, "xyz:SILVER");
        addUnique(cands, "SILVER");
        addUnique(cands, "XAG");
        addHlMidsKey(cands, "silver-usdc");
    } else if (r == "WTI" || r == "WTIOIL" || r == "USOIL") {
        // CL = WTI Crude Light futures on HL xyz dex
        addHlMidsKey(cands, "xyz:CL");
        addHlMidsKey(cands, "xyz:WTI");
        addUnique(cands, "WTI");
        addUnique(cands, "WTIOIL");
        addHlMidsKey(cands, "wtioil-usdc");
    } else if (r == "BRENT" || r == "BRNTOIL") {
        addHlMidsKey(cands, "xyz:BRENTOIL");
        addUnique(cands, "BRENT");
    } else if (r == "NATGAS" || r == "NATURALGAS" || r == "GAS" || r == "NG" || r == "NGAS") {
        addHlMidsKey(cands, "xyz:NG");
        addUnique(cands, "GAS");
    } else if (r == "SPX" || r == "SP500" || r == "ES" || r == "SP" || r == "SPF") {
        addHlMidsKey(cands, "xyz:SP500");
        addUnique(cands, "SPX");
        addUnique(cands, "SP");
    } else if (!r.empty()) {
        addUnique(cands, leg.reference_fix_symbol);
        addUnique(cands, r);
    }
}

} // namespace

ExternalFeedManager& ExternalFeedManager::getInstance() {
    static ExternalFeedManager instance;
    return instance;
}

ExternalFeedManager::ExternalFeedManager() {
    auto& config = config::Config::getInstance();
    enabled_ = config.isExternalFeedEnabled();
    display_symbol_ = config.getExternalFeedDisplaySymbol();
}

ExternalFeedManager::~ExternalFeedManager() {
    stop();
}

bool ExternalFeedManager::isNeonFixProvider(const std::string& provider) {
    std::string p = provider;
    for (char& c : p) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return p == "neon_fix" || p == "fix_neon" || p == "integral_fix" || p == "fix_integral";
}

bool ExternalFeedManager::isHyperliquidProvider(const std::string& provider) {
    std::string p = provider;
    for (char& c : p) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return p == "hyperliquid" || p == "hyper_liquid" || p == "hl";
}

void ExternalFeedManager::ensureStunnelOrThrow() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getExternalFeedFixStunnelAutostart()) {
        return;
    }
#ifdef _WIN32
    throw std::runtime_error(
        "Neon FIX: stunnel autostart is not supported on Windows. Set "
        "external_feed.fix.stunnel.autostart=false and start stunnel manually, "
        "or run on macOS/Linux.");
#else
    const std::string conf = cfg.getExternalFeedFixStunnelConfig();
    if (conf.empty()) {
        throw std::runtime_error(
            "Neon FIX: external_feed.fix.stunnel.autostart is true but "
            "external_feed.fix.stunnel.config is empty (path to .conf required)");
    }
    if (!fs::exists(conf)) {
        throw std::runtime_error("Neon FIX: stunnel config file not found: " + conf);
    }
    const std::string host = cfg.getExternalFeedFixHost();
    const int port = cfg.getExternalFeedFixPort();
    if (tcp_port_accepting(host, port, 400)) {
        return;
    }
    const std::string exe = cfg.getExternalFeedFixStunnelExecutable();
    const int timeout_ms = std::max(3000, cfg.getExternalFeedFixStunnelStartTimeoutMs());
    std::string log_path = cfg.getExternalFeedFixStunnelLogFile();
    if (log_path.empty()) {
        log_path = "logs/stunnel_autostart.log";
    }
    {
        fs::path lp(log_path);
        if (lp.has_parent_path()) {
            std::error_code ec;
            fs::create_directories(lp.parent_path(), ec);
        }
    }

    const pid_t child = fork();
    if (child < 0) {
        throw std::runtime_error(std::string("Neon FIX: fork() for stunnel failed: ") + std::strerror(errno));
    }
    if (child == 0) {
        setsid();
        const int log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (log_fd >= 0) {
            dup2(log_fd, STDOUT_FILENO);
            dup2(log_fd, STDERR_FILENO);
            close(log_fd);
        }
        execlp(exe.c_str(), exe.c_str(), conf.c_str(), nullptr);
        _exit(127);
    }

    stunnel_pid_ = static_cast<int>(child);
    stunnel_we_started_ = true;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        int st = 0;
        const pid_t w = waitpid(child, &st, WNOHANG);
        if (w == child) {
            stunnel_pid_ = -1;
            stunnel_we_started_ = false;
            if (WIFEXITED(st) && WEXITSTATUS(st) == 127) {
                throw std::runtime_error(
                    "Neon FIX: could not exec '" + exe +
                    "' — install stunnel and ensure it is in PATH (e.g. brew install stunnel). Log: " +
                    log_path);
            }
            std::string tail;
            {
                std::ifstream in(log_path);
                if (in) {
                    in.seekg(0, std::ios::end);
                    const auto n = in.tellg();
                    if (n > 0 && n < 4000) {
                        in.seekg(0);
                    } else if (n > 0) {
                        in.seekg(-3000, std::ios::end);
                    }
                    tail.assign(std::istreambuf_iterator<char>(in), {});
                }
            }
            std::string msg = "Neon FIX: stunnel exited before binding port " + std::to_string(port) +
                ". Check " + log_path + " and stunnel config " + conf;
            if (!tail.empty()) {
                msg += " | log tail: ";
                msg += tail.substr(0, 500);
            }
            throw std::runtime_error(msg);
        }
        if (tcp_port_accepting(host, port, 300)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    kill(child, SIGTERM);
    for (int i = 0; i < 30; ++i) {
        int st = 0;
        if (waitpid(child, &st, WNOHANG) == child) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    stunnel_pid_ = -1;
    stunnel_we_started_ = false;
    throw std::runtime_error(
        "Neon FIX: stunnel did not open " + host + ":" + std::to_string(port) + " within " +
        std::to_string(timeout_ms) + "ms. See " + log_path);
#endif
}

void ExternalFeedManager::stopManagedStunnel() {
#ifndef _WIN32
    if (!stunnel_we_started_ || stunnel_pid_ <= 0) {
        return;
    }
    const pid_t pid = static_cast<pid_t>(stunnel_pid_);
    kill(pid, SIGTERM);
    for (int i = 0; i < 40; ++i) {
        int st = 0;
        if (waitpid(pid, &st, WNOHANG) == pid) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    stunnel_pid_ = -1;
    stunnel_we_started_ = false;
#endif
}

void ExternalFeedManager::start() {
    if (!enabled_.load()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    if (cfg.isMultiTheoFeeds() && cfg.hasMarketMakerInstrumentList()) {
        // Multi-feed mode: ALWAYS attempt all three transports (Mettraders WS,
        // Hyperliquid REST, Neon FIX) regardless of which legs need them.
        // The startup gate (step 22) crashes the process only when 0/3 come
        // up; a single live feed is enough to continue, with dead-feed legs
        // disabled at quote time. Per-leg `legs_need_*` is still tracked for
        // diagnostics (see `isPricingTransportConnected`).
        const auto legs = cfg.getMarketMakerInstruments();
        bool legs_need_neon = false;
        bool legs_need_hl = false;
        bool legs_need_mettraders = false;
        for (const auto& leg : legs) {
            const std::string r = cfg.getResolvedTheoSourceForLeg(leg);
            if (r == "neon_fix") {
                legs_need_neon = true;
            } else if (r == "hyperliquid") {
                legs_need_hl = true;
            } else if (r == "mettraders") {
                legs_need_mettraders = true;
            }
        }
        (void)legs_need_neon; (void)legs_need_hl;

        try {
            ensureStunnelOrThrow();
        } catch (const std::exception& e) {
            // Stunnel for Neon failed: log + mark Neon down, keep CME/HL alive.
            neon_up_.store(false);
            fix_session_logged_on_.store(false);
            {
                std::lock_guard<std::mutex> lock(feed_err_mu_);
                neon_last_err_ = std::string("stunnel: ") + e.what();
            }
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().warn(
                    "[FEED:Neon] stunnel start failed — Neon FIX will be marked down: {}",
                    e.what());
            }
        }
        if (running_.exchange(true)) {
            return;
        }
        // Neon FIX thread (only meaningful if md_symbols configured; loop
        // exits early on misconfig and leaves neon_up_=false).
        poll_thread_ = std::thread(&ExternalFeedManager::fixFeedLoop, this);
        // Combined Mettraders + HL poll thread.
        aux_poll_thread_ = std::thread(&ExternalFeedManager::auxRestPollLoop, this);
        // Only spin up the Mettraders WS thread when at least one leg has
        // theo_source=mettraders AND the feed is enabled. Previously the
        // thread started unconditionally, which (a) wasted a thread when
        // no leg consumed it and (b) polluted logs with handshake failures
        // (Cloudflare 530) against the default endpoint when no operator
        // even wanted the feed running.
        if (legs_need_mettraders && cfg.getMettradersFeedEnabled() &&
            !cfg.getMettradersFeedUrl().empty()) {
            mettraders_thread_ = std::thread(&ExternalFeedManager::mettradersFeedLoop, this);
        } else if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().info(
                "[FEED:Mettraders] skipping ws thread — "
                "legs_need_mettraders={} feed_enabled={} url_empty={}",
                legs_need_mettraders ? "true" : "false",
                cfg.getMettradersFeedEnabled() ? "true" : "false",
                cfg.getMettradersFeedUrl().empty() ? "true" : "false");
        }
        return;
    }
    if (isNeonFixProvider(cfg.getExternalFeedProvider())) {
        ensureStunnelOrThrow();
    }
    if (running_.exchange(true)) {
        return;
    }
    if (isNeonFixProvider(cfg.getExternalFeedProvider())) {
        poll_thread_ = std::thread(&ExternalFeedManager::fixFeedLoop, this);
        return;
    }
    fetchAndUpdate();
    poll_thread_ = std::thread(&ExternalFeedManager::pollLoop, this);
}

void ExternalFeedManager::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (poll_thread_.joinable()) {
        poll_thread_.join();
    }
    if (aux_poll_thread_.joinable()) {
        aux_poll_thread_.join();
    }
    if (mettraders_thread_.joinable()) {
        mettraders_thread_.join();
    }
    stopManagedStunnel();
}

void ExternalFeedManager::fixFeedLoop() {
    auto& cfg = config::Config::getInstance();
    NeonFixSettings s;
    s.host = cfg.getExternalFeedFixHost();
    s.port = cfg.getExternalFeedFixPort();
    s.sender_comp_id = cfg.getExternalFeedFixSenderCompId();
    s.target_comp_id = cfg.getExternalFeedFixTargetCompId();
    s.deliver_to_comp_id = cfg.getExternalFeedFixDeliverToCompId();
    s.sender_sub_id = cfg.getExternalFeedFixSenderSubId();
    s.username = cfg.getExternalFeedFixUsername();
    s.password = cfg.getExternalFeedFixPassword();
    s.heart_bt_int = cfg.getExternalFeedFixHeartBtInt();
    s.md_update_type = cfg.getExternalFeedFixMdUpdateType();
    s.md_instrument_extra = cfg.getExternalFeedFixMdInstrumentExtra();
    s.logon_extra = cfg.getExternalFeedFixLogonExtra();
    s.md_request_root_extra = cfg.getExternalFeedFixMdRequestRootExtra();
    s.md_snapshot_only = cfg.getExternalFeedFixMdSnapshotOnly();
    s.md_market_depth = cfg.getExternalFeedFixMdMarketDepth();
    s.pricing_symbol = cfg.getExternalFeedSymbol();
    s.md_symbols = cfg.getExternalFeedFixMdSymbols();
    if (s.pricing_symbol.empty() || s.sender_comp_id.empty() || s.target_comp_id.empty() ||
        s.username.empty()) {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        last_fetch_error_ =
            "Neon FIX: set external_feed.symbol (FIX 55) and external_feed.fix "
            "sender_comp_id, target_comp_id, username";
        ++stats_.error_count;
        running_.store(false);
        return;
    }
    bool found = false;
    for (const auto& sym : s.md_symbols) {
        if (sym == s.pricing_symbol) {
            found = true;
            break;
        }
    }
    if (!found) {
        s.md_symbols.insert(s.md_symbols.begin(), s.pricing_symbol);
    }
    if (cfg.hasMarketMakerInstrumentList() && cfg.isMultiTheoFeeds()) {
        std::string first_neon;
        for (const auto& leg : cfg.getMarketMakerInstruments()) {
            if (cfg.getResolvedTheoSourceForLeg(leg) == "neon_fix" && !leg.reference_fix_symbol.empty()) {
                if (first_neon.empty()) {
                    first_neon = leg.reference_fix_symbol;
                }
            }
        }
        if (!first_neon.empty()) {
            s.pricing_symbol = first_neon;
        }
    }
    if (cfg.hasMarketMakerInstrumentList()) {
        auto merge_md = [&s](const std::string& sym) {
            if (sym.empty()) {
                return;
            }
            const std::string want = marketdata::fixSymbolCanonical(sym);
            for (const auto& x : s.md_symbols) {
                if (marketdata::fixSymbolCanonical(x) == want) {
                    return;
                }
            }
            s.md_symbols.push_back(sym);
        };
        for (const auto& leg : cfg.getMarketMakerInstruments()) {
            if (cfg.isMultiTheoFeeds()) {
                if (cfg.getResolvedTheoSourceForLeg(leg) == "neon_fix" && !leg.reference_fix_symbol.empty()) {
                    merge_md(leg.reference_fix_symbol);
                }
            } else {
                merge_md(leg.reference_fix_symbol);
            }
        }
    }

    runNeonFixFeed(
        s,
        running_,
        [this](const ExternalFeedQuote& q) {
            publishQuote(q);
            // Per-feed liveness for the desk pill row + per-leg gate.
            if (q.valid) {
                const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                last_neon_ok_ms_.store(now_ms);
                neon_up_.store(true);
                if (!neon_sanity_logged_.exchange(true) && utils::Logger::isInitialized()) {
                    utils::Logger::getInstance().info(
                        "[FEED_SANITY:Neon] first quote ok sym={} mid={}",
                        q.fix_symbol,
                        q.mid);
                }
                {
                    std::lock_guard<std::mutex> lock(feed_err_mu_);
                    neon_last_err_.clear();
                }
            }
        },
        [this](std::string err) {
            fix_session_logged_on_.store(false);
            neon_up_.store(false);
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                last_fetch_error_ = err;
                ++stats_.error_count;
            }
            {
                std::lock_guard<std::mutex> lock(feed_err_mu_);
                neon_last_err_ = std::move(err);
            }
            recordQuoteErrorForProtectiveInvalidate();
        },
        [this](const std::vector<std::pair<double, double>>& bids, const std::vector<std::pair<double, double>>& asks) {
            maybeWriteDeskDepthCache(bids, asks);
        },
        [this](bool up) {
            fix_session_logged_on_.store(up);
            if (!up) {
                neon_up_.store(false);
            }
        });
}

void ExternalFeedManager::writeDeskTheoCacheLocked() const {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getBool("external_feed.write_desk_theo_cache", true)) {
        return;
    }
    if (!last_quote_.valid && quotes_by_canonical_.empty()) {
        return;
    }
    const std::string rel = cfg.getString("external_feed.desk_theo_cache_path", "logs/mm_external_theo.json");
    fs::path out_path(rel);
    if (!out_path.is_absolute()) {
        out_path = fs::current_path() / out_path;
    }
    std::error_code ec;
    fs::create_directories(out_path.parent_path(), ec);
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    nlohmann::json j;
    j["display_symbol"] = display_symbol_;
    j["fix_symbol"] = cfg.getExternalFeedSymbol();
    if (last_quote_.valid) {
        j["bid"] = last_quote_.bid;
        j["ask"] = last_quote_.ask;
        j["mid"] = last_quote_.mid;
    }
    j["primary_fix_symbol"] =
        last_quote_.fix_symbol.empty() ? cfg.getExternalFeedSymbol() : last_quote_.fix_symbol;
    json qb = json::object();
    for (const auto& [k, q] : quotes_by_canonical_) {
        if (!q.valid) {
            continue;
        }
        qb[k] = json{{"bid", q.bid}, {"ask", q.ask}, {"mid", q.mid}};
    }
    j["quotes_by_canonical"] = std::move(qb);
    j["updated_ms"] = now_ms;
    j["provider"] = cfg.getExternalFeedProvider();
    j["theo_mode"] = cfg.isMultiTheoFeeds() ? "multi" : "single";
    const std::string tmp_str = out_path.string() + ".tmp";
    {
        std::ofstream f(tmp_str, std::ios::trunc | std::ios::binary);
        if (!f) {
            return;
        }
        f << j.dump();
        f.flush();
    }
    fs::rename(tmp_str, out_path, ec);
}

void ExternalFeedManager::maybeWriteDeskDepthCache(const std::vector<std::pair<double, double>>& bids,
                                                   const std::vector<std::pair<double, double>>& asks) const {
    if (bids.empty() || asks.empty()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    if (!cfg.getExternalFeedWriteDeskDepthCache()) {
        return;
    }
    const int max_lv = cfg.getExternalFeedDeskDepthLevels();
    if (max_lv <= 0) {
        return;
    }
    const int interval_ms = std::max(100, cfg.getExternalFeedDeskDepthWriteIntervalMs());
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(desk_depth_mu_);
        if (desk_depth_has_written_) {
            if (now - last_desk_depth_write_ < std::chrono::milliseconds(interval_ms)) {
                return;
            }
        }
        last_desk_depth_write_ = now;
        desk_depth_has_written_ = true;
    }

    const std::string rel = cfg.getExternalFeedDeskDepthCachePath();
    fs::path out_path(rel);
    if (!out_path.is_absolute()) {
        out_path = fs::current_path() / out_path;
    }
    std::error_code ec;
    fs::create_directories(out_path.parent_path(), ec);

    json jb = json::array();
    json ja = json::array();
    const std::size_t n = static_cast<std::size_t>(max_lv);
    for (std::size_t i = 0; i < bids.size() && i < n; ++i) {
        jb.push_back(json::array({bids[i].first, bids[i].second}));
    }
    for (std::size_t i = 0; i < asks.size() && i < n; ++i) {
        ja.push_back(json::array({asks[i].first, asks[i].second}));
    }

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    nlohmann::json j;
    j["display_symbol"] = display_symbol_;
    j["fix_symbol"] = cfg.getExternalFeedSymbol();
    j["bids"] = std::move(jb);
    j["asks"] = std::move(ja);
    j["levels"] = max_lv;
    j["md_market_depth"] = cfg.getExternalFeedFixMdMarketDepth();
    j["updated_ms"] = now_ms;
    j["provider"] = cfg.getExternalFeedProvider();

    const std::string tmp_str = out_path.string() + ".tmp";
    {
        std::ofstream f(tmp_str, std::ios::trunc | std::ios::binary);
        if (!f) {
            return;
        }
        f << j.dump();
        f.flush();
    }
    fs::rename(tmp_str, out_path, ec);
}

void ExternalFeedManager::invalidateQuoteAndNotifyCallbacks() {
    ExternalFeedQuote q{};
    q.valid = false;
    {
        std::lock_guard<std::mutex> lock(quote_mutex_);
        last_quote_.valid = false;
        quotes_by_canonical_.clear();
    }
    notifyCallbacks(q);
}

void ExternalFeedManager::recordQuoteErrorForProtectiveInvalidate() {
    auto& cfg = config::Config::getInstance();
    // Multi-feed mode: per-feed `*_up_` flags are already maintained at every error site
    // (cme_up_/hl_up_/neon_up_) and consumed per-leg via isFeedUpForSource(theo_source).
    // A global theo invalidation here would broadcast a valid=false quote with empty
    // fix_symbol, which MakeMarketStrategy treats as "global" and cancels every leg —
    // including legs whose feed is healthy. Suppress in multi-feed mode.
    if (cfg.isMultiTheoFeeds()) {
        // Reset the streak so that if multi-feed is later turned off (or for diagnostics)
        // we don't carry stale counters.
        consecutive_quote_errors_ = 0;
        return;
    }
    const int n = cfg.getExternalFeedConsecutiveErrorsBeforeInvalidate();
    if (n <= 0) {
        return;
    }
    const int streak = ++consecutive_quote_errors_;
    if (streak >= n) {
        consecutive_quote_errors_ = 0;
        if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().warn(
                "[ExternalFeed] {} consecutive quote errors — invalidating theo; MM should pull quotes",
                n);
        }
        invalidateQuoteAndNotifyCallbacks();
    }
}

void ExternalFeedManager::resetQuoteErrorStreak() {
    consecutive_quote_errors_ = 0;
}

void ExternalFeedManager::publishQuote(const ExternalFeedQuote& quote) {
    resetQuoteErrorStreak();
    auto& cfg = config::Config::getInstance();
    std::string tick_label = display_symbol_;
    {
        std::lock_guard<std::mutex> lock(quote_mutex_);
        last_quote_ = quote;
        if (quote.valid) {
            const std::string key = quote.fix_symbol.empty()
                                        ? fixSymbolCanonical(cfg.getExternalFeedSymbol())
                                        : fixSymbolCanonical(quote.fix_symbol);
            quotes_by_canonical_[key] = quote;
        }
        writeDeskTheoCacheLocked();
        if (!quote.fix_symbol.empty()) {
            tick_label = quote.fix_symbol;
        }
    }
    if (cfg.getBool("logging.log_ticks", true) && utils::Logger::isInitialized()) {
        utils::Logger::getInstance().log_tick(
            tick_label,
            quote.bid,
            quote.ask,
            quote.mid,
            0.0,
            0.0);
    }
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.success_count;
        stats_.last_success_at = quote.updated_at;
        last_fetch_error_.clear();
    }
    notifyCallbacks(quote);
}

void ExternalFeedManager::pollLoop() {
    auto& config = config::Config::getInstance();
    int interval_ms = config.getExternalFeedPollIntervalMs();
    if (interval_ms <= 0) {
        interval_ms = 2000;
    }
    auto interval = std::chrono::milliseconds(interval_ms);

    while (running_.load()) {
        fetchAndUpdate();
        auto deadline = std::chrono::steady_clock::now() + interval;
        while (running_.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

bool ExternalFeedManager::fetchAndUpdate() {
    auto& config = config::Config::getInstance();
    if (isHyperliquidProvider(config.getExternalFeedProvider())) {
        return fetchAndUpdateHyperliquid();
    }
    std::string base_url = config.getExternalFeedRestUrl();
    std::string symbol = config.getExternalFeedSymbol();

    while (!base_url.empty() && base_url.back() == '/') {
        base_url.pop_back();
    }
    if (base_url.empty()) {
        rest_last_fetch_ok_.store(false);
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ =
                "external_feed.rest_url is empty — set a base URL for REST theo or use provider neon_fix";
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }
    const bool use_spot_path = config.getExternalFeedRestUsesSpotTickerPath();
    std::string path = use_spot_path ? ("/api/v3/ticker/bookTicker?symbol=" + symbol)
                                     : ("/fapi/v1/ticker/bookTicker?symbol=" + symbol);
    std::string absolute_url = base_url + path;

    api::HttpRequest req(api::HttpMethod::GET, "");
    req.absolute_url = absolute_url;
    req.timeout = std::chrono::milliseconds(5000);
    req.skip_auth = true;

    api::RestClient& rest = api::RestClient::getInstance();
    api::HttpResponse resp = rest.request(req);

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.poll_count;
    }

    if (!resp.is_success || resp.body.empty()) {
        rest_last_fetch_ok_.store(false);
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = "status=" + std::to_string(resp.status_code) +
                (resp.body.empty() ? " " + resp.error_message : " body=" + resp.body.substr(0, 200));
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }

    try {
        json j = json::parse(resp.body);
        std::string bid_str = j.value("bidPrice", "");
        std::string ask_str = j.value("askPrice", "");
        if (bid_str.empty() || ask_str.empty()) {
            rest_last_fetch_ok_.store(false);
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                ++stats_.error_count;
                last_fetch_error_ = "missing bidPrice/askPrice";
            }
            recordQuoteErrorForProtectiveInvalidate();
            return false;
        }
        double bid = std::stod(bid_str);
        double ask = std::stod(ask_str);

        ExternalFeedQuote q;
        q.bid = bid;
        q.ask = ask;
        q.mid = (bid + ask) * 0.5;
        q.updated_at = std::chrono::steady_clock::now();
        q.valid = true;
        q.fix_symbol = symbol;

        publishQuote(q);
        rest_last_fetch_ok_.store(true);
        return true;
    } catch (const std::exception& e) {
        rest_last_fetch_ok_.store(false);
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = std::string("parse: ") + e.what();
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }
}

void ExternalFeedManager::auxRestPollLoop() {
    while (running_.load()) {
        auto& cfg = config::Config::getInstance();
        int interval_ms = cfg.getExternalFeedPollIntervalMs();
        if (interval_ms <= 0) {
            interval_ms = 2000;
        }
        // Hyperliquid still uses REST + the aux poll. Mettraders is
        // WebSocket-driven (event-pushed quotes from `mettradersFeedLoop`),
        // so it does NOT need to be polled here — `fetchAndUpdateMettraders`
        // was a no-op health reader, and calling it on the aux cadence
        // contributed nothing beyond an extra mutex acquire. We probe HL
        // unconditionally in multi-theo mode (matches prior "always all 3"
        // semantics for HL).
        const bool always_probe = cfg.isMultiTheoFeeds() && cfg.hasMarketMakerInstrumentList();
        bool any_hl = always_probe;
        if (!always_probe && cfg.hasMarketMakerInstrumentList()) {
            for (const auto& leg : cfg.getMarketMakerInstruments()) {
                const std::string r = cfg.getResolvedTheoSourceForLeg(leg);
                if (r == "hyperliquid") {
                    any_hl = true;
                    break;
                }
            }
        }
        if (any_hl) {
            (void)fetchAndUpdateHyperliquid();
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms);
        while (running_.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

void ExternalFeedManager::mettradersFeedLoop() {
    auto& cfg = config::Config::getInstance();
    const std::string url = cfg.getMettradersFeedUrl();
    if (!cfg.getMettradersFeedEnabled() || url.empty()) {
        mettraders_up_.store(false);
        mettraders_ws_connected_.store(false);
        std::lock_guard<std::mutex> lock(feed_err_mu_);
        mettraders_last_err_ = "mettraders_feed disabled or URL empty";
        return;
    }

    // The Mettraders WS quote stream is a fire-hose for ONE product (Gold).
    // The wire payload (`{"ts":...,"bid":...,"ask":...}`) carries no symbol,
    // so we tag every published quote with the config-supplied fix_symbol
    // ("GOLD" by default). Multiple FX/metal/index feeds on this venue would
    // each need a separate WS thread (or a multiplexed stream that includes
    // the symbol on the wire), but the current operational use is gold-only.
    const std::string fix_sym = cfg.getMettradersFeedFixSymbol();
    if (fix_sym.empty()) {
        mettraders_up_.store(false);
        mettraders_ws_connected_.store(false);
        std::lock_guard<std::mutex> lock(feed_err_mu_);
        mettraders_last_err_ = "mettraders_feed.fix_symbol is empty — set the product tag (e.g. \"GOLD\")";
        if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().warn("[FEED:Mettraders] {}", mettraders_last_err_);
        }
        return;
    }

    mettraders_raw_msgs_logged_.store(0);
    mettraders_up_.store(false);
    mettraders_ws_connected_.store(false);

    MettradersWsClient ws;
    ws.clear_access_channels(websocketpp::log::alevel::all);
    ws.clear_error_channels(websocketpp::log::elevel::all);
    ws.init_asio();

    websocketpp::connection_hdl conn_hdl;
    std::atomic<bool> conn_open{false};

    ws.set_tls_init_handler([](websocketpp::connection_hdl) {
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

    ws.set_open_handler([this, &conn_hdl, &conn_open, url, fix_sym](websocketpp::connection_hdl hdl) {
        conn_hdl = hdl;
        conn_open.store(true);
        // Transport up — but do NOT flip `mettraders_up_` here. Public health
        // only goes green once we have parsed and published at least one
        // quote, so a connected-but-silent socket can no longer paint the
        // pill green or fool `isFeedUpForSource("mettraders")`.
        mettraders_ws_connected_.store(true);
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            // Don't clear the parse-side error here; preserve it until the
            // first successful parse arrives.
        }
        if (!mettraders_sanity_logged_.exchange(true) && utils::Logger::isInitialized()) {
            utils::Logger::getInstance().info(
                "[FEED_SANITY:Mettraders] websocket handshake ok url={} fix_symbol={}",
                url,
                fix_sym);
        }
    });

    ws.set_message_handler([this, fix_sym](websocketpp::connection_hdl,
                                           MettradersWsClient::message_ptr msg) {
        if (!msg) return;
        const std::string payload = msg->get_payload();
        const int idx = mettraders_raw_msgs_logged_.fetch_add(1);
        // Best-effort JSON parse of the Mettraders wire format. The stream
        // currently sends one bid/ask tick per message:
        //     {"ts":1778600032265,"bid":4668.3,"ask":4668.6}
        // We don't ack/heartbeat back — the connection stays open as long as
        // the client polls. Any malformed payload is dropped silently after
        // the first-few-messages diagnostic logs.
        double bid = 0.0, ask = 0.0;
        bool parsed = false;
        std::string parse_err;
        try {
            json j = json::parse(payload);
            if (j.is_object() && j.contains("bid") && j.contains("ask")) {
                auto read_num = [](const json& v) -> double {
                    if (v.is_number()) return v.get<double>();
                    if (v.is_string()) return std::stod(v.get<std::string>());
                    return 0.0;
                };
                bid = read_num(j["bid"]);
                ask = read_num(j["ask"]);
                if (std::isfinite(bid) && std::isfinite(ask) && bid > 0.0 && ask > 0.0 && ask >= bid) {
                    parsed = true;
                }
            } else {
                parse_err = "missing bid/ask fields";
            }
        } catch (const std::exception& ex) {
            parse_err = ex.what();
        }

        // Throttled raw logging: keep the first 5 messages so the operator
        // can verify the schema on a new venue. After parsing is wired,
        // these logs always carry the parse result alongside the raw bytes.
        if (idx < 5 && utils::Logger::isInitialized()) {
            std::string truncated = payload;
            if (truncated.size() > 1000) {
                truncated.resize(1000);
                truncated += "...";
            }
            if (parsed) {
                utils::Logger::getInstance().info(
                    "[METTRADERS_FEED] raw msg #{} parsed bid={} ask={} for fix_symbol={} payload={}",
                    idx, bid, ask, fix_sym, truncated);
            } else {
                utils::Logger::getInstance().warn(
                    "[METTRADERS_FEED] raw msg #{} parse_failed err=\"{}\" payload={}",
                    idx, parse_err, truncated);
            }
        }

        if (!parsed) {
            // Only update parse-side error every ~16 failures so a permanently
            // unparseable stream doesn't spam the err mutex.
            if ((idx & 0xF) == 0) {
                std::lock_guard<std::mutex> lock(feed_err_mu_);
                mettraders_last_err_ = std::string("parse_failed: ") +
                                       (parse_err.empty() ? "missing fields" : parse_err);
            }
            return;
        }

        ExternalFeedQuote q;
        q.bid = bid;
        q.ask = ask;
        q.mid = (bid + ask) / 2.0;
        q.updated_at = std::chrono::steady_clock::now();
        q.valid = true;
        q.fix_symbol = fix_sym;
        publishQuote(q);

        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        last_mettraders_ok_ms_.store(now_ms);
        const bool was_up = mettraders_up_.exchange(true);
        if (!was_up) {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            mettraders_last_err_.clear();
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().info(
                    "[FEED_SANITY:Mettraders] first parsed quote fix_symbol={} bid={} ask={} mid={}",
                    fix_sym, bid, ask, q.mid);
            }
        }
    });

    ws.set_fail_handler([this, &ws, url](websocketpp::connection_hdl hdl) {
        mettraders_up_.store(false);
        mettraders_ws_connected_.store(false);
        const std::string detail = mettradersHandshakeFailureDetail(ws, hdl, url);
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            mettraders_last_err_ = detail;
        }
        if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().warn("[FEED:Mettraders] handshake_failed {}", detail);
        }
    });

    ws.set_close_handler([this](websocketpp::connection_hdl) {
        mettraders_up_.store(false);
        mettraders_ws_connected_.store(false);
    });

    websocketpp::lib::error_code ec;
    auto con = ws.get_connection(url, ec);
    if (ec || !con) {
        mettraders_up_.store(false);
        mettraders_ws_connected_.store(false);
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            mettraders_last_err_ = ec ? ec.message() : "failed to create websocket connection";
        }
        if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().warn(
                "[FEED:Mettraders] connect setup failed url={} err={}",
                url,
                mettraders_last_err_);
        }
        return;
    }
    ws.connect(con);

    while (running_.load()) {
        ws.poll();
        // NOTE: do NOT touch last_mettraders_ok_ms_ from the loop — that
        // timestamp is now driven by parsed quotes (set inside the message
        // handler). A connected-but-silent socket must not keep the
        // "last_ok_ms" wall clock advancing or the desk's stale-pill heuristic
        // (>5s old = stale) is defeated.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (conn_open.load()) {
        websocketpp::lib::error_code close_ec;
        ws.close(conn_hdl, websocketpp::close::status::normal, "shutdown", close_ec);
        for (int i = 0; i < 20; ++i) {
            ws.poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    mettraders_up_.store(false);
    mettraders_ws_connected_.store(false);
}

bool ExternalFeedManager::fetchAndUpdateMettraders() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getMettradersFeedEnabled() || cfg.getMettradersFeedUrl().empty()) {
        mettraders_up_.store(false);
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            mettraders_last_err_ = "mettraders_feed disabled or URL empty";
        }
        return false;
    }
    // Health is driven by the dedicated websocket loop.
    return mettraders_up_.load();
}

bool ExternalFeedManager::fetchAndUpdateHyperliquidMulti() {
    auto& config = config::Config::getInstance();
    std::string base_url = config.getExternalFeedRestUrl();
    if (base_url.empty()) {
        base_url = "https://api.hyperliquid.xyz";
    }
    while (!base_url.empty() && base_url.back() == '/') {
        base_url.pop_back();
    }
    if (base_url.empty()) {
        rest_hl_last_ok_.store(false);
        hl_up_.store(false);
        const std::string err = "external_feed.rest_url is empty for hyperliquid (multi_theo)";
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = err;
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = err;
        }
        return false;
    }
    std::vector<config::MarketMakerInstrumentLeg> hl_legs;
    for (const auto& leg : config.getMarketMakerInstruments()) {
        if (config.getResolvedTheoSourceForLeg(leg) == "hyperliquid" && !leg.reference_fix_symbol.empty()) {
            hl_legs.push_back(leg);
        }
    }
    // No early return when hl_legs is empty: we still poll HL for health so
    // the desk pill row reflects real connectivity even when no leg uses HL.
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.poll_count;
    }
    api::HttpRequest req(api::HttpMethod::POST, "");
    req.absolute_url = base_url + "/info";
    req.timeout = std::chrono::milliseconds(5000);
    req.skip_auth = true;
    req.body = R"({"type":"allMids"})";
    api::HttpResponse resp = api::RestClient::getInstance().request(req);
    if (!resp.is_success || resp.body.empty()) {
        rest_hl_last_ok_.store(false);
        hl_up_.store(false);
        const std::string err =
            "hyperliquid allMids failed status=" + std::to_string(resp.status_code) +
            (resp.body.empty() ? " " + resp.error_message : " body=" + resp.body.substr(0, 200));
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = err;
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = err;
        }
        if (!config.isMultiTheoFeeds()) {
            recordQuoteErrorForProtectiveInvalidate();
        }
        return false;
    }
    try {
        json j = json::parse(resp.body);
        json mids = j;
        if (j.contains("mids") && j["mids"].is_object()) {
            mids = j["mids"];
        }
        if (!mids.is_object()) {
            throw std::runtime_error("allMids response is not an object");
        }
        std::unordered_map<std::string, double> midsU;
        for (auto it = mids.begin(); it != mids.end(); ++it) {
            std::string K = it.key();
            for (char& c : K) {
                c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            }
            const json& v = it.value();
            double m = 0.0;
            if (v.is_string()) {
                m = std::stod(v.get<std::string>());
            } else if (v.is_number()) {
                m = v.get<double>();
            } else {
                continue;
            }
            if (m > 0.0) {
                midsU[std::move(K)] = m;
            }
        }
        // ---- HIP-3 "xyz" perp DEX (real-world FX/commodity perps) ----
        // Hyperliquid hosts a deployer-managed perp DEX named "xyz" that lists
        // real-world assets on HIP-3 xyz (FX, metals, energy, indices, e.g. xyz:JPY, xyz:GOLD,
        // xyz:SP500, xyz:BRENTOIL). These are NOT in the default `allMids`;
        // we have to query `{"type":"allMids","dex":"xyz"}` and merge.
        // Failure here is non-fatal: base allMids already established health.
        bool xyz_ok = false;
        std::size_t xyz_count = 0;
        std::string xyz_err;
        try {
            api::HttpRequest xreq(api::HttpMethod::POST, "");
            xreq.absolute_url = base_url + "/info";
            xreq.timeout = std::chrono::milliseconds(5000);
            xreq.skip_auth = true;
            xreq.body = R"({"type":"allMids","dex":"xyz"})";
            api::HttpResponse xresp = api::RestClient::getInstance().request(xreq);
            if (xresp.is_success && !xresp.body.empty()) {
                json xj = json::parse(xresp.body);
                json xmids = xj;
                if (xj.contains("mids") && xj["mids"].is_object()) {
                    xmids = xj["mids"];
                }
                if (xmids.is_object()) {
                    for (auto it = xmids.begin(); it != xmids.end(); ++it) {
                        std::string K = it.key();
                        for (char& c : K) {
                            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
                        }
                        const json& v = it.value();
                        double m = 0.0;
                        if (v.is_string()) {
                            try { m = std::stod(v.get<std::string>()); } catch (...) { continue; }
                        } else if (v.is_number()) {
                            m = v.get<double>();
                        } else {
                            continue;
                        }
                        if (m > 0.0) {
                            midsU[std::move(K)] = m;
                            ++xyz_count;
                        }
                    }
                    xyz_ok = true;
                    // Throttled dump of the xyz dex response so the user can see
                    // exactly which xyz:* keys (and prices) HL is offering.
                    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
                    const auto last_ms = hl_allmids_dump_last_ms_.load();
                    if (last_ms == 0 || (now_ms - last_ms) > 30000) {
                        std::error_code ec;
                        fs::create_directories("logs", ec);
                        const std::string out = "logs/hl_allmids_xyz.json";
                        const std::string tmp = out + ".tmp";
                        std::ofstream xf(tmp, std::ios::trunc | std::ios::binary);
                        if (xf) {
                            xf << xj.dump();
                            xf.flush();
                            fs::rename(tmp, out, ec);
                        }
                    }
                }
            } else {
                xyz_err = "status=" + std::to_string(xresp.status_code) +
                          (xresp.body.empty() ? (" " + xresp.error_message)
                                              : (" body=" + xresp.body.substr(0, 160)));
            }
        } catch (const std::exception& e) {
            xyz_err = std::string("xyz parse: ") + e.what();
        }
        // One-shot info log so the user sees the xyz DEX is wired in.
        if (utils::Logger::isInitialized() && !hl_xyz_logged_.exchange(true)) {
            if (xyz_ok) {
                utils::Logger::getInstance().info(
                    "[FEED:Hyperliquid] xyz HIP-3 DEX merged keys={} (xyz:… FX/metals/energy/indices)",
                    xyz_count);
            } else {
                utils::Logger::getInstance().warn(
                    "[FEED:Hyperliquid] xyz HIP-3 DEX fetch failed ({}); FX/commodity legs will lack theo until it recovers",
                    xyz_err.empty() ? "unknown error" : xyz_err);
            }
        }

        // Atomic dump of the full allMids JSON to logs/hl_allmids.json so the
        // user (and the desk) can inspect exactly which keys HL is offering —
        // critical for fixing `theo_venue_symbol` mismatches without restarts.
        // Throttled to ~1 write / 30s to avoid disk thrash.
        {
            const auto now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
            const auto last_ms = hl_allmids_dump_last_ms_.load();
            if (last_ms == 0 || (now_ms - last_ms) > 30000) {
                hl_allmids_dump_last_ms_.store(now_ms);
                std::error_code ec;
                fs::create_directories("logs", ec);
                const std::string out = "logs/hl_allmids.json";
                const std::string tmp = out + ".tmp";
                std::ofstream f(tmp, std::ios::trunc | std::ios::binary);
                if (f) {
                    f << j.dump();
                    f.flush();
                    fs::rename(tmp, out, ec);
                }
            }
        }

        // Substring fallback for spot-style venue symbols: if a candidate like
        // "SILVER-USDC" / "WTIOIL-USDC" isn't an exact key, scan midsU for any
        // key that *contains* the candidate token (case-insensitive). Avoids
        // false positives on very short tokens (≤3 chars) like "SP" / "XAG".
        auto tryFuzzyMatch = [&midsU](const std::string& cand,
                                      std::string& out_key,
                                      double& out_mid) -> bool {
            if (cand.size() < 4) {
                return false;
            }
            for (const auto& kv : midsU) {
                if (kv.first.find(cand) != std::string::npos && kv.second > 0.0) {
                    out_key = kv.first;
                    out_mid = kv.second;
                    return true;
                }
            }
            return false;
        };

        bool any = false;
        std::string sanity_sym;
        double sanity_mid = 0.0;
        for (const auto& leg : hl_legs) {
            std::vector<std::string> cands;
            appendHyperliquidMidsKeyCandidates(leg, cands);
            double mid = 0.0;
            std::string matched_key;
            for (const auto& c : cands) {
                const auto it = midsU.find(c);
                if (it != midsU.end() && it->second > 0.0) {
                    mid = it->second;
                    matched_key = it->first;
                    break;
                }
            }
            if (mid <= 0.0) {
                for (const auto& c : cands) {
                    if (tryFuzzyMatch(c, matched_key, mid)) {
                        break;
                    }
                }
            }

            // Per-leg one-shot diagnostic — surfaces the matched HL key + mid,
            // OR the candidates we tried so the user can compare against the
            // dumped logs/hl_allmids.json and fix `theo_venue_symbol`.
            const std::string leg_key =
                leg.ax_symbol.empty() ? leg.order_symbol : leg.ax_symbol;
            bool already_logged = false;
            {
                std::lock_guard<std::mutex> g(hl_leg_match_mu_);
                already_logged = hl_leg_match_logged_.count(leg_key) > 0;
            }
            if (!already_logged && utils::Logger::isInitialized()) {
                std::string cand_list;
                for (size_t i = 0; i < cands.size(); ++i) {
                    if (i) cand_list += ",";
                    cand_list += cands[i];
                }
                if (mid > 0.0) {
                    utils::Logger::getInstance().info(
                        "[FEED:Hyperliquid] leg={} ref_fix={} matched key='{}' mid={:.6f} "
                        "(candidates tried: [{}])",
                        leg_key, leg.reference_fix_symbol, matched_key, mid, cand_list);
                } else {
                    utils::Logger::getInstance().warn(
                        "[FEED:Hyperliquid] leg={} ref_fix={} NO MATCH — tried candidates: [{}] "
                        "(HL has {} keys; inspect logs/hl_allmids.json and update "
                        "market_maker.instruments[].theo_venue_symbol)",
                        leg_key, leg.reference_fix_symbol, cand_list, midsU.size());
                }
                std::lock_guard<std::mutex> g(hl_leg_match_mu_);
                hl_leg_match_logged_.insert(leg_key);
            }

            if (mid <= 0.0) {
                continue;
            }
            ExternalFeedQuote q;
            q.bid = mid;
            q.ask = mid;
            q.mid = mid;
            q.updated_at = std::chrono::steady_clock::now();
            q.valid = true;
            q.fix_symbol = leg.reference_fix_symbol;
            publishQuote(q);
            if (!any) {
                sanity_sym = leg.reference_fix_symbol;
                sanity_mid = mid;
            }
            any = true;
        }
        // === Global fast-market watch symbol (ADDITIVE; HL SPX breaker, finding B) ===
        // Publish the configured market_maker.fast_market.hl_spx_symbol straight from this same
        // allMids/xyz response so FastMarketMonitor can read it via getQuoteForTheoSymbol even
        // when NO MM leg references it. Leg-driven polling above is unchanged. We insert directly
        // into quotes_by_canonical_ (NOT publishQuote) so last_quote_/health/desk-cache are not
        // perturbed by a symbol we only watch. No-op when unset.
        {
            const std::string watch_sym =
                config.getString("market_maker.fast_market.hl_spx_symbol", "");
            if (!watch_sym.empty()) {
                std::string wk = watch_sym;
                for (char& c : wk) {
                    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
                }
                double wmid = 0.0;
                std::string wkey;
                const auto it = midsU.find(wk);
                if (it != midsU.end() && it->second > 0.0) {
                    wmid = it->second;
                } else {
                    (void)tryFuzzyMatch(wk, wkey, wmid);
                }
                if (wmid > 0.0) {
                    ExternalFeedQuote q;
                    q.bid = wmid;
                    q.ask = wmid;
                    q.mid = wmid;
                    q.updated_at = std::chrono::steady_clock::now();
                    q.valid = true;
                    q.fix_symbol = watch_sym;
                    std::lock_guard<std::mutex> lock(quote_mutex_);
                    quotes_by_canonical_[fixSymbolCanonical(watch_sym)] = q;
                }
            }
        }
        // Health: HTTP succeeded, response parsed, response has at least one
        // numeric mid → HL transport is up. Whether *our* legs were matched
        // affects the per-leg trade gate, not feed health.
        const bool transport_ok = !midsU.empty();
        if (!hl_legs.empty() && !any) {
            // Configured legs but none matched: still treat HL as up (the
            // server is alive), but log it as a warning so the user fixes
            // theo_venue_symbol mappings.
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().warn(
                    "[FEED:Hyperliquid] allMids ok but no configured HL leg matched; "
                    "inspect logs/hl_allmids.json then update "
                    "market_maker.instruments[].theo_venue_symbol");
            }
        }
        if (!transport_ok) {
            rest_hl_last_ok_.store(false);
            hl_up_.store(false);
            const std::string err = "hyperliquid allMids returned 0 mids";
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                ++stats_.error_count;
                last_fetch_error_ = err;
            }
            {
                std::lock_guard<std::mutex> lock(feed_err_mu_);
                hl_last_err_ = err;
            }
            if (!config.isMultiTheoFeeds()) {
                recordQuoteErrorForProtectiveInvalidate();
            }
            return false;
        }
        rest_hl_last_ok_.store(true);
        hl_up_.store(true);
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        last_hl_ok_ms_.store(now_ms);
        if (!hl_sanity_logged_.exchange(true) && utils::Logger::isInitialized()) {
            std::string ssym = sanity_sym;
            double smid = sanity_mid;
            if (ssym.empty() && !midsU.empty()) {
                // Probe-only: report a representative mid from the response.
                const auto it = midsU.begin();
                ssym = it->first;
                smid = it->second;
            }
            utils::Logger::getInstance().info(
                "[FEED_SANITY:Hyperliquid] first quote ok url={}/info mids_count={} sample_sym={} sample_mid={}",
                base_url, midsU.size(), ssym, smid);
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_.clear();
        }
        return true;
    } catch (const std::exception& e) {
        rest_hl_last_ok_.store(false);
        hl_up_.store(false);
        const std::string err = std::string("hyperliquid multi parse: ") + e.what();
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = err;
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = err;
        }
        if (!config.isMultiTheoFeeds()) {
            recordQuoteErrorForProtectiveInvalidate();
        }
        return false;
    }
}

bool ExternalFeedManager::fetchAndUpdateHyperliquid() {
    auto& config = config::Config::getInstance();
    if (config.isMultiTheoFeeds() && config.hasMarketMakerInstrumentList()) {
        return fetchAndUpdateHyperliquidMulti();
    }
    std::string base_url = config.getExternalFeedRestUrl();
    if (base_url.empty()) {
        base_url = "https://api.hyperliquid.xyz";
    }
    while (!base_url.empty() && base_url.back() == '/') {
        base_url.pop_back();
    }
    if (base_url.empty()) {
        rest_last_fetch_ok_.store(false);
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = "external_feed.rest_url is empty for hyperliquid";
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }

    auto canonical = [](const std::string& sym) {
        return fixSymbolCanonical(sym);
    };
    auto map_symbol = [&canonical](const std::string& sym) {
        std::string k = canonical(sym);
        if (k.size() > 4 && k.rfind("PERP") == k.size() - 4) {
            k = k.substr(0, k.size() - 4);
        }
        if (k == "USDJPY" || k == "JPY") return std::string("JPY");
        if (k == "XAU" || k == "GOLD") return std::string("XAU");
        if (k == "XAG" || k == "SILVER") return std::string("XAG");
        if (k == "WTI" || k == "WTIOIL" || k == "USOIL") return std::string("WTI");
        if (k == "BRENT" || k == "BRNTOIL") return std::string("BRENT");
        if (k == "NATGAS" || k == "NATURALGAS" || k == "NGAS" || k == "NG" || k == "GAS") return std::string("GAS");
        return k;
    };

    std::vector<std::string> wanted_raw;
    wanted_raw.push_back(config.getExternalFeedSymbol());
    if (config.hasMarketMakerInstrumentList()) {
        for (const auto& leg : config.getMarketMakerInstruments()) {
            if (!leg.reference_fix_symbol.empty()) {
                wanted_raw.push_back(leg.reference_fix_symbol);
            }
        }
    }

    std::vector<std::string> wanted;
    {
        std::unordered_map<std::string, bool> seen;
        for (const auto& s : wanted_raw) {
            const std::string m = map_symbol(s);
            if (m.empty() || seen.count(m) != 0) {
                continue;
            }
            seen[m] = true;
            wanted.push_back(m);
        }
    }
    if (wanted.empty()) {
        rest_last_fetch_ok_.store(false);
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ =
                "hyperliquid allMids: no symbol keys to fetch; set external_feed.symbol, "
                "market_maker.theo_symbol or instruments[].reference_fix_symbol, or theo_venue_symbol";
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = last_fetch_error_;
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }

    api::HttpRequest req(api::HttpMethod::POST, "");
    req.absolute_url = base_url + "/info";
    req.timeout = std::chrono::milliseconds(5000);
    req.skip_auth = true;
    req.body = R"({"type":"allMids"})";

    api::RestClient& rest = api::RestClient::getInstance();
    api::HttpResponse resp = rest.request(req);

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.poll_count;
    }

    if (!resp.is_success || resp.body.empty()) {
        rest_last_fetch_ok_.store(false);
        rest_hl_last_ok_.store(false);
        hl_up_.store(false);
        const std::string err =
            "hyperliquid allMids failed status=" + std::to_string(resp.status_code) +
            (resp.body.empty() ? " " + resp.error_message : " body=" + resp.body.substr(0, 200));
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = err;
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = err;
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }

    try {
        json j = json::parse(resp.body);
        json mids = j;
        if (j.contains("mids") && j["mids"].is_object()) {
            mids = j["mids"];
        }
        if (!mids.is_object()) {
            throw std::runtime_error("allMids response is not an object");
        }

        bool any = false;
        std::string sanity_sym;
        double sanity_mid = 0.0;
        for (const auto& key : wanted) {
            if (!mids.contains(key)) {
                continue;
            }
            double mid = 0.0;
            if (mids[key].is_string()) {
                mid = std::stod(mids[key].get<std::string>());
            } else if (mids[key].is_number()) {
                mid = mids[key].get<double>();
            } else {
                continue;
            }
            if (!(mid > 0.0)) {
                continue;
            }
            ExternalFeedQuote q;
            q.bid = mid;
            q.ask = mid;
            q.mid = mid;
            q.updated_at = std::chrono::steady_clock::now();
            q.valid = true;
            q.fix_symbol = key;
            publishQuote(q);
            if (!any) {
                sanity_sym = key;
                sanity_mid = mid;
            }
            any = true;
        }

        if (!any) {
            rest_last_fetch_ok_.store(false);
            rest_hl_last_ok_.store(false);
            hl_up_.store(false);
            const std::string err = "hyperliquid allMids missing requested symbols";
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                ++stats_.error_count;
                last_fetch_error_ = err;
            }
            {
                std::lock_guard<std::mutex> lock(feed_err_mu_);
                hl_last_err_ = err;
            }
            recordQuoteErrorForProtectiveInvalidate();
            return false;
        }

        rest_last_fetch_ok_.store(true);
        rest_hl_last_ok_.store(true);
        hl_up_.store(true);
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        last_hl_ok_ms_.store(now_ms);
        if (!hl_sanity_logged_.exchange(true) && utils::Logger::isInitialized()) {
            utils::Logger::getInstance().info(
                "[FEED_SANITY:Hyperliquid] first quote ok url={}/info sym={} mid={}",
                base_url, sanity_sym, sanity_mid);
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_.clear();
        }
        return true;
    } catch (const std::exception& e) {
        rest_last_fetch_ok_.store(false);
        rest_hl_last_ok_.store(false);
        hl_up_.store(false);
        const std::string err = std::string("hyperliquid parse: ") + e.what();
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.error_count;
            last_fetch_error_ = err;
        }
        {
            std::lock_guard<std::mutex> lock(feed_err_mu_);
            hl_last_err_ = err;
        }
        recordQuoteErrorForProtectiveInvalidate();
        return false;
    }
}

std::optional<core::Price> ExternalFeedManager::getTheoPrice(const std::string& theo_symbol) const {
    if (!enabled_.load()) {
        return std::nullopt;
    }
    const std::string k = fixSymbolCanonical(theo_symbol);
    std::lock_guard<std::mutex> lock(quote_mutex_);
    if (!k.empty()) {
        auto it = quotes_by_canonical_.find(k);
        if (it != quotes_by_canonical_.end() && it->second.valid) {
            return it->second.mid;
        }
    }
    if (!last_quote_.valid) {
        return std::nullopt;
    }
    if (k.empty()) {
        return last_quote_.mid;
    }
    if (!last_quote_.fix_symbol.empty() && fixSymbolCanonical(last_quote_.fix_symbol) == k) {
        return last_quote_.mid;
    }
    return std::nullopt;
}

std::optional<ExternalFeedQuote> ExternalFeedManager::getQuoteForTheoSymbol(const std::string& theo_symbol) const {
    if (!enabled_.load()) {
        return std::nullopt;
    }
    const std::string k = fixSymbolCanonical(theo_symbol);
    std::lock_guard<std::mutex> lock(quote_mutex_);
    if (!k.empty()) {
        auto it = quotes_by_canonical_.find(k);
        if (it != quotes_by_canonical_.end() && it->second.valid) {
            return it->second;
        }
    }
    if (!last_quote_.valid) {
        return std::nullopt;
    }
    if (k.empty()) {
        return last_quote_;
    }
    if (!last_quote_.fix_symbol.empty() && fixSymbolCanonical(last_quote_.fix_symbol) == k) {
        return last_quote_;
    }
    return std::nullopt;
}

std::optional<ExternalFeedQuote> ExternalFeedManager::getLastQuote() const {
    if (!enabled_.load()) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(quote_mutex_);
    if (!last_quote_.valid) {
        return std::nullopt;
    }
    return last_quote_;
}

ExternalFeedManager::Stats ExternalFeedManager::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

std::string ExternalFeedManager::getLastFetchError() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return last_fetch_error_;
}

std::string ExternalFeedManager::lastMettradersError() const {
    std::lock_guard<std::mutex> lock(feed_err_mu_);
    return mettraders_last_err_;
}
std::string ExternalFeedManager::lastHlError() const {
    std::lock_guard<std::mutex> lock(feed_err_mu_);
    return hl_last_err_;
}
std::string ExternalFeedManager::lastNeonError() const {
    std::lock_guard<std::mutex> lock(feed_err_mu_);
    return neon_last_err_;
}

bool ExternalFeedManager::isFeedUpForSource(const std::string& source) const {
    if (source == "mettraders" || source == "cme") return mettraders_up_.load();
    if (source == "hyperliquid") return hl_up_.load();
    if (source == "neon_fix" || source == "neon") return neon_up_.load();
    return true; // unknown source — do not block legs we cannot classify
}

bool ExternalFeedManager::isPricingTransportConnected() const {
    if (!enabled_.load()) {
        return true;
    }
    if (!running_.load()) {
        return false;
    }
    const auto& cfg = config::Config::getInstance();
    if (cfg.isMultiTheoFeeds() && cfg.hasMarketMakerInstrumentList()) {
        bool needN = false;
        bool needH = false;
        bool needM = false;
        for (const auto& leg : cfg.getMarketMakerInstruments()) {
            const std::string r = cfg.getResolvedTheoSourceForLeg(leg);
            if (r == "neon_fix") {
                needN = true;
            } else if (r == "hyperliquid") {
                needH = true;
            } else if (r == "mettraders" && cfg.getMettradersFeedEnabled() &&
                       !cfg.getMettradersFeedUrl().empty()) {
                needM = true;
            }
        }
        const bool n = !needN || fix_session_logged_on_.load();
        const bool h = !needH || rest_hl_last_ok_.load();
        const bool m = !needM || mettraders_up_.load();
        return n && h && m;
    }
    if (isNeonFixProvider(cfg.getExternalFeedProvider())) {
        return fix_session_logged_on_.load();
    }
    return rest_last_fetch_ok_.load();
}

int ExternalFeedManager::registerCallback(FeedUpdateCallback callback) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    int id = next_callback_id_++;
    auto entry = std::make_shared<CallbackEntry>();
    entry->cb = std::move(callback);
    callbacks_.emplace_back(id, std::move(entry));
    return id;
}

void ExternalFeedManager::unregisterCallback(int callback_id) {
    // Remove the entry from the live list, but keep it alive locally so we can drain any
    // dispatch that already pinned it before we erased it.
    std::shared_ptr<CallbackEntry> removed;
    {
        std::lock_guard<std::mutex> lock(callbacks_mutex_);
        for (auto it = callbacks_.begin(); it != callbacks_.end(); ++it) {
            if (it->first == callback_id) {
                removed = it->second;
                callbacks_.erase(it);
                break;
            }
        }
    }
    if (!removed) {
        return;
    }
    // Block until any in-flight dispatch that captured this entry has finished. This is what
    // makes ~MakeMarketStrategy safe: the destructor cannot return (and the object cannot be
    // freed) while a feed thread is still executing the captured [this] callback. The pin is
    // taken under callbacks_mutex_ in notifyCallbacks, so once we hold the lock here and observe
    // in_flight==0 no new dispatch can start for this (already-erased) entry.
    {
        std::unique_lock<std::mutex> lk(callbacks_mutex_);
        callbacks_drain_cv_.wait(lk, [&removed] {
            return removed->in_flight.load(std::memory_order_acquire) == 0;
        });
    }
    if (utils::Logger::isInitialized()) {
        utils::Logger::getInstance().debug(
            "[ExternalFeed] unregisterCallback: drained in-flight dispatch for id={}", callback_id);
    }
}

void ExternalFeedManager::notifyCallbacks(const ExternalFeedQuote& quote) {
    // Pin the callbacks AND bump their in-flight count under the lock. Doing the increment under
    // the same lock that unregisterCallback uses to erase guarantees no torn window: an unregister
    // either runs before this copy (entry already gone, never pinned) or after (sees in_flight>0
    // and drains). Never can it observe in_flight==0 and free the object while we are about to call.
    std::vector<std::shared_ptr<CallbackEntry>> entries;
    {
        std::lock_guard<std::mutex> lock(callbacks_mutex_);
        entries.reserve(callbacks_.size());
        for (const auto& [id, entry] : callbacks_) {
            if (entry) {
                entry->in_flight.fetch_add(1, std::memory_order_acq_rel);
                entries.push_back(entry);
            }
        }
    }
    for (const auto& entry : entries) {
        // Bullet-proof dispatch: a feed callback that throws ANYTHING (including non-std::exception
        // types or terminate conditions like a deadlock-driven abort) must not be allowed to silently
        // kill the worker thread that drives every MM strategy. Live incident 2026-05-06: a desk
        // reconcile-race issued `desk_reseed_cancel` against just-placed legs; the next onFeedUpdate
        // hit something inside the cancel-replace path and the worker stopped emitting any further
        // logs (no [Callback exception] line either, ruling out std::exception). With the existing
        // narrow catch(std::exception&) the failure was silent. catch(...) + an "ANY-THROW" tag in
        // the log lets us see we caught a non-std exception so the next bug surface is obvious.
        try {
            entry->cb(quote);
        } catch (const std::exception& e) {
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().error("[ExternalFeed] Callback exception: {}", e.what());
            }
        } catch (...) {
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().error(
                    "[ExternalFeed] Callback exception: ANY-THROW (non-std::exception) — worker survived");
            }
        }
        // Release the in-flight pin. When this is the last in-flight dispatch for the entry, wake
        // any unregisterCallback that is draining it. Notify under the lock so the waiter (which
        // re-checks the atomic predicate under the same lock) cannot miss the wakeup.
        if (entry->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lk(callbacks_mutex_);
            callbacks_drain_cv_.notify_all();
        }
    }
}

} // namespace marketdata
} // namespace architect
