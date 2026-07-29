#include "marketdata/NeonFixFeed.h"
#include "utils/Logger.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <mutex>
#include <netdb.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>
#include <map>

namespace architect {
namespace marketdata {

namespace {

constexpr char SOH = '\x01';

std::string utc_timestamp() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const std::time_t t = system_clock::to_time_t(now);
    const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    struct tm tm_buf {};
    gmtime_r(&t, &tm_buf);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d%02d%02d-%02d:%02d:%02d.%03d",
                  tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                  tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                  static_cast<int>(ms.count()));
    return std::string(buf);
}

unsigned fix_checksum_sum(const std::string& msg) {
    unsigned s = 0;
    for (unsigned char c : msg) {
        s += c;
    }
    return s % 256;
}

std::string wrap_fix(const std::string& body_from_35) {
    const std::string ver = "8=FIX.4.4\x01";
    const std::string len_field = "9=" + std::to_string(body_from_35.size()) + "\x01";
    const std::string without_10 = ver + len_field + body_from_35;
    char cs[8];
    std::snprintf(cs, sizeof(cs), "%03u", fix_checksum_sum(without_10));
    return without_10 + "10=" + std::string(cs) + "\x01";
}

std::vector<std::pair<int, std::string>> parse_fields(const std::string& msg) {
    std::vector<std::pair<int, std::string>> out;
    std::size_t i = 0;
    while (i < msg.size()) {
        std::size_t eq = msg.find('=', i);
        if (eq == std::string::npos) {
            break;
        }
        std::size_t soh = msg.find(SOH, eq);
        if (soh == std::string::npos) {
            break;
        }
        try {
            const int tag = std::stoi(msg.substr(i, eq - i));
            out.emplace_back(tag, msg.substr(eq + 1, soh - eq - 1));
        } catch (...) {
            break;
        }
        i = soh + 1;
    }
    return out;
}

const std::string* find_tag(const std::vector<std::pair<int, std::string>>& f, int tag) {
    for (const auto& p : f) {
        if (p.first == tag) {
            return &p.second;
        }
    }
    return nullptr;
}

bool extract_tob(const std::vector<std::pair<int, std::string>>& f, double& bid_out, double& ask_out,
                 bool& have_bid, bool& have_ask) {
    have_bid = false;
    have_ask = false;
    double best_bid = -std::numeric_limits<double>::infinity();
    double best_ask = std::numeric_limits<double>::infinity();

    for (std::size_t i = 0; i < f.size(); ++i) {
        if (f[i].first != 269) {
            continue;
        }
        int entry_type = 0;
        try {
            entry_type = std::stoi(f[i].second);
        } catch (...) {
            continue;
        }
        double px = 0.0;
        bool got_px = false;
        for (std::size_t j = i + 1; j < f.size(); ++j) {
            if (f[j].first == 269 || f[j].first == 279) {
                break;
            }
            if (f[j].first == 270) {
                try {
                    px = std::stod(f[j].second);
                    got_px = true;
                } catch (...) {
                }
                break;
            }
        }
        if (!got_px) {
            continue;
        }
        if (entry_type == 0) {
            have_bid = true;
            best_bid = std::max(best_bid, px);
        } else if (entry_type == 1) {
            have_ask = true;
            best_ask = std::min(best_ask, px);
        }
    }

    if (!have_bid || !have_ask) {
        for (const auto& p : f) {
            if (p.first == 188) {
                try {
                    const double v = std::stod(p.second);
                    have_bid = true;
                    best_bid = std::max(best_bid, v);
                } catch (...) {
                }
            } else if (p.first == 190) {
                try {
                    const double v = std::stod(p.second);
                    have_ask = true;
                    best_ask = std::min(best_ask, v);
                } catch (...) {
                }
            }
        }
    }

    if (have_bid) {
        bid_out = best_bid;
    }
    if (have_ask) {
        ask_out = best_ask;
    }
    return have_bid && have_ask;
}

struct MdEntryRow {
    int side{-1}; // 269: 0 bid, 1 ask
    int update_action{-1}; // 279: -1 absent, 0 new, 1 change, 2 delete
    double price{0.0};
    double size{0.0};
    bool have_price{false};
    bool have_size{false};
};

void collect_md_entry_rows(const std::vector<std::pair<int, std::string>>& f, std::vector<MdEntryRow>& out) {
    out.clear();
    for (std::size_t i = 0; i < f.size(); ++i) {
        if (f[i].first != 269) {
            continue;
        }
        MdEntryRow row;
        try {
            row.side = std::stoi(f[i].second);
        } catch (...) {
            continue;
        }
        row.update_action = -1;
        row.have_price = false;
        row.have_size = false;
        for (std::size_t j = i + 1; j < f.size(); ++j) {
            const int tag = f[j].first;
            if (tag == 269 || tag == 55) {
                break;
            }
            if (tag == 279) {
                try {
                    row.update_action = std::stoi(f[j].second);
                } catch (...) {
                    row.update_action = -1;
                }
                continue;
            }
            if (tag == 270) {
                try {
                    row.price = std::stod(f[j].second);
                    row.have_price = true;
                } catch (...) {
                }
                continue;
            }
            if (tag == 271) {
                try {
                    row.size = std::stod(f[j].second);
                    row.have_size = true;
                } catch (...) {
                }
                continue;
            }
        }
        out.push_back(row);
    }
}

void apply_snapshot_rows(const std::vector<MdEntryRow>& rows,
                         std::map<double, double, std::greater<double>>& bids,
                         std::map<double, double>& asks) {
    bids.clear();
    asks.clear();
    for (const auto& r : rows) {
        if (!r.have_price) {
            continue;
        }
        const double sz = r.have_size ? r.size : 0.0;
        if (r.side == 0) {
            bids[r.price] += sz;
        } else if (r.side == 1) {
            asks[r.price] += sz;
        }
    }
    for (auto it = bids.begin(); it != bids.end();) {
        if (it->second <= 0.0) {
            it = bids.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = asks.begin(); it != asks.end();) {
        if (it->second <= 0.0) {
            it = asks.erase(it);
        } else {
            ++it;
        }
    }
}

void apply_incremental_rows(const std::vector<MdEntryRow>& rows,
                            std::map<double, double, std::greater<double>>& bids,
                            std::map<double, double>& asks) {
    for (const auto& r : rows) {
        if (r.side == 0) {
            if (r.update_action == 2) {
                if (r.have_price) {
                    bids.erase(r.price);
                }
            } else if (r.have_price && r.have_size) {
                if (r.size <= 0.0) {
                    bids.erase(r.price);
                } else {
                    bids[r.price] = r.size;
                }
            }
        } else if (r.side == 1) {
            if (r.update_action == 2) {
                if (r.have_price) {
                    asks.erase(r.price);
                }
            } else if (r.have_price && r.have_size) {
                if (r.size <= 0.0) {
                    asks.erase(r.price);
                } else {
                    asks[r.price] = r.size;
                }
            }
        }
    }
}

bool best_from_book_maps(const std::map<double, double, std::greater<double>>& bids,
                         const std::map<double, double>& asks,
                         double& bid_out,
                         double& ask_out) {
    if (bids.empty() || asks.empty()) {
        return false;
    }
    bid_out = bids.begin()->first;
    ask_out = asks.begin()->first;
    return ask_out > bid_out;
}

void ladders_from_maps(const std::map<double, double, std::greater<double>>& bids,
                       const std::map<double, double>& asks,
                       std::vector<std::pair<double, double>>& bid_ladder,
                       std::vector<std::pair<double, double>>& ask_ladder) {
    bid_ladder.clear();
    ask_ladder.clear();
    bid_ladder.reserve(bids.size());
    for (const auto& p : bids) {
        bid_ladder.emplace_back(p.first, p.second);
    }
    ask_ladder.reserve(asks.size());
    for (const auto& p : asks) {
        ask_ladder.emplace_back(p.first, p.second);
    }
}

int connect_tcp(const std::string& host, int port, std::string& err_out) {
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    const std::string port_str = std::to_string(port);
    const int gai = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (gai != 0 || res == nullptr) {
        err_out = std::string("getaddrinfo: ") + gai_strerror(gai);
        return -1;
    }
    int sock = -1;
    int last_errno = 0;
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        sock = static_cast<int>(socket(p->ai_family, p->ai_socktype, p->ai_protocol));
        if (sock < 0) {
            last_errno = errno;
            continue;
        }
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        last_errno = errno;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(res);
    if (sock < 0) {
        err_out = "connect failed to " + host + ":" + std::to_string(port);
        if (last_errno != 0) {
            err_out += " — ";
            err_out += std::strerror(last_errno);
        }
        err_out += " (is stunnel listening on this port?)";
        return -1;
    }
    return sock;
}

bool send_all(int sock, const std::string& data, std::string& err_out) {
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::send(sock, data.data() + off, data.size() - off, 0);
        if (n <= 0) {
            err_out = n == 0 ? "send closed" : std::string("send: ") + std::strerror(errno);
            return false;
        }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

std::string build_session_prefix(const NeonFixSettings& s, int& out_seq) {
    std::ostringstream o;
    o << "49=" << s.sender_comp_id << SOH;
    o << "56=" << s.target_comp_id << SOH;
    if (!s.deliver_to_comp_id.empty()) {
        o << "128=" << s.deliver_to_comp_id << SOH;
    }
    if (!s.sender_sub_id.empty()) {
        o << "50=" << s.sender_sub_id << SOH;
    }
    o << "34=" << out_seq++ << SOH;
    o << "52=" << utc_timestamp() << SOH;
    return o.str();
}

std::string build_logon(const NeonFixSettings& s, int& out_seq) {
    std::ostringstream body;
    body << "35=A" << SOH;
    body << build_session_prefix(s, out_seq);
    body << "98=0" << SOH;
    body << "108=" << s.heart_bt_int << SOH;
    body << "553=" << s.username << SOH;
    body << "554=" << s.password << SOH;
    body << "141=Y" << SOH;
    for (const auto& [tag, val] : s.logon_extra) {
        if (!tag.empty()) {
            body << tag << "=" << val << SOH;
        }
    }
    return wrap_fix(body.str());
}

/** NoRelatedSym leg: strict venues require FIX component order (e.g. 460 Product before 167 SecurityType). */
void append_instrument_extras(std::ostringstream& body,
                              const std::vector<std::pair<std::string, std::string>>& extras) {
    std::unordered_map<std::string, std::string> m;
    m.reserve(extras.size());
    for (const auto& [tag, val] : extras) {
        if (!tag.empty()) {
            m[tag] = val;
        }
    }
    auto emit_tag = [&](const char* tag) {
        auto it = m.find(tag);
        if (it == m.end()) {
            return;
        }
        body << it->first << "=" << it->second << SOH;
        m.erase(it);
    };
    emit_tag("460");
    emit_tag("167");
    std::vector<std::pair<int, std::string>> rest;
    for (const auto& [tag, val] : m) {
        try {
            rest.emplace_back(std::stoi(tag), tag);
        } catch (...) {
            rest.emplace_back(1'000'000, tag);
        }
    }
    std::sort(rest.begin(), rest.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [num, tag] : rest) {
        (void)num;
        auto it = m.find(tag);
        if (it != m.end()) {
            body << it->first << "=" << it->second << SOH;
        }
    }
}

/**
 * One FIX 55 per MarketDataRequest. demo.fxgrid / some venues reject
 * 146>1 with "RequestValidationError.MoreThanOneGroup" on the same 35=V.
 * After logon we send one 35=V per entry in `md_symbols`.
 */
static std::string build_md_request_one_symbol(const NeonFixSettings& s, int& out_seq, const std::string& sym) {
    static std::mutex md_id_mu;
    static int md_counter = 0;
    std::string md_req_id;
    {
        std::lock_guard<std::mutex> lock(md_id_mu);
        md_req_id = "MDPC" + std::to_string(++md_counter);
    }
    std::ostringstream body;
    body << "35=V" << SOH;
    body << build_session_prefix(s, out_seq);
    body << "262=" << md_req_id << SOH;
    const int md264 = (s.md_market_depth <= 0) ? 0 : 1;
    if (s.md_snapshot_only) {
        body << "263=0" << SOH;
        body << "264=" << md264 << SOH;
    } else {
        body << "263=1" << SOH;
        body << "264=" << md264 << SOH;
        if (s.md_update_type >= 0) {
            body << "265=" << s.md_update_type << SOH;
        }
    }
    for (const auto& [tag, val] : s.md_request_root_extra) {
        if (!tag.empty()) {
            body << tag << "=" << val << SOH;
        }
    }
    // FIX 4.4: NoMDEntryTypes (267/269*) must precede NoRelatedSym (146/55*).
    body << "267=2" << SOH;
    body << "269=0" << SOH;
    body << "269=1" << SOH;
    body << "146=1" << SOH;
    body << "55=" << sym << SOH;
    append_instrument_extras(body, s.md_instrument_extra);
    return wrap_fix(body.str());
}

std::string build_heartbeat(const NeonFixSettings& s, int& out_seq) {
    std::ostringstream body;
    body << "35=0" << SOH;
    body << build_session_prefix(s, out_seq);
    return wrap_fix(body.str());
}

std::string build_heartbeat_test_rsp(const NeonFixSettings& s, int& out_seq, const std::string& test_req_id) {
    std::ostringstream body;
    body << "35=0" << SOH;
    body << build_session_prefix(s, out_seq);
    body << "112=" << test_req_id << SOH;
    return wrap_fix(body.str());
}

std::string build_logout(const NeonFixSettings& s, int& out_seq, const char* text) {
    std::ostringstream body;
    body << "35=5" << SOH;
    body << build_session_prefix(s, out_seq);
    if (text && text[0]) {
        body << "58=" << text << SOH;
    }
    return wrap_fix(body.str());
}

bool md_list_contains(const NeonFixSettings& s, const std::string& wire_sym) {
    const std::string c = fixSymbolCanonical(wire_sym);
    for (const auto& m : s.md_symbols) {
        if (fixSymbolCanonical(m) == c) {
            return true;
        }
    }
    return false;
}

struct NeonSymMdState {
    std::map<double, double, std::greater<double>> bid_book;
    std::map<double, double> ask_book;
    double last_bid{0.0};
    double last_ask{0.0};
    bool have_bid_leg{false};
    bool have_ask_leg{false};
};

bool extract_one_fix_message(std::string& buf, std::string& msg_out) {
    while (!buf.empty() && buf[0] != '8') {
        const std::size_t sync = buf.find("8=FIX");
        if (sync == std::string::npos) {
            return false;
        }
        buf.erase(0, sync);
    }
    if (buf.size() < 16) {
        return false;
    }
    const std::size_t soh0 = buf.find(SOH);
    if (soh0 == std::string::npos || soh0 + 4 >= buf.size()) {
        return false;
    }
    if (buf.compare(soh0 + 1, 2, "9=") != 0) {
        buf.erase(0, 1);
        return false;
    }
    const std::size_t len_start = soh0 + 3;
    const std::size_t soh1 = buf.find(SOH, len_start);
    if (soh1 == std::string::npos) {
        return false;
    }
    int body_len = 0;
    try {
        body_len = std::stoi(buf.substr(len_start, soh1 - len_start));
    } catch (...) {
        buf.erase(0, 1);
        return false;
    }
    if (body_len <= 0 || body_len > 1 << 20) {
        buf.erase(0, 1);
        return false;
    }
    const std::size_t body_start = soh1 + 1;
    const std::size_t cksum_pos = body_start + static_cast<std::size_t>(body_len);
    const std::size_t total = cksum_pos + 7;
    if (buf.size() < total) {
        return false;
    }
    if (buf.compare(cksum_pos, 3, "10=") != 0) {
        buf.erase(0, 1);
        return false;
    }
    for (int k = 0; k < 3; ++k) {
        if (!std::isdigit(static_cast<unsigned char>(buf[cksum_pos + 3 + k]))) {
            buf.erase(0, 1);
            return false;
        }
    }
    if (buf[cksum_pos + 6] != SOH) {
        buf.erase(0, 1);
        return false;
    }
    msg_out.assign(buf.data(), total);
    buf.erase(0, total);
    return true;
}

} // namespace

void runNeonFixFeed(
    const NeonFixSettings& settings,
    std::atomic<bool>& running,
    const std::function<void(const ExternalFeedQuote&)>& sink,
    const std::function<void(std::string)>& on_error,
    const std::function<void(const std::vector<std::pair<double, double>>&, const std::vector<std::pair<double, double>>&)>&
        depth_emit,
    const std::function<void(bool fix_session_logged_on)>& on_fix_session_state) {
    if (settings.md_symbols.empty()) {
        on_error("Neon FIX: md_symbols empty");
        return;
    }
    if (settings.sender_comp_id.empty() || settings.target_comp_id.empty()) {
        on_error("Neon FIX: sender_comp_id / target_comp_id required");
        return;
    }
    while (running.load()) {
        if (on_fix_session_state) {
            on_fix_session_state(false);
        }
        std::string err;
        const int sock = connect_tcp(settings.host, settings.port, err);
        if (sock < 0) {
            on_error(err);
            for (int i = 0; i < 50 && running.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            continue;
        }

        int out_seq = 1;
        timeval tv{};
        tv.tv_sec = std::max(1, settings.heart_bt_int / 2);
        tv.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (!send_all(sock, build_logon(settings, out_seq), err)) {
            close(sock);
            on_error(err);
            continue;
        }

        bool logged_on = false;
        bool drop_session = false;
        std::string read_buf;
        char tmp[65536];
        std::unordered_map<std::string, NeonSymMdState> sym_md;

        auto process_incoming = [&](const std::string& raw) {
            const auto fields = parse_fields(raw);
            const std::string* mt = find_tag(fields, 35);
            if (!mt) {
                return;
            }
            if (*mt == "A") {
                logged_on = true;
                if (on_fix_session_state) {
                    on_fix_session_state(true);
                }
                sym_md.clear();
                std::string e2;
                for (const auto& mdsym : settings.md_symbols) {
                    const std::string md_one = build_md_request_one_symbol(settings, out_seq, mdsym);
                    if (utils::Logger::isInitialized()) {
                        std::string vis;
                        vis.reserve(md_one.size());
                        for (char c : md_one) {
                            vis += (c == SOH) ? '|' : c;
                        }
                        utils::Logger::getInstance().info(
                            "[Neon FIX] MD request wire (one symbol): 55={} : {}", mdsym, vis);
                    }
                    if (!send_all(sock, md_one, e2)) {
                        if (on_fix_session_state) {
                            on_fix_session_state(false);
                        }
                        on_error(e2);
                        drop_session = true;
                        return;
                    }
                }
                return;
            }
            if (*mt == "5") {
                if (on_fix_session_state) {
                    on_fix_session_state(false);
                }
                const std::string* t58 = find_tag(fields, 58);
                on_error(std::string("Neon FIX Logout: ") + (t58 ? *t58 : ""));
                drop_session = true;
                return;
            }
            if (*mt == "3") {
                const std::string* t58 = find_tag(fields, 58);
                const std::string* t371 = find_tag(fields, 371);
                const std::string* t372 = find_tag(fields, 372);
                const std::string* t373 = find_tag(fields, 373);
                std::string msg = "Neon FIX SessionReject: ";
                msg += t58 ? *t58 : "(no text)";
                if (t371) {
                    msg += " [refTag=" + *t371 + "]";
                }
                if (t372) {
                    msg += " [refMsgType=" + *t372 + "]";
                }
                if (t373) {
                    msg += " [rejectReason=" + *t373 + "]";
                }
                if (on_fix_session_state) {
                    on_fix_session_state(false);
                }
                on_error(msg);
                drop_session = true;
                return;
            }
            if (*mt == "Y") {
                // One bad symbol (e.g. demo account does not list a given pair) must not tear down
                // the whole FIX session — other subscribed instruments can still be live. Dropping the
                // session used to set neon_up=false and blocked every neon_fix leg.
                const std::string* t58 = find_tag(fields, 58);
                const std::string* t262 = find_tag(fields, 262);
                const std::string* t280 = find_tag(fields, 280);
                std::string msg = "Neon FIX MDReqReject: ";
                msg += t58 ? *t58 : "(no text)";
                if (t262) {
                    msg += " [MDReqID=" + *t262 + "]";
                }
                if (t280) {
                    msg += " [rejReason=" + *t280 + "]";
                }
                if (utils::Logger::isInitialized()) {
                    utils::Logger::getInstance().warn("{}", msg);
                }
                return;
            }
            if (*mt == "1") {
                const std::string* tr = find_tag(fields, 112);
                if (tr) {
                    std::string e2;
                    send_all(sock, build_heartbeat_test_rsp(settings, out_seq, *tr), e2);
                }
                return;
            }
            if (*mt == "0") {
                return;
            }
            if (*mt != "W" && *mt != "X") {
                return;
            }

            const std::string* sym = find_tag(fields, 55);
            const std::string* use_sym = sym;
            std::string sym_storage;
            if (!sym && (*mt == "X" || *mt == "W")) {
                sym_storage = settings.pricing_symbol;
                use_sym = &sym_storage;
            }
            if (!use_sym || !md_list_contains(settings, *use_sym)) {
                return;
            }

            const std::string sym_key = fixSymbolCanonical(*use_sym);
            NeonSymMdState& st = sym_md[sym_key];

            const bool want_l2 = settings.md_market_depth <= 0;
            double b = 0.0;
            double a = 0.0;
            bool hb = false;
            bool ha = false;

            if (want_l2) {
                std::vector<MdEntryRow> rows;
                collect_md_entry_rows(fields, rows);
                if (*mt == "W") {
                    apply_snapshot_rows(rows, st.bid_book, st.ask_book);
                } else {
                    apply_incremental_rows(rows, st.bid_book, st.ask_book);
                }
                if (!best_from_book_maps(st.bid_book, st.ask_book, b, a)) {
                    if (!extract_tob(fields, b, a, hb, ha)) {
                        return;
                    }
                    if (*mt == "W") {
                        st.last_bid = b;
                        st.last_ask = a;
                        st.have_bid_leg = true;
                        st.have_ask_leg = true;
                    } else {
                        if (hb) {
                            st.last_bid = b;
                            st.have_bid_leg = true;
                        }
                        if (ha) {
                            st.last_ask = a;
                            st.have_ask_leg = true;
                        }
                    }
                } else {
                    st.last_bid = b;
                    st.last_ask = a;
                    st.have_bid_leg = true;
                    st.have_ask_leg = true;
                }
            } else {
                if (!extract_tob(fields, b, a, hb, ha)) {
                    return;
                }
                if (*mt == "W") {
                    st.last_bid = b;
                    st.last_ask = a;
                    st.have_bid_leg = true;
                    st.have_ask_leg = true;
                } else {
                    if (hb) {
                        st.last_bid = b;
                        st.have_bid_leg = true;
                    }
                    if (ha) {
                        st.last_ask = a;
                        st.have_ask_leg = true;
                    }
                }
            }

            if (!st.have_bid_leg || !st.have_ask_leg || st.last_ask <= st.last_bid) {
                return;
            }

            ExternalFeedQuote q;
            q.bid = st.last_bid;
            q.ask = st.last_ask;
            q.mid = (st.last_bid + st.last_ask) * 0.5;
            q.updated_at = std::chrono::steady_clock::now();
            q.valid = true;
            q.fix_symbol = *use_sym;
            sink(q);

            if (depth_emit) {
                const std::string pk = fixSymbolCanonical(settings.pricing_symbol);
                NeonSymMdState* depth_st = nullptr;
                auto dit = sym_md.find(pk);
                if (dit != sym_md.end()) {
                    depth_st = &dit->second;
                } else if (!sym_md.empty()) {
                    depth_st = &sym_md.begin()->second;
                }
                if (!depth_st) {
                    return;
                }
                std::vector<std::pair<double, double>> vb;
                std::vector<std::pair<double, double>> va;
                if (want_l2 && !depth_st->bid_book.empty() && !depth_st->ask_book.empty()) {
                    ladders_from_maps(depth_st->bid_book, depth_st->ask_book, vb, va);
                } else {
                    vb.push_back({depth_st->last_bid, 1.0});
                    va.push_back({depth_st->last_ask, 1.0});
                }
                depth_emit(vb, va);
            }
        };

        while (running.load() && !drop_session) {
            const ssize_t n = ::recv(sock, tmp, sizeof(tmp), 0);
            if (n <= 0) {
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    if (logged_on) {
                        std::string e2;
                        if (!send_all(sock, build_heartbeat(settings, out_seq), e2)) {
                            on_error(e2);
                            break;
                        }
                    } else if (!logged_on) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    }
                    continue;
                }
                err = n == 0 ? "Neon FIX disconnected" : std::string("recv: ") + std::strerror(errno);
                if (on_fix_session_state) {
                    on_fix_session_state(false);
                }
                on_error(err);
                break;
            }
            read_buf.append(tmp, static_cast<std::size_t>(n));
            std::string one;
            while (extract_one_fix_message(read_buf, one)) {
                process_incoming(one);
                if (drop_session) {
                    break;
                }
            }
        }

        if (on_fix_session_state) {
            on_fix_session_state(false);
        }
        std::string e3;
        send_all(sock, build_logout(settings, out_seq, "shutdown"), e3);
        close(sock);

        if (!running.load()) {
            break;
        }
        for (int i = 0; i < 30 && running.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

} // namespace marketdata
} // namespace architect
