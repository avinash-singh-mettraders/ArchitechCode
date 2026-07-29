// =============================================================================
// ws_probe/rest.cpp — libcurl easy-API REST client.
// =============================================================================
#include "rest.h"

#include <chrono>

#include <curl/curl.h>

namespace wsprobe {

namespace {

std::size_t writeCb(char* ptr, std::size_t size, std::size_t nmemb, void* userdata) {
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, size * nmemb);
    return size * nmemb;
}

struct HttpReply {
    bool ok = false;
    long status = 0;
    long long latency_ns = 0;
    std::string body;
    std::string err;
};

HttpReply httpDo(const std::string& method, const std::string& url, const std::string& token,
                 const std::string* body) {
    HttpReply r;
    CURL* curl = curl_easy_init();
    if (!curl) {
        r.err = "curl_easy_init failed";
        return r;
    }
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    const std::string auth = "Authorization: Bearer " + token;
    headers = curl_slist_append(headers, auth.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &r.body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 8000L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        if (body) curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body->c_str());
    } else if (method == "DELETE") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        if (body) curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body->c_str());
    }

    const auto t0 = std::chrono::steady_clock::now();
    const CURLcode code = curl_easy_perform(curl);
    const auto t1 = std::chrono::steady_clock::now();
    r.latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

    if (code != CURLE_OK) {
        r.err = curl_easy_strerror(code);
    } else {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &r.status);
        r.ok = (r.status >= 200 && r.status < 300);
        if (!r.ok) r.err = "http status " + std::to_string(r.status);
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return r;
}

double readNum(const json& v) {
    if (v.is_number()) return v.get<double>();
    if (v.is_string()) { try { return std::stod(v.get<std::string>()); } catch (...) {} }
    return 0.0;
}

}  // namespace

LiveRestApi::LiveRestApi(std::string base_url, std::string token, double qty_default)
    : base_(std::move(base_url)), token_(std::move(token)), qty_default_(qty_default) {
    if (!base_.empty() && base_.back() == '/') base_.pop_back();
}

TopOfBook LiveRestApi::fetchTopOfBook(const std::string& symbol) {
    TopOfBook tob;
    const std::string url = base_ + "/ticker?symbol=" + symbol;
    HttpReply rep = httpDo("GET", url, token_, nullptr);
    if (!rep.ok) return tob;
    try {
        json j = json::parse(rep.body);
        const json* o = &j;
        if (j.is_array() && !j.empty()) o = &j.front();
        else if (j.contains("data")) o = &j["data"];
        auto pick = [&](std::initializer_list<const char*> keys) -> double {
            for (const char* k : keys) if (o->contains(k)) return readNum((*o)[k]);
            return 0.0;
        };
        tob.best_bid = pick({"best_bid", "bid", "bidPrice", "b"});
        tob.best_ask = pick({"best_ask", "ask", "askPrice", "a"});
        tob.ok = (tob.best_bid > 0.0 && tob.best_ask > 0.0 && tob.best_ask >= tob.best_bid);
    } catch (...) {
    }
    return tob;
}

RestResult LiveRestApi::place(const std::string& symbol, const std::string& side, double price,
                              double qty) {
    json body{{"symbol", symbol},
              {"side", side},
              {"type", "limit"},
              {"price", price},
              {"quantity", qty > 0 ? qty : qty_default_},
              {"time_in_force", "GTC"}};
    const std::string s = body.dump();
    HttpReply rep = httpDo("POST", base_ + "/orders", token_, &s);
    RestResult r;
    r.ok = rep.ok;
    r.latency_ns = rep.latency_ns;
    r.body = rep.body;
    r.err = rep.err;
    if (rep.ok) {
        try {
            json j = json::parse(rep.body);
            auto oid = frameStr(j, {"order_id", "orderId", "oid", "id"});
            if (!oid && j.contains("data")) oid = frameStr(j["data"], {"order_id", "orderId", "oid", "id"});
            r.order_id = oid.value_or("");
        } catch (...) {
        }
    }
    return r;
}

RestResult LiveRestApi::cancel(const std::string& order_id) {
    HttpReply rep = httpDo("DELETE", base_ + "/orders/" + order_id, token_, nullptr);
    RestResult r;
    r.ok = rep.ok;
    r.latency_ns = rep.latency_ns;
    r.body = rep.body;
    r.err = rep.err;
    return r;
}

std::vector<std::string> LiveRestApi::openOrderIds(const std::string& symbol) {
    std::vector<std::string> ids;
    const std::string url = base_ + "/orders?status=open&symbol=" + symbol;
    HttpReply rep = httpDo("GET", url, token_, nullptr);
    if (!rep.ok) return ids;
    try {
        json j = json::parse(rep.body);
        const json* arr = &j;
        if (j.is_object() && j.contains("data")) arr = &j["data"];
        else if (j.is_object() && j.contains("orders")) arr = &j["orders"];
        if (arr->is_array()) {
            for (const auto& o : *arr) {
                auto oid = frameStr(o, {"order_id", "orderId", "oid", "id"});
                if (oid) ids.push_back(*oid);
            }
        }
    } catch (...) {
    }
    return ids;
}

}  // namespace wsprobe
