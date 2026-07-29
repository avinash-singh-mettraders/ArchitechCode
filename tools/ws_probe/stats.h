// =============================================================================
// ws_probe/stats.h — latency sample accumulation + percentile summary.
// Header-only, STL-only. Shared by the live probe and the dry-run self-tests.
// =============================================================================
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

namespace wsprobe {

// One named metric: a vector of millisecond samples with percentile readout.
class Metric {
public:
    explicit Metric(std::string name) : name_(std::move(name)) {}

    void add(double ms) { samples_.push_back(ms); }
    std::size_t n() const { return samples_.size(); }
    const std::string& name() const { return name_; }

    // Nearest-rank percentile (p in [0,1]). Empty -> NaN.
    double pct(double p) const {
        if (samples_.empty()) return std::nan("");
        std::vector<double> s = samples_;
        std::sort(s.begin(), s.end());
        if (p <= 0.0) return s.front();
        if (p >= 1.0) return s.back();
        // nearest-rank: ceil(p*N) - 1, clamped
        long idx = static_cast<long>(std::ceil(p * static_cast<double>(s.size()))) - 1;
        if (idx < 0) idx = 0;
        if (idx >= static_cast<long>(s.size())) idx = static_cast<long>(s.size()) - 1;
        return s[static_cast<std::size_t>(idx)];
    }

    double min() const { return samples_.empty() ? std::nan("") : *std::min_element(samples_.begin(), samples_.end()); }
    double max() const { return samples_.empty() ? std::nan("") : *std::max_element(samples_.begin(), samples_.end()); }

private:
    std::string name_;
    std::vector<double> samples_;
};

// Pretty-print one summary table row. `transport` is "WS" / "REST" / "-".
inline void printSummaryRow(const char* metric, const char* transport, const Metric& m) {
    auto cell = [](double v) -> std::string {
        if (std::isnan(v)) return "     -";
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%9.3f", v);
        return std::string(buf);
    };
    std::printf("| %-20s | %-5s | %s | %s | %s | %s | %s | %5zu |\n",
                metric, transport,
                cell(m.min()).c_str(), cell(m.pct(0.50)).c_str(), cell(m.pct(0.90)).c_str(),
                cell(m.pct(0.99)).c_str(), cell(m.max()).c_str(), m.n());
}

inline void printSummaryHeader() {
    std::printf("\n");
    std::printf("| %-20s | %-5s | %9s | %9s | %9s | %9s | %9s | %5s |\n",
                "metric", "trans", "min", "p50", "p90", "p99", "max", "n");
    std::printf("|----------------------|-------|-----------|-----------|-----------|"
                "-----------|-----------|-------|\n");
}

}  // namespace wsprobe
