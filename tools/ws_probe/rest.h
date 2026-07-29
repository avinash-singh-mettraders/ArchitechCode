// =============================================================================
// ws_probe/rest.h — minimal standalone libcurl REST client for the probe:
// one-shot top-of-book (far-price guard), REST baseline place/cancel (mode B),
// and the exit-time open-orders sweep. Endpoint paths follow common venue
// conventions and are documented as adaptable in the README; every raw
// response body is returned for verbatim logging.
// =============================================================================
#pragma once

#include <string>

#include "transport.h"

namespace wsprobe {

class LiveRestApi : public RestApi {
public:
    LiveRestApi(std::string base_url, std::string token, double qty_default);

    TopOfBook fetchTopOfBook(const std::string& symbol) override;
    RestResult place(const std::string& symbol, const std::string& side,
                     double price, double qty) override;
    RestResult cancel(const std::string& order_id) override;
    std::vector<std::string> openOrderIds(const std::string& symbol) override;

private:
    std::string base_;
    std::string token_;
    double qty_default_;
};

}  // namespace wsprobe
