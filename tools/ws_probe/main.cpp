// =============================================================================
// ws_probe/main.cpp — CLI, safety banner, signal-driven cleanup, and the
// in-process --dry-run self-tests (the ctest entry point).
//
// STANDALONE. Does not include or link any trading-engine header. Credentials
// and endpoints come ONLY from env vars / flags — never from config/.
// =============================================================================
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>

#include "live_ws.h"
#include "mock_server.h"
#include "probe.h"
#include "rest.h"

using namespace wsprobe;

namespace {

void onSignal(int sig) {
    // Async-signal-safe: flip the atomic the probe polls between steps, and
    // write a short notice. Actual cancel-all runs on the main thread (network
    // I/O is not async-signal-safe) as soon as the current bounded wait returns.
    g_stop.store(true);
    const char* msg = "\n[signal] stop requested — running cancel-all cleanup...\n";
    ssize_t r = ::write(STDERR_FILENO, msg, std::strlen(msg));
    (void)r;
    (void)sig;
}

std::string envOr(const char* key, const std::string& dflt) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : dflt;
}

bool argVal(int argc, char** argv, const char* flag, std::string& out) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], flag) == 0 && i + 1 < argc) { out = argv[i + 1]; return true; }
    }
    return false;
}
bool hasFlag(int argc, char** argv, const char* flag) {
    for (int i = 1; i < argc; ++i)
        if (std::strcmp(argv[i], flag) == 0) return true;
    return false;
}

void printUsage() {
    std::printf(
        "ws_probe — standalone AX WebSocket order-entry latency/ordering probe\n\n"
        "Live usage (operator sandbox only):\n"
        "  AX_WS_URL=wss://gateway.architect.exchange/orders/ws \\\n"
        "  AX_REST_URL=https://gateway.architect.exchange/api \\\n"
        "  AX_API_TOKEN=... AX_PROBE_SYMBOL=SYMBOL \\\n"
        "  ws_probe --yes [--iterations 50] [--far-offset 0.20] [--qty 1]\n\n"
        "Flags:\n"
        "  --yes               required to place REAL orders\n"
        "  --dry-run           run in-process self-tests (no network); ctest entry\n"
        "  --iterations N      per-mode iterations (default 50, max 100)\n"
        "  --far-offset F      fraction away from touch (default 0.20)\n"
        "  --qty Q             order qty / min lot (default 1)\n"
        "  --symbol S          overrides AX_PROBE_SYMBOL\n"
        "  --ws-url / --rest-url / --token   override env\n"
        "  --modes ABC         subset of modes to run (default ABC)\n"
        "  --runtime-cap-sec S global runtime cap (default 600)\n");
}

// -----------------------------------------------------------------------------
// --dry-run: exercise the FULL state machine + safety guards against the mock.
// Returns 0 on success, 1 on any failed assertion.
// -----------------------------------------------------------------------------
int runDryRun() {
    int failures = 0;
    auto check = [&](bool cond, const char* what) {
        std::printf("  [%s] %s\n", cond ? "PASS" : "FAIL", what);
        if (!cond) failures++;
    };
    std::printf("=== ws_probe --dry-run self-tests ===\n");

    // Test 1: far-price refusal guard (2%% offset < 5%% floor -> refuse).
    {
        MockBook book;
        MockRestApi rest(&book, 100.0, 100.1);
        MockWsTransport ws(&book);
        ProbeConfig cfg;
        cfg.symbol = "DRY-SYM";
        cfg.far_offset = 0.02;
        cfg.iterations = 3;
        cfg.csv_path = "ws_probe_dryrun_results.csv";
        cfg.frames_path = "ws_probe_dryrun_frames.jsonl";
        Probe p(cfg, &ws, &rest);
        std::string err;
        const bool safe = p.preflightSafety(err);
        check(!safe, "far-price guard REFUSES a within-5%-of-touch price");
    }

    // Test 2: full A/B/C run against the mock — plumbing, metrics, cleanup.
    {
        MockBook book;
        MockRestApi rest(&book, 100.0, 100.1, /*place_ms=*/5.0, /*cancel_ms=*/8.0);
        MockWsTransport ws(&book, /*latency_ms=*/2.0);
        ProbeConfig cfg;
        cfg.symbol = "DRY-SYM";
        cfg.far_offset = 0.20;
        cfg.iterations = 4;
        cfg.csv_path = "ws_probe_dryrun_results.csv";
        cfg.frames_path = "ws_probe_dryrun_frames.jsonl";
        Probe p(cfg, &ws, &rest);

        std::string err;
        check(p.connect(err), "mock WS connect + login snapshot");
        check(p.preflightSafety(err), "far-price guard ACCEPTS a 20%-away price");

        p.runModeA();
        p.runModeB();
        p.runModeC();

        check(p.nPlaceWs() > 0, "mode A captured WS place-ack samples");
        check(p.nCancelWs() > 0, "mode A captured WS cancel-ack samples");
        check(p.nAckNextWs() > 0, "mode A captured ack->next-place samples");
        check(p.nPlaceRest() > 0, "mode B captured REST place samples");
        check(p.nCancelRest() > 0, "mode B captured REST cancel samples");
        check(p.c3Sequences() > 0, "mode C recorded cancel-ordering sequences");
        check(p.c3Clean() == p.c3Sequences(), "mode C: all c-sequences clean (one terminal, monotonic)");
        check(p.nReplaceWs() > 0, "mode C captured atomic-replace-ack samples");
        check(p.c4Trials() > 0 && p.c4SingleLive() == p.c4Trials(),
              "mode C: exactly ONE live order at P2 on every replace");

        p.report();
        p.cleanup();
        check(book.count() == 0, "exit-cleanup leaves ZERO live orders (no leaked OIDs)");
    }

    std::printf("\n=== dry-run %s (%d failure%s) ===\n",
                failures == 0 ? "OK" : "FAILED", failures, failures == 1 ? "" : "s");
    return failures == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    if (hasFlag(argc, argv, "--help") || hasFlag(argc, argv, "-h")) {
        printUsage();
        return 0;
    }
    if (hasFlag(argc, argv, "--dry-run")) {
        return runDryRun();
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    ProbeConfig cfg;
    cfg.ws_url = envOr("AX_WS_URL", "");
    cfg.rest_url = envOr("AX_REST_URL", "");
    cfg.token = envOr("AX_API_TOKEN", "");
    cfg.symbol = envOr("AX_PROBE_SYMBOL", "");

    std::string v;
    if (argVal(argc, argv, "--ws-url", v)) cfg.ws_url = v;
    if (argVal(argc, argv, "--rest-url", v)) cfg.rest_url = v;
    if (argVal(argc, argv, "--token", v)) cfg.token = v;
    if (argVal(argc, argv, "--symbol", v)) cfg.symbol = v;
    if (argVal(argc, argv, "--iterations", v)) cfg.iterations = std::atoi(v.c_str());
    if (argVal(argc, argv, "--far-offset", v)) cfg.far_offset = std::atof(v.c_str());
    if (argVal(argc, argv, "--qty", v)) cfg.qty = std::atof(v.c_str());
    if (argVal(argc, argv, "--runtime-cap-sec", v)) cfg.runtime_cap_sec = std::atof(v.c_str());
    if (argVal(argc, argv, "--modes", v)) {
        cfg.run_a = v.find('A') != std::string::npos || v.find('a') != std::string::npos;
        cfg.run_b = v.find('B') != std::string::npos || v.find('b') != std::string::npos;
        cfg.run_c = v.find('C') != std::string::npos || v.find('c') != std::string::npos;
    }

    // Hard caps.
    if (cfg.iterations < 1) cfg.iterations = 1;
    if (cfg.iterations > 100) { std::printf("clamping --iterations to hard cap 100\n"); cfg.iterations = 100; }
    if (cfg.runtime_cap_sec > 600.0) cfg.runtime_cap_sec = 600.0;

    if (cfg.ws_url.empty() || cfg.token.empty() || cfg.symbol.empty()) {
        std::fprintf(stderr,
                     "ERROR: AX_WS_URL, AX_API_TOKEN and AX_PROBE_SYMBOL are required "
                     "(via env or --ws-url/--token/--symbol).\n");
        printUsage();
        return 2;
    }
    if (cfg.rest_url.empty()) {
        std::fprintf(stderr,
                     "ERROR: AX_REST_URL is required for the far-price guard, REST baseline "
                     "(mode B) and exit-time open-orders verification.\n");
        return 2;
    }

    // -------------------------------------------------------------------------
    // Safety banner.
    // -------------------------------------------------------------------------
    std::printf("\n===================== ws_probe =====================\n");
    std::printf(" endpoint     : %s\n", cfg.ws_url.c_str());
    std::printf(" rest         : %s\n", cfg.rest_url.c_str());
    std::printf(" symbol       : %s   qty(min lot): %g\n", cfg.symbol.c_str(), cfg.qty);
    std::printf(" far offset   : %.1f%% from touch\n", cfg.far_offset * 100.0);
    std::printf(" iterations   : %d per mode   modes: %s%s%s\n", cfg.iterations,
                cfg.run_a ? "A" : "", cfg.run_b ? "B" : "", cfg.run_c ? "C" : "");
    std::printf(" runtime cap  : %.0fs\n", cfg.runtime_cap_sec);
    std::printf("\n \033[31m*** THIS WILL PLACE REAL ORDERS ***\033[0m\n");
    std::printf("====================================================\n\n");

    if (!hasFlag(argc, argv, "--yes")) {
        std::fprintf(stderr, "Refusing to place real orders without --yes. Aborting.\n");
        return 3;
    }

    LiveRestApi rest(cfg.rest_url, cfg.token, cfg.qty);
    LiveWsTransport ws(cfg.ws_url, cfg.token, cfg.cancel_on_disconnect);
    Probe p(cfg, &ws, &rest);

    std::string err;
    if (!p.connect(err)) {
        std::fprintf(stderr, "WS connect/login failed: %s\n", err.c_str());
        return 4;
    }
    if (!p.preflightSafety(err)) {
        std::fprintf(stderr, "%s\n", err.c_str());
        p.cleanup();
        return 5;
    }

    if (cfg.run_a && !g_stop.load()) p.runModeA();
    if (cfg.run_b && !g_stop.load()) p.runModeB();
    if (cfg.run_c && !g_stop.load()) p.runModeC();

    p.cleanup();
    p.report();
    std::printf("\nCSV: %s   frames: %s\n", cfg.csv_path.c_str(), cfg.frames_path.c_str());
    return 0;
}
