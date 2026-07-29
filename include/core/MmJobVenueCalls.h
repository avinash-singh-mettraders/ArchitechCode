// =============================================================================
// MmJobVenueCalls — per-mover-job venue-call counter (proof instrumentation).
//
// Enforce-mode directive (2026-07): a mover (order) job must issue ZERO venue
// GETs. This is the regression detector: RestClient bumps the thread-local
// counter on every venue GET/POST/cancel, but ONLY while a mover job has marked
// the thread "active" (begin/end around the job). At job end the strategy logs
//   [MM_JOB_VENUE_CALLS] sym=... gets=G posts=P cancels=C
// and WARNs loudly if gets>0 in enforce — same philosophy as the boot banners.
//
// Header-only with `inline thread_local` (one instance per thread, merged across
// TUs) so both api_lib (RestClient) and strategy_lib can touch it with no link
// dependency and no cycle.
// =============================================================================
#pragma once

namespace architect {
namespace core {

struct MmJobVenueCounters {
    long gets{0};
    long posts{0};
    long cancels{0};
    bool active{false};
};

inline thread_local MmJobVenueCounters t_mm_job_venue_calls;

/** Mover-job entry: reset counters and start counting on this thread. */
inline void mmJobVenueBegin() {
    t_mm_job_venue_calls = MmJobVenueCounters{};
    t_mm_job_venue_calls.active = true;
}

/** Mover-job exit: stop counting on this thread. */
inline void mmJobVenueEnd() { t_mm_job_venue_calls.active = false; }

/** Snapshot the current counters (valid any time; zero when no job active). */
inline MmJobVenueCounters mmJobVenueSnapshot() { return t_mm_job_venue_calls; }

inline void mmJobVenueNoteGet() {
    if (t_mm_job_venue_calls.active) ++t_mm_job_venue_calls.gets;
}
inline void mmJobVenueNotePost() {
    if (t_mm_job_venue_calls.active) ++t_mm_job_venue_calls.posts;
}
inline void mmJobVenueNoteCancel() {
    if (t_mm_job_venue_calls.active) ++t_mm_job_venue_calls.cancels;
}

}  // namespace core
}  // namespace architect
