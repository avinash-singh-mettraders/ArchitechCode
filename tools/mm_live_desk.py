#!/usr/bin/env python3
"""
MM Live Desk — web UI launcher.

This script is a thin entry point that boots the canonical web desk implemented
in :mod:`mm_live_desk_core`. The legacy Tk desktop app (``--tk``) was removed
because it embedded a parallel pricing path (legacy ``skew_raw`` using
``mid ± width_ticks * tick`` with no pricer-snapshot transform) that could place
orders at prices inconsistent with the canonical pipeline.

All operator workflows now go through:

    python3 tools/mm_live_desk.py            # default — opens web UI on :8765
    python3 tools/mm_live_desk.py --port N   # custom port
    python3 tools/mm_live_desk.py --web      # same as default

Pricing for every quoted order is computed by
``mm_live_desk_core.compute_skewed_raw_bid_ask`` (Python mirror) or the C++
``MakeMarketStrategy::computeInventorySkewedRawBidAsk``. There is no other
``skew_raw`` definition anywhere in ``tools/``.

Set ``MM_LIVE_DESK_NO_BROWSER=1`` to skip opening a browser tab.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _print_tk_removed_and_exit() -> None:
    msg = (
        "[mm_live_desk] --tk is no longer supported.\n"
        "The legacy Tk desktop app was removed because it used a parallel pricing\n"
        "path that did not honor the pricer-snapshot transform. Use the web UI:\n"
        "    python3 tools/mm_live_desk.py            # default web UI\n"
        "    python3 tools/mm_live_desk.py --port N   # custom port\n"
    )
    print(msg, file=sys.stderr, flush=True)
    raise SystemExit(2)


def _parse_port(argv: list[str], default: int = 8765) -> int:
    for i, a in enumerate(argv):
        if a == "--port" and i + 1 < len(argv):
            try:
                return int(argv[i + 1])
            except ValueError:
                pass
    return default


if __name__ == "__main__":
    if "--tk" in sys.argv:
        _print_tk_removed_and_exit()

    port = _parse_port(sys.argv)

    _tools_dir = str(Path(__file__).resolve().parent)
    if sys.path[0] != _tools_dir:
        sys.path.insert(0, _tools_dir)
    import mm_live_desk_core as _mmc  # noqa: E402

    print(f"[mm_live_desk] Using mm_live_desk_core: {_mmc.MM_LIVE_DESK_CORE_FILE}", flush=True)
    try:
        _mmc.run_web_main(port=port)
    except KeyboardInterrupt:
        pass
    raise SystemExit(0)
