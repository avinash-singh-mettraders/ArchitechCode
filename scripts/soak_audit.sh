#!/usr/bin/env bash
# soak_audit.sh — grep mrinal / trading logs and print a short MM soak verdict.
#
# Usage:
#   ./scripts/soak_audit.sh
#   LOG=logs/app.log STACKS='abc|def' MAX_PO=5 SYMBOL=XAG-PERP ./scripts/soak_audit.sh
#
# Optional: mark soak start in the log:
#   echo "SOAK_START=$(date -Iseconds)" | tee -a mrinal.txt

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG_PATH="${LOG:-mrinal.txt}"
SYMBOL="${SYMBOL:-XAG-PERP}"
SYM_U="${SYMBOL//-/_}"   # XAG-PERP -> XAG_PERP (strategy name segment)
# Pipe-separated 8-char stack id suffixes. Default: auto-detect mm_req_${SYM_U}_<id>
STACKS="${STACKS:-auto}"
MAX_PO="${MAX_PO:-auto}"
TAIL="${TAIL:-80}"
INCIDENT_TAIL="${INCIDENT_TAIL:-60}"
SOAK_ONLY="${SOAK_ONLY:-auto}"   # auto | 0 | 1 — when auto, slice log from last SOAK_START=

if [[ ! -f "$LOG_PATH" ]]; then
  echo "error: log file not found: $LOG_PATH" >&2
  exit 1
fi

# Optional: only audit lines after the latest SOAK_START= marker
LOG="$LOG_PATH"
_AUDIT_TMP=""
if [[ "$SOAK_ONLY" == "auto" ]]; then
  if grep -q '^SOAK_START=' "$LOG_PATH" 2>/dev/null; then
    SOAK_ONLY=1
  else
    SOAK_ONLY=0
  fi
fi
if [[ "$SOAK_ONLY" == "1" ]]; then
  _soak_ln="$(grep -n '^SOAK_START=' "$LOG_PATH" 2>/dev/null | tail -1 | cut -d: -f1 || true)"
  if [[ -n "$_soak_ln" && "$_soak_ln" -gt 0 ]]; then
    _AUDIT_TMP="$(mktemp "${TMPDIR:-/tmp}/soak_audit.XXXXXX")"
    tail -n +"${_soak_ln}" "$LOG_PATH" > "$_AUDIT_TMP"
    LOG="$_AUDIT_TMP"
    trap '[[ -n "${_AUDIT_TMP:-}" ]] && rm -f "$_AUDIT_TMP"' EXIT
  fi
fi

desk_strat() { printf 'mm_req_%s_%s' "$SYM_U" "$1"; }

if [[ "$STACKS" == "auto" ]]; then
  STACKS="$(
    grep -oE "mm_req_${SYM_U}_[a-f0-9]{8}" "$LOG" 2>/dev/null \
      | sed -E "s/^mm_req_${SYM_U}_//" \
      | sort -u \
      | paste -sd'|' -
  )"
  [[ -z "$STACKS" ]] && STACKS='*'
fi

if [[ "$MAX_PO" == "auto" ]]; then
  MAX_PO="$(
    grep -E "${SYMBOL}|mm_req_${SYM_U}" "$LOG" 2>/dev/null \
      | grep -oE 'max_po=[0-9]+' \
      | sed 's/max_po=//' \
      | sort -n \
      | uniq -c \
      | sort -rn \
      | awk '{print $2; exit}'
  )"
  [[ -z "$MAX_PO" ]] && MAX_PO=5
fi

# Strategy name filter: desk stacks + optional legacy make_market on same symbol
STRAT_FILTER="mm_req_.*${SYMBOL//-/_}|make_market_.*${SYMBOL//-/_}|${STACKS}"
AX_FILTER="${SYMBOL}|mm_req_.*${SYMBOL//-/_}|${STACKS}"

section() { printf '\n%s\n' "$*"; }

warn() { printf '  [!] %s\n' "$*"; }
ok()   { printf '  [ok] %s\n' "$*"; }
info() { printf '  [..] %s\n' "$*"; }

# --- helpers ---
soak_start() {
  grep -m1 '^SOAK_START=' "$LOG" 2>/dev/null | tail -1 || true
}

log_first_ts() {
  grep -m1 -oE '^[[][0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' "$LOG" 2>/dev/null || true
}

log_last_ts() {
  grep -oE '^[[][0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' "$LOG" 2>/dev/null | tail -1 || true
}

max_net_po() {
  grep -E "$AX_FILTER" "$LOG" 2>/dev/null \
    | grep -oE 'net_po=-?[0-9]+|live_net=-?[0-9]+' \
    | sed -E 's/.*=(-?[0-9]+)/\1/' \
    | awk '
      { v=$1+0; if (v<0) v=-v; if (v>m) m=v }
      END { if (m=="") print "none"; else print m+0 }'
}

max_position_qty() {
  grep '\[POSITION\].*'"$SYMBOL" "$LOG" 2>/dev/null \
    | grep -oE 'qty=[0-9.]+' \
    | awk -F= '
      { gsub(/[^0-9.]/,"",$2); v=$2+0; if (v>m) m=v }
      END { if (m=="") print "none"; else printf "%.0f", m+0 }' 2>/dev/null || echo "none"
}

count_pat() {
  local pat="$1" n=0
  n="$(grep -cE "$pat" "$LOG" 2>/dev/null)" || true
  [[ -z "$n" ]] && n=0
  echo "$n"
}

# Count pattern lines scoped to this symbol / its desk stacks (multi-product safe).
count_pat_ax() {
  local pat="$1" n=0
  n="$(grep -E "$pat" "$LOG" 2>/dev/null | grep -E "$AX_FILTER" | wc -l | tr -d ' ')" || true
  [[ -z "$n" ]] && n=0
  echo "$n"
}

# Symbols with MM activity in this audit window (for multi-product warning).
detect_mm_symbols() {
  grep -oE 'sym=[A-Z0-9]+-PERP|ax=[A-Z0-9]+-PERP|\[POSITION\] [A-Z0-9]+-PERP' "$LOG" 2>/dev/null \
    | sed -E 's/^(sym=|ax=|\[POSITION\] )//' \
    | sort -u \
    | paste -sd',' -
}

# Per-stack max_po from STACK_INIT / MM_SKEW (multi-stack caps may differ).
stack_max_po() {
  local s="$1"
  local strat
  strat="$(desk_strat "$s")"
  grep -E "stack=${strat}|strategy=${strat}" "$LOG" 2>/dev/null \
    | grep -oE 'max_po=[0-9]+|effective_max_po=[0-9]+' \
    | sed -E 's/.*=//' \
    | sort -n \
    | tail -1
}

last_line() {
  local pat="$1"
  grep -E "$pat" "$LOG" 2>/dev/null | tail -1 || true
}

theo_gap_minutes() {
  local strat="$1"
  local lines
  lines="$(grep "${strat}.*MM_CYCLE_DIAG cycle=theo_move" "$LOG" 2>/dev/null | tail -2 || true)"
  [[ -z "$lines" ]] && { echo ""; return; }
  local n
  n="$(printf '%s\n' "$lines" | wc -l | tr -d ' ')"
  [[ "$n" -lt 2 ]] && { echo ""; return; }
  python3 - "$lines" <<'PY' 2>/dev/null || echo ""
import sys, re
from datetime import datetime
lines = sys.stdin.read().strip().splitlines()
if len(lines) < 2:
    sys.exit(0)
TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
def ts(line):
    m = TS_RE.search(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
t0, t1 = ts(lines[-2]), ts(lines[-1])
if not t0 or not t1:
    sys.exit(0)
print(int((t1 - t0).total_seconds() // 60))
PY
}

last_theo_to_end_minutes() {
  local strat="$1"
  local last end
  last="$(grep "${strat}.*MM_CYCLE_DIAG cycle=theo_move" "$LOG" 2>/dev/null | tail -1 || true)"
  end="$(log_last_ts)"
  [[ -z "$last" || -z "$end" ]] && { echo ""; return; }
  python3 - "$last" "$end" <<'PY' 2>/dev/null || echo ""
import sys, re
from datetime import datetime
last, end = sys.argv[1], sys.argv[2]
TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
def parse(s):
    m = TS_RE.search(s)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
t0, t1 = parse(last), parse(end)
if not t0 or not t1:
    sys.exit(0)
print(int((t1 - t0).total_seconds() // 60))
PY
}

# --- header ---
section "=== MM SOAK AUDIT $(date) ==="
info "log=$LOG_PATH  symbol=$SYMBOL  max_po=$MAX_PO  stacks=$STACKS"
[[ "$LOG" != "$LOG_PATH" ]] && info "audit window: from last SOAK_START= in log (SOAK_ONLY=1)"
ss="$(soak_start)"
[[ -n "$ss" ]] && info "soak marker: $ss" || info "soak marker: (none — run: echo SOAK_START=\$(date -Iseconds) | tee -a mrinal.txt)"
info "log span: $(log_first_ts) → $(log_last_ts)"
_mm_syms="$(detect_mm_symbols)"
if [[ -n "$_mm_syms" && "$_mm_syms" != *"$SYMBOL"* ]]; then
  warn "log contains other MM symbols: $_mm_syms — this run audits SYMBOL=$SYMBOL only; re-run per product."
elif [[ -n "$_mm_syms" && "$_mm_syms" == *","* ]]; then
  warn "multiple products in log: $_mm_syms — run once per SYMBOL= (caps/position are per exchange symbol)."
fi
if [[ "$STACKS" == "*" ]]; then
  warn "no desk stacks auto-detected for $SYMBOL — section 3 may be empty; set STACKS=id1|id2 manually."
fi

# --- section 1: position / cap ---
section "========== 1) NET_PO vs MAX_POSITION (breach?) =========="
grep -E '\[POSITION\]|live_net=|CAP_GATE|HARD_CAP|max_po|MM_CAP|MM_PRODUCT_SUBMIT.*CAP|MM_REDUCE_ONLY|cross_stack_grow_prune|pull_grow_side|breach|projected.*max_po' "$LOG" \
  | grep -E "$AX_FILTER" \
  | grep -v 'net_po=0 ' \
  | tail -"$TAIL" || true

section "--- cap summary ---"
mn="$(max_net_po)"
pq="$(max_position_qty)"
acc="$(count_pat_ax 'MM_CAP.*BREACH_ACCEPTABLE')"
unexp="$(count_pat_ax 'MM_CAP.*BREACH_UNEXPECTED')"
if [[ "$mn" != "none" && "$mn" -gt "$MAX_PO" ]] 2>/dev/null; then
  if [[ "$unexp" -gt 0 ]]; then
    warn "max |net_po|/|live_net| = $mn (> max_po=$MAX_PO) AND BREACH_UNEXPECTED=$unexp — investigate (Joe-style)"
  elif [[ "$acc" -gt 0 ]]; then
    ok "max |net| = $mn (> max_po=$MAX_PO) with BREACH_ACCEPTABLE=$acc (Tim: resting-fill burst — OK)"
  else
    warn "max |net_po|/|live_net| = $mn (> max_po=$MAX_PO) — no BREACH_ACCEPTABLE log; check pull_grow_side / fills"
  fi
else
  ok "max |net_po|/|live_net| in log = ${mn:-none} (cap=$MAX_PO)"
fi
if [[ "$pq" != "none" && "$pq" -gt "$MAX_PO" ]] 2>/dev/null; then
  if [[ "$unexp" -gt 0 ]]; then
    warn "max [POSITION] qty = $pq (> max_po=$MAX_PO) with BREACH_UNEXPECTED"
  elif [[ "$acc" -gt 0 ]]; then
    ok "max [POSITION] qty = $pq (> cap) — Tim acceptable resting-fill bound (see BREACH_ACCEPTABLE)"
  else
    warn "max [POSITION] qty = $pq (> max_po=$MAX_PO)"
  fi
else
  ok "max [POSITION] qty = ${pq:-none} (cap=$MAX_PO)"
fi
[[ "$acc" -gt 0 ]] && info "MM_CAP BREACH_ACCEPTABLE ($SYMBOL): $acc"
[[ "$unexp" -gt 0 ]] && warn "MM_CAP BREACH_UNEXPECTED ($SYMBOL): $unexp — not Tim-acceptable"
ro="$(count_pat_ax 'MM_REDUCE_ONLY_ENTERED')"
[[ "$ro" -gt 0 ]] && info "MM_REDUCE_ONLY_ENTERED count ($SYMBOL): $ro" || true
info "note: max_position is per exchange symbol; all stacks on $SYMBOL share one net position."

# --- section 2: why orders stopped ---
section "========== 2) ORDERS DIED — why? (tail) =========="
grep -E 'MM_RELOAD_LIMIT|gated_by_max_reload|reload_engaged=1|feed_down|external_feed_invalid|ws_disconnected|mm_ax_ws|VENUE_TRUTH|desk_stack_paused|mm_desk_active=false|MM cancel symbol=.*tag=|DESK_RECOVERY|desk_orders_json_stack_missing|product_placement_stalled|lease_denied|gated_by_no_theo|placement_blocked|Orphan fill' "$LOG" \
  | grep -E "$STRAT_FILTER" \
  | tail -"$INCIDENT_TAIL" || true

section "--- incident counts ---"
INCIDENT_ROWS=(
  'feed_down / skip enforce|feed_down:hyperliquid|tag=feed_down'
  'VENUE_TRUTH|VENUE_TRUTH'
  'DESK_RECOVERY|DESK_RECOVERY'
  'lease_denied|product_placement_lease_denied'
  'reload limit|MM_RELOAD_LIMIT|reload_engaged=1'
  'orphan REST net change|Orphan fill: REST NetPo'
)
for row in "${INCIDENT_ROWS[@]}"; do
  label="${row%%|*}"
  pat="${row#*|}"
  c="$(count_pat_ax "$pat")"
  info "$label ($SYMBOL): $c"
done

# --- section 3: stuck / requoting ---
section "========== 3) STUCK / STOPPED REQUOTING? =========="
IFS='|' read -r -a stack_arr <<< "$STACKS"
for s in "${stack_arr[@]}"; do
  [[ "$s" == "*" ]] && continue
  strat="$(desk_strat "$s")"
  _stack_cap="$(stack_max_po "$s")"
  section "--- $strat ---"
  [[ -n "$_stack_cap" && "$_stack_cap" != "$MAX_PO" ]] && info "stack effective max_po=$_stack_cap (audit MAX_PO=$MAX_PO)"
  echo -n "  last theo_move: "
  last_line "${strat}.*MM_CYCLE_DIAG cycle=theo_move" || echo "(none)"
  echo -n "  last MM_GATE:     "
  last_line "${strat}.*MM_GATE" || echo "(none)"
  echo -n "  theo_move count:  "
  count_pat "${strat}.*MM_CYCLE_DIAG cycle=theo_move" || echo 0
  echo -n "  last bid_id state: "
  last_line "${strat}.*MM_GATE" | grep -oE 'bid_id=[0-9]+ ask_id=[0-9]+' || echo "(none)"
  gap_end="$(last_theo_to_end_minutes "$strat")"
  if [[ -n "$gap_end" && "$gap_end" -ge 30 ]]; then
    warn "last theo_move was ${gap_end}m before end of log (possible freeze)"
  fi
done

section "--- last 5 theo_move (all desk stacks) ---"
grep -E 'mm_req_.*'"${SYMBOL//-/_}"'.*MM_CYCLE_DIAG cycle=theo_move' "$LOG" 2>/dev/null | tail -5 || true

pos_gap=""
last_pos="$(grep '\[POSITION\].*'"$SYMBOL" "$LOG" 2>/dev/null | tail -1 || true)"
if [[ -n "$last_pos" ]]; then
  pos_gap="$(python3 - "$last_pos" "$(log_last_ts)" <<'PY' 2>/dev/null || echo ""
import sys, re
from datetime import datetime
last, end = sys.argv[1], sys.argv[2]
TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
def parse(s):
    m = TS_RE.search(s)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
t0, t1 = parse(last), parse(end)
if t0 and t1:
    print(int((t1 - t0).total_seconds() // 60))
PY
)"
fi
[[ -n "$pos_gap" && "$pos_gap" -ge 30 ]] && warn "[POSITION] last update ${pos_gap}m before end of log" || true

# --- conclusions ---
section "========== CONCLUSION =========="

BREACH=0
STALE=0
FEED=0
RELOAD=0
FREEZE=0

[[ "$mn" != "none" && "$mn" -gt "$MAX_PO" ]] 2>/dev/null && BREACH=1
[[ "$pq" != "none" && "$pq" -gt "$MAX_PO" ]] 2>/dev/null && BREACH=1

fd="$(count_pat_ax 'feed_down:hyperliquid|tag=feed_down')"
[[ "$fd" -gt 0 ]] && FEED=1

rl="$(count_pat_ax 'MM_RELOAD_LIMIT|reload_engaged=1')"
[[ "$rl" -gt 0 ]] && RELOAD=1

orph="$(count_pat_ax 'Orphan fill: REST NetPo')"
[[ "$orph" -gt 0 ]] && STALE=1

for s in "${stack_arr[@]}"; do
  strat="$(desk_strat "$s")"
  g="$(last_theo_to_end_minutes "$strat")"
  [[ -n "$g" && "$g" -ge 30 ]] && FREEZE=1
done
[[ -n "$pos_gap" && "$pos_gap" -ge 30 ]] && FREEZE=1

if [[ "$unexp" -gt 0 ]]; then
  warn "CAP: BREACH_UNEXPECTED — |net| exceeded Tim bound (max + resting grow qty); review section 1."
  BREACH=1
elif [[ "$BREACH" -eq 1 && "$acc" -gt 0 ]]; then
  ok "CAP: |net| exceeded max_po=$MAX_PO but logged BREACH_ACCEPTABLE (resting orders filled together — Tim OK)."
elif [[ "$BREACH" -eq 1 ]]; then
  warn "CAP: |net| > max_po=$MAX_PO without BREACH_ACCEPTABLE — review section 1."
elif [[ "$mn" != "none" && "$mn" -eq "$MAX_PO" ]] 2>/dev/null; then
  ok "CAP: at cap |net|=$mn — reduce-only (grow side pulled; cover side quotes expected)."
else
  ok "CAP: |net| stayed within max_po=$MAX_PO in this log."
fi

if [[ "$RELOAD" -eq 1 ]]; then
  warn "RELOAD: reload limit engaged — new MM placement may be halted."
else
  ok "RELOAD: reload limit not engaged in this log."
fi

if [[ "$FEED" -eq 1 ]]; then
  warn "FEED: hyperliquid feed_down seen ($fd hits) — expect cancel-all / skip pair-enforce."
else
  ok "FEED: no feed_down markers in this log."
fi

if [[ "$STALE" -eq 1 ]]; then
  warn "STALE: orphan REST NetPo changes — fills may have missed WS; check reduce-only vs true inventory."
  grep -E 'Orphan fill: REST NetPo' "$LOG" 2>/dev/null | grep -E "$STRAT_FILTER" | tail -5 | sed 's/^/       /' || true
else
  ok "STALE: no orphan REST net jumps in this log."
fi

if [[ "$FREEZE" -eq 1 ]]; then
  warn "FREEZE: gap >= 30m with no theo_move and/or [POSITION] before log end — see section 3."
else
  ok "FREEZE: no long theo_move / POSITION gap before end of log."
fi

last_ro="$(grep 'MM_REDUCE_ONLY_ENTERED' "$LOG" 2>/dev/null | grep -E "$AX_FILTER" | tail -1 || true)"
last_ex="$(grep 'MM_REDUCE_ONLY_EXITED' "$LOG" 2>/dev/null | grep -E "$AX_FILTER" | tail -1 || true)"
if [[ -n "$last_ro" && ( -z "$last_ex" || "$(printf '%s\n' "$last_ro" "$last_ex" | tail -1)" == "$last_ro" ) ]]; then
  info "STATE: likely still in reduce-only at cap (last MM_REDUCE_ONLY_ENTERED after last EXITED)."
elif [[ -n "$last_ex" ]]; then
  info "STATE: reduce-only was exited recently — pairs may be quoting both sides."
fi

section "========== done =========="
