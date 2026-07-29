# Scripts

## `soak_audit.sh`

Grep an MM soak log and print cap / incident / freeze checks plus a short verdict.

```bash
chmod +x scripts/soak_audit.sh

echo "SOAK_START=$(date -Iseconds)" | tee -a mrinal.txt

MAX_PO=auto STACKS=auto ./scripts/soak_audit.sh
```

Run each line separately in zsh (do not paste comment lines starting with `#`).

Environment variables:

| Variable | Default | Meaning |
|----------|---------|---------|
| `LOG` | `mrinal.txt` | Log file path |
| `SYMBOL` | `XAG-PERP` | AX symbol |
| `STACKS` | `auto` | `auto` finds `mm_req_XAG_PERP_<8hex>` stack ids in log |
| `MAX_PO` | `auto` | Cap for breach checks; `auto` = mode of `max_po=` in log |
| `SOAK_ONLY` | `auto` | If log has `SOAK_START=`, audit only from that line onward |
| `TAIL` | `80` | Lines in section 1 |
| `INCIDENT_TAIL` | `60` | Lines in section 2 |

Desk strategy names are `mm_req_XAG_PERP_<stackId>` (not `…_PERP_…`).

### Multi-stack / multi-product

| Scenario | Supported? | Notes |
|----------|------------|--------|
| **Several stacks, one symbol** (e.g. 3× XAG) | Yes | `STACKS=auto` finds all; exchange **net position is shared** — cap/reduce-only are product-wide (`cross_stack_grow_prune`). |
| **Several symbols** (XAG + BTC + …) | **One run per symbol** | Set `SYMBOL=BTC-PERP` (and re-run). Script warns if other symbols appear in the same log. |
| **Different `max_position` per stack** | Partial | `MAX_PO=auto` uses the most common value in log; per-stack overrides are shown in section 3 when they differ. |
| **Legacy `make_market_*` + desk stacks on same AX** | Yes | Both included in filters for that symbol. |

Incident counts and conclusions are scoped to `SYMBOL` (not the whole log file).

Exit code is `0` when the log exists; read the `CONCLUSION` block for pass/fail signals.
