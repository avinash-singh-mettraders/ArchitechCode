#pragma once

/**
 * Mirrors examples/main.cpp: when true, desk orders.json reconcile (and stack
 * presence checks in MakeMarketStrategy) read mm_desk.orders_config_frozen_copy_path
 * instead of the live file while the Python UI holds a freeze.
 */
extern bool g_mm_orders_reconcile_from_frozen_copy;
