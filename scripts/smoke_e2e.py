import asyncio
import os
import sys
import logging
from datetime import datetime

# Add project root to path to allow module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from enhanced_trading_system_final_fixed import FinalTradingMonitor
    from stage2_copy_system import AdaptiveOrderManager
    from config import get_api_credentials, TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID
except ImportError as e:
    print(f"Failed to import necessary modules: {e}", file=sys.stderr)
    print("Please ensure the script is run from the project root or the path is set correctly.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
log = logging.getLogger("SmokeE2E")

# --- Environment Configuration ---
SMOKE_SYMBOL = os.getenv("SMOKE_SYMBOL", "ETHUSDT")
SMOKE_LEVERAGE = int(os.getenv("SMOKE_LEVERAGE", "10"))
SMOKE_ACCOUNT_ID_TARGET = int(os.getenv("SMOKE_ACCOUNT_ID_TARGET", TARGET_ACCOUNT_ID))
SMOKE_ACCOUNT_ID_DONOR = int(os.getenv("SMOKE_ACCOUNT_ID_DONOR", DONOR_ACCOUNT_ID))

# --- Main Script ---
async def main():
    """
    Production Smoke E2E test script.
    Runs against real configs and prints a summary.
    """
    summary = {
        "reload": "FAIL",
        "ws_resub": "FAIL",
        "reconcile": "FAIL",
        "leverage": "FAIL",
        "ts_reset": "FAIL",
    }
    ws_sub_count = 0

    log.info(f"SMOKE_START: symbol={SMOKE_SYMBOL}, leverage={SMOKE_LEVERAGE}, "
             f"target_account={SMOKE_ACCOUNT_ID_TARGET}, donor_account={SMOKE_ACCOUNT_ID_DONOR}")

    monitor = None
    try:
        # --- Instantiate Monitor ---
        if not get_api_credentials(SMOKE_ACCOUNT_ID_TARGET) or not get_api_credentials(SMOKE_ACCOUNT_ID_DONOR):
             log.error("FATAL: API credentials not found for smoke test accounts. Exiting.")
             sys.exit(1)

        monitor = FinalTradingMonitor()
        await monitor.start() # Start the monitor to establish connections

        # --- 1. Key Hot-Reload ---
        log.info("--- Step 1: Testing Key Hot-Reload ---")
        await monitor.reload_credentials_and_reconnect()
        if monitor.websocket_manager and monitor.websocket_manager.status == 'authenticated':
            log.info("SMOKE_RELOAD_OK: WebSocket is authenticated after reload.")
            summary["reload"] = "OK"
        else:
            ws_status = monitor.websocket_manager.status if monitor.websocket_manager else "N/A"
            log.error(f"SMOKE_RELOAD_FAIL: WebSocket status is '{ws_status}' after reload.")
            sys.exit(1)

        # --- 2. Resubscribe Check ---
        log.info("--- Step 2: Testing WebSocket Resubscribe ---")
        await monitor.websocket_manager.resubscribe_all()
        subscriptions = monitor.websocket_manager.subscriptions
        ws_sub_count = len(subscriptions)
        log.info(f"SMOKE_WS_RESUBSCRIBED: count={ws_sub_count}, topics={list(subscriptions.keys())}")
        if ws_sub_count == 0:
            log.error("SMOKE_WS_RESUBSCRIBE_FAIL: No subscriptions found after resubscribe call.")
            sys.exit(2)
        summary["ws_resub"] = f"OK ({ws_sub_count})"

        # --- 3. Reconciliation ---
        log.info("--- Step 3: Testing Reconciliation Cycle ---")
        await monitor.run_reconciliation_cycle(enqueue=False)
        log.info("SMOKE_RECONCILE_OK")
        summary["reconcile"] = "OK"

        # --- 4. Set Leverage ---
        log.info(f"--- Step 4: Testing Set Leverage for {SMOKE_SYMBOL} to {SMOKE_LEVERAGE}x ---")
        leverage_result = await monitor.main_client.set_leverage(
            category="linear",
            symbol=SMOKE_SYMBOL,
            leverage=str(SMOKE_LEVERAGE)
        )
        if leverage_result.get("success"):
            ret_code = (leverage_result.get("result") or {}).get("retCode", "N/A")
            log.info(f"SMOKE_LEVERAGE_OK: symbol={SMOKE_SYMBOL}, leverage={SMOKE_LEVERAGE}, retCode={ret_code}")
            summary["leverage"] = "OK"
        else:
            log.error(f"SMOKE_LEVERAGE_FAIL: symbol={SMOKE_SYMBOL}, leverage={SMOKE_LEVERAGE}, result={leverage_result}")
            sys.exit(3)

        # --- 5. Trailing Stop Reset ---
        log.info(f"--- Step 5: Testing Trailing Stop Reset for {SMOKE_SYMBOL} ---")
        order_manager = AdaptiveOrderManager(main_client=monitor.main_client, logger=log)
        ts_reset_result = await order_manager.place_trailing_stop(
            symbol=SMOKE_SYMBOL,
            trailing_stop_price="0",
            position_idx=0
        )
        if ts_reset_result.get("success"):
            log.info(f"SMOKE_TS_RESET_OK: symbol={SMOKE_SYMBOL}")
            summary["ts_reset"] = "OK"
        else:
            log.error(f"SMOKE_TS_RESET_FAIL: symbol={SMOKE_SYMBOL}, result={ts_reset_result}")
            sys.exit(4)

    except SystemExit as e:
        # Catch sys.exit to allow the finally block to run
        raise e
    except Exception as e:
        log.error(f"SMOKE_FATAL_ERROR: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # --- Summary & Exit ---
        summary_line = (
            f"SMOKE_SUMMARY: reload={summary['reload']}, ws_resub={summary['ws_resub']}, "
            f"reconcile={summary['reconcile']}, leverage={summary['leverage']}, "
            f"ts_reset={summary['ts_reset']}"
        )
        log.info(summary_line)
        if monitor and monitor.running:
            await monitor._shutdown()

        if all(val != "FAIL" for val in summary.values()):
            log.info("Smoke test PASSED.")
            sys.exit(0)
        else:
            log.error("Smoke test FAILED.")
            # A specific exit code should have already been called, but this is a fallback.
            sys.exit(1)

if __name__ == "__main__":
    exit_code = 0
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Smoke test interrupted by user.")
        exit_code = 130
    except SystemExit as e:
        exit_code = e.code if e.code is not None else 1
    except Exception:
        exit_code = 1

    sys.exit(exit_code)