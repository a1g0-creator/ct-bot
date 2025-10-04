import asyncio
import argparse
import logging
import os
import sys

# Ensure the root directory is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from enhanced_trading_system_final_fixed import FinalTradingMonitor, send_telegram_alert
from config import TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID

# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_test(target_id: int, donor_id: int):
    """
    Initializes the monitor and runs the hot-swap test.
    """
    logger.info("Initializing FinalTradingMonitor for hot-swap test...")
    monitor = FinalTradingMonitor()

    # Minimal start to have components ready
    await monitor.start()

    logger.info("Triggering hot_swap_credentials...")
    success = await monitor.hot_swap_credentials(target_account_id=target_id, donor_account_id=donor_id)

    if success:
        logger.info("PASS: hot_swap_credentials completed successfully.")
        await send_telegram_alert("✅ Manual hot-swap test PASSED.")
    else:
        logger.error("FAIL: hot_swap_credentials failed.")
        await send_telegram_alert("❌ Manual hot-swap test FAILED.")

    # Clean shutdown
    await monitor._shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manual Hot-Swap Test Harness")
    parser.add_argument("--target-id", type=int, default=TARGET_ACCOUNT_ID, help="Target account ID for hot-swap.")
    parser.add_argument("--donor-id", type=int, default=DONOR_ACCOUNT_ID, help="Donor account ID for hot-swap.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(f"Starting manual hot-swap test with Target ID: {args.target_id}, Donor ID: {args.donor_id}")

    # It's recommended to stop the systemd service before running this test
    logger.warning("Please ensure the systemd service 'trading-bot' is stopped to avoid interference.")

    try:
        asyncio.run(run_test(args.target_id, args.donor_id))
    except KeyboardInterrupt:
        logger.info("Test interrupted by user.")
    except Exception as e:
        logger.critical(f"Test harness encountered a critical error: {e}", exc_info=True)