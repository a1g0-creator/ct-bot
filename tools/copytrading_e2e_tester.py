import argparse
import subprocess
import sys
import os

def main():
    """
    E2E Test Runner for the Bybit Copy Trading System.

    This script acts as a driver for the pytest-based E2E tests.
    It allows selecting scenarios and modes, passing parameters to the tests
    via environment variables.
    """
    parser = argparse.ArgumentParser(description="E2E Tester for Bybit Copy Trading System.")

    parser.add_argument(
        '--mode',
        type=str,
        choices=['mock', 'testnet'],
        default='mock',
        help="The testing mode. 'mock' uses pre-defined fixtures, 'testnet' connects to Bybit's testnet."
    )

    parser.add_argument(
        '--scenario',
        type=str,
        choices=['s1_margin', 's2_trailing_sell', 's3_side_inversion', 's4_kelly', 's5_web', 'all'],
        default='all',
        help="Which test scenario to run. Use 'all' to run all scenarios."
    )

    # Optional parameters for specific scenarios, allowing for flexible testing
    parser.add_argument('--symbol', type=str, default='BTCUSDT', help="Trading symbol for the test.")
    parser.add_argument('--leverage', type=int, default=10, help="Leverage to use in tests.")
    parser.add_argument('--margin-mode', type=str, choices=['isolated', 'cross'], default='isolated', help="Margin mode for tests.")
    parser.add_argument('--notional', type=float, default=1000, help="Notional value for position size tests.")

    args = parser.parse_args()

    print("--- Starting E2E Test Runner ---")
    print(f"Mode: {args.mode}")
    print(f"Scenario: {args.scenario}")
    print("---------------------------------")

    # Build the pytest command. We target the specific test file.
    pytest_command = [sys.executable, '-m', 'pytest', 'tests/test_copytrading_e2e.py', '-v']

    # Select a specific scenario using pytest's -k flag (keyword matching)
    if args.scenario != 'all':
        # The tests will be named like `test_s1_margin`, `test_s2_trailing_sell`, etc.
        test_name_pattern = f'test_{args.scenario}'
        pytest_command.extend(['-k', test_name_pattern])
        print(f"Filtering for test: {test_name_pattern}")

    # Pass arguments to the test environment using environment variables.
    # This is a clean and standard way to configure tests.
    env = os.environ.copy()
    env['TEST_MODE'] = args.mode
    env['TEST_SYMBOL'] = args.symbol
    env['TEST_LEVERAGE'] = str(args.leverage)
    env['TEST_MARGIN_MODE'] = args.margin_mode
    env['TEST_NOTIONAL'] = str(args.notional)
    # Set a dummy DATABASE_URL for the test environment to allow module imports
    # The tests themselves will mock any actual DB interactions.
    env['DATABASE_URL'] = 'sqlite:///:memory:'

    # Execute pytest as a subprocess
    try:
        # Using check=True to raise CalledProcessError on non-zero exit codes
        result = subprocess.run(pytest_command, env=env, check=True)
        print("\n--- ✅ All selected tests passed! ---")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"\n--- ❌ Some tests failed! (Exit code: {e.returncode}) ---")
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("\n--- ❌ Error: 'pytest' command not found. ---")
        print("Please ensure pytest is installed in your environment (`pip install pytest`).")
        sys.exit(1)

if __name__ == "__main__":
    main()