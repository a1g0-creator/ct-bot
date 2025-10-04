import asyncio
import logging
import pytest
import re
from unittest.mock import AsyncMock, patch

from enhanced_trading_system_final_fixed import FinalTradingMonitor
from config import TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def monitor():
    """Provides a FinalTradingMonitor instance for testing."""
    # Patch dependencies that make real network calls or require heavy setup
    with patch('enhanced_trading_system_final_fixed.CredentialsStore') as MockCredStore, \
         patch('enhanced_trading_system_final_fixed.EnhancedBybitClient') as MockBybitClient, \
         patch('enhanced_trading_system_final_fixed.FinalFixedWebSocketManager') as MockWebSocketManager:

        # Configure mocks
        MockCredStore.return_value.get_account_credentials.return_value = ("test_key", "test_secret")

        # Configure client mocks to have awaitable methods where needed
        mock_client_instance = MockBybitClient.return_value
        mock_client_instance.invalidate_caches = AsyncMock(return_value=None)

        mock_ws_manager = MockWebSocketManager.return_value
        mock_ws_manager.close = AsyncMock(return_value=None)
        mock_ws_manager.connect = AsyncMock(return_value=None)
        mock_ws_manager.resubscribe_all = AsyncMock(return_value=None)
        # Mock the 'authenticated' property to be True after connect is called
        type(mock_ws_manager).authenticated = property(fget=lambda s: True)


        monitor_instance = FinalTradingMonitor()
        monitor_instance.run_reconciliation_cycle = AsyncMock(return_value=None)

        yield monitor_instance

import io

import io

async def test_hot_swap_log_sequence(monitor: FinalTradingMonitor):
    """
    Verifies that hot_swap_credentials emits logs in the correct sequence by
    manually capturing logs and matching them against specific start-of-step patterns.
    """
    app_logger = logging.getLogger("bybit_trading_system")
    log_stream = io.StringIO()
    stream_handler = logging.StreamHandler(log_stream)
    app_logger.addHandler(stream_handler)

    # These patterns uniquely identify the start of each major step
    step_patterns = {
        "PAUSE": r"HSWAP: PAUSE ctx->\d+ - Hot-swap initiated",
        "CANCEL": r"HSWAP: CANCEL ws - Shutting down WebSocket manager",
        "CLEAR": r"HSWAP: CLEAR caches/stores - Invalidating all in-memory data",
        "REBUILD": r"HSWAP: REBUILD creds - Rebuilding clients with new credentials",
        "RESUB": r"HSWAP: RESUB connect/auth/resubscribe - Reconnecting WebSocket",
        "WARMUP": r"HSWAP: WARMUP reconciliation - Fetching initial state via REST",
        "RESUME": r"HSWAP: RESUME done",
    }
    expected_sequence = list(step_patterns.keys())

    try:
        await monitor.hot_swap_credentials(target_account_id=TARGET_ACCOUNT_ID, donor_account_id=DONOR_ACCOUNT_ID)

        log_contents = log_stream.getvalue()
        hswap_lines = [line for line in log_contents.splitlines() if "HSWAP:" in line]

        # Find the sequence of main steps in the logs
        found_sequence = []
        for line in hswap_lines:
            for step, pattern in step_patterns.items():
                if re.search(pattern, line):
                    if step not in found_sequence: # Add step only once
                        found_sequence.append(step)
                    break

        assert found_sequence == expected_sequence, f"Expected {expected_sequence}, but got {found_sequence}"

    finally:
        app_logger.removeHandler(stream_handler)