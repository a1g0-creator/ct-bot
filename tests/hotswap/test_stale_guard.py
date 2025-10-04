import asyncio
import logging
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from enhanced_trading_system_final_fixed import FinalTradingMonitor, FinalFixedWebSocketManager, ProductionSignalProcessor

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def monitor_with_components():
    """Provides a monitor with real child components for integration testing the guard."""
    monitor = FinalTradingMonitor()
    # Replace clients and other external dependencies, but keep the core logic
    monitor.source_client = MagicMock()
    monitor.main_client = MagicMock()

    # The guards are in the WS manager and Signal Processor, so we use real instances
    monitor.websocket_manager = FinalFixedWebSocketManager(api_key="test", api_secret="test", name="TestWS", final_monitor=monitor)
    monitor.signal_processor = ProductionSignalProcessor(account_id=2, monitor=monitor)

    # Link the signal processor as the handler for position updates
    monitor.websocket_manager.register_handler('position', monitor.signal_processor.process_position_update)

    return monitor

async def test_position_update_stale_guard(monitor_with_components: FinalTradingMonitor):
    """
    Tests that the stale context version guard in process_position_update prevents processing.
    """
    monitor = monitor_with_components
    processor = monitor.signal_processor

    # Mock the method that would be called if the guard fails
    processor.add_signal = AsyncMock()

    # Simulate a post-hot-swap state
    monitor.context_version = 5

    # Simulate a stale event from an old WebSocket context
    stale_context_version = 4
    stale_position_data = {'symbol': 'BTCUSDT', 'size': '1', 'side': 'Buy', 'positionIdx': 1}

    # Call the processor directly, as the WS manager would
    await processor.process_position_update(stale_position_data, context_version=stale_context_version)

    # Assert that no signal was added because the event was stale
    processor.add_signal.assert_not_called()

@pytest.mark.parametrize("topic, handler_name, payload", [
    ("wallet", "_handle_wallet_update", {'data': [{'coin': 'USDT', 'walletBalance': '1000'}]}),
    ("order", "_handle_order_update", {'data': [{'symbol': 'BTCUSDT', 'orderStatus': 'Filled'}]}),
    ("execution", "_handle_execution_update", {'data': [{'symbol': 'BTCUSDT', 'execQty': '1'}]}),
])
async def test_generic_stale_event_guards(monitor_with_components: FinalTradingMonitor, topic, handler_name, payload):
    """
    Tests the stale context guard in other WebSocket handlers (wallet, order, execution).
    """
    monitor = monitor_with_components
    ws_manager = monitor.websocket_manager

    # Create a mock handler for the specific topic
    mock_handler = AsyncMock()
    ws_manager.register_handler(topic, mock_handler)

    # Simulate a post-hot-swap state on the main monitor
    monitor.context_version = 10

    # Simulate the WebSocket manager still having an old context version
    ws_manager.context_version = 9

    # Get the handler method from the WebSocket manager instance
    handler_method = getattr(ws_manager, handler_name)

    # Call the handler with a stale payload
    await handler_method(payload)

    # Assert that our registered mock handler was NOT called because the guard caught the stale event
    mock_handler.assert_not_called()