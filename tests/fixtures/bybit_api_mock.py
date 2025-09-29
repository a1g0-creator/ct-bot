import pytest
from unittest.mock import MagicMock, AsyncMock

@pytest.fixture
def mock_bybit_client():
    """
    A pytest fixture that provides a mock of the EnhancedBybitClient.

    This mock is designed to:
    - Simulate the methods that the copy trading system will call.
    - Record the calls made to it for later assertion.
    - Return predictable data.
    """
    client = MagicMock()

    # Mock async methods using AsyncMock
    client.set_leverage = AsyncMock(return_value={'success': True, 'retCode': 0})
    client.place_order = AsyncMock(return_value={'success': True, 'retCode': 0, 'result': {'orderId': 'mock_order_id_123'}})
    client.get_positions = AsyncMock(return_value=[])
    client.get_wallet_balance = AsyncMock(return_value=10000.0) # Default balance for tests
    client.get_symbol_filters = AsyncMock(return_value={
        'min_qty': 0.001,
        'qty_step': 0.001,
        'tick_size': 0.01,
        'min_notional': 5.0
    })

    # A mock for the yet-to-be-implemented set_margin_mode
    # The real implementation would call a specific endpoint. Here we just mock the client method.
    client.set_margin_mode = AsyncMock(return_value={'success': True})

    # A mock for setting a trailing stop
    client.place_trailing_stop = AsyncMock(return_value={'success': True})

    # Add mock for the low-level request method to fix S3 test setup
    client._make_request_with_retry = AsyncMock(return_value={'success': True, 'retCode': 0, 'result': {'orderId': 'mock_order_id_123'}})

    return client