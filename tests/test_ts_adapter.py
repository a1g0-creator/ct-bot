import pytest
import asyncio
from unittest.mock import AsyncMock, patch
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from stage2_copy_system import DynamicTrailingStopManager, AdaptiveOrderManager, EnhancedBybitClient

@pytest.fixture
def mock_main_client():
    """Fixture for a mocked EnhancedBybitClient."""
    client = AsyncMock(spec=EnhancedBybitClient)
    client.get_symbol_filters = AsyncMock(return_value={
        'tick_size': 0.01,
        'qty_step': 0.001,
        'min_qty': 0.001,
        'min_notional': 5.0
    })
    client.get_positions = AsyncMock(return_value=[])
    client._make_single_request = AsyncMock(return_value={'retCode': 0, 'result': {}})
    return client

@pytest.fixture
def mock_order_manager(mock_main_client):
    """Fixture for a mocked AdaptiveOrderManager."""
    manager = AdaptiveOrderManager(main_client=mock_main_client)
    manager.place_trailing_stop = AsyncMock(return_value={'success': True})
    return manager

@pytest.fixture
def ts_manager(mock_main_client, mock_order_manager):
    """Fixture for the DynamicTrailingStopManager."""
    config = {
        'enabled': True,
        'mode': 'conservative',
        'activation_pct': 0.05, # 5%
        'step_pct': 0.02, # 2%
        'min_notional_for_ts': 50.0,
    }
    manager = DynamicTrailingStopManager(
        main_client=mock_main_client,
        order_manager=mock_order_manager,
        trailing_config=config
    )
    return manager

@pytest.mark.asyncio
async def test_ts_reset_sends_correct_payload(ts_manager, mock_order_manager):
    """
    Tests that resetting a trailing stop sends the correct payload
    with all required empty keys to avoid a 10001 error.
    """
    position_data = {'symbol': 'DOGEUSDT', 'position_idx': 0}

    # This call should trigger the reset logic in place_trailing_stop
    await ts_manager.remove_trailing_stop(position_data)

    # Verify that place_trailing_stop was called with an empty string
    mock_order_manager.place_trailing_stop.assert_awaited_once_with(
        symbol='DOGEUSDT',
        trailing_stop_price="",
        position_idx=0
    )

    # To test the actual payload, we need to look inside the real place_trailing_stop
    # Let's re-configure the mock to use the real method instead of a mock
    real_order_manager = AdaptiveOrderManager(main_client=ts_manager.main_client)
    ts_manager.order_manager = real_order_manager

    await ts_manager.remove_trailing_stop(position_data)

    # Check the arguments of the underlying API call
    ts_manager.main_client._make_single_request.assert_awaited_with(
        "POST",
        "position/trading-stop",
        data={
            "category": "linear",
            "symbol": "DOGEUSDT",
            "positionIdx": 0,
            "takeProfit": "",
            "stopLoss": "",
            "trailingStop": ""
        }
    )

@pytest.mark.asyncio
async def test_ts_create_sends_correct_payload(ts_manager):
    """
    Tests that creating a new trailing stop sends the correct payload.
    """
    ts_manager.main_client.get_positions = AsyncMock(return_value=[
        {'symbol': 'DOGEUSDT', 'positionIdx': 0, 'side': 'Buy', 'markPrice': '0.15', 'trailingStop': '0', 'activePrice': '0'}
    ])

    position_data = {'symbol': 'DOGEUSDT', 'position_idx': 0, 'entryPrice': 0.14, 'size': 500, 'side': 'Buy'}

    await ts_manager.create_or_update_trailing_stop(position_data)

    ts_manager.order_manager.place_trailing_stop.assert_awaited_once()
    args, kwargs = ts_manager.order_manager.place_trailing_stop.call_args

    assert kwargs['symbol'] == 'DOGEUSDT'
    assert float(kwargs['trailing_stop_price']) > 0
    assert float(kwargs['active_price']) > 0
    assert kwargs['position_idx'] == 0

@pytest.mark.asyncio
async def test_ts_update_is_idempotent(ts_manager):
    """
    Tests that if the trailing stop is already set correctly, no API call is made.
    """
    # New calculated values
    new_ts = 0.003
    new_active_price = 0.1414

    # Mock the position to have almost identical values
    ts_manager.main_client.get_positions = AsyncMock(return_value=[
        {'symbol': 'DOGEUSDT', 'positionIdx': 0, 'side': 'Buy', 'markPrice': '0.15',
         'trailingStop': '0.00301', 'activePrice': '0.14139'}
    ])

    # Mock the _round_to_tick to return deterministic values
    with patch.object(ts_manager, '_round_to_tick', side_effect=[new_ts, new_active_price]) as mock_round:
        position_data = {'symbol': 'DOGEUSDT', 'position_idx': 0, 'entryPrice': 0.14, 'size': 500, 'side': 'Buy'}
        await ts_manager.create_or_update_trailing_stop(position_data)

    # Assert that place_trailing_stop was NOT called
    ts_manager.order_manager.place_trailing_stop.assert_not_awaited()

@pytest.mark.asyncio
async def test_ts_update_triggers_on_value_change(ts_manager):
    """
    Tests that if the trailing stop values differ, an API call is made.
    """
    # Mock the position to have old values
    ts_manager.main_client.get_positions = AsyncMock(return_value=[
        {'symbol': 'DOGEUSDT', 'positionIdx': 0, 'side': 'Buy', 'markPrice': '0.15',
         'trailingStop': '0.001', 'activePrice': '0.13'}
    ])

    position_data = {'symbol': 'DOGEUSDT', 'position_idx': 0, 'entryPrice': 0.14, 'size': 500, 'side': 'Buy'}
    await ts_manager.create_or_update_trailing_stop(position_data)

    # Assert that place_trailing_stop WAS called
    ts_manager.order_manager.place_trailing_stop.assert_awaited_once()
