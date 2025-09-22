import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# We need to add the project root to the path to import the modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from stage2_copy_system import Stage2CopyTradingSystem
from enhanced_trading_system_final_fixed import FinalTradingMonitor, EnhancedBybitClient

class TestStage2HandlerIntegration(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # 1. Mock Clients
        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)

        # This side effect function will simulate the Bybit API responses
        async def make_request_side_effect(method, endpoint, params=None, data=None):
            if endpoint == "market/tickers":
                return {'retCode': 0, 'result': {'list': [{'symbol': params.get('symbol'), 'lastPrice': '50100', 'bid1Price': '50099', 'ask1Price': '50101'}]}}
            if endpoint == "order/create":
                # Return a successful order creation response
                return {'retCode': 0, 'result': {'orderId': f"order-{data.get('symbol')}"}}
            # Default response for any other calls
            return {'retCode': 0, 'result': {}}

        # Assign mocks to the client's async methods
        self.mock_main_client.get_positions = AsyncMock(return_value=[])
        self.mock_main_client.get_symbol_filters = AsyncMock(return_value={'min_qty': '0.001', 'tick_size': '0.01', 'qty_step': '0.001', 'min_notional': '5.0'})
        self.mock_main_client.get_balance = AsyncMock(return_value=10000.0)
        # The key change: _make_request_with_retry is now a smart mock that simulates the API
        self.mock_main_client._make_request_with_retry = AsyncMock(side_effect=make_request_side_effect)

        self.mock_source_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_source_client.get_balance = AsyncMock(return_value=10000.0)

        # 2. Mock Monitor
        self.mock_monitor = MagicMock(spec=FinalTradingMonitor)
        self.mock_monitor.source_client = self.mock_source_client
        self.mock_monitor.main_client = self.mock_main_client

        # 3. Instantiate the System Under Test with a REAL order manager
        # This is the core of the test refactoring. We no longer mock the order manager.
        self.copy_system = Stage2CopyTradingSystem(base_monitor=self.mock_monitor)

        # 4. Set logger
        self.copy_system.logger = MagicMock()
        # Also mock the loggers inside the real managers
        if hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'order_manager'):
            self.copy_system.copy_manager.order_manager.logger = MagicMock()
        if hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'trailing_manager'):
            self.copy_system.copy_manager.trailing_manager.logger = MagicMock()

    @patch('stage2_copy_system.AdvancedKellyCalculator.calculate_optimal_size', new_callable=AsyncMock)
    @patch('stage2_copy_system.format_quantity_for_symbol_live', new_callable=AsyncMock)
    async def test_open_new_position_when_target_has_none(self, mock_format_qty, mock_kelly_calc):
        """
        Test case: Donor opens a new position, target account has no existing position.
        Expected: An adaptive market order to open a new position is placed via the client.
        """
        # Arrange
        mock_format_qty.side_effect = lambda client, symbol, qty, price: str(round(qty, 3))
        mock_kelly_calc.return_value = {'recommended_size': 0.1, 'kelly_fraction': 0.01}
        self.mock_main_client.get_positions.return_value = []

        donor_position_message = {
            'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1',
            'entryPrice': '50000', 'positionIdx': 0, 'markPrice': '50100'
        }

        # Act
        await self.copy_system.on_position_item(donor_position_message)

        # Assert
        # Check that the order creation endpoint was called by the real AdaptiveOrderManager
        create_order_call = None
        for c in self.mock_main_client._make_request_with_retry.call_args_list:
            if c.args[1] == 'order/create':
                create_order_call = c
                break

        self.assertIsNotNone(create_order_call, "order/create endpoint was not called")

        order_data = create_order_call.kwargs['data']
        self.assertEqual(order_data['symbol'], 'BTCUSDT')
        self.assertEqual(order_data['side'], 'Buy')
        self.assertEqual(order_data['orderType'], 'Market')
        self.assertAlmostEqual(float(order_data['qty']), 0.1)
        self.assertEqual(order_data.get('reduceOnly'), False, "reduceOnly should be False for opening orders")

    @patch('stage2_copy_system.AdvancedKellyCalculator.calculate_optimal_size', new_callable=AsyncMock)
    @patch('stage2_copy_system.format_quantity_for_symbol_live', new_callable=AsyncMock)
    async def test_close_position_when_donor_closes(self, mock_format_qty, mock_kelly_calc):
        """
        Test case: Donor closes a position, target account has an equivalent open position.
        Expected: A market order to close the position on the target account is placed.
        """
        # Arrange
        mock_format_qty.side_effect = lambda client, symbol, qty, price: str(round(qty, 3))
        existing_target_position = {'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1', 'avgPrice': '50000', 'positionIdx': 0}
        self.mock_main_client.get_positions.return_value = [existing_target_position]

        donor_close_message = {'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0', 'positionIdx': 0, 'entryPrice': '50000'}

        # Act
        await self.copy_system.on_position_item(donor_close_message)

        # Assert
        create_order_call = None
        for c in self.mock_main_client._make_request_with_retry.call_args_list:
            if c.args[1] == 'order/create':
                create_order_call = c
                break

        self.assertIsNotNone(create_order_call, "order/create endpoint was not called")

        order_data = create_order_call.kwargs['data']
        self.assertEqual(order_data['symbol'], 'BTCUSDT')
        self.assertEqual(order_data['side'], 'Sell')
        self.assertEqual(order_data['orderType'], 'Market')
        self.assertAlmostEqual(float(order_data['qty']), 0.1)
        self.assertEqual(order_data.get('reduceOnly'), True, "reduceOnly should be True for closing orders")

    @patch('stage2_copy_system.AdvancedKellyCalculator.calculate_optimal_size', new_callable=AsyncMock)
    @patch('stage2_copy_system.format_quantity_for_symbol_live', new_callable=AsyncMock)
    async def test_reduce_position_when_donor_reduces(self, mock_format_qty, mock_kelly_calc):
        """
        Test case: Donor reduces a position, target account has a larger equivalent position.
        Expected: A market order to reduce the position on the target account is placed.
        """
        # Arrange
        mock_format_qty.side_effect = lambda client, symbol, qty, price: str(round(qty, 3))
        existing_target_position = {'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.2', 'avgPrice': '50000', 'positionIdx': 0}
        self.mock_main_client.get_positions.return_value = [existing_target_position]

        # Donor reduces their position to 0.1, so our target proportional size is also 0.1
        mock_kelly_calc.return_value = {'recommended_size': 0.1, 'kelly_fraction': 0.01}

        donor_reduce_message = {'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1', 'positionIdx': 0, 'entryPrice': '50000'}

        # Act
        await self.copy_system.on_position_item(donor_reduce_message)

        # Assert
        create_order_call = None
        for c in self.mock_main_client._make_request_with_retry.call_args_list:
            if c.args[1] == 'order/create':
                create_order_call = c
                break

        self.assertIsNotNone(create_order_call, "order/create endpoint was not called")

        order_data = create_order_call.kwargs['data']
        self.assertEqual(order_data['symbol'], 'BTCUSDT')
        self.assertEqual(order_data['side'], 'Sell')
        self.assertEqual(order_data['orderType'], 'Market')
        # We had 0.2, new target is 0.1, so we sell 0.1
        self.assertAlmostEqual(float(order_data['qty']), 0.1)
        self.assertEqual(order_data.get('reduceOnly'), True, "reduceOnly should be True for reducing orders")

if __name__ == '__main__':
    unittest.main()
