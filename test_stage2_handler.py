import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# We need to add the project root to the path to import the modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from stage2_copy_system import Stage2CopyTradingSystem, AdaptiveOrderManager, CopyOrder, OrderStrategy
from enhanced_trading_system_final_fixed import FinalTradingMonitor, EnhancedBybitClient, ConnectionStatus

class TestStage2Handler(unittest.IsolatedAsyncioTestCase):

    @patch('stage2_copy_system.format_quantity_for_symbol_live', new_callable=AsyncMock)
    async def asyncSetUp(self, mock_format_qty):
        # Mock the formatter to just return the quantity as a string without modification
        mock_format_qty.side_effect = lambda client, symbol, qty, price: str(qty)

        # 1. Mock Clients
        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client.get_positions.return_value = []
        self.mock_main_client.get_symbol_filters.return_value = {'tick_size': '0.01', 'qty_step': '0.001'}
        # The place_adaptive_order will call the client's place_order, so we mock that
        self.mock_main_client.place_order = AsyncMock(return_value={'orderId': 'mock-order-id-123'})
        self.mock_main_client._make_request_with_retry.return_value = {'retCode': 0, 'result': {'list': [{'lastPrice': '50100'}]}}


        self.mock_source_client = AsyncMock(spec=EnhancedBybitClient)

        # 2. Mock Monitor and its sub-components
        self.mock_monitor = MagicMock(spec=FinalTradingMonitor)
        self.mock_monitor.source_client = self.mock_source_client
        self.mock_monitor.main_client = self.mock_main_client
        self.mock_monitor.websocket_manager = MagicMock() # Basic mock for ws_manager

        # 3. Instantiate the System Under Test
        self.copy_system = Stage2CopyTradingSystem(base_monitor=self.mock_monitor)

        # 4. We need a real AdaptiveOrderManager, but it uses the mocked client
        self.copy_system.order_manager = AdaptiveOrderManager(main_client=self.mock_main_client)

        # Set logger to avoid errors if it's not configured in test environment
        self.copy_system.logger = MagicMock()


    async def test_open_new_position_when_target_has_none(self):
        """
        Test case: Donor opens a new position, target account has no existing position.
        Expected: A market order to open a new position on the target account is placed.
        """
        # Arrange: No positions on the main account
        self.mock_main_client.get_positions.return_value = []

        # A sample normalized position message from the donor's WebSocket
        donor_position_message = {
            'symbol': 'BTCUSDT',
            'side': 'Buy',
            'size': '0.1',
            'entryPrice': '50000',
            'positionIdx': 0,
            'leverage': '10',
            'markPrice': '50100'
        }

        # Act: Process the message
        await self.copy_system.on_position_item(donor_position_message)

        # Assert: Check that place_order was called on the mock client
        self.mock_main_client.place_order.assert_called_once()

        # Check the details of the called order
        call_kwargs = self.mock_main_client.place_order.call_args.kwargs

        self.assertEqual(call_kwargs['symbol'], 'BTCUSDT')
        self.assertEqual(call_kwargs['side'], 'Buy')
        # For now, we assume a 1:1 copy ratio without Kelly, so qty matches donor size
        self.assertEqual(call_kwargs['qty'], '0.1')
        self.assertEqual(call_kwargs['orderType'], 'Market')
        self.assertFalse(call_kwargs.get('reduceOnly', False)) # Should not be reduceOnly for opening

    async def test_close_position_when_donor_closes(self):
        """
        Test case: Donor closes a position, target account has an equivalent open position.
        Expected: A market order to close the position on the target account is placed.
        """
        # Arrange: An existing position on the main account
        existing_target_position = {
            'symbol': 'BTCUSDT',
            'side': 'Buy',
            'size': '0.1',
            'avgPrice': '50000',
            'positionIdx': 0
        }
        self.mock_main_client.get_positions.return_value = [existing_target_position]

        # A sample "close" message from the donor's WebSocket (size is 0)
        donor_close_message = {
            'symbol': 'BTCUSDT',
            'side': 'Buy', # Side can be present even on close
            'size': '0',
            'positionIdx': 0,
            'entryPrice': '50000',
        }

        # Act
        await self.copy_system.on_position_item(donor_close_message)

        # Assert
        self.mock_main_client.place_order.assert_called_once()

        call_kwargs = self.mock_main_client.place_order.call_args.kwargs

        self.assertEqual(call_kwargs['symbol'], 'BTCUSDT')
        self.assertEqual(call_kwargs['side'], 'Sell') # Opposite side to close
        self.assertEqual(call_kwargs['qty'], '0.1') # Close the full size
        self.assertTrue(call_kwargs.get('reduceOnly'))

if __name__ == '__main__':
    # This allows running the test script directly
    unittest.main()
