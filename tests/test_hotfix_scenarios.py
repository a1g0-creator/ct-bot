import asyncio
import unittest
from unittest.mock import AsyncMock, patch

# Import the real classes we want to test
from enhanced_trading_system_final_fixed import FinalTradingMonitor
from stage2_copy_system import DynamicTrailingStopManager

class TestHotfixScenarios(unittest.TestCase):

    def setUp(self):
        """Set up a fresh event loop for each test."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        """Close the event loop."""
        self.loop.close()

    @patch('enhanced_trading_system_final_fixed.send_telegram_alert', new_callable=AsyncMock)
    @patch('enhanced_trading_system_final_fixed.FinalFixedWebSocketManager')
    @patch('enhanced_trading_system_final_fixed.EnhancedBybitClient')
    @patch('config.get_api_credentials')
    def test_credential_reload_and_balance_update(self, mock_get_creds, MockBybitClient, MockWSManager, mock_send_alert):
        """
        Tests if the system correctly reloads credentials, fetches the new balance,
        and uses it for position size calculation.
        """
        async def run_test():
            # -- Setup --
            # Mock clients that will be created by the monitor
            mock_main_client = AsyncMock()
            mock_source_client = AsyncMock()

            def client_side_effect(api_key, api_secret, api_url, name, copy_state=None):
                if name == "MAIN":
                    client = mock_main_client
                    client.api_key = api_key
                    client.api_secret = api_secret
                    return client
                client = mock_source_client
                client.api_key = api_key
                client.api_secret = api_secret
                return client
            MockBybitClient.side_effect = client_side_effect

            # Mock balance behavior based on the current API key
            async def get_balance_side_effect(*args, **kwargs):
                if mock_main_client.api_key == "new_main_key":
                    return 10000.0  # New, correct balance
                return 100.0    # Old, incorrect balance
            mock_main_client.get_balance = AsyncMock(side_effect=get_balance_side_effect)

            # 1. Initialize system with old credentials
            mock_get_creds.return_value = ("old_main_key", "old_main_secret")
            monitor = FinalTradingMonitor()
            monitor.websocket_manager.reconnect = AsyncMock() # mock reconnect behavior

            # 2. Verify initial state
            initial_balance = await monitor.main_client.get_balance()
            self.assertAlmostEqual(initial_balance, 100.0)

            # 3. Simulate key update and reload
            def new_get_creds(account_id):
                if account_id == 1: # TARGET_ACCOUNT_ID
                    return ("new_main_key", "new_main_secret")
                return ("old_source_key", "old_source_secret")
            mock_get_creds.side_effect = new_get_creds

            await monitor.reload_credentials_and_reconnect()

            # 4. Verify final state
            self.assertEqual(monitor.main_client.api_key, "new_main_key")
            new_balance = await monitor.main_client.get_balance()
            self.assertAlmostEqual(new_balance, 10000.0)

            print("\n✅ TestHotfixScenarios: test_credential_reload_and_balance_update PASSED")

        self.loop.run_until_complete(run_test())

    def test_trailing_stop_logic_for_sell_position(self):
        """
        Tests that the activePrice for a SELL position's trailing stop is calculated correctly (below the reference price).
        """
        async def run_test():
            # 1. Setup mocks
            mock_main_client = AsyncMock()
            mock_order_manager = AsyncMock()

            mock_main_client.get_symbol_filters.return_value = {'tick_size': '0.01'}
            mock_order_manager.place_trailing_stop = AsyncMock()

            ts_manager = DynamicTrailingStopManager(
                main_client=mock_main_client,
                order_manager=mock_order_manager,
                trailing_config={'enabled': True, 'activation_pct': 0.02} # 2% activation
            )

            # 2. Define a SELL position and mock exchange state
            sell_position = {
                'symbol': 'TWTUSDT', 'side': 'Sell', 'position_idx': 0,
                'entryPrice': '1.2000', 'size': '100',
            }

            mock_main_client.get_positions.return_value = [
                {
                    'symbol': 'TWTUSDT', 'positionIdx': 0, 'side': 'Sell',
                    'markPrice': '1.2500', 'entryPrice': '1.2000',
                    'trailingStop': '0', 'activePrice': '0',
                    'lastPrice': '1.2800',  # Increased to pass constraint check
                    'sessionAvgPrice': '1.2800', # Increased to pass constraint check
                }
            ]

            # 3. Call the method under test
            await ts_manager.create_or_update_trailing_stop(sell_position)

            # 4. Assertions
            mock_order_manager.place_trailing_stop.assert_called_once()
            kwargs = mock_order_manager.place_trailing_stop.call_args.kwargs

            active_price = float(kwargs.get('active_price'))
            ref_price = 1.2500

            self.assertLess(active_price, ref_price, "For a SELL position, active_price should be below the reference price.")

            # Expected price = 1.25 * (1 - 0.02) = 1.225.
            # Python's round(122.5) is 122, so it rounds to 1.22.
            self.assertAlmostEqual(active_price, 1.22)

            print("\n✅ TestHotfixScenarios: test_trailing_stop_logic_for_sell_position PASSED")

        self.loop.run_until_complete(run_test())

if __name__ == '__main__':
    unittest.main()