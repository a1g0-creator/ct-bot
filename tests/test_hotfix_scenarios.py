import asyncio
import unittest
import time
import os
from unittest.mock import AsyncMock, patch, MagicMock, call
from collections import deque

# It's crucial to patch modules BEFORE they are imported by other modules.
# We patch 'config' and other low-level modules first.
MOCK_DB_PATH = 'sqlite:///:memory:'
os_environ_patch = patch.dict('os.environ', {'DATABASE_URL': MOCK_DB_PATH})
os_environ_patch.start()

# Mock the database store before it's imported
mock_credentials_db = {
    1: ("old_main_key", "old_main_secret"),
    2: ("old_source_key", "old_source_secret"),
}
def mock_get_api_credentials(account_id):
    return mock_credentials_db.get(account_id)

# Apply patches
patcher_config = patch('config.get_api_credentials', side_effect=mock_get_api_credentials)
patcher_config.start()


# Now, with patches active, we can import the system components
from enhanced_trading_system_final_fixed import FinalTradingMonitor, TradingSignal, SignalType
from stage2_copy_system import Stage2CopyTradingSystem, DynamicTrailingStopManager
from db_models import Base
from db_session import engine


# Stop the initial patchers as we will use more specific ones inside tests
patcher_config.stop()
os_environ_patch.stop()


class TestHotfixAndReloadLogic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ['DATABASE_URL'] = MOCK_DB_PATH
        Base.metadata.drop_all(bind=engine) # Ensure a clean slate
        Base.metadata.drop_all(bind=engine) # Ensure a clean slate
        Base.metadata.create_all(bind=engine)

    @classmethod
    def tearDownClass(cls):
        Base.metadata.drop_all(bind=engine)
        engine.dispose()

    def setUp(self):
        """Set up a fresh event loop for each test."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # Apply os.environ patch for every test
        self.os_patch = patch.dict('os.environ', {'DATABASE_URL': MOCK_DB_PATH})
        self.os_patch.start()


    def tearDown(self):
        """Close the event loop and stop patches."""
        self.os_patch.stop()
        self.loop.close()

    @patch('enhanced_trading_system_final_fixed.send_telegram_alert', new_callable=AsyncMock)
    @patch('enhanced_trading_system_final_fixed.EnhancedBybitClient')
    def test_full_reload_and_sizing_flow(self, MockBybitClient, mock_send_alert):
        """
        Comprehensive test for the hot-reload feature.
        Verifies the entire flow: pause, key swap, cache invalidation, reconnect,
        state refresh, and correct position sizing after resume.
        """
        async def run_test():
            # -- Setup Mocks --
            mock_main_client = AsyncMock()
            mock_source_client = AsyncMock()

            # Configure side effect to assign mocks based on name
            def client_side_effect(api_key, api_secret, api_url, name, **kwargs):
                if name == "MAIN":
                    client = mock_main_client
                else:
                    client = mock_source_client
                client.api_key = api_key
                client.api_secret = api_secret
                client.invalidate_caches = AsyncMock() # Add mock for invalidate_caches
                return client
            MockBybitClient.side_effect = client_side_effect

            # Balance behavior depends on the API key used
            async def get_balance_side_effect(*args, **kwargs):
                if mock_main_client.api_key == "new_main_key":
                    return 10000.0
                return 100.0
            mock_main_client.get_balance = AsyncMock(side_effect=get_balance_side_effect)
            mock_source_client.get_balance = AsyncMock(return_value=50000.0)
            mock_main_client.get_positions.return_value = []
            mock_source_client.get_positions.return_value = []

            # Mock get_api_credentials
            mock_get_api_credentials_func = MagicMock()
            def get_creds_side_effect(account_id):
                if account_id == 1:
                    return ("old_main_key", "old_main_secret")
                return ("old_source_key", "old_source_secret")
            mock_get_api_credentials_func.side_effect = get_creds_side_effect

            with patch('config.get_api_credentials', mock_get_api_credentials_func):
                # -- Test Execution --
                # 1. Initialize system
                monitor = FinalTradingMonitor()

                async def mock_connect_and_set_state(*args, **kwargs):
                    monitor.websocket_manager.ws = MagicMock()
                    monitor.websocket_manager.status = 'authenticated'
                    return True

                monitor.websocket_manager.connect = AsyncMock(side_effect=mock_connect_and_set_state)
                monitor.websocket_manager.close = AsyncMock()

                # 2. Verify initial state (old balance)
                initial_balance = await monitor._get_balance_safe(monitor.main_client, "initial")
                self.assertAlmostEqual(initial_balance, 100.0)

                # 3. Pause the system and defer a signal
                monitor._is_paused = True
                test_signal = TradingSignal(SignalType.POSITION_OPEN, "BTCUSDT", "Buy", 1, 50000, time.time(), {})
                await monitor.signal_processor.add_signal(test_signal)
                self.assertEqual(len(monitor._deferred_queue), 1)
                monitor._is_paused = False

                # 4. Simulate key update and trigger hot-reload
                def new_get_creds_side_effect(account_id):
                    if account_id == 1:
                        return ("new_main_key", "new_main_secret")
                    return ("old_source_key", "old_source_secret")
                mock_get_api_credentials_func.side_effect = new_get_creds_side_effect

                await monitor.reload_credentials_and_reconnect()

                # -- Assertions --
                # a. Correct methods were called
                monitor.main_client.invalidate_caches.assert_called_once()
                monitor.source_client.invalidate_caches.assert_called_once()
                monitor.websocket_manager.close.assert_called_once()
                monitor.websocket_manager.connect.assert_called_once()

                # b. Keys were updated
                self.assertEqual(monitor.main_client.api_key, "new_main_key")

                # c. Balance is updated
                new_balance = await monitor._get_balance_safe(monitor.main_client, "final")
                self.assertAlmostEqual(new_balance, 10000.0)

                # d. Deferred queue was processed
                self.assertEqual(len(monitor._deferred_queue), 0)
                # The signal processor queue should now have the item
                self.assertEqual(monitor.signal_processor.signal_queue.qsize(), 1)

                print("\n✅ TestHotfixAndReloadLogic: test_full_reload_and_sizing_flow PASSED")

        self.loop.run_until_complete(run_test())

    @patch('stage2_copy_system.send_telegram_alert', new_callable=AsyncMock)
    @patch('stage2_copy_system.EnhancedBybitClient')
    def test_trailing_stop_logic_for_sell_position(self, MockBybitClient, mock_send_alert):
        """
        Tests that the activePrice for a SELL position's trailing stop is calculated correctly (below the reference price).
        """
        async def run_test():
            # 1. Setup mocks
            mock_main_client = AsyncMock()
            mock_order_manager = AsyncMock()
            mock_order_manager.place_trailing_stop = AsyncMock()

            mock_main_client.get_symbol_filters.return_value = {'tick_size': '0.01'}

            ts_manager = DynamicTrailingStopManager(
                main_client=mock_main_client,
                order_manager=mock_order_manager,
                trailing_config={'enabled': True, 'activation_pct': 0.02}
            )

            # 2. Define a SELL position and mock exchange state that allows setting a TS
            sell_position = {
                'symbol': 'TWTUSDT', 'side': 'Sell', 'position_idx': 0,
                'entryPrice': '1.2000', 'size': '100',
            }

            # The key fix: lastPrice and sessionAvgPrice must be > calculated active_price for the constraint to pass
            mock_main_client.get_positions.return_value = [
                {
                    'symbol': 'TWTUSDT', 'positionIdx': 0, 'side': 'Sell',
                    'markPrice': '1.2500', 'entryPrice': '1.2000',
                    'trailingStop': '0', 'activePrice': '0',
                    'lastPrice': '1.2300', # Must be > 1.22
                    'sessionAvgPrice': '1.2300', # Must be > 1.22
                }
            ]

            # 3. Call the method
            await ts_manager.create_or_update_trailing_stop(sell_position)

            # 4. Assertions
            mock_order_manager.place_trailing_stop.assert_called_once()
            kwargs = mock_order_manager.place_trailing_stop.call_args.kwargs

            active_price = float(kwargs.get('active_price'))
            ref_price = 1.2500

            self.assertLess(active_price, ref_price)

            # Expected: 1.25 * (1 - 0.02) = 1.225. Rounded to tick 0.01 -> 1.22
            self.assertAlmostEqual(active_price, 1.22)

            print("\n✅ TestHotfixAndReloadLogic: test_trailing_stop_logic_for_sell_position PASSED")

        self.loop.run_until_complete(run_test())

if __name__ == '__main__':
    unittest.main()