import asyncio
import unittest
import time
import os
from unittest.mock import AsyncMock, patch, MagicMock

# It's crucial to patch modules BEFORE they are imported by other modules.
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
from enhanced_trading_system_final_fixed import FinalTradingMonitor, FinalFixedWebSocketManager, TradingSignal, SignalType
from db_models import Base
from db_session import engine

# Stop the initial patchers as we will use more specific ones inside tests
patcher_config.stop()
os_environ_patch.stop()


class TestHotReloadFinalization(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create tables once for the entire test class
        os.environ['DATABASE_URL'] = MOCK_DB_PATH
        Base.metadata.drop_all(bind=engine) # Ensure a clean slate
        Base.metadata.create_all(bind=engine)

    @classmethod
    def tearDownClass(cls):
        # Drop all tables once after all tests in the class have run
        Base.metadata.drop_all(bind=engine)

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
    def test_resubscribe_called_and_topics_nonempty(self, MockBybitClient, mock_send_alert):
        """
        Tests that after a hot-reload, the resubscribe_all method is called
        and that the subscriptions map is not empty.
        """
        async def run_test():
            # -- Setup Mocks --
            mock_main_client = AsyncMock()
            mock_source_client = AsyncMock()
            MockBybitClient.side_effect = lambda *args, **kwargs: mock_main_client if kwargs.get('name') == "MAIN" else mock_source_client
            mock_main_client.get_balance.return_value = 1000.0
            mock_source_client.get_balance.return_value = 50000.0
            mock_main_client.get_positions.return_value = []
            mock_source_client.get_positions.return_value = []
            mock_main_client.invalidate_caches = AsyncMock()
            mock_source_client.invalidate_caches = AsyncMock()

            # Mock get_api_credentials
            with patch('config.get_api_credentials', return_value=("key", "secret")):
                # -- Test Execution --
                monitor = FinalTradingMonitor()

                # Replace the real websocket manager with a mock
                monitor.websocket_manager = AsyncMock(spec=FinalFixedWebSocketManager)
                monitor.websocket_manager.subscriptions = {}

                # Simulate initial connection and subscription
                async def mock_connect(*args, **kwargs):
                    monitor.websocket_manager.status = 'authenticated'
                    monitor.websocket_manager.ws = MagicMock()
                    # Simulate _subscribe_to_events populating the map
                    monitor.websocket_manager.subscriptions = {
                        "position": "position", "order": "order", "execution": "execution", "wallet": "wallet"
                    }
                    return True
                monitor.websocket_manager.connect.side_effect = mock_connect
                monitor.websocket_manager.resubscribe_all = AsyncMock(return_value=4)

                # Start the system to populate subscriptions
                await monitor.websocket_manager.connect()

                # Verify subscriptions are populated
                self.assertGreater(len(monitor.websocket_manager.subscriptions), 0, "Subscriptions should be populated after connect")

                # Trigger hot-reload
                await monitor.reload_credentials_and_reconnect()

                # -- Assertions --
                monitor.websocket_manager.resubscribe_all.assert_called_once()

                # Check that the log message confirms the number of resubscribed topics
                alert_call = mock_send_alert.call_args_list[0]
                alert_text = alert_call.args[0]
                self.assertIn("✅ Горячая перезагрузка завершена", alert_text)

                print("\n✅ TestHotReloadFinalization: test_resubscribe_called_and_topics_nonempty PASSED")

        self.loop.run_until_complete(run_test())

    @patch('enhanced_trading_system_final_fixed.send_telegram_alert', new_callable=AsyncMock)
    @patch('enhanced_trading_system_final_fixed.EnhancedBybitClient')
    def test_pause_waits_for_inflight(self, MockBybitClient, mock_send_alert):
        """
        Tests that pause_processing correctly waits for an in-flight signal to finish
        by using the new asyncio.Event mechanism.
        """
        async def run_test():
            # -- Setup --
            MockBybitClient.return_value = AsyncMock()

            with patch('config.get_api_credentials', return_value=("key", "secret")):
                monitor = FinalTradingMonitor()
                processor = monitor.signal_processor

                # Start the processor so it's ready to accept signals
                await processor.start_processing()

                # Patch the signal execution to simulate a long-running task
                original_execute = processor._execute_signal_processing
                signal_processed = asyncio.Event()
                async def slow_execute(signal):
                    print("Slow task started...")
                    await asyncio.sleep(1) # Simulate 1 second of work
                    await original_execute(signal)
                    signal_processed.set()
                    print("Slow task finished.")

                processor._execute_signal_processing = slow_execute

                # -- Test Execution --
                # 1. Add a signal to the queue.
                test_signal = TradingSignal(SignalType.POSITION_OPEN, "ETHUSDT", "Buy", 0.1, 3000, time.time(), {})
                await processor.add_signal(test_signal)

                # Give the event loop a moment to pick up the signal
                await asyncio.sleep(0.01)

                # 2. Call pause_processing and measure the time
                start_time = time.time()
                # This should now block until the slow task is complete
                await monitor.pause_processing(timeout=3)
                end_time = time.time()

                # -- Assertions --
                # a. The pause should have taken at least as long as the slow task
                self.assertGreaterEqual(end_time - start_time, 1.0)

                # b. The signal_processed event should be set, confirming completion
                self.assertTrue(signal_processed.is_set(), "The slow signal should have been fully processed.")

                # c. The system should be in a paused state
                self.assertTrue(monitor._is_paused, "System should be in a paused state after pause_processing.")

                await processor.stop_processing()
                print("\n✅ TestHotReloadFinalization: test_pause_waits_for_inflight PASSED")

        self.loop.run_until_complete(run_test())

    @patch('enhanced_trading_system_final_fixed.send_telegram_alert', new_callable=AsyncMock)
    @patch('enhanced_trading_system_final_fixed.EnhancedBybitClient')
    def test_deferred_ops_delivery_idempotent(self, MockBybitClient, mock_send_alert):
        """
        Tests that a critical operation (e.g., close) deferred during a pause
        is processed exactly once upon resume, even if duplicated.
        """
        async def run_test():
            # -- Setup --
            MockBybitClient.return_value = AsyncMock()
            with patch('config.get_api_credentials', return_value=("key", "secret")):
                monitor = FinalTradingMonitor()
                processor = monitor.signal_processor

                # Mock the underlying method that puts signals into the actual processing queue
                processor.signal_queue.put_nowait = MagicMock()

                # -- Test Execution --
                # 1. Pause the system
                await monitor.pause_processing()

                # 2. Create and defer a critical signal TWICE with the same data
                ts = time.time()
                close_signal = TradingSignal(
                    SignalType.POSITION_CLOSE, "ADAUSDT", "Sell", 100, 0.45, ts, {'position_idx': 1}
                )

                await processor.add_signal(close_signal)
                await processor.add_signal(close_signal)

                # -- Assertions (before resume) --
                self.assertEqual(len(monitor._deferred_ops_queue), 2)
                self.assertTrue(processor.signal_queue.put_nowait.call_count == 0)

                # 3. Resume processing
                await monitor.resume_processing()

                # -- Assertions (after resume) --
                # a. The signal should have been put into the actual processing queue only ONCE
                processor.signal_queue.put_nowait.assert_called_once()

                # b. The deferred ops queue should now be empty
                self.assertEqual(len(monitor._deferred_ops_queue), 0)

                # c. One idempotency key should be in the processed set
                self.assertEqual(len(monitor._processed_op_keys), 1)

                expected_key = f"ADAUSDT|1|{SignalType.POSITION_CLOSE.value}|{int(ts)}"
                self.assertIn(expected_key, monitor._processed_op_keys)

                print("\n✅ TestHotReloadFinalization: test_deferred_ops_delivery_idempotent PASSED")

        self.loop.run_until_complete(run_test())

if __name__ == '__main__':
    unittest.main()