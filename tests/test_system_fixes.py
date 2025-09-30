import asyncio
import time
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

# Temporarily add project root to path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from enhanced_trading_system_final_fixed import (
    FinalTradingMonitor,
    FinalFixedWebSocketManager,
    TradingSignal,
    SignalType
)
from stage2_copy_system import Stage2CopyTradingSystem


class TestSystemFixes(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Mock dependencies for FinalTradingMonitor
        self.mock_source_client = MagicMock()
        self.mock_main_client = MagicMock()

        # Patch the constructors of the clients to avoid real API calls
        patcher_source = patch('enhanced_trading_system_final_fixed.EnhancedBybitClient', return_value=self.mock_source_client)
        patcher_main = patch('enhanced_trading_system_final_fixed.EnhancedBybitClient', return_value=self.mock_main_client)

        self.addCleanup(patcher_source.stop)
        self.addCleanup(patcher_main.stop)

        patcher_source.start()
        patcher_main.start()

        # Patch loggers to prevent DB errors during tests
        patcher_sys_logger = patch('enhanced_trading_system_final_fixed.sys_logger', MagicMock())
        patcher_risk_logger = patch('stage2_copy_system.risk_events_logger', MagicMock())
        self.addCleanup(patcher_sys_logger.stop)
        self.addCleanup(patcher_risk_logger.stop)
        patcher_sys_logger.start()
        patcher_risk_logger.start()

    async def test_websocket_escalation_fix(self):
        """
        Tests that FinalFixedWebSocketManager._escalate_shutdown_after_timeout
        is called on disconnect and does not raise an AttributeError.
        """
        # 1. Create a mock monitor with a spy method
        mock_monitor = FinalTradingMonitor()
        mock_monitor.request_graceful_restart = AsyncMock()
        mock_monitor._request_full_restart = AsyncMock()

        # 2. Create the WebSocket manager instance, linking it to the monitor
        ws_manager = FinalFixedWebSocketManager(
            api_key="test",
            api_secret="test",
            final_monitor=mock_monitor
        )
        ws_manager.stats = {'ws_escalations_total': 0} # Initialize stats

        # 3. Simulate a disconnected state
        with patch('enhanced_trading_system_final_fixed.is_websocket_open', return_value=False):
            # 4. Call the escalation method
            await ws_manager._escalate_shutdown_after_timeout(timeout_seconds=0.01)

            # 5. Assert the monitor's restart method was called
            self.assertTrue(
                mock_monitor.request_graceful_restart.called or mock_monitor._request_full_restart.called,
                "Neither restart method was called on the monitor."
            )

            # 6. Assert metric was incremented
            self.assertEqual(ws_manager.stats['ws_escalations_total'], 1)

        # Also test the abort path
        mock_monitor.request_graceful_restart.reset_mock()
        with patch('enhanced_trading_system_final_fixed.is_websocket_open', return_value=True):
            await ws_manager._escalate_shutdown_after_timeout(timeout_seconds=0.01)
            mock_monitor.request_graceful_restart.assert_not_called()

    async def test_callback_binding_and_signal_buffering(self):
        """
        Tests that the _copy_system_callback is bound correctly and that signals
        are buffered and drained as expected.
        """
        # 1. Setup Mocks
        monitor = FinalTradingMonitor()
        stage2 = Stage2CopyTradingSystem(base_monitor=monitor)

        # Mock the actual signal processing in stage2 to just record calls
        mock_signal_handler = AsyncMock()
        stage2.enqueue_signal = mock_signal_handler

        monitor.stage2_system = stage2

        # 2. Test initial state (no callback) and buffering
        monitor._copy_system_callback = None
        test_signal = TradingSignal(SignalType.POSITION_OPEN, "BTCUSDT", "Buy", 1.0, 50000, time.time(), {})

        # We need to mock _ensure_stage2_ready to return True but not set the callback yet
        async def mock_ensure_ready_no_callback():
            monitor.stage2_system.copy_connected = True
            return True

        with patch.object(monitor, '_ensure_stage2_ready', side_effect=mock_ensure_ready_no_callback):
            await monitor.signal_processor._handle_signal_with_stage2_check(test_signal, "OPEN")

        # Assert signal was buffered
        self.assertEqual(monitor._copy_signal_buffer.qsize(), 1)
        self.assertEqual(monitor.metrics['signals_buffered_total'], 1)
        mock_signal_handler.assert_not_called()

        # 3. Test callback binding
        await monitor.connect_copy_system(stage2)
        self.assertIsNotNone(monitor._copy_system_callback)

        # 4. Test buffer draining
        # Manually start the drainer and poke it to simulate real conditions
        drainer_task = asyncio.create_task(monitor._run_buffer_drainer())
        monitor._buffer_drainer_event.set()

        await asyncio.sleep(0.1) # Give the drainer time to run

        # Assert the buffered signal was processed
        mock_signal_handler.assert_called_once_with(test_signal)
        self.assertEqual(monitor._copy_signal_buffer.qsize(), 0)
        self.assertEqual(monitor.metrics['signals_forwarded_total'], 1)

        # 5. Test direct signal forwarding
        mock_signal_handler.reset_mock()

        async def mock_ensure_ready_with_callback():
            monitor.stage2_system.copy_connected = True
            # In a real scenario, connect_copy_system would have set this
            monitor._copy_system_callback = stage2.enqueue_signal
            return True

        with patch.object(monitor, '_ensure_stage2_ready', side_effect=mock_ensure_ready_with_callback):
            await monitor.signal_processor._handle_signal_with_stage2_check(test_signal, "OPEN")

        mock_signal_handler.assert_called_once_with(test_signal)
        self.assertEqual(monitor._copy_signal_buffer.qsize(), 0)

        # Clean up the background task
        drainer_task.cancel()
        try:
            # Await the task to allow it to finish after cancellation.
            await drainer_task
        except asyncio.CancelledError:
            # This is an expected outcome of cancelling the task.
            pass


if __name__ == '__main__':
    unittest.main()