import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Add project root to the path to allow imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mocks for missing 'app' module dependencies
MOCK_APP_IMPORTS = {
    'app.sys_events_logger': MagicMock(),
    'app.signals_logger': MagicMock(),
    'app.state.positions_store': MagicMock(),
    'app.orders_logger': MagicMock(),
    'app.risk_events_logger': MagicMock(),
    'app.balance_snapshots_logger': MagicMock(),
    'config': MagicMock(),
    'telegram_cfg': MagicMock(),
    'risk_state_classes': MagicMock(),
    'pandas': MagicMock(),
    'scipy.optimize': MagicMock(),
    'scipy.stats': MagicMock(),
    'psutil': MagicMock(),
}

# Create a mock object for the config module to control its attributes
mock_config = MagicMock()
mock_config.DEFAULT_TRADE_ACCOUNT_ID = 1
mock_config.BYBIT_RECV_WINDOW = 20000
mock_config.get_api_credentials.return_value = ('dummy_key', 'dummy_secret')


# Mocks for missing 'app' module dependencies and other externals
MOCK_MODULES = {
    'app.sys_events_logger': MagicMock(),
    'app.signals_logger': MagicMock(),
    'app.state.positions_store': MagicMock(),
    'app.orders_logger': MagicMock(),
    'app.risk_events_logger': MagicMock(),
    'app.balance_snapshots_logger': MagicMock(),
    'telegram_cfg': MagicMock(),
    'risk_state_classes': MagicMock(),
    'pandas': MagicMock(),
    'scipy.optimize': MagicMock(),
    'scipy.stats': MagicMock(),
    'psutil': MagicMock(),
    'config': mock_config, # Use the custom mock object for config
}

@patch.dict('sys.modules', MOCK_MODULES)
class TestStage2Wrapper(unittest.IsolatedAsyncioTestCase):

    def test_import_stage2_does_not_fail(self):
        """
        Verifies that importing Stage2CopyTradingSystem does not raise an IndentationError or other import-time errors.
        """
        try:
            from stage2_copy_system import Stage2CopyTradingSystem
            self.assertTrue(True, "Successfully imported Stage2CopyTradingSystem")
        except Exception as e:
            self.fail(f"Importing Stage2CopyTradingSystem failed with an unexpected error: {e}")

    def test_wrapper_delegates_to_on_position_item(self):
        """
        Verifies that process_copy_signal correctly normalizes its input
        and calls the new on_position_item handler.
        """
        # Arrange
        from stage2_copy_system import Stage2CopyTradingSystem, TradingSignal, SignalType

        # We need to mock the dependencies for the constructor
        mock_monitor = MagicMock()
        mock_monitor.source_client = MagicMock()
        mock_monitor.main_client = MagicMock()

        # Instantiate the system
        copy_system = Stage2CopyTradingSystem(base_monitor=mock_monitor)

        # Mock the target handler to spy on it
        copy_system.on_position_item = AsyncMock()
        copy_system.logger = MagicMock()

        # Create a sample TradingSignal object
        sample_signal = TradingSignal(
            signal_type=SignalType.POSITION_OPEN,
            symbol="ETHUSDT",
            side="Buy",
            size=0.5,
            price=2500.0,
            timestamp=1678886400.0,
            metadata={'position_idx': 1}
        )

        # Act
        asyncio.run(copy_system.process_copy_signal(sample_signal))

        # Assert
        copy_system.on_position_item.assert_called_once()

        # Verify the content of the normalized 'item' passed to the handler
        call_args = copy_system.on_position_item.call_args[0]
        normalized_item = call_args[0]

        self.assertEqual(normalized_item['symbol'], 'ETHUSDT')
        self.assertEqual(normalized_item['side'], 'Buy')
        self.assertEqual(normalized_item['size'], '0.5')
        self.assertEqual(normalized_item['entryPrice'], '2500.0')
        self.assertEqual(normalized_item['positionIdx'], 1)

if __name__ == '__main__':
    unittest.main()
