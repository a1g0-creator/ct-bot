import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Add project root to the path to allow imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mocks for missing 'app' module dependencies and other externals
mock_config = MagicMock()
mock_config.DEFAULT_TRADE_ACCOUNT_ID = 1
mock_config.BYBIT_RECV_WINDOW = 20000
mock_config.get_api_credentials.return_value = ('dummy_key', 'dummy_secret')

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
    'config': mock_config,
    'aiohttp': MagicMock(),
    'sys_events_logger': MagicMock(),
    'orders_logger': MagicMock(),
    'risk_events_logger': MagicMock(),
    'balance_snapshots_logger': MagicMock(),
    'db_session': MagicMock(), # Mock out the database session module
}

# Patch modules before importing the system under test
@patch.dict('sys.modules', MOCK_MODULES)
class TestStage2Logic(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
        # Import here after modules are patched
        from stage2_copy_system import format_quantity_for_symbol_live, Stage2CopyTradingSystem, TradingSignal, SignalType
        global format_quantity_for_symbol_live_imported
        global Stage2CopyTradingSystem_imported
        global TradingSignal_imported
        global SignalType_imported
        format_quantity_for_symbol_live_imported = format_quantity_for_symbol_live
        Stage2CopyTradingSystem_imported = Stage2CopyTradingSystem
        TradingSignal_imported = TradingSignal
        SignalType_imported = SignalType

    async def test_format_quantity_bump_up_to_min_notional(self):
        """
        Tests if the quantity is correctly bumped up to meet the minimum notional value,
        respecting the quantity step.
        """
        mock_client = MagicMock()
        mock_client.get_symbol_filters = AsyncMock(return_value={
            "qty_step": "1",
            "min_qty": "1",
            "min_notional": "5"
        })

        # Input: 37 DOGE at $0.1/DOGE = $3.70 value. Min notional is $5.
        # Expected: Should be bumped to 50 DOGE to meet the $5 minimum.
        formatted_qty = await format_quantity_for_symbol_live_imported(
            mock_client, "DOGEUSDT", 37.0, 0.1
        )
        self.assertEqual(formatted_qty, "50")

    async def test_format_quantity_round_down(self):
        """
        Tests if a quantity that meets the minimum notional is correctly rounded down
        to the nearest step.
        """
        mock_client = MagicMock()
        mock_client.get_symbol_filters = AsyncMock(return_value={
            "qty_step": "1",
            "min_qty": "1",
            "min_notional": "5"
        })

        # Input: 55.9 DOGE at $0.1/DOGE = $5.59 value. This is above the minimum.
        # Expected: Should be rounded down to the nearest step, which is 55.
        formatted_qty = await format_quantity_for_symbol_live_imported(
            mock_client, "DOGEUSDT", 55.9, 0.1
        )
        self.assertEqual(formatted_qty, "55")

    async def test_format_quantity_original_bug_case(self):
        """
        Tests the specific bug case where 20.9 was incorrectly formatted to 2.
        This likely happened with a higher min_notional value.
        """
        mock_client = MagicMock()
        # Simulate a higher min_notional to replicate the bug's conditions
        mock_client.get_symbol_filters = AsyncMock(return_value={
            "qty_step": "1",
            "min_qty": "1",
            "min_notional": "10"
        })

        # Input: 20.9 units at $0.40 = $8.36 value. Min notional is $10.
        # Expected: Should be bumped up to 25 units ($10 / $0.40) to meet the minimum.
        formatted_qty = await format_quantity_for_symbol_live_imported(
            mock_client, "SOMECOINUSDT", 20.9, 0.4
        )
        self.assertEqual(formatted_qty, "25")

    async def test_format_quantity_with_small_step(self):
        """
        Tests formatting with a small quantity step and decimal places.
        """
        mock_client = MagicMock()
        mock_client.get_symbol_filters = AsyncMock(return_value={
            "qty_step": "0.01",
            "min_qty": "0.01",
            "min_notional": "5"
        })

        # Input: 123.456 ETH at $3000 = $370368 value. Well above min notional.
        # Expected: Should be rounded down to 123.45
        formatted_qty = await format_quantity_for_symbol_live_imported(
            mock_client, "ETHUSDT", 123.456, 3000
        )
        self.assertEqual(formatted_qty, "123.45")

if __name__ == '__main__':
    unittest.main()


@patch.dict('sys.modules', MOCK_MODULES)
class TestTrailingStopManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
        from stage2_copy_system import DynamicTrailingStopManager
        from enhanced_trading_system_final_fixed import EnhancedBybitClient

        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_order_manager = MagicMock()

        # Default config for the manager
        self.trailing_config = {
            'enabled': True,
            'activation_pct': 0.01, # 1%
            'min_notional_for_ts': 50,
        }

        self.ts_manager = DynamicTrailingStopManager(
            main_client=self.mock_main_client,
            order_manager=self.mock_order_manager,
            trailing_config=self.trailing_config
        )

    async def test_ts_price_conversion_and_rounding(self):
        """
        Tests if the trailing stop distance is correctly converted from percentage
        to an absolute price distance and rounded to the symbol's tick size.
        """
        # --- Arrange ---
        position_data = {
            'symbol': 'BTCUSDT',
            'side': 'Buy',
            'quantity': 0.1,
            'entryPrice': 50000,
            'position_idx': 0,
        }
        self.mock_main_client.get_symbol_filters.return_value = {"tick_size": "0.5"}
        self.mock_main_client.get_positions.return_value = [
            {'symbol': 'BTCUSDT', 'positionIdx': 0, 'side': 'Buy', 'markPrice': '51000', 'lastPrice': '51000', 'sessionAvgPrice': '51000'}
        ]
        self.mock_order_manager.place_trailing_stop = AsyncMock(return_value={'success': True})

        # --- Act ---
        await self.ts_manager.create_or_update_trailing_stop(position_data)

        # --- Assert ---
        self.mock_order_manager.place_trailing_stop.assert_called_once()
        call_args = self.mock_order_manager.place_trailing_stop.call_args.kwargs
        self.assertEqual(call_args.get('symbol'), 'BTCUSDT')
        self.assertEqual(call_args.get('position_idx'), 0)


    async def test_remove_trailing_stop(self):
        """
        Tests if removing a trailing stop calls the correct method with a zero string.
        """
        # --- Arrange ---
        position_data = {'symbol': 'BTCUSDT', 'position_idx': 0}
        self.mock_main_client.get_positions.return_value = [{'symbol': 'BTCUSDT', 'size': '0.1', 'positionIdx': 0}]
        self.mock_order_manager.place_trailing_stop = AsyncMock(return_value={'success': True})

        # --- Act ---
        await self.ts_manager.remove_trailing_stop(position_data)

        # --- Assert ---
        self.mock_order_manager.place_trailing_stop.assert_called_once_with(
            symbol='BTCUSDT',
            trailing_stop_price="0",
            position_idx=0
        )


@patch.dict('sys.modules', MOCK_MODULES)
class TestMarginMirroring(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
        from stage2_copy_system import Stage2CopyTradingSystem, MARGIN_CONFIG
        from enhanced_trading_system_final_fixed import EnhancedBybitClient

        self.mock_source_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client.api_key = "main_key"
        self.mock_main_client.api_secret = "main_secret"

        mock_monitor = MagicMock()
        mock_monitor.source_client = self.mock_source_client
        mock_monitor.main_client = self.mock_main_client

        self.system = Stage2CopyTradingSystem(base_monitor=mock_monitor)

        # Configure margin mirroring for tests
        MARGIN_CONFIG['enabled'] = True
        MARGIN_CONFIG['debounce_sec'] = 1
        MARGIN_CONFIG['min_usdt_delta'] = 5.0

    async def test_proportional_margin_addition(self):
        """Tests if a margin addition on the donor is proportionally mirrored."""
        # --- Arrange ---
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0 # 0.5x ratio
        self.mock_main_client.add_margin = AsyncMock(return_value={'success': True})

        donor_margin_change = 50.0 # Donor adds 50 USDT

        # --- Act ---
        await self.system._mirror_margin_adjustment("BTCUSDT", donor_margin_change, 0)

        # --- Assert ---
        # Expected main change = 50.0 * (5000 / 10000) = 25.0
        expected_margin_str = "25.0000"
        self.mock_main_client.add_margin.assert_called_once_with(
            symbol='BTCUSDT',
            margin=expected_margin_str,
            position_idx=0
        )

    async def test_debounce_logic(self):
        """Tests that rapid margin changes are debounced."""
        # --- Arrange ---
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0
        self.mock_main_client.add_margin = AsyncMock(return_value={'success': True})

        # --- Act ---
        # Call twice in quick succession
        await self.system._mirror_margin_adjustment("BTCUSDT", 50.0, 0)
        await self.system._mirror_margin_adjustment("BTCUSDT", 20.0, 0) # This one should be ignored

        # --- Assert ---
        self.mock_main_client.add_margin.assert_called_once() # Should only be called once

    async def test_minimum_delta_check(self):
        """Tests that small margin changes are ignored."""
        # --- Arrange ---
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 1000.0 # 0.1x ratio
        self.mock_main_client.add_margin = AsyncMock(return_value={'success': True})

        # Proportional change will be 20.0 * 0.1 = 2.0 USDT, which is less than min_usdt_delta of 5.0
        donor_margin_change = 20.0

        # --- Act ---
        await self.system._mirror_margin_adjustment("BTCUSDT", donor_margin_change, 0)

        # --- Assert ---
        self.mock_main_client.add_margin.assert_not_called()

@patch.dict('sys.modules', MOCK_MODULES)
class TestUniversalCopyLogic(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up a fresh system for each test."""
        os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
        from stage2_copy_system import Stage2CopyTradingSystem, PositionMode
        from enhanced_trading_system_final_fixed import EnhancedBybitClient, SignalType

        # Make modules available to tests
        self.SignalType_imported = SignalType
        self.PositionMode_imported = PositionMode

        # Mock clients
        self.mock_source_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client.api_key = "main_key"
        self.mock_main_client.api_secret = "main_secret"

        # Mock base monitor
        mock_monitor = MagicMock()
        mock_monitor.source_client = self.mock_source_client
        mock_monitor.main_client = self.mock_main_client

        # Instantiate the system under test
        self.system = Stage2CopyTradingSystem(base_monitor=mock_monitor)
        self.system.trade_executor_connected = True # Assume system is ready to trade
        self.system.copy_manager.copy_queue = asyncio.Queue()

        # Mock dependencies that make external calls
        self.system.copy_manager.kelly_calculator.calculate_optimal_size = AsyncMock(
            # Default mock: return the proportional size without Kelly adjustment
            side_effect=lambda **kwargs: {"recommended_size": kwargs.get('current_size', kwargs.get('proportional', 0))}
        )
        self.system.copy_manager.order_manager.get_min_order_qty = AsyncMock(return_value=0.001)

    # --- HEDGE Mode Tests ---

    async def test_hedge_open_new_short(self):
        """HEDGE: Opening a new short position should create a Sell order with posIdx=2 and reduceOnly=False."""
        # Arrange
        donor_pos = [{'symbol': 'BTCUSDT', 'side': 'Sell', 'size': '0.1', 'positionIdx': 2, 'entryPrice': '50000'}]
        self.mock_source_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_positions.return_value = [] # No existing position on main
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.HEDGE)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        self.assertEqual(order.target_side, 'Sell')
        self.assertEqual(order.metadata['position_idx'], 2)
        self.assertFalse(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_OPEN)

    async def test_hedge_reduce_long_position(self):
        """HEDGE: Reducing a long position should create a Sell order with posIdx=1 and reduceOnly=True."""
        # Arrange
        donor_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.05', 'positionIdx': 1, 'entryPrice': '50000'}]
        main_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1', 'positionIdx': 1}]
        self.mock_source_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.HEDGE)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        self.assertEqual(order.target_side, 'Sell')
        self.assertAlmostEqual(order.target_quantity, 0.05)
        self.assertEqual(order.metadata['position_idx'], 1)
        self.assertTrue(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_MODIFY)

    async def test_hedge_full_close_long_position(self):
        """HEDGE: Closing a long position fully should create a Sell order with posIdx=1 and reduceOnly=True."""
        # Arrange
        donor_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0', 'positionIdx': 1, 'entryPrice': '50000'}]
        main_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1', 'positionIdx': 1}]
        self.mock_source_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.HEDGE)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        self.assertEqual(order.target_side, 'Sell')
        self.assertAlmostEqual(order.target_quantity, 0.1)
        self.assertEqual(order.metadata['position_idx'], 1)
        self.assertTrue(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_CLOSE)

    async def test_hedge_skip_when_delta_below_min_qty(self):
        """HEDGE: Should not queue an order if the absolute delta is smaller than the minimum order quantity."""
        # Arrange
        donor_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1001', 'positionIdx': 1, 'entryPrice': '50000'}]
        main_pos = [{'symbol': 'BTCUSDT', 'side': 'Buy', 'size': '0.1', 'positionIdx': 1}]
        self.mock_source_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_balance.return_value = 1000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.HEDGE)
        # Delta will be ~0.0001, which is less than the mock min_qty
        self.system.copy_manager.order_manager.get_min_order_qty = AsyncMock(return_value=0.001)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 0)

    async def test_oneway_skip_when_delta_below_min_qty(self):
        """ONEWAY: Should not queue an order if the absolute delta is smaller than the minimum order quantity."""
        # Arrange
        donor_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.5001', 'positionIdx': 0, 'markPrice': '3000'}]
        main_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.5', 'positionIdx': 0}]
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.ONEWAY)
        # Delta will be ~0.0001, which is less than the mock min_qty
        self.system.copy_manager.order_manager.get_min_order_qty = AsyncMock(return_value=0.01)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 0)

    async def test_mode_detector_schedules_probe_when_mode_is_unknown(self):
        """Tests that ensure_rest_probe is scheduled and run when the mode is None."""
        # Arrange
        donor_pos = [{'symbol': 'XRPUSDT', 'side': 'Buy', 'size': '100', 'positionIdx': 0}]
        self.system.mode_detector.get_mode = MagicMock(return_value=None)
        # Mock the method that gets called in the background
        self.system.mode_detector.ensure_rest_probe = AsyncMock()

        # Act
        await self.system.on_position_item(donor_pos)
        # Yield control to the event loop to allow the created task to run
        await asyncio.sleep(0)

        # Assert
        # Check that the probe method was actually called
        self.system.mode_detector.ensure_rest_probe.assert_awaited_once_with('XRPUSDT')
        # Ensure no order was queued because the mode was unknown
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 0)


    # --- ONE-WAY Mode Tests ---

    async def test_oneway_open_from_zero(self):
        """ONE-WAY: Opening a new position from zero should create a Buy order with posIdx=0 and reduceOnly=False."""
        # Arrange
        donor_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.5', 'positionIdx': 0, 'markPrice': '3000'}]
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_positions.return_value = [] # No existing position
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.ONEWAY)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        self.assertEqual(order.target_side, 'Buy')
        self.assertAlmostEqual(order.target_quantity, 1.5)
        self.assertEqual(order.metadata['position_idx'], 0)
        self.assertFalse(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_OPEN)

    async def test_oneway_pure_reduce_position(self):
        """ONE-WAY: A pure reduction of a long position should create a Sell order with reduceOnly=True and posIdx=0."""
        # Arrange
        donor_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.0', 'positionIdx': 0, 'markPrice': '3000'}]
        main_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.5', 'positionIdx': 0}]
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.ONEWAY)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        self.assertEqual(order.target_side, 'Sell')
        self.assertAlmostEqual(order.target_quantity, 0.5)
        self.assertEqual(order.metadata['position_idx'], 0)
        self.assertTrue(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_MODIFY)

    async def test_oneway_reversal_through_zero(self):
        """ONE-WAY: A reversal from short to long should create a single Buy order with reduceOnly=False."""
        # Arrange
        donor_pos = [{'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.0', 'positionIdx': 0, 'markPrice': '3000'}]
        main_pos = [{'symbol': 'ETHUSDT', 'side': 'Sell', 'size': '1.5', 'positionIdx': 0}]
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.ONEWAY)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        # Delta = target_net(1.0) - main_net(-1.5) = 2.5
        self.assertEqual(order.target_side, 'Buy')
        self.assertAlmostEqual(order.target_quantity, 2.5)
        self.assertEqual(order.metadata['position_idx'], 0)
        self.assertFalse(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_MODIFY)

    async def test_oneway_full_close_short_position(self):
        """ONE-WAY: Closing a short position fully should create a Buy order with reduceOnly=True."""
        # Arrange
        donor_pos = [{'symbol': 'ETHUSDT', 'side': 'Sell', 'size': '0', 'positionIdx': 0, 'markPrice': '3000'}]
        main_pos = [{'symbol': 'ETHUSDT', 'side': 'Sell', 'size': '1.5', 'positionIdx': 0}]
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_positions.return_value = main_pos
        self.system.mode_detector.get_mode = MagicMock(return_value=self.PositionMode_imported.ONEWAY)

        # Act
        await self.system.on_position_item(donor_pos)

        # Assert
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        _, _, order = await self.system.copy_manager.copy_queue.get()
        # Delta = target_net(0) - main_net(-1.5) = 1.5
        self.assertEqual(order.target_side, 'Buy')
        self.assertAlmostEqual(order.target_quantity, 1.5)
        self.assertEqual(order.metadata['position_idx'], 0)
        self.assertTrue(order.metadata['reduceOnly'])
        self.assertEqual(order.source_signal.signal_type, self.SignalType_imported.POSITION_CLOSE)
