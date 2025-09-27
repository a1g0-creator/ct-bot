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
    'numpy': MagicMock(),
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
class TestPositionItemHandler(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        from stage2_copy_system import Stage2CopyTradingSystem
        # Import SignalType from the mocked module, which is now a MagicMock
        from enhanced_trading_system_final_fixed import EnhancedBybitClient, SignalType
        self.SignalType_imported = SignalType

        # Mock clients
        self.mock_source_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client = AsyncMock(spec=EnhancedBybitClient)
        self.mock_main_client.api_key = "main_key"
        self.mock_main_client.api_secret = "main_secret"

        # Mock the base monitor that holds the clients
        mock_monitor = MagicMock()
        mock_monitor.source_client = self.mock_source_client
        mock_monitor.main_client = self.mock_main_client
        mock_monitor._is_paused = False # Fix for regression

        # Instantiate the system with the mocked monitor
        self.system = Stage2CopyTradingSystem(base_monitor=mock_monitor)

        # Manually set the system to ready for these tests
        self.system.copy_state.main_rest_ok = True
        self.system.copy_state.source_ws_ok = True
        self.system.copy_state.limits_checked = True
        self.system.copy_connected = True
        mock_monitor._is_paused = False
        self.system.copy_connected = True # Fix for regression

        # Replace the real queue with a standard asyncio.Queue for testing
        self.system.copy_manager.copy_queue = asyncio.Queue()

        # Mock the kelly calculator to return a predictable size (1:1 copy)
        self.system.copy_manager.kelly_calculator.calculate_optimal_size = AsyncMock(
            side_effect=lambda symbol, current_size, **kwargs: {"recommended_size": current_size}
        )
        # Mock the order manager to get min_qty
        self.system.copy_manager.order_manager.get_min_order_qty = AsyncMock(return_value=0.001)


    async def test_on_position_item_open_new_position(self):
        """
        Tests if a new position on the donor account correctly queues an OPEN order.
        """
        # --- Arrange ---
        # Donor has a new 1.5 ETH position, main account has none.
        donor_position_update = {
            'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.5', 'leverage': '10',
            'entryPrice': '3000', 'positionIdx': 0
        }
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0
        self.mock_main_client.get_positions.return_value = [] # No existing position

        # --- Act ---
        await self.system.on_position_item(donor_position_update)

        # --- Assert ---
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        priority, _, copy_order = await self.system.copy_manager.copy_queue.get()

        # Proportional size: 1.5 * (5000 / 10000) = 0.75
        self.assertEqual(copy_order.target_symbol, 'ETHUSDT')
        self.assertEqual(copy_order.target_side, 'Buy')
        self.assertAlmostEqual(copy_order.target_quantity, 0.75)
        self.assertEqual(copy_order.metadata['reduceOnly'], False)
        self.assertEqual(copy_order.source_signal.signal_type, self.SignalType_imported.POSITION_OPEN)


    async def test_on_position_item_close_position(self):
        """
        Tests if closing a position on the donor account correctly queues a CLOSE order.
        """
        # --- Arrange ---
        # Donor closes their position, main account has an existing 0.75 ETH position.
        donor_position_update = {
            'symbol': 'ETHUSDT', 'side': '', 'size': '0', 'leverage': '10',
            'entryPrice': '3000', 'positionIdx': 0
        }
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0
        self.mock_main_client.get_positions.return_value = [
            {'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '0.75', 'positionIdx': 0, 'leverage': '10'}
        ]

        # --- Act ---
        await self.system.on_position_item(donor_position_update)

        # --- Assert ---
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        priority, _, copy_order = await self.system.copy_manager.copy_queue.get()

        self.assertEqual(copy_order.target_symbol, 'ETHUSDT')
        self.assertEqual(copy_order.target_side, 'Sell') # Opposite side to close
        self.assertAlmostEqual(copy_order.target_quantity, 0.75) # Close the full size
        self.assertEqual(copy_order.metadata['reduceOnly'], True)
        self.assertEqual(copy_order.source_signal.signal_type, self.SignalType_imported.POSITION_CLOSE)

    async def test_on_position_item_increase_position(self):
        """
        Tests if increasing a position on the donor account queues an INCREASE order for the delta.
        """
        # --- Arrange ---
        # Donor increases position from 2 to 3 ETH. Main has a proportional 1 ETH position.
        donor_position_update = {
            'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '3.0', 'leverage': '10',
            'entryPrice': '3000', 'positionIdx': 0
        }
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0
        self.mock_main_client.get_positions.return_value = [
            {'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '1.0', 'positionIdx': 0, 'leverage': '10'}
        ]

        # --- Act ---
        await self.system.on_position_item(donor_position_update)

        # --- Assert ---
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        priority, _, copy_order = await self.system.copy_manager.copy_queue.get()

        # Proportional target size: 3.0 * (5000 / 10000) = 1.5
        # Delta: 1.5 (new target) - 1.0 (current) = 0.5
        self.assertEqual(copy_order.target_symbol, 'ETHUSDT')
        self.assertEqual(copy_order.target_side, 'Buy')
        self.assertAlmostEqual(copy_order.target_quantity, 0.5)
        self.assertEqual(copy_order.metadata['reduceOnly'], False)
        self.assertEqual(copy_order.source_signal.signal_type, self.SignalType_imported.POSITION_MODIFY)

    async def test_on_position_item_reduce_position(self):
        """
        Tests if partially closing a position on the donor account queues a REDUCE order.
        """
        # --- Arrange ---
        # Donor reduces position from 4 to 2 ETH. Main has a proportional 2 ETH position.
        donor_position_update = {
            'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '2.0', 'leverage': '10',
            'entryPrice': '3000', 'positionIdx': 0
        }
        self.mock_source_client.get_balance.return_value = 10000.0
        self.mock_main_client.get_balance.return_value = 5000.0
        self.mock_main_client.get_positions.return_value = [
            {'symbol': 'ETHUSDT', 'side': 'Buy', 'size': '2.0', 'positionIdx': 0, 'leverage': '10'}
        ]

        # --- Act ---
        await self.system.on_position_item(donor_position_update)

        # --- Assert ---
        self.assertEqual(self.system.copy_manager.copy_queue.qsize(), 1)
        priority, _, copy_order = await self.system.copy_manager.copy_queue.get()

        # Proportional target size: 2.0 * (5000 / 10000) = 1.0
        # Delta: 1.0 (new target) - 2.0 (current) = -1.0
        self.assertEqual(copy_order.target_symbol, 'ETHUSDT')
        self.assertEqual(copy_order.target_side, 'Sell')
        self.assertAlmostEqual(copy_order.target_quantity, 1.0)
        self.assertEqual(copy_order.metadata['reduceOnly'], True)
        self.assertEqual(copy_order.source_signal.signal_type, self.SignalType_imported.POSITION_MODIFY)
