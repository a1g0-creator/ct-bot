#!/usr/bin/env python3
"""
BYBIT COPY TRADING SYSTEM - ЭТАП 2: СИСТЕМА КОПИРОВАНИЯ
Версия 2.0 - ПРОФЕССИОНАЛЬНАЯ РЕАЛИЗАЦИЯ

🎯 РЕАЛИЗАЦИЯ ЭТАПА 2 СОГЛАСНО ДОРОЖНОЙ КАРТЕ:
- ✅ Управление ордерами с адаптивной системой выбора типа ордера
- ✅ Логика копирования позиций с Kelly Criterion масштабированием
- ✅ Синхронизация торговых операций с приоритизацией
- ✅ Kelly Criterion implementation для математически обоснованного управления капиталом
- ✅ Trailing Stop-Loss System с ATR-based адаптацией
- ✅ Контроль просадки с автоматической остановкой

🔧 ИНТЕГРАЦИЯ С ЭТАПОМ 1:
- ✅ Использует FinalTradingMonitor из enhanced_trading_system_final_fixed.py
- ✅ Расширяет ProductionSignalProcessor для обработки сигналов копирования
- ✅ Интегрируется с FinalFixedWebSocketManager для real-time данных
- ✅ Полная совместимость с исправленными WebSocket функциями
"""

import asyncio
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict, namedtuple
import math
import random
import statistics
import traceback
import uuid
import os
from decimal import Decimal
import aiohttp

from sys_events_logger import sys_logger
from orders_logger import orders_logger, OrderStatus
from risk_events_logger import risk_events_logger, RiskEventType
from balance_snapshots_logger import balance_logger
from positions_db_writer import positions_writer

logger = logging.getLogger(__name__)

# Импортируем все компоненты из Этапа 1
try:
    from enhanced_trading_system_final_fixed import (
        FinalTradingMonitor, ProductionSignalProcessor, TradingSignal, SignalType,
        EnhancedBybitClient, FinalFixedWebSocketManager,
        safe_float, send_telegram_alert, logger, MAIN_API_KEY, MAIN_API_SECRET,
        MAIN_API_URL, SOURCE_API_KEY, SOURCE_API_SECRET, SOURCE_API_URL,
        BALANCE_ACCOUNT_TYPE
    )
    from config import dry_run
    print("✅ Успешно импортированы все компоненты Этапа 1")
except ImportError as e:
    print(f"❌ Не удалось импортировать компоненты Этапа 1: {e}")
    print("Убедитесь, что файл enhanced_trading_system_final_fixed.py находится в той же директории")
    raise

from risk_state_classes import RiskDataContext, HealthSupervisor


def _sign(x: float) -> int: return (x > 0) - (x < 0)

# ================================
# НАСТРОЙКИ И КОНСТАНТЫ ЭТАПА 2
# ================================

# Настройки копирования
COPY_CONFIG = {
    'default_copy_ratio': 1.0,              # Базовый коэффициент копирования
    'min_copy_size': 0.001,                 # Минимальный размер копируемой позиции
    'max_copy_size': 10.0,                  # Максимальный размер копируемой позиции
    'max_concurrent_positions': 10,         # Максимальное количество одновременных позиций
    'sync_timeout': 5.0,                    # Таймаут синхронизации в секундах
    'order_retry_attempts': 3,              # Количество попыток размещения ордера
    'order_retry_delay': 1.0,               # Задержка между попытками
    'slippage_tolerance': 0.002,            # Допустимое проскальзывание (0.2%)
    'market_impact_threshold': 0.05,        # Порог воздействия на рынок (5%)
    'kelly_min_mult': 0.5,                  # Kelly не уменьшает размер более чем в 2 раза
    'kelly_max_mult': 2.0,                  # и не увеличивает более чем в 2 раза
    'idempotency_window_sec': 5,            # Окно идемпотентности в секундах
}

# Kelly Criterion настройки
KELLY_CONFIG = {
    'min_trades_required': 30,              # Минимальное количество сделок для расчета
    'lookback_window': 100,                 # Окно анализа сделок
    'confidence_threshold': 0.6,            # Минимальная уверенность в расчетах
    'max_kelly_fraction': 0.25,             # Максимальная позиция по Kelly (25%)
    'conservative_factor': 0.5,             # Консервативный коэффициент (50% от Kelly)
    'min_position_size': 0.01,              # Минимальная позиция (1%)
    'rebalance_threshold': 0.1              # Порог для ребалансировки (10%)
}

def _parse_bool_env(var_name: str, default: bool) -> bool:
    """Parses a boolean value from an environment variable."""
    value = os.getenv(var_name, str(default)).lower()
    return value in ("1", "true", "yes", "on")

# Trailing Stop-Loss настройки
TRAILING_CONFIG = {
    'enabled': _parse_bool_env('TRAILING_ENABLED', True),
    'mode': 'conservative', # "aggressive" or "conservative"
    'activation_pct': 0.015, # 1.5%
    'step_pct': 0.002, # 0.2%
    'max_pct': 0.05, # 5%
    'atr_period': 14,
    'atr_multiplier': 2.0,
    'rearm_on_modify': True,
    'update_on_add': True,
    'only_on_open': False,
    'min_notional_for_ts': 100.0, # $100
    # Deprecated/internal - use mode
    'use_atr': True,
}

# Контроль рисков
RISK_CONFIG = {
    'max_daily_loss': 0.05,                # Максимальная дневная просадка (5%)
    'max_total_drawdown': 0.15,            # Максимальная общая просадка (15%)      
    'position_correlation_limit': 0.7,     # Лимит корреляции между позициями
    'max_exposure_per_symbol': 0.2,        # Максимальная экспозиция на символ (20%)
    'emergency_stop_threshold': 0.1,       # Порог экстренной остановки (10%)
    'recovery_mode_threshold': 0.08        # Порог режима восстановления (8%)
}

# Настройки зеркалирования маржи
MARGIN_CONFIG = {
    'enabled': True,
    'min_usdt_delta': 2.0,
    'max_pct_of_equity': 0.5,
    'debounce_sec': 2
}

async def format_quantity_for_symbol_live(bybit_client, symbol: str, quantity: float, price: float = None) -> str:
    """
    Formats quantity based on Bybit's live exchange filters.
    - If the quantity is below the effective minimum (considering min_qty and min_notional), it's bumped UP.
    - Otherwise, it's rounded DOWN to the nearest quantity step to avoid increasing risk.
    """
    try:
        filters = await bybit_client.get_symbol_filters(symbol, category="linear")
        qty_step = float(filters.get("qty_step") or 0.001)
        min_qty = float(filters.get("min_qty") or 0.001)
        min_notional = float(filters.get("min_notional") or 5.0)

        if qty_step <= 0: # Avoid division by zero
            return str(quantity)

        # Determine the number of decimal places from the qty_step
        decimals = 0
        if qty_step < 1:
            s = f"{qty_step:.12f}".rstrip('0')
            decimals = len(s.split('.')[-1]) if '.' in s else 0

        # Calculate the effective minimum quantity
        notional_min_qty = 0.0
        effective_min_qty = min_qty
        if price and price > 0 and min_notional > 0:
            notional_min_qty = (min_notional / price)
            # This is the required formula: ceil(min_notional/price/step)*step
            min_qty_from_notional = math.ceil(notional_min_qty / qty_step) * qty_step
            effective_min_qty = max(min_qty, min_qty_from_notional)

        # Decide on the final quantity
        if quantity < effective_min_qty:
            final_qty = effective_min_qty
        else:
            # Round down to the nearest step
            steps = quantity / qty_step
            final_qty = math.floor(steps) * qty_step

        # Final check to ensure we don't fall below the absolute min_qty after rounding down
        if final_qty < min_qty:
             final_qty = min_qty

        formatted_qty = f"{final_qty:.{decimals}f}"
        if '.' in formatted_qty:
            formatted_qty = formatted_qty.rstrip('0').rstrip('.')

        # New logging
        logger.info(
            f"[live-format] {symbol}: qty_in={quantity:.8f}, price=${price or 0:.4f}, step={qty_step}, "
            f"rule_min_qty={min_qty}, min_notional={min_notional}, notional_min_qty={notional_min_qty:.8f}, "
            f"effective_min_qty={effective_min_qty:.8f} -> final_qty={formatted_qty}"
        )
        return formatted_qty or "0"

    except Exception as e:
        logger.warning(f"format_quantity_for_symbol_live fallback due to: {e}", exc_info=True)
        # Fallback to the old static function to prevent crashes
        return format_quantity_for_symbol(symbol, quantity, price)


def format_quantity_for_symbol(symbol: str, quantity: float, price: float = None) -> str:
    """
    Форматирование количества с учётом minNotional и шага лота.
    Правила:
      - если до минимальной стоимости по USDT не дотягиваем — поднимаем количество ВВЕРХ;
      - иначе округляем ВНИЗ к шагу, чтобы не завышать риск.
    """
    MIN_ORDER_VALUE = 10.0  # безопасный дефолт для линейных контрактов

    # Базовые правила точности (лучше тянуть с биржи — см. метод в EnhancedBybitClient)
    SYMBOL_PRECISION = {
        'BTCUSDT':  {'min': 0.001, 'step': 0.001, 'decimals': 3},
        'ETHUSDT':  {'min': 0.001, 'step': 0.001, 'decimals': 3},  # ✔ исправлено
        'BNBUSDT':  {'min': 0.01,  'step': 0.01,  'decimals': 2},
        'XRPUSDT':  {'min': 1,     'step': 1,     'decimals': 0},
        'ADAUSDT':  {'min': 1,     'step': 1,     'decimals': 0},
        'DOGEUSDT': {'min': 1,     'step': 1,     'decimals': 0},
        'SOLUSDT':  {'min': 0.1,   'step': 0.1,   'decimals': 1},
        'DOTUSDT':  {'min': 0.1,   'step': 0.1,   'decimals': 1},
        'MATICUSDT':{'min': 1,     'step': 1,     'decimals': 0},
        'LTCUSDT':  {'min': 0.01,  'step': 0.01,  'decimals': 2},
        'AVAXUSDT': {'min': 0.1,   'step': 0.1,   'decimals': 1},
        'LINKUSDT': {'min': 0.1,   'step': 0.1,   'decimals': 1},
    }

    rules    = SYMBOL_PRECISION.get(symbol, {'min': 0.001, 'step': 0.001, 'decimals': 3})
    step     = rules['step']
    min_qty  = rules['min']
    decimals = rules['decimals']

    need_bump_to_min = False
    if price and price > 0:
        min_qty_by_value = MIN_ORDER_VALUE / float(price)
        effective_min     = max(min_qty, min_qty_by_value)
        if quantity < effective_min:
            quantity = effective_min
            need_bump_to_min = True

    steps = quantity / step
    rounded_qty = (math.ceil(steps) * step) if need_bump_to_min else (math.floor(steps) * step)
    if rounded_qty < min_qty:
        rounded_qty = min_qty

    formatted = f"{rounded_qty:.{decimals}f}".rstrip('0').rstrip('.')
    logger.info(
        f"Formatted quantity for {symbol}: {quantity:.6f} -> {formatted} "
        f"(price: ${float(price) if price else 0:.2f}, "
        f"value: {(float(formatted) * float(price)) if price else 0:.2f})"
    )
    return formatted


# ================================
# ДОПОЛНИТЕЛЬНЫЕ ENUM И DATACLASS
# ================================

class CopyMode(Enum):
    DISABLED = "disabled"
    PROPORTIONAL = "proportional"
    KELLY_OPTIMAL = "kelly_optimal"
    FIXED_RATIO = "fixed_ratio"

class OrderStrategy(Enum):
    MARKET = "market"
    LIMIT = "limit"
    AGGRESSIVE_LIMIT = "aggressive_limit"
    ADAPTIVE = "adaptive"

class TrailingStyle(Enum):
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"

class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class MarketConditions:
    """Рыночные условия для адаптивного трейдинга"""
    volatility: float = 0.0
    spread_percent: float = 0.0
    volume_ratio: float = 1.0
    trend_strength: float = 0.0
    liquidity_score: float = 1.0
    timestamp: float = field(default_factory=time.time)

@dataclass
class KellyCalculation:
    """Результат расчета Kelly Criterion"""
    symbol: str
    kelly_fraction: float
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    sample_size: int
    confidence_score: float
    recommended_size: float
    timestamp: float = field(default_factory=time.time)

@dataclass
class TrailingStop:
    """Trailing Stop-Loss данные"""
    symbol: str
    side: str
    current_price: float
    stop_price: float
    distance: float
    distance_percent: float
    trail_style: TrailingStyle
    atr_value: float
    last_update: float = field(default_factory=time.time)

@dataclass
class CopyState:
    """A single source of truth for the copy system's readiness."""
    main_rest_ok: bool = False
    source_ws_ok: bool = False
    keys_loaded: bool = False
    limits_checked: bool = False
    last_error: Optional[str] = None

    @property
    def ready(self) -> bool:
        """The system is ready only if all components are okay."""
        return all([self.main_rest_ok, self.source_ws_ok, self.keys_loaded, self.limits_checked])

@dataclass
class CopyOrder:
    """Ордер для копирования"""
    source_signal: TradingSignal
    target_symbol: str
    target_side: str
    target_quantity: float
    target_price: Optional[float]
    order_strategy: OrderStrategy
    kelly_fraction: float
    priority: int
    retry_count: int = 0
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class PositionMode(Enum):
    HEDGE = 1
    ONEWAY = 0

class DonorPositionModeDetector:
    """Detects and caches the position mode (Hedge/One-Way) for a given symbol."""
    def __init__(self, source_client: EnhancedBybitClient, cache_ttl_sec: int = 300):
        self._source_client = source_client
        self._cache_ttl = timedelta(seconds=cache_ttl_sec)
        self._modes_cache: Dict[str, Tuple[PositionMode, datetime]] = {}
        self._rest_probe_locks = defaultdict(asyncio.Lock)
        self._last_probe_time: Dict[str, float] = {}

    def _get_cache_key(self, symbol: str, category: str = "linear") -> str:
        """Creates a standardized cache key."""
        return f"{category}:{symbol}"

    def update_from_ws(self, symbol: str, positions: list, category: str = "linear") -> None:
        """Updates the position mode from incoming WebSocket position data."""
        key = self._get_cache_key(symbol, category)
        has_hedge_indices = any(p.get('positionIdx') in [1, 2] for p in positions)
        has_only_oneway_idx = bool(positions) and all(p.get('positionIdx') == 0 for p in positions)

        mode = None
        if has_hedge_indices:
            mode = PositionMode.HEDGE
        elif has_only_oneway_idx and positions:
            mode = PositionMode.ONEWAY

        if mode is not None:
            if self._modes_cache.get(key, (None, None))[0] != mode:
                logger.info(f"POSITION_MODE: symbol={key}, mode={mode.name} (src=ws)")
            self._modes_cache[key] = (mode, datetime.utcnow())

    async def ensure_rest_probe(self, symbol: str, category: str = "linear") -> None:
        """
        Ensures the position mode is known, falling back to a REST API probe if needed.
        This method is throttled to prevent API spam.
        """
        key = self._get_cache_key(symbol, category)

        async with self._rest_probe_locks[key]:
            # Check if mode is already known or if a probe was sent recently
            if self.get_mode(symbol, category) is not None:
                return

            now = time.time()
            if now - self._last_probe_time.get(key, 0) < 60: # Throttle: 1 probe per 60s
                return

            # Mark that we are probing now
            self._last_probe_time[key] = now
            logger.info(f"MODE_PENDING: Probing REST for position mode for {key}.")
            try:
                # A light way to get position info. get_positions is suitable.
                positions = await self._source_client.get_positions(category=category, symbol=symbol)
                if positions:
                    # Logic is same as WS update, but on REST data
                    self.update_from_ws(symbol, positions, category)
                    mode = self.get_mode(symbol, category)
                    if mode:
                         logger.info(f"POSITION_MODE: symbol={key}, mode={mode.name} (src=rest)")
                    else:
                         # This can happen if a symbol has no positions at all.
                         # We don't cache this, will retry later.
                         logger.info(f"MODE_PROBE_INCONCLUSIVE: No active positions found for {key} via REST.")
                else:
                    logger.warning(f"REST probe for {key} returned no position data.")

            except Exception as e:
                logger.error(f"ensure_rest_probe for {key} failed: {e}", exc_info=True)

    def get_mode(self, symbol: str, category: str = "linear") -> Optional[PositionMode]:
        """
        Retrieves the cached position mode for a symbol if it's not stale.
        Returns None if the mode is unknown or the cache is expired.
        """
        key = self._get_cache_key(symbol, category)
        cached_data = self._modes_cache.get(key)
        if cached_data:
            mode, timestamp = cached_data
            if datetime.utcnow() - timestamp < self._cache_ttl:
                return mode
            else:
                # Cache expired, remove it
                del self._modes_cache[key]
        return None

# ================================
# KELLY CRITERION IMPLEMENTATION
# ================================

class AdvancedKellyCalculator:
    """
    Продвинутая реализация Kelly Criterion для управления капиталом
    
    Основано на исследованиях:
    - John L. Kelly Jr. "A New Interpretation of Information Rate" (1956)
    - Edward Thorp "Beat the Dealer" (1962)
    - William Poundstone "Fortune's Formula" (2005)
    """
    
    def __init__(self):
        self.trade_history = deque(maxlen=KELLY_CONFIG['lookback_window'])
        self.symbol_stats = defaultdict(lambda: {'trades': [], 'last_update': 0})
        self.kelly_cache = {}
        self.cache_ttl = 300  # 5 минут кэш
        self.max_position_size = 0.25  # Максимум 25% капитала на позицию
        self.min_position_size = 0.001  # Минимум 0.1% капитала
        self.default_position_size = 0.02  # По умолчанию 2%
        
        # Настройки безопасности
        self.max_drawdown_threshold = 0.15  # Максимальная просадка 15%
        self.kelly_multiplier = 0.5  # Консервативный множитель Kelly
        
        # История для адаптивных расчетов
        self.trade_history = deque(maxlen=100)
        self.performance_metrics = {
            'win_rate': 0.6,  # Начальная оценка
            'avg_win': 0.02,  # 2% в среднем
            'avg_loss': 0.01,  # 1% в среднем
            'sharpe_ratio': 1.0
        }
        
        logger.info("Advanced Kelly Calculator initialized with safety limits")

    def apply_config(self, cfg: dict) -> None:
        """
        Применяет новую конфигурацию в рантайме.
        Обеспечивает совместимость с Telegram и внутренними механизмами.
        """
        try:
            # Используем get_config_snapshot для получения "старого" состояния, если метод уже есть
            old_config = self.get_config_snapshot() if hasattr(self, 'get_config_snapshot') else {}

            # 1. Обновляем атрибуты, которые напрямую читает Telegram
            self.confidence_threshold = float(cfg.get('confidence_threshold', 0.6))
            self.max_kelly_fraction = float(cfg.get('max_kelly_fraction', 0.25))
            self.conservative_factor = float(cfg.get('conservative_factor', 0.5))
            self.min_trades_required = int(cfg.get('min_trades_required', 30))
            self.lookback_period = int(cfg.get('lookback_window', 100))  # Важный маппинг
            self.min_position_size = float(cfg.get('min_position_size', 0.01))
            self.rebalance_threshold = float(cfg.get('rebalance_threshold', 0.1))

            # 2. Синхронизируем внутренние переменные калькулятора для обратной совместимости
            self.kelly_multiplier = self.conservative_factor
            self.max_position_size = self.max_kelly_fraction
            self.default_position_size = max(self.min_position_size, 0.001)
            self.max_drawdown_threshold = float(cfg.get('max_drawdown_threshold', 0.15))

            # 3. Обновляем размер окна истории, если он изменился
            new_lookback = self.lookback_period
            cache_cleared = False
            if not hasattr(self, 'trade_history') or new_lookback != getattr(self.trade_history, 'maxlen', new_lookback):
                from collections import deque
                current_history = list(getattr(self, 'trade_history', []))
                self.trade_history = deque(current_history, maxlen=new_lookback)
                cache_cleared = True

            # 4. Сбрасываем кэш для немедленного применения настроек
            if hasattr(self, 'kelly_cache'):
                self.kelly_cache.clear()

            # 5. Логируем изменения
            new_config = self.get_config_snapshot()
            sys_logger.log_event(
                "INFO", "KellyCalculator", "Kelly configuration updated",
                {"old_config": old_config, "new_config": new_config, "cache_cleared": cache_cleared}
            )
            logger.info(
                f"Kelly config applied: multiplier={self.conservative_factor}, "
                f"max_size={self.max_kelly_fraction}, lookback={self.lookback_period}"
            )

        except Exception as e:
            sys_logger.log_error(
                "KellyCalculator", f"Failed to apply config: {str(e)}",
                {"config": cfg, "error": str(e)}
            )
            logger.error(f"Failed to apply Kelly config: {e}", exc_info=True)
            raise

    def get_config_snapshot(self) -> dict:
        """
        Возвращает снимок текущей конфигурации калькулятора для логов и отладки.
        """
        # Используем дефолты из глобального конфига, если атрибут еще не установлен
        return {
            'confidence_threshold': getattr(self, 'confidence_threshold', KELLY_CONFIG.get('confidence_threshold')),
            'max_kelly_fraction': getattr(self, 'max_kelly_fraction', KELLY_CONFIG.get('max_kelly_fraction')),
            'conservative_factor': getattr(self, 'conservative_factor', KELLY_CONFIG.get('conservative_factor')),
            'min_trades_required': getattr(self, 'min_trades_required', KELLY_CONFIG.get('min_trades_required')),
            'lookback_window': getattr(self, 'lookback_period', KELLY_CONFIG.get('lookback_window')),
            'min_position_size': getattr(self, 'min_position_size', KELLY_CONFIG.get('min_position_size')),
            'rebalance_threshold': getattr(self, 'rebalance_threshold', KELLY_CONFIG.get('rebalance_threshold')),
        }

    def add_trade_result(self, symbol: str, pnl: float, trade_data: Dict[str, Any]):
        """Добавление результата сделки для анализа"""
        trade_record = {
            'symbol': symbol,
            'pnl': pnl,
            'pnl_percent': pnl / trade_data.get('position_value', 1),
            'timestamp': time.time(),
            'data': trade_data
        }
        
        self.trade_history.append(trade_record)
        self.symbol_stats[symbol]['trades'].append(trade_record)
        self.symbol_stats[symbol]['last_update'] = time.time()
        
        # Очищаем старые данные для символа (последние 100 сделок)
        if len(self.symbol_stats[symbol]['trades']) > 100:
            self.symbol_stats[symbol]['trades'] = self.symbol_stats[symbol]['trades'][-100:]
        
        # Инвалидируем кэш для символа
        if symbol in self.kelly_cache:
            del self.kelly_cache[symbol]
    
    async def calculate_optimal_size(self, symbol: str, current_size: float, price: float, 
                                   balance: float = None, source_balance: float = None) -> Dict[str, Any]:
        """
        Адаптированный метод для копи-трейдинга
        Использует все существующие возможности вашего класса + специфика копирования
    
        Args:
            symbol: Торговый символ
            current_size: Размер позиции источника
            price: Текущая цена
            balance: Баланс целевого аккаунта (опционально)
            source_balance: Баланс источника (опционально)
        
        Returns:
            Dict с рекомендуемым размером и деталями расчета
        """
        try:
            # Используем ваш существующий мощный метод calculate_kelly_fraction
            estimated_balance = balance or current_size * price * 20  # Разумная оценка
            kelly_calculation = self.calculate_kelly_fraction(symbol, estimated_balance)
        
            if kelly_calculation is None:
                # Fallback на простой расчет
                fallback_fraction = await self.calculate_optimal_size(
                    balance=estimated_balance,
                    win_rate=self.performance_metrics['win_rate'],
                    avg_win=self.performance_metrics['avg_win'],
                    avg_loss=self.performance_metrics['avg_loss'],
                    current_drawdown=0.0
                )
                recommended_size = current_size * fallback_fraction * 5  # Масштабирование
            
                return {
                    'success': True,
                    'recommended_size': recommended_size,
                    'kelly_fraction': fallback_fraction,
                    'confidence': 0.3,
                    'method': 'fallback_calculation',
                    'details': {
                        'original_size': current_size,
                        'reason': 'insufficient_data',
                        'scaling_factor': 5.0
                    }
                }
        
            # Используем результат вашего продвинутого Kelly расчета
            base_kelly_size = kelly_calculation.recommended_size / price if price > 0 else current_size
        
            # СПЕЦИФИЧЕСКИЕ КОРРЕКТИРОВКИ ДЛЯ КОПИ-ТРЕЙДИНГА:
        
            # 1. Корректировка на волатильность символа (используя ваши данные)
            volatility_factor = self._get_copy_volatility_adjustment(symbol, kelly_calculation)
        
            # 2. Корректировка на соотношение капиталов
            if source_balance and balance:
                capital_ratio = balance / source_balance
                capital_adjustment = self._calculate_capital_adjustment(capital_ratio)
            else:
                capital_adjustment = 1.0
        
            # 3. Ограничения для копи-трейдинга
            copy_limits = self._apply_copy_trading_limits(current_size, kelly_calculation.confidence_score)
        
            # Финальный расчет
            adjusted_size = base_kelly_size * volatility_factor * capital_adjustment
            final_size = max(copy_limits['min_size'], min(copy_limits['max_size'], adjusted_size))
        
            # Проверка минимального размера ордера
            min_order_size = self._get_min_order_size(symbol)
            if final_size < min_order_size and current_size >= min_order_size:
                final_size = min_order_size
            elif final_size < min_order_size:
                final_size = 0  # Слишком мала для копирования
        
            return {
                'success': True,
                'recommended_size': final_size,
                'kelly_fraction': kelly_calculation.kelly_fraction,
                'confidence': kelly_calculation.confidence_score,
                'method': 'advanced_kelly_copy_trading',
                'details': {
                    'original_size': current_size,
                    'base_kelly_size': base_kelly_size,
                    'volatility_factor': volatility_factor,
                    'capital_adjustment': capital_adjustment,
                    'final_size': final_size,
                    'kelly_data': {
                        'win_rate': kelly_calculation.win_rate,
                        'avg_win': kelly_calculation.avg_win,
                        'avg_loss': kelly_calculation.avg_loss,
                        'profit_factor': kelly_calculation.profit_factor,
                        'sample_size': kelly_calculation.sample_size
                    },
                    'copy_limits': copy_limits,
                    'min_order_size': min_order_size
                }
            }
        
        except Exception as e:
            logger.error(f"Enhanced Kelly copy calculation error for {symbol}: {e}")
        
            # Безопасный fallback
            safe_size = current_size * 0.5  # 50% от оригинала
            return {
                'success': False,
                'recommended_size': safe_size,
                'kelly_fraction': self.default_position_size,
                'confidence': 0.0,
                'method': 'error_fallback',
                'error': str(e),
                'details': {
                    'original_size': current_size,
                    'fallback_ratio': 0.5
                }
            }

    def calculate_kelly_fraction(self, symbol: str, current_balance: float) -> Optional[KellyCalculation]:
        """
        Расчет оптимального размера позиции по Kelly Criterion
    
        Формула: f = (bp - q) / b
        где:
        f = оптимальная доля капитала
        b = отношение средней прибыли к средней убытку
        p = вероятность выигрыша
        q = вероятность проигрыша (1-p)
        """
        try:
            # Проверяем кэш
            cache_key = f"{symbol}_{current_balance}"
            if cache_key in self.kelly_cache:
                cached_result, cache_time = self.kelly_cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    return cached_result
        
            # Получаем историю сделок для символа
            symbol_trades = self.symbol_stats[symbol]['trades']
        
            if len(symbol_trades) < KELLY_CONFIG['min_trades_required']:
                logger.debug(f"Insufficient trade history for {symbol}: {len(symbol_trades)} trades")
                return self._default_kelly_calculation(symbol, current_balance)
        
            # Анализируем результаты сделок
            pnl_values = [trade['pnl_percent'] for trade in symbol_trades]
            wins = [pnl for pnl in pnl_values if pnl > 0]
            losses = [abs(pnl) for pnl in pnl_values if pnl < 0]
        
            if not wins or not losses:
                logger.debug(f"No wins or losses for {symbol}")
                return self._default_kelly_calculation(symbol, current_balance)
        
            # Основные статистики
            win_rate = len(wins) / len(pnl_values)
            avg_win = statistics.mean(wins) if wins else 0.0
            avg_loss = statistics.mean(losses) if losses else 0.0
        
            # Дополнительные метрики
            profit_factor = (avg_win * len(wins)) / (avg_loss * len(losses)) if (avg_loss * len(losses)) > 0 else float('inf')
            max_drawdown = self._calculate_max_drawdown(pnl_values)
            sharpe_ratio = self._calculate_sharpe_ratio(pnl_values)
        
            # Kelly расчет
            b = avg_win / avg_loss  # Отношение прибыли к убытку
            p = win_rate
            q = 1 - p
        
            kelly_fraction = (b * p - q) / b
            raw_kelly = kelly_fraction  # Сохраняем исходное значение
        
            # Применяем поправки
            adjusted_kelly = self._apply_kelly_adjustments(
                kelly_fraction, win_rate, profit_factor, max_drawdown, 
                sharpe_ratio, len(pnl_values)
            )
        
            # НОВОЕ: Логируем если Kelly значительно уменьшен
            if adjusted_kelly < raw_kelly * 0.8 and current_balance > 0:
                risk_events_logger.log_kelly_adjustment(
                    account_id=2,
                    original_size=current_balance * raw_kelly,
                    adjusted_size=current_balance * adjusted_kelly,
                    kelly_fraction=adjusted_kelly
                )
        
            # Рассчитываем рекомендуемый размер позиции
            recommended_value = current_balance * adjusted_kelly
        
            # Создаем результат
            result = KellyCalculation(
                symbol=symbol,
                kelly_fraction=adjusted_kelly,
                win_rate=win_rate,
                avg_win=avg_win,
                avg_loss=avg_loss,
                profit_factor=profit_factor,
                sample_size=len(pnl_values),
                confidence_score=self._calculate_confidence_score(len(pnl_values), sharpe_ratio),
                recommended_size=recommended_value
            )
        
            # Кэшируем результат
            self.kelly_cache[cache_key] = (result, time.time())
        
            logger.info(f"Kelly calculation for {symbol}: fraction={adjusted_kelly:.3f}, size=${recommended_value:.2f}")
            return result
        
        except Exception as e:
            logger.error(f"Kelly calculation error for {symbol}: {e}")
            return self._default_kelly_calculation(symbol, current_balance)
    
    def _apply_kelly_adjustments(self, kelly_fraction: float, win_rate: float, 
                                profit_factor: float, max_drawdown: float, 
                                sharpe_ratio: float, sample_size: int) -> float:
        """Применение корректировок к базовому Kelly коэффициенту"""
        
        adjusted_kelly = kelly_fraction
        
        # Корректировка на размер выборки
        if sample_size < 50:
            confidence_factor = sample_size / 50
            adjusted_kelly *= confidence_factor
        
        # Корректировка на максимальную просадку
        if max_drawdown > 0.2:  # Более 20%
            drawdown_factor = 0.2 / max_drawdown
            adjusted_kelly *= drawdown_factor
        
        # Корректировка на Sharpe ratio
        if sharpe_ratio < 1.0:
            sharpe_factor = max(0.5, sharpe_ratio)
            adjusted_kelly *= sharpe_factor
        
        # Корректировка на Profit Factor
        if profit_factor < 1.2:  # PF менее 1.2 считается слабым
            pf_factor = max(0.5, profit_factor / 1.2)
            adjusted_kelly *= pf_factor
        
        # Применяем консервативный коэффициент
        adjusted_kelly *= KELLY_CONFIG['conservative_factor']
        
        # Ограничиваем максимальную позицию
        adjusted_kelly = min(adjusted_kelly, KELLY_CONFIG['max_kelly_fraction'])
        
        # Минимальная позиция
        adjusted_kelly = max(adjusted_kelly, KELLY_CONFIG['min_position_size'])
        
        return adjusted_kelly
    
    def _calculate_max_drawdown(self, returns: List[float]) -> float:
        """Расчет максимальной просадки"""
        if not returns:
            return 0.0
        try:
            equity_curve = [1.0]
            for r in returns:
                equity_curve.append(equity_curve[-1] * (1 + r))

            peak = equity_curve[0]
            max_drawdown = 0.0
            for equity in equity_curve:
                if equity > peak:
                    peak = equity
                drawdown = (peak - equity) / peak if peak > 0 else 0
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
            return max_drawdown
        except Exception:
            # Fallback for any math errors
            return 0.0

    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Расчет коэффициента Шарпа"""
        try:
            if len(returns) < 2:
                return 0.0
            mean_return = statistics.mean(returns)
            # Use population standard deviation for consistency with numpy's default
            std_return = statistics.pstdev(returns)
            if std_return == 0:
                return 0.0
            return mean_return / std_return
        except Exception:
            return 0.0
    
    def _calculate_confidence_score(self, sample_size: int, sharpe_ratio: float) -> float:
        """Расчет уверенности в расчетах"""
        # Базовая уверенность на основе размера выборки
        size_confidence = min(1.0, sample_size / 100)
        
        # Уверенность на основе качества стратегии
        quality_confidence = min(1.0, max(0.2, sharpe_ratio / 2.0))
        
        return (size_confidence + quality_confidence) / 2
    
    def _default_kelly_calculation(self, symbol: str, current_balance: float) -> KellyCalculation:
        """Дефолтный расчет Kelly при недостатке данных"""
        return KellyCalculation(
            symbol=symbol,
            kelly_fraction=KELLY_CONFIG['min_position_size'],
            win_rate=0.5,
            avg_win=0.01,
            avg_loss=0.01,
            profit_factor=1.0,
            sample_size=0,
            confidence_score=0.1,
            recommended_size=current_balance * KELLY_CONFIG['min_position_size']
        )
    
    def get_portfolio_kelly_allocation(self, symbols: List[str], current_balance: float) -> Dict[str, KellyCalculation]:
        """Расчет Kelly распределения для портфеля символов"""
        results = {}
        total_kelly = 0.0
        
        # Рассчитываем Kelly для каждого символа
        for symbol in symbols:
            kelly_calc = self.calculate_kelly_fraction(symbol, current_balance)
            if kelly_calc:
                results[symbol] = kelly_calc
                total_kelly += kelly_calc.kelly_fraction
        
        # Нормализуем если общий Kelly превышает лимит
        if total_kelly > KELLY_CONFIG['max_kelly_fraction']:
            normalization_factor = KELLY_CONFIG['max_kelly_fraction'] / total_kelly
            for symbol in results:
                results[symbol].kelly_fraction *= normalization_factor
                results[symbol].recommended_size *= normalization_factor
        
        return results
    
    def _get_copy_volatility_adjustment(self, symbol: str, kelly_calc: KellyCalculation) -> float:
        """Корректировка на волатильность для копи-трейдинга"""
        # Базовая корректировка по типу актива
        if 'BTC' in symbol:
            base_factor = 0.9  # BTC относительно стабилен
        elif symbol in ['ETHUSDT', 'ADAUSDT', 'SOLUSDT']:
            base_factor = 0.8  # Крупные альткоины
        elif 'USDT' in symbol:
            base_factor = 0.7  # Мелкие альткоины более волатильны
        else:
            base_factor = 0.8  # По умолчанию
    
        # Дополнительная корректировка на основе ваших данных
        if kelly_calc.sample_size > 50:
            # Если много данных, используем реальную статистику
            if kelly_calc.avg_loss > 0.03:  # Высокие потери = высокая волатильность
                base_factor *= 0.8
            elif kelly_calc.avg_loss < 0.01:  # Низкие потери = низкая волатильность
                base_factor *= 1.1
    
        return min(1.0, base_factor)

    def _calculate_capital_adjustment(self, capital_ratio: float) -> float:
        """Корректировка на соотношение капиталов"""
        if capital_ratio > 1:
            # Больше капитала = можем позволить чуть больше, но осторожно
            return min(1.3, 1 + (capital_ratio - 1) * 0.15)
        else:
            # Меньше капитала = пропорционально меньше
            return max(0.5, capital_ratio)

    def _apply_copy_trading_limits(self, source_size: float, confidence: float) -> Dict[str, float]:
        """Применение ограничений специфичных для копи-трейдинга"""
    
        # Базовые лимиты зависят от уверенности
        if confidence > 0.8:
            max_ratio = 2.5  # Высокая уверенность = можем больше
            min_ratio = 0.3
        elif confidence > 0.5:
            max_ratio = 1.5  # Средняя уверенность
            min_ratio = 0.2
        else:
            max_ratio = 1.0  # Низкая уверенность = консервативно
            min_ratio = 0.1
    
        return {
            'max_size': source_size * max_ratio,
            'min_size': source_size * min_ratio
        }

    def _get_min_order_size(self, symbol: str) -> float:
        """Минимальные размеры ордеров для основных символов"""
        min_sizes = {
            'BTCUSDT': 0.0001,
            'ETHUSDT': 0.001,
            'ADAUSDT': 1.0,
            'SOLUSDT': 0.01,
            'DOGEUSDT': 10.0,
            'AVAXUSDT': 0.01,
            'DOTUSDT': 0.1,
            'LINKUSDT': 0.01,
            'UNIUSDT': 0.01,
            'LTCUSDT': 0.001
        }
        return min_sizes.get(symbol, 0.001)  # По умолчанию

    # ============================================
    # ДОПОЛНИТЕЛЬНЫЙ МЕТОД ДЛЯ СТАТИСТИКИ КОПИРОВАНИЯ
    # ============================================

    def add_copy_trade_result(self, symbol: str, source_size: float, copied_size: float, 
                             pnl: float, trade_data: Dict[str, Any]):
        """
        Расширение вашего метода add_trade_result для копи-трейдинга
        """
        copy_ratio = copied_size / source_size if source_size > 0 else 1
    
        # Расширенные данные для копи-трейдинга
        enhanced_trade_data = trade_data.copy()
        enhanced_trade_data.update({
            'source_size': source_size,
            'copied_size': copied_size,
            'copy_ratio': copy_ratio,
            'copy_efficiency': pnl / (copied_size * trade_data.get('avg_price', 1)) if copied_size > 0 else 0,
            'copy_type': 'copy_trade'
        })
    
        # Используем ваш существующий метод
        self.add_trade_result(symbol, pnl, enhanced_trade_data)

    def get_copy_trading_stats(self) -> Dict[str, Any]:
        """Статистика копи-трейдинга на основе ваших данных"""
        copy_trades = []
    
        # Собираем данные копи-трейдинга из общей истории
        for trade in self.trade_history:
            if trade.get('data', {}).get('copy_type') == 'copy_trade':
                copy_trades.append(trade)
    
        if not copy_trades:
            return {'message': 'No copy trading data available'}
    
        total_trades = len(copy_trades)
        winning_trades = len([t for t in copy_trades if t['pnl'] > 0])
    
        copy_ratios = [t['data']['copy_ratio'] for t in copy_trades if 'copy_ratio' in t['data']]
        avg_copy_ratio = sum(copy_ratios) / len(copy_ratios) if copy_ratios else 1.0
    
        total_pnl = sum(t['pnl'] for t in copy_trades)
    
        return {
            'total_copy_trades': total_trades,
            'copy_win_rate': winning_trades / total_trades,
            'avg_copy_ratio': avg_copy_ratio,
            'total_copy_pnl': total_pnl,
            'copy_profit_factor': self._calculate_profit_factor(copy_trades)
        }

    def _calculate_profit_factor(self, trades: List[Dict]) -> float:
        """Расчет Profit Factor для копи-сделок"""
        wins = [t['pnl'] for t in trades if t['pnl'] > 0]
        losses = [abs(t['pnl']) for t in trades if t['pnl'] < 0]
    
        total_wins = sum(wins) if wins else 0
        total_losses = sum(losses) if losses else 1  # Избегаем деления на ноль
    
        return total_wins / total_losses if total_losses > 0 else float('inf')

# ================================
# TRAILING STOP-LOSS SYSTEM
# ================================

class DynamicTrailingStopManager:
    """
    Manages native exchange trailing stop orders. It is self-contained and backward-compatible.
    It stores its configuration in `self.cfg` but also sets legacy attributes like `self.atr_period`
    to ensure old code that references them continues to work.
    """
    def __init__(self, main_client: EnhancedBybitClient, order_manager, trailing_config: dict):
        self.main_client = main_client
        self.order_manager = order_manager

        # 1. Establish complete defaults
        self.cfg = {
            "enabled": True,
            "mode": "conservative",
            "activation_pct": 0.02,
            "step_pct": 0.003,
            "max_pct": 0.05,
            "atr_period": 14,
            "atr_multiplier": 1.5,
        }

        # 2. Normalize and merge incoming config
        normalized_initial_cfg = self._normalize_keys(trailing_config)
        self.cfg.update(normalized_initial_cfg)

        # 3. Rebind legacy attributes to ensure they exist
        self._rebind_legacy_attrs()
        self._stops_cache = []  # list[dict]: локальный кэш открытых стоп/трейлинг ордеров
        self._ts_update_timestamps = {} # For debounce mechanism

        logger.info(f"DynamicTrailingStopManager initialized with config: {self.cfg}")

    def get_all_stops(self, symbol: Optional[str] = None) -> list:
        """
        Back-compat для Telegram UI: синхронно возвращает список стоп-ордеров.
        Меню использует только len(...) и первые несколько элементов.
        """
        try:
            cache = self._stops_cache
        except AttributeError:
            self._stops_cache = []
            cache = self._stops_cache
        if symbol:
            return [s for s in cache if s.get("symbol") == symbol]
        return list(cache)

    async def refresh_stops_cache(self, symbol: Optional[str] = None) -> list:
        """
        Опционально: подтянуть открытые стоп/TS ордера с биржи и обновить локальный кэш.
        Не используется телеграм-меню напрямую (оно вызывает sync get_all_stops()).
        """
        try:
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
            params["orderFilter"] = "StopOrder"
            res = await self.main_client._make_request_with_retry("GET", "open-orders", params)

            items = (res or {}).get("result", {}).get("list", []) if (res and res.get("retCode") == 0) else []
            norm = [{
                "symbol": it.get("symbol"),
                "orderId": it.get("orderId"),
                "orderType": it.get("orderType"),
                "stopOrderType": it.get("stopOrderType"),
                "activatePrice": it.get("triggerPrice") or it.get("activatePrice"),
            } for it in items]

            # если фильтровали по символу — обновим только его; иначе заменим целиком
            if symbol:
                self._stops_cache = [s for s in self._stops_cache if s.get("symbol") != symbol] + norm
            else:
                self._stops_cache = norm
            return self.get_all_stops(symbol)
        except Exception as e:
            logger.warning(f"refresh_stops_cache failed: {e}")
            return self.get_all_stops(symbol)

    @staticmethod
    def _normalize_keys(in_cfg: dict) -> dict:
        """Maps legacy keys to new keys and normalizes values."""
        if not in_cfg:
            return {}

        out = {}

        # Map legacy keys
        if 'min_trail_distance' in in_cfg:
            out['activation_pct'] = in_cfg['min_trail_distance']
        if 'update_threshold' in in_cfg:
            out['step_pct'] = in_cfg['update_threshold']
        if 'max_trail_distance' in in_cfg:
            out['max_pct'] = in_cfg['max_trail_distance']
        if 'aggressive_mode' in in_cfg:
            out['mode'] = "aggressive" if in_cfg['aggressive_mode'] else "conservative"

        # Pass through new-style keys
        for key in ["activation_pct", "step_pct", "max_pct", "mode", "enabled", "atr_period", "atr_multiplier"]:
            if key in in_cfg:
                out[key] = in_cfg[key]

        return out

    def _rebind_legacy_attrs(self):
        """Sets legacy attributes from the self.cfg dictionary for backward compatibility."""
        # Percent-based
        self.default_distance_percent = float(self.cfg.get('activation_pct', 0.015))
        self.min_trail_step           = float(self.cfg.get('step_pct', 0.002))
        self.max_distance_percent     = float(self.cfg.get('max_pct', 0.05))
        # ATR
        self.atr_period               = int(self.cfg.get('atr_period', 14))
        self.atr_multiplier           = float(self.cfg.get('atr_multiplier', 2.0))
        # Mode flag
        self.aggressive_mode          = (str(self.cfg.get('mode', 'conservative')).lower() == 'aggressive')

    def reload_config(self, new_cfg_or_patch: dict) -> dict:
        """
        Updates the trailing stop configuration at runtime, normalizes keys,
        and rebinds legacy attributes to maintain compatibility.
        """
        old_cfg = self.cfg.copy()

        normalized_patch = self._normalize_keys(new_cfg_or_patch)
        self.cfg.update(normalized_patch)

        self._rebind_legacy_attrs() # Ensure legacy attrs are always in sync

        logger.info("Trailing stop config updated.")
        sys_logger.log_event(
            "INFO", "TrailingStopManager", "Trailing stop configuration updated",
            {"old_config": old_cfg, "new_config": self.cfg}
        )
        return self.get_config_snapshot()

    def get_config_snapshot(self) -> dict:
        """
        Returns a snapshot of the current configuration, ensuring values are
        consistent with the active (and backward-compatible) legacy attributes.
        """
        snap = dict(self.cfg)
        # also mirror legacy names to help any other code paths:
        snap.update({
            "activation_pct": self.default_distance_percent,
            "step_pct": self.min_trail_step,
            "max_pct": self.max_distance_percent,
            "mode": "aggressive" if self.aggressive_mode else "conservative",
            "atr_period": self.atr_period,
            "atr_multiplier": self.atr_multiplier,
        })
        return snap

    def _round_to_tick(self, value: float, tick_size: float) -> float:
        """Rounds a value to the nearest tick size."""
        if tick_size <= 0:
            return value
        return round(value / tick_size) * tick_size

    async def create_or_update_trailing_stop(self, position_data: Dict[str, Any], ref_price_type: str = "entry"):
        """
        Calculates and sets a trailing stop using Bybit v5 API.
        Determines the correct MAIN positionIdx (ignores donor's), applies debounce and idempotency.
        """
        symbol = position_data.get("symbol")
        donor_pos_idx = int(position_data.get("position_idx", 0))  # logging only
        donor_side = (position_data.get("side") or "").strip()
        try:
            main_positions = await self.main_client.get_positions(category="linear", symbol=symbol) or []
            active_positions = [p for p in main_positions if safe_float(p.get("size")) > 0]
            if not active_positions:
                logger.info(f"TS_SKIP: No active position on MAIN for {symbol}.")
                return
            is_hedge_main = any(int(p.get("positionIdx", 0)) in (1, 2) for p in active_positions)
            if is_hedge_main:
                target_side = "Buy" if donor_pos_idx == 1 else "Sell" if donor_pos_idx == 2 else donor_side
                current_pos = next((p for p in active_positions if (p.get("side") or "").strip() == target_side), active_positions[0])
            else:
                current_pos = active_positions[0]

            main_pos_idx = int(current_pos.get("positionIdx", 0))
            logger.info(f"IDX_MAP: symbol={symbol}, donor_idx={donor_pos_idx}, main_idx={main_pos_idx}, main_side={(current_pos.get('side') or '').strip()}")

            now = time.time()
            key = (symbol, main_pos_idx)
            if now - self._ts_update_timestamps.get(key, 0) < 2:
                logger.info(f"TS_DEBOUNCE: Skip {symbol} idx={main_pos_idx}")
                return
            self._ts_update_timestamps[key] = now

            if not self.cfg.get('enabled'):
                return

            entry_price = safe_float(current_pos.get("entryPrice")) or safe_float(current_pos.get("markPrice")) or safe_float(current_pos.get("lastPrice"))
            position_value = safe_float(current_pos.get("size")) * entry_price
            if position_value < self.cfg.get('min_notional_for_ts', 0):
                logger.info(f"TS_SKIP: Position value ${position_value:.2f} for {symbol} idx={main_pos_idx} is below threshold.")
                return

            curr_ts = safe_float(current_pos.get("trailingStop"))
            curr_ap = safe_float(current_pos.get("activePrice"))
            side = (current_pos.get("side") or "").strip()  # 'Buy' | 'Sell'
            ref_price = safe_float(current_pos.get("markPrice")) or entry_price
            filters = await self.main_client.get_symbol_filters(symbol, category="linear")
            tick = float(filters.get("tick_size") or 0.01)
            activation_pct = float(self.cfg.get('activation_pct', 0.015))
            step_pct = float(self.cfg.get('step_pct', 0.002))

            if side not in ("Buy", "Sell"):
                logger.error(f"TS_FAIL: Invalid side '{side}' for {symbol} on MAIN.")
                return
            active_price = self._round_to_tick(ref_price * (1 + activation_pct if side == "Buy" else 1 - activation_pct), tick)
            trail_abs    = self._round_to_tick(ref_price * step_pct, tick)   # absolute distance
            trigger_dir  = 2 if side == "Buy" else 1

            is_ts_same = curr_ts and abs(curr_ts - trail_abs) < (tick / 2)
            is_ap_same = curr_ap and abs(curr_ap - active_price) < (tick / 2)
            if is_ts_same and is_ap_same:
                logger.info(f"TS_SKIP_IDENTICAL: symbol={symbol} idx={main_pos_idx} activePrice={curr_ap} trailingStop={curr_ts}")
                return

            await self.order_manager.place_trailing_stop(
                symbol=symbol,
                trailing_stop_price=str(trail_abs),
                active_price=(None if is_ap_same else str(active_price)),
                position_idx=main_pos_idx,
                trigger_direction=trigger_dir,
            )
        except Exception as e:
            logger.error(f"TS_FAIL(create_or_update): Failed for {symbol} (donor_idx={donor_pos_idx})", exc_info=True)

    # --- Adapter Methods for Lifecycle Integration ---

    async def create_trailing_stop(self, position: Dict[str, Any]):
        """Adapter to create a trailing stop for a new position."""
        logger.info(f"TS_ADAPTER(create): Triggered for {position.get('symbol')}")
        await self.create_or_update_trailing_stop(position, ref_price_type='entry')

    async def update_trailing_stop(self, position: Dict[str, Any]):
        """Adapter to update a trailing stop for a modified position."""
        logger.info(f"TS_ADAPTER(update): Triggered for {position.get('symbol')}")
        await self.create_or_update_trailing_stop(position, ref_price_type='mark') # Or entry, based on config

    async def remove_trailing_stop(self, position: Dict[str, Any]):
        """Adapter to remove a trailing stop by setting it to '0'."""
        symbol = position.get('symbol')
        position_idx = int(position.get('position_idx', 0))
        logger.info(f"TS_ADAPTER(remove): Triggered for {symbol}")
        try:
            # Pre-check: only attempt to reset if there is an active position on MAIN.
            main_positions = await self.main_client.get_positions(category="linear", symbol=symbol)
            active_pos = next((p for p in main_positions if safe_float(p.get('size', 0)) > 0 and int(p.get('positionIdx', -1)) == position_idx), None)

            if not active_pos:
                logger.debug(f"TS_RESET_SKIP: {symbol} (no active position)")
                return

            # According to Bybit V5 API, sending "0" resets the trailing stop.
            result = await self.order_manager.place_trailing_stop(
                symbol=symbol,
                trailing_stop_price="0",
                position_idx=position_idx
            )
            if result.get("success"):
                logger.info(f"TS_RESET_OK: Trailing stop for {symbol} reset successfully.")
            else:
                # The place_trailing_stop method already logs detailed errors.
                logger.error(f"TS_ADAPTER_FAIL(remove): place_trailing_stop failed for {symbol}.")

        except Exception as e:
            logger.error(f"TS_ADAPTER_FAIL(remove): Unhandled exception for {symbol}", exc_info=True)

# ================================
# СИСТЕМА УПРАВЛЕНИЯ ОРДЕРАМИ
# ================================

class AdaptiveOrderManager:
    """
    Интеллектуальная система управления ордерами
    
    Особенности:
    - Адаптивный выбор типа ордера на основе рыночных условий
    - Защита от проскальзывания
    - Retry механизм с exponential backoff
    - Оптимизация времени исполнения
    """
    
    def __init__(self, main_client: EnhancedBybitClient, order_formatter=None, risk_manager=None, logger=None):
        import logging

        # клиенты/зависимости
        self.main_client = main_client
        # сохраняем совместимость, если где‑то обращаются как self.client
        self.client = main_client
        self.order_formatter = order_formatter
        self.risk_manager = risk_manager

        # гарантируем наличие логгера (и совместимый алиас self.log)
        self.logger = logger or logging.getLogger("AdaptiveOrderManager")
        self.log = self.logger

        # очереди/состояние, как в вашей версии
        self.order_queue = asyncio.PriorityQueue()
        self.pending_orders = {}
        self.order_history = deque(maxlen=1000)
        self.execution_stats = defaultdict(lambda: {'success': 0, 'failed': 0, 'avg_time': 0})

    async def get_min_order_qty(self, symbol: str, price: float) -> float:
        """
        Получает минимально допустимый для ордера размер (в единицах qty) для символа.
        Учитывает как minQty, так и minNotional из фильтров биржи.
        """
        try:
            filters = await self.main_client.get_symbol_filters(symbol, category="linear")
            min_qty = float(filters.get("min_qty", 0.001))
            min_notional = float(filters.get("min_notional", 5.0))

            if price and price > 0 and min_notional > 0:
                min_qty_by_notional = min_notional / price
                effective_min_qty = max(min_qty, min_qty_by_notional)
                self.logger.info(f"get_min_order_qty for {symbol}: min_qty={min_qty}, min_notional={min_notional}, price={price} -> effective_min_qty={effective_min_qty}")
                return effective_min_qty

            return min_qty
        except Exception as e:
            self.logger.warning(f"get_min_order_qty failed for {symbol}, falling back to default: {e}")
            # Возвращаем безопасное, но не нулевое значение по умолчанию
            return 0.001


    async def get_market_analysis(self, symbol: str) -> MarketConditions:
        """Анализ рыночных условий для символа"""
        try:
            # Получаем данные о стакане
            orderbook_params = {
                "category": "linear",
                "symbol": symbol,
                "limit": 50
            }
            
            orderbook_result = await self.main_client._make_request_with_retry(
                "GET", "market/orderbook", orderbook_params
            )
            
            # Получаем тикер данные
            ticker_params = {
                "category": "linear", 
                "symbol": symbol
            }
            
            ticker_result = await self.main_client._make_request_with_retry(
                "GET", "market/tickers", ticker_params
            )
            
            if not orderbook_result or not ticker_result:
                return self._default_market_conditions()
            
            # Анализируем стакан
            orderbook_data = orderbook_result.get('result', {})
            bids = orderbook_data.get('b', [])
            asks = orderbook_data.get('a', [])
            
            if not bids or not asks:
                return self._default_market_conditions()
            
            best_bid = safe_float(bids[0][0])
            best_ask = safe_float(asks[0][0])
            spread = best_ask - best_bid
            spread_percent = (spread / best_ask) * 100 if best_ask > 0 else 0
            
            # Анализируем ликвидность
            bid_volume = sum(safe_float(bid[1]) for bid in bids[:10])
            ask_volume = sum(safe_float(ask[1]) for ask in asks[:10])
            total_volume = bid_volume + ask_volume
            
            # Анализируем тикер
            ticker_data = ticker_result.get('result', {}).get('list', [])
            if ticker_data:
                ticker = ticker_data[0]
                volume_24h = safe_float(ticker.get('volume24h', 0))
                turnover_24h = safe_float(ticker.get('turnover24h', 0))
                price_change_24h = safe_float(ticker.get('price24hPcnt', 0))
            else:
                volume_24h = 0
                turnover_24h = 0
                price_change_24h = 0
            
            # Рассчитываем метрики
            volatility = abs(price_change_24h)
            volume_ratio = min(2.0, volume_24h / 1000000) if volume_24h > 0 else 0.1
            trend_strength = min(1.0, abs(price_change_24h) / 5.0)  # Нормализуем к 5%
            liquidity_score = min(1.0, total_volume / 1000) if total_volume > 0 else 0.1
            
            return MarketConditions(
                volatility=volatility,
                spread_percent=spread_percent, 
                volume_ratio=volume_ratio,
                trend_strength=trend_strength,
                liquidity_score=liquidity_score
            )
            
        except Exception as e:
            logger.error(f"Market analysis error for {symbol}: {e}")
            return self._default_market_conditions()
    
    def _default_market_conditions(self) -> MarketConditions:
        """Дефолтные рыночные условия при ошибке анализа"""
        return MarketConditions(
            volatility=0.02,
            spread_percent=0.1,
            volume_ratio=1.0,
            trend_strength=0.5,
            liquidity_score=0.8
        )
    
    def select_order_strategy(self, market_conditions: MarketConditions, 
                            urgency: str = 'normal') -> OrderStrategy:
        """Выбор оптимальной стратегии ордера"""
        try:
            spread_pct = market_conditions.spread_percent
            volume_ratio = market_conditions.volume_ratio
            volatility = market_conditions.volatility
            liquidity = market_conditions.liquidity_score
            
            # Высокий приоритет или узкий спред = Market ордер
            if urgency == 'high' or spread_pct < 0.05:
                return OrderStrategy.MARKET
            
            # Высокая волатильность = Aggressive Limit
            if volatility > 0.02:
                return OrderStrategy.AGGRESSIVE_LIMIT
            
            # Низкая ликвидность = осторожный Limit
            if liquidity < 0.5 or volume_ratio < 0.3:
                return OrderStrategy.LIMIT
            
            # Нормальные условия = Adaptive
            return OrderStrategy.ADAPTIVE
            
        except Exception as e:
            logger.error(f"Order strategy selection error: {e}")
            return OrderStrategy.MARKET
    
    async def place_adaptive_order(self, copy_order: CopyOrder) -> Dict[str, Any]:
        """Размещение адаптивного ордера"""
        try:
            start_time = time.time()
            
            # Анализируем рынок
            market_conditions = await self.get_market_analysis(copy_order.target_symbol)
            
            # Выбираем стратегию если не задана
            if copy_order.order_strategy == OrderStrategy.ADAPTIVE:
                urgency = 'high' if copy_order.priority >= 3 else 'normal'
                copy_order.order_strategy = self.select_order_strategy(market_conditions, urgency)
            
            # Размещаем ордер согласно выбранной стратегии
            if copy_order.order_strategy == OrderStrategy.MARKET:
                result = await self._place_market_order(copy_order)
            elif copy_order.order_strategy == OrderStrategy.LIMIT:
                result = await self._place_smart_limit_order(copy_order, market_conditions)
            elif copy_order.order_strategy == OrderStrategy.AGGRESSIVE_LIMIT:
                result = await self._place_aggressive_limit_order(copy_order, market_conditions)
            else:
                result = await self._place_market_order(copy_order)  # Fallback
            
            # Записываем статистику
            execution_time = time.time() - start_time
            self._update_execution_stats(copy_order.target_symbol, True, execution_time)
            
            # Добавляем в историю
            self.order_history.append({
                'copy_order': copy_order,
                'result': result,
                'execution_time': execution_time,
                'market_conditions': market_conditions,
                'timestamp': time.time()
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Adaptive order placement error: {e}")
            self._update_execution_stats(copy_order.target_symbol, False, 0)
            return {'success': False, 'error': str(e)}
    
    async def _normalize_price_to_tick(self, symbol: str, raw_price: float, side: str | None = None) -> str:
        """
        Приводим цену к биржевому tickSize.
        Для buy – обычно округляем ВВЕРХ (чтобы не промахнуться),
        для sell – ВНИЗ. Если side не задан – обычный round к ближайшему тику.
        Возвращаем строку с корректным числом знаков.
        """
        try:
            f = await self.main_client.get_symbol_filters(symbol, category="linear")
            tick = float(f.get("tick_size") or 0.01)

            # определим необходимую точность по tick
            dec = 0
            if tick < 1:
                s = f"{tick:.12f}".rstrip('0')
                dec = len(s.split('.')[-1]) if '.' in s else 0

            if tick <= 0:
                return f"{raw_price:.{dec}f}"

            q = raw_price / tick
            if side and side.lower() == "buy":
                snapped = math.ceil(q) * tick
            elif side and side.lower() == "sell":
                snapped = math.floor(q) * tick
            else:
                snapped = round(q) * tick

            return f"{snapped:.{dec}f}"
        except Exception as e:
            logger.warning(f"_normalize_price_to_tick fallback: {e}")
            return f"{raw_price:.2f}"

    async def _format_qty_live(self, symbol: str, quantity: float, price: float | None) -> str:
        """
        Обёртка над format_quantity_for_symbol_live, чтобы не тянуть импорт утилиты в каждом месте.
        """
        try:
            return await format_quantity_for_symbol_live(self.main_client, symbol, quantity, price)
        except Exception as e:
            logger.warning(f"_format_qty_live fallback: {e}")
            return format_quantity_for_symbol(symbol, quantity, price)

    async def _place_market_order(self, copy_order: CopyOrder) -> Dict[str, Any]:
        """
        Размещение рыночного ордера (v5) с orderLinkId и live‑форматированием количества.
        Единственный источник истины по учёту успешной сделки — retCode == 0.
        """
        try:
            # 1) Текущая цена
            current_price = copy_order.target_price
            if not current_price or current_price <= 0:
                params = {"category": "linear", "symbol": copy_order.target_symbol}
                tickers = await self.main_client._make_request_with_retry("GET", "market/tickers", params)
                if tickers and tickers.get("retCode") == 0:
                    lst = tickers.get("result", {}).get("list", [])
                    if lst:
                        current_price = float(lst[0].get("lastPrice") or 0.0)

            if not current_price or current_price <= 0:
                raise RuntimeError("Unable to obtain current price for market order")

            # 2) Количество с учётом реальных фильтров (stepSize, minQty, minNotional)
            formatted_qty = await format_quantity_for_symbol_live(
                self.main_client, copy_order.target_symbol, copy_order.target_quantity, current_price
            )

            # Подстрахуем min notional (часть бирж возвращает через instruments-info; форматер это учитывает,
            # но оставим safety‑check на $5 как разумный нижний порог)
            order_value = float(formatted_qty) * float(current_price)
            if order_value < 5.0:
                min_qty = 5.0 / float(current_price)
                formatted_qty = await format_quantity_for_symbol_live(
                    self.main_client, copy_order.target_symbol, min_qty, current_price
                )
                order_value = float(formatted_qty) * float(current_price)

            # 3) Идемпотентность на уровне ордера
            link_id = f"copy:{copy_order.target_symbol}:{int(time.time()*1000)}:{uuid.uuid4().hex[:8]}"

            order_data = {
                "category": "linear",
                "symbol": copy_order.target_symbol,
                "side": copy_order.target_side,          # 'Buy' / 'Sell'
                "orderType": "Market",
                "qty": formatted_qty,                     # строка, как требует v5
                "timeInForce": "IOC",
                "orderLinkId": link_id,
                "positionIdx": copy_order.metadata.get("position_idx", 0)
            }
            order_data['reduceOnly'] = copy_order.metadata.get('reduceOnly', False)

            self.logger.info(
                f"Placing MARKET: {copy_order.target_symbol} {copy_order.target_side} "
                f"qty={formatted_qty} value=${order_value:.2f}"
            )

            result = await self.main_client._make_request_with_retry("POST", "order/create", data=order_data)

            if result and result.get("retCode") == 0:
                order_id = result.get("result", {}).get("orderId")
                self.logger.info(f"Market order placed successfully: {order_id} link={link_id}")
                return {
                    "success": True,
                    "order_id": order_id,
                    "order_link_id": link_id,
                    "type": "market",
                    "qty": formatted_qty,
                    "price": current_price
                }

            # Handle specific error for min notional value
            ret_code = (result or {}).get("retCode")
            if ret_code == 110094: # Order does not meet minimum order value
                err_msg = (result or {}).get("retMsg", "Min notional error")
                self.logger.warning(
                    f"NO-OP: Market order for {copy_order.target_symbol} failed with 110094 ({err_msg}). "
                    f"This will not be retried. Qty: {formatted_qty}, Value: ${order_value:.2f}"
                )
                # Return success=False but with a specific error type to prevent retries by the caller
                return {"success": False, "error": err_msg, "no_retry": True}


            err = (result or {}).get("retMsg") or "No response"
            self.logger.error(f"Market order failed: {err}")
            if "Qty invalid" in err:
                self.logger.error(
                    f"Qty details: formatted={formatted_qty}, value=${float(formatted_qty) * float(current_price):.2f}, "
                    f"price=${float(current_price):.2f}"
                )
            return {"success": False, "error": err}

        except Exception as e:
            self.logger.exception(f"Market order error: {e}")
            return {"success": False, "error": str(e)}


    async def _place_smart_limit_order(self, copy_order: CopyOrder, market_conditions: MarketConditions) -> Dict[str, Any]:
        """
        Умный лимит с учётом лучшего bid/ask и orderLinkId.
        Цена округляется до двух знаков — при желании можно расширить до tickSize.
        """
        try:
            params = {"category": "linear", "symbol": copy_order.target_symbol}
            tickers = await self.main_client._make_request_with_retry("GET", "market/tickers", params)
            if not tickers or tickers.get("retCode") != 0:
                return await self._place_market_order(copy_order)

            lst = tickers.get("result", {}).get("list", [])
            if not lst:
                return await self._place_market_order(copy_order)

            t = lst[0]
            best_bid = float(t.get("bid1Price") or 0.0)
            best_ask = float(t.get("ask1Price") or 0.0)
            if best_bid <= 0 or best_ask <= 0:
                return await self._place_market_order(copy_order)

            spread = best_ask - best_bid
            if copy_order.target_side.lower() == "buy":
                limit_price = best_bid + spread * 0.30
            else:
                limit_price = best_ask - spread * 0.30

            formatted_qty = await format_quantity_for_symbol_live(
                self.main_client, copy_order.target_symbol, copy_order.target_quantity, limit_price
            )

            link_id = f"copy:{copy_order.target_symbol}:{int(time.time()*1000)}:{uuid.uuid4().hex[:8]}"

            order_data = {
                "category": "linear",
                "symbol": copy_order.target_symbol,
                "side": copy_order.target_side,
                "orderType": "Limit",
                "qty": formatted_qty,
                "price": f"{limit_price:.2f}",     # при желании можно округлять к tickSize (см. примечание выше)
                "timeInForce": "GTC",
                "orderLinkId": link_id
            }

            result = await self.main_client._make_request_with_retry("POST", "order/create", data=order_data)
            if result and result.get("retCode") == 0:
                order_id = result.get("result", {}).get("orderId")
                self.logger.info(
                    f"Smart LIMIT placed: {copy_order.target_symbol} {copy_order.target_side} "
                    f"{formatted_qty} @ {limit_price:.2f} (ID: {order_id}, link={link_id})"
                )
                return {
                    "success": True,
                    "order_id": order_id,
                    "order_link_id": link_id,
                    "type": "smart_limit",
                    "price": limit_price,
                    "qty": formatted_qty
                }

            err = (result or {}).get("retMsg") or "No response"
            self.logger.error(f"Smart limit order failed: {err}")
            return {"success": False, "error": err}

        except Exception as e:
            self.logger.exception(f"Smart limit order error: {e}")
            return {"success": False, "error": str(e)}


    async def _place_aggressive_limit_order(self, copy_order: CopyOrder, market_conditions: MarketConditions) -> Dict[str, Any]:
        """
        Агрессивный лимит по лучшей стороне (IOC) с orderLinkId и live‑форматированием qty.
        """
        try:
            params = {"category": "linear", "symbol": copy_order.target_symbol}
            tickers = await self.main_client._make_request_with_retry("GET", "market/tickers", params)
            if not tickers or tickers.get("retCode") != 0:
                return await self._place_market_order(copy_order)

            lst = tickers.get("result", {}).get("list", [])
            if not lst:
                return await self._place_market_order(copy_order)

            t = lst[0]
            best_bid = float(t.get("bid1Price") or 0.0)
            best_ask = float(t.get("ask1Price") or 0.0)
            if best_bid <= 0 or best_ask <= 0:
                return await self._place_market_order(copy_order)

            limit_price = best_ask if copy_order.target_side.lower() == "buy" else best_bid

            formatted_qty = await format_quantity_for_symbol_live(
                self.main_client, copy_order.target_symbol, copy_order.target_quantity, limit_price
            )

            link_id = f"copy:{copy_order.target_symbol}:{int(time.time()*1000)}:{uuid.uuid4().hex[:8]}"

            order_data = {
                "category": "linear",
                "symbol": copy_order.target_symbol,
                "side": copy_order.target_side,
                "orderType": "Limit",
                "qty": formatted_qty,
                "price": f"{limit_price:.2f}",
                "timeInForce": "IOC",
                "orderLinkId": link_id
            }

            result = await self.main_client._make_request_with_retry("POST", "order/create", data=order_data)
            if result and result.get("retCode") == 0:
                order_id = result.get("result", {}).get("orderId")
                self.logger.info(
                    f"Aggressive LIMIT placed: {copy_order.target_symbol} {copy_order.target_side} "
                    f"{formatted_qty} @ {limit_price:.2f} (ID: {order_id}, link={link_id})"
                )
                return {
                    "success": True,
                    "order_id": order_id,
                    "order_link_id": link_id,
                    "type": "aggressive_limit",
                    "price": limit_price,
                    "qty": formatted_qty
                }

            err = (result or {}).get("retMsg") or "No response"
            self.logger.error(f"Aggressive limit order failed: {err}")
            return {"success": False, "error": err}

        except Exception as e:
            self.logger.exception(f"Aggressive limit order error: {e}")
            return {"success": False, "error": str(e)}

    async def place_trailing_stop(self, symbol: str, trailing_stop_price: str, active_price: Optional[str] = None, position_idx: int = 0, trigger_direction: Optional[int] = None):
        sent_ts = str(trailing_stop_price) if trailing_stop_price not in ["", None] else "0"
        is_reset = sent_ts == "0"

        data = {"category": "linear", "symbol": symbol, "positionIdx": position_idx}
        log_payload = {}

        if is_reset:
            data.update({"trailingStop": "0", "takeProfit": "0", "stopLoss": "0"})
            log_payload = {"trailingStop": "0"}
            self.logger.info(f"TS_CLEAR: Attempting for {symbol}, idx={position_idx}")
        else:
            data["trailingStop"] = sent_ts
            log_payload["trailingStop"] = sent_ts
            if active_price:
                data["activePrice"] = active_price
                log_payload["activePrice"] = active_price
            if trigger_direction:
                data["triggerDirection"] = trigger_direction
                log_payload["triggerDirection"] = trigger_direction
            self.logger.info(f"TS_APPLY: Attempting for {symbol}, idx={position_idx}, payload={json.dumps(log_payload)}")

        try:
            result = await self.main_client._make_single_request("POST", "position/trading-stop", data=data)
            ret_code = (result or {}).get("retCode")
            err_msg = (result or {}).get("retMsg", "Unknown API error")

            if ret_code == 0:
                self.logger.info(f"{'TS_CLEAR_OK' if is_reset else 'TS_APPLY_OK'}: Success for {symbol}, idx={position_idx}")
                return {"success": True, "result": result}

            if ret_code == 34040:
                self.logger.info(f"TS_NOOP: Not modified for {symbol}, idx={position_idx} (retCode=34040)")
                return {"success": True, "noop": True}

            if ret_code == 10001:
                if is_reset:
                    self.logger.info(f"TS_CLEAR_IGNORE: Already clear or no position for {symbol}, idx={position_idx} (retCode=10001)")
                    return {"success": True, "already_set": True}
                else:
                    self.logger.error(f"TS_APPLY_FAIL: Invalid params for {symbol}, idx={position_idx}. Error: '{err_msg}'. Payload: {json.dumps(data)}")
                    return {"success": False, "error": err_msg, "no_retry": True}

            self.logger.error(f"TS_FAIL: API error for {symbol}, idx={position_idx}. Error: '{err_msg}' (retCode={ret_code})")
            return {"success": False, "error": err_msg}

        except Exception as e:
            self.logger.exception(f"TS_EXCEPTION: Unhandled error in place_trailing_stop for {symbol}, idx={position_idx}: {e}")
            return {"success": False, "error": str(e)}

    async def cancel_all_symbol_orders(self, symbol: str, order_type_filter: Optional[str] = None) -> Dict[str, Any]:
        """Cancels all open orders for a symbol, optionally filtering by type (e.g., 'Stop')."""
        try:
            self.logger.info(f"Canceling all '{order_type_filter or 'All'}' orders for {symbol}...")

            data = {
                "category": "linear",
                "symbol": symbol,
            }
            if order_type_filter and order_type_filter.lower() == 'stop':
                data["orderFilter"] = "StopOrder"

            result = await self.main_client._make_request_with_retry("POST", "order/cancel-all", data=data)

            if result and result.get("retCode") == 0:
                cancelled_ids = [item.get('orderId') for item in result.get('result', {}).get('list', [])]
                self.logger.info(f"Successfully canceled {len(cancelled_ids)} orders for {symbol}. IDs: {cancelled_ids}")
                return {"success": True, "cancelled_ids": cancelled_ids}

            err = (result or {}).get("retMsg") or "No response"
            self.logger.error(f"Failed to cancel orders for {symbol}: {err}")
            return {"success": False, "error": err}
        except Exception as e:
            self.logger.exception(f"Exception in cancel_all_symbol_orders for {symbol}: {e}")
            return {"success": False, "error": str(e)}

    async def _monitor_order_execution(self, order_id: str, timeout: float = 30) -> Dict[str, Any]:
        """Мониторинг исполнения ордера"""
        try:
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                # Проверяем статус ордера
                order_params = {
                    "category": "linear",
                    "orderId": order_id
                }
                
                result = await self.main_client._make_request_with_retry(
                    "GET", "order/realtime", order_params
                )
                
                if result and result.get('retCode') == 0:
                    orders = result.get('result', {}).get('list', [])
                    if orders:
                        order = orders[0]
                        status = order.get('orderStatus')
                        
                        if status in ['Filled', 'PartiallyFilled']:
                            filled_qty = safe_float(order.get('cumExecQty', 0))
                            avg_price = safe_float(order.get('avgPrice', 0))
                            
                            return {
                                'status': status,
                                'filled_qty': filled_qty,
                                'avg_price': avg_price,
                                'execution_time': time.time() - start_time
                            }
                        elif status in ['Cancelled', 'Rejected']:
                            return {
                                'status': status,
                                'filled_qty': 0,
                                'execution_time': time.time() - start_time
                            }
                
                await asyncio.sleep(1)  # Проверяем каждую секунду
            
            # Timeout - отменяем ордер
            await self._cancel_order(order_id)
            return {
                'status': 'Timeout',
                'filled_qty': 0,
                'execution_time': timeout
            }
            
        except Exception as e:
            logger.error(f"Order monitoring error: {e}")
            return {'status': 'Error', 'error': str(e)}
    
    async def _cancel_order(self, order_id: str) -> bool:
        """Отмена ордера"""
        try:
            cancel_data = {
                "category": "linear",
                "orderId": order_id
            }
            
            result = await self.main_client._make_request_with_retry("POST", "order/cancel", data=cancel_data)
            
            if result and result.get('retCode') == 0:
                logger.info(f"Order cancelled: {order_id}")
                return True
            else:
                logger.error(f"Order cancellation failed: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Order cancellation error: {e}")
            return False
    
    def _update_execution_stats(self, symbol: str, success: bool, execution_time: float):
        """Обновление статистики исполнения"""
        try:
            stats = self.execution_stats[symbol]
            
            if success:
                stats['success'] += 1
                # Скользящее среднее времени исполнения
                if stats['avg_time'] == 0:
                    stats['avg_time'] = execution_time
                else:
                    stats['avg_time'] = stats['avg_time'] * 0.8 + execution_time * 0.2
            else:
                stats['failed'] += 1
                
        except Exception as e:
            logger.error(f"Stats update error: {e}")
    
    def get_execution_stats(self) -> Dict[str, Dict[str, Any]]:
        """Получение статистики исполнения"""
        return dict(self.execution_stats)

# ================================
# СИСТЕМА КОПИРОВАНИЯ ПОЗИЦИЙ
# ================================

class PositionCopyManager:
    """
    Основная система копирования позиций
    
    Интегрирует:
    - Kelly Criterion для оптимального размера позиций
    - Adaptive Order Manager для исполнения
    - Trailing Stop Manager для управления рисками
    - Synchronization Manager для минимизации задержек
    """
    
    def __init__(self, source_client: EnhancedBybitClient, main_client: EnhancedBybitClient, trailing_config: dict):
        self.source_client = source_client
        self.main_client = main_client
        
        # Основные компоненты
        self.kelly_calculator = AdvancedKellyCalculator()
        self.order_manager = AdaptiveOrderManager(main_client)
        self.trailing_manager = DynamicTrailingStopManager(main_client, self.order_manager, trailing_config)
        
        # Состояние системы
        self.copy_mode = CopyMode.KELLY_OPTIMAL
        self.active_positions = {}  # symbol -> position_data
        self.copy_queue = asyncio.PriorityQueue()
        self.position_mapping = {}  # source_id -> target_id
        
        # Статистика
        self.copy_stats = {
            'positions_copied': 0,
            'positions_closed': 0,
            'total_volume_copied': 0.0,
            'avg_sync_delay': 0.0,
            'copy_success_rate': 0.0,
            'kelly_adjustments': 0
        }
        
        # Управление задачами
        self.processing_active = False
        self._copy_processor_task = None
        self.should_stop = False

        #Защита от задвоения счетчика копирования позиций и задвоения копирования позиций
        self._copy_success_lock = asyncio.Lock()
        self._processed_link_ids: set[str] = set()

        self._recent_copy_window = 5  # сек — антидубль для force_copy и фонового сканера
        self._recent_copies: dict[tuple[str, str], float] = {}  # (symbol, side) -> ts

        self._copy_success_lock = asyncio.Lock()
        self._processed_link_ids: set[str] = set()

    async def _mark_copy_success(self, order_link_id: str) -> bool:
        """
        Идемпотентная фиксация успеха копирования.
        Возвращает True, если учтено впервые; False — если такой link уже считался.
        """
        if not order_link_id:
            # подстраховка — не считаем пустые link_id
            return False
        async with self._copy_success_lock:
            if order_link_id in self._processed_link_ids:
                return False
            self._processed_link_ids.add(order_link_id)
            # ЕДИНСТВЕННОЕ место инкремента
            ##self.system_stats['successful_copies'] = self.system_stats.get('successful_copies', 0) + 1
            return True

    def _should_skip_duplicate(self, symbol: str, side: str) -> bool:
        """
        Простая защита от дублей при параллельном запуске (/force_copy + фон)
        в течение _recent_copy_window секунд по одному инструменту и стороне.
        """
        now = time.time()
        key = (symbol, side.lower())
        last = self._recent_copies.get(key, 0.0)
        if now - last < self._recent_copy_window:
            self.logger.warning(f"Skip duplicate copy within {self._recent_copy_window}s for {symbol} {side}")
            return True
        self._recent_copies[key] = now
        return False

        
    async def start_copying(self):
        """Запуск системы копирования"""
        if not self.processing_active:
            if self._copy_processor_task and not self._copy_processor_task.done():
                self._copy_processor_task.cancel()
                
            self._copy_processor_task = asyncio.create_task(self._process_copy_queue())
            self.processing_active = True
            logger.info("Position copying system started")
    
    async def stop_copying(self):
        """Остановка системы копирования"""
        self.should_stop = True
        self.processing_active = False
        
        if self._copy_processor_task and not self._copy_processor_task.done():
            self._copy_processor_task.cancel()
            try:
                await asyncio.wait_for(self._copy_processor_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        logger.info("Position copying system stopped")
    
    
    
    
    async def _process_copy_queue(self):
        """Обработчик очереди копирования"""
        try:
            while not self.should_stop:
                try:
                    # Получаем ордер с наивысшим приоритетом
                    priority, timestamp, copy_order = await asyncio.wait_for(
                        self.copy_queue.get(), timeout=1.0
                    )
                    
                    # Выполняем копирование
                    await self._execute_copy_order(copy_order, timestamp)
                    
                    self.copy_queue.task_done()
                    
                except asyncio.TimeoutError:
                    continue  # Периодически проверяем should_stop
                except Exception as e:
                    logger.error(f"Copy queue processing error: {e}")
        except asyncio.CancelledError:
            logger.debug("Copy processor cancelled")
        except Exception as e:
            logger.error(f"Copy processor error: {e}")
    
    async def _account_success_once(self, result: dict, copy_order) -> bool:
        """
        Идемпотентная фиксация успешного копирования.
        Инкрементит counters только один раз на уникальный orderLinkId/ orderId.
        Возвращает True, если учтено впервые.
        """
        link_id = (result.get('order_link_id')
                   or result.get('order_id')
                   or f"{copy_order.target_symbol}:{copy_order.target_side}:{int(time.time())}")

        async with self._copy_success_lock:
            if link_id in self._processed_link_ids:
                return False
            self._processed_link_ids.add(link_id)

            # Инкременты — здесь и только здесь
            if hasattr(self, 'system_stats') and self.system_stats is not None:
                self.system_stats['successful_copies'] = self.system_stats.get('successful_copies', 0) + 1
            if hasattr(self, 'copy_stats'):
                self.copy_stats['positions_copied'] = self.copy_stats.get('positions_copied', 0) + 1
            return True


    async def _execute_copy_order(self, copy_order: CopyOrder, queue_timestamp: float):
        """Выполнение ордера копирования с логированием в orders_log и positions_db"""
        internal_order_id = str(uuid.uuid4())
        try:
            sync_delay = time.time() - queue_timestamp
            logger.info(f"Executing copy order for {copy_order.target_symbol} (delay: {sync_delay:.3f}s)")

            # Log pending order to DB
            orders_logger.log_order(
                account_id=1, # MAIN account
                symbol=copy_order.target_symbol,
                side=copy_order.target_side,
                qty=copy_order.target_quantity,
                status=OrderStatus.PENDING.value,
                exchange_order_id=internal_order_id,
                attempt=copy_order.retry_count + 1
            )

            # 1) Place the order
            start_time = time.time()
            result = await self.order_manager.place_adaptive_order(copy_order)
            latency_ms = int((time.time() - start_time) * 1000)
            
            # 2) Update order log with the result
            if result.get('success'):
                orders_logger.log_order(
                    account_id=1,
                    symbol=copy_order.target_symbol,
                    side=copy_order.target_side,
                    qty=copy_order.target_quantity,
                    status=OrderStatus.PLACED.value, # Assuming market order is placed/filled instantly for logging
                    exchange_order_id=result.get('order_id', internal_order_id),
                    latency_ms=latency_ms
                )

                # 3) Update position tracking and log to positions DB
                accounted = await self._account_success_once(result, copy_order)
                if accounted:
                    if copy_order.source_signal.signal_type == SignalType.POSITION_OPEN:
                        await self._setup_position_tracking(copy_order, result)
                    elif copy_order.source_signal.signal_type == SignalType.POSITION_MODIFY:
                        await self._handle_position_modify(copy_order, result)
                    elif copy_order.source_signal.signal_type == SignalType.POSITION_CLOSE:
                        await self._cleanup_position_tracking(copy_order, result)

                    # 4) Manage Trailing Stop (Asynchronously with Retries)
                    try:
                        signal_type = copy_order.source_signal.signal_type
                        # Trigger for OPEN or MODIFY that increases position size
                        is_increase = (signal_type == SignalType.POSITION_OPEN or
                                       (signal_type == SignalType.POSITION_MODIFY and not copy_order.metadata.get('reduceOnly', False)))

                        if is_increase:
                            logger.info(f"Scheduling trailing stop for {copy_order.target_symbol} (signal: {signal_type.value})")

                            async def _set_trailing_with_retry(order_to_trail: CopyOrder):
                                """Retries setting the trailing stop until the position is visible on the main account."""
                                max_retries = 10
                                delay = 0.2  # Initial delay
                                for attempt in range(max_retries):
                                    try:
                                        pos_idx = order_to_trail.metadata.get('position_idx', 0)
                                        symbol = order_to_trail.target_symbol

                                        positions = await self.main_client.get_positions(category="linear", symbol=symbol)
                                        main_pos = next((p for p in positions if int(p.get('positionIdx', -1)) == pos_idx), None)

                                        if main_pos and safe_float(main_pos.get('size', 0)) > 0:
                                            logger.info(f"Position {symbol} (idx={pos_idx}) found on MAIN. Setting/updating trailing stop (attempt {attempt + 1}).")
                                            await self.trailing_manager.create_or_update_trailing_stop(main_pos)
                                            return  # Success
                                        else:
                                            logger.info(f"Trailing deferred for {symbol} (idx={pos_idx}), position not yet visible. Attempt {attempt + 1}/{max_retries}.")

                                    except Exception as e:
                                        logger.error(f"Error in trailing stop retry task (attempt {attempt + 1}): {e}", exc_info=True)

                                    await asyncio.sleep(delay)
                                    delay = min(delay * 2, 15)  # Exponential backoff up to 15s

                                logger.error(f"Failed to set trailing stop for {order_to_trail.target_symbol} after {max_retries} retries.")

                            # Create the background task to avoid blocking the copy pipeline
                            asyncio.create_task(_set_trailing_with_retry(copy_order))

                        elif signal_type == SignalType.POSITION_CLOSE:
                            # For full close, we can still attempt to remove the trailing stop
                            await self.trailing_manager.remove_trailing_stop({
                                'symbol': copy_order.target_symbol,
                                'position_idx': copy_order.metadata.get('position_idx', 0)
                            })

                    except Exception as e:
                        logger.error(f"TS_LIFECYCLE_FAIL: Unhandled exception in trailing stop management for {copy_order.target_symbol}", exc_info=True)

                # 5) Send notification
                await send_telegram_alert(
                    f"✅ Position copied: {copy_order.target_symbol} {copy_order.target_side} "
                    f"{copy_order.target_quantity:.6f}"
                )
            else:
                # Log failed order
                orders_logger.log_order(
                    account_id=1,
                    symbol=copy_order.target_symbol,
                    side=copy_order.target_side,
                    qty=copy_order.target_quantity,
                    status=OrderStatus.FAILED.value,
                    reason=result.get('error', 'Unknown error'),
                    exchange_order_id=internal_order_id,
                    latency_ms=latency_ms
                )

                # Retry logic
                if not result.get("no_retry") and copy_order.retry_count < COPY_CONFIG['order_retry_attempts']:
                    copy_order.retry_count += 1
                    await asyncio.sleep(COPY_CONFIG['order_retry_delay'] * copy_order.retry_count)
                    await self.copy_queue.put((copy_order.priority, time.time(), copy_order))
                    logger.warning(f"Retrying copy order for {copy_order.target_symbol} (attempt {copy_order.retry_count})")
                else:
                    logger.error(f"Copy order failed after {COPY_CONFIG['order_retry_attempts']} attempts: {copy_order.target_symbol}")
                    await send_telegram_alert(f"❌ Copy order failed: {copy_order.target_symbol} {copy_order.target_side} {copy_order.target_quantity:.6f}")

        except Exception as e:
            logger.error(f"Copy order execution error: {e}", exc_info=True)
            orders_logger.log_order(
                account_id=1,
                symbol=copy_order.target_symbol,
                side=copy_order.target_side,
                qty=copy_order.target_quantity,
                status=OrderStatus.FAILED.value,
                reason=str(e),
                exchange_order_id=internal_order_id
            )

    async def _setup_position_tracking(self, copy_order: CopyOrder, execution_result: Dict[str, Any]):
        """Настройка отслеживания позиции после открытия и запись в БД."""
        try:
            symbol = copy_order.target_symbol
            position_data = {
                'symbol': symbol,
                'side': copy_order.target_side,
                'quantity': copy_order.target_quantity,
                'entry_price': execution_result.get('avg_price', copy_order.target_price),
                'leverage': copy_order.source_signal.metadata.get('leverage', '10'),
                'position_idx': copy_order.source_signal.metadata.get('pos_idx', 0),
                'open_time': time.time(),
                'order_id': execution_result.get('order_id'),
                'raw': execution_result,
            }
            self.active_positions[symbol] = position_data

            # Log to positions_db
            await positions_writer.update_position({
                "account_id": 1,
                "symbol": symbol,
                "side": position_data['side'],
                "qty": position_data['quantity'],
                "entry_price": position_data['entry_price'],
                "leverage": position_data['leverage'],
                "position_idx": position_data['position_idx'],
                "raw": position_data['raw'],
                "opened_at": datetime.fromtimestamp(position_data['open_time'])
            })
            logger.info(f"DB_LOG_OPEN: Position {symbol} logged to DB.")
        except Exception as e:
            logger.error(f"Position tracking setup or DB logging error: {e}", exc_info=True)
    
    async def _cleanup_position_tracking(self, copy_order: CopyOrder, execution_result: Dict[str, Any]):
        """Очистка отслеживания позиции после закрытия и запись в БД."""
        try:
            symbol = copy_order.target_symbol
            if symbol in self.active_positions:
                # Log close to DB
                await positions_writer.update_position({
                    "account_id": 1,
                    "symbol": symbol,
                    "qty": 0, # qty=0 signals a close
                    "position_idx": copy_order.source_signal.metadata.get('pos_idx', 0),
                    "raw": execution_result # Pass execution result as raw close data
                })
                logger.info(f"DB_LOG_CLOSE: Position {symbol} closure logged to DB.")
                del self.active_positions[symbol]
            else:
                logger.warning(f"Attempted to cleanup untracked position: {symbol}")
        except Exception as e:
            logger.error(f"Position tracking cleanup or DB logging error: {e}", exc_info=True)
    
    def _estimate_trade_pnl(self, copy_order: CopyOrder, execution_result: Dict[str, Any]) -> float:
        """Оценка P&L сделки (приблизительная для открывающих позиций)"""
        try:
            if copy_order.source_signal.signal_type == SignalType.POSITION_OPEN:
                return 0.0
            
            if copy_order.target_symbol in self.active_positions:
                position = self.active_positions[copy_order.target_symbol]
                entry_price = position['entry_price']
                exit_price = execution_result.get('avg_price', copy_order.target_price)
                quantity = copy_order.target_quantity
                
                if position['side'].lower() == 'buy':
                    return (exit_price - entry_price) * quantity
                else:
                    return (entry_price - exit_price) * quantity
            
            return 0.0
        except Exception as e:
            logger.error(f"P&L estimation error: {e}")
            return 0.0
    
    def _update_copy_stats(self, success: bool, sync_delay: float, copy_order: CopyOrder):
        """Обновление статистики копирования"""
        try:
            if success:
                if copy_order.source_signal.signal_type == SignalType.POSITION_OPEN:
                    self.copy_stats['positions_copied'] += 1
                elif copy_order.source_signal.signal_type == SignalType.POSITION_CLOSE:
                    self.copy_stats['positions_closed'] += 1
                
                self.copy_stats['total_volume_copied'] += copy_order.target_quantity * (copy_order.target_price or 0)
            
            if self.copy_stats['avg_sync_delay'] == 0:
                self.copy_stats['avg_sync_delay'] = sync_delay
            else:
                self.copy_stats['avg_sync_delay'] = (self.copy_stats['avg_sync_delay'] * 0.9 + sync_delay * 0.1)
            
            total_attempts = self.copy_stats['positions_copied'] + self.copy_stats['positions_closed']
            if total_attempts > 0:
                self.copy_stats['copy_success_rate'] = total_attempts / (total_attempts + 1) * 100
        except Exception as e:
            logger.error(f"Stats update error: {e}")
    
    async def update_trailing_stops(self):
        """Обновление всех trailing stops с валидацией и обработкой ошибок"""
        try:
            symbols_to_check = set(self.active_positions.keys())
            if not symbols_to_check:
                return

            prices_cache = {}
            for symbol in symbols_to_check:
                try:
                    ticker_params = {"category": "linear", "symbol": symbol}
                    ticker_result = await self.main_client._make_request_with_retry("GET", "market/tickers", ticker_params)
                    if ticker_result and ticker_result.get('retCode') == 0:
                        lst = ticker_result.get('result', {}).get('list', [])
                        if lst:
                            current_price = safe_float(lst[0].get('lastPrice', 0))
                            if current_price > 0:
                                prices_cache[symbol] = current_price
                except Exception as e:
                    logger.error(f"Error fetching price for {symbol}: {e}")

            for symbol, current_price in prices_cache.items():
                try:
                    position = self.active_positions.get(symbol)
                    if position:
                        # Assuming update_trailing_stop needs the full position dict
                        await self.trailing_manager.update_trailing_stop(position)
                except Exception as e:
                    logger.error(f"Error handling trailing stop for {symbol}: {e}")

        except Exception as e:
            logger.error(f"Critical error in trailing stops update: {e}", exc_info=True)

    
    async def _execute_trailing_stop_exit(self, symbol: str, trigger_price: float):
        """Исполнение выхода по trailing stop"""
        try:
            if symbol not in self.active_positions:
                return
            
            position = self.active_positions[symbol]
            close_side = "Sell" if position['side'] == "Buy" else "Buy"
            
            emergency_signal = TradingSignal(
                signal_type=SignalType.POSITION_CLOSE,
                symbol=symbol,
                side=close_side,
                size=position['quantity'],
                price=trigger_price,
                timestamp=time.time(),
                metadata={'reason': 'trailing_stop_triggered'},
                priority=3
            )
            
            copy_order = CopyOrder(
                source_signal=emergency_signal,
                target_symbol=symbol,
                target_side=close_side,
                target_quantity=position['quantity'],
                target_price=trigger_price,
                order_strategy=OrderStrategy.MARKET,
                kelly_fraction=0.0,
                priority=3
            )
            
            result = await self.order_manager.place_adaptive_order(copy_order)
            
            if result.get('success'):
                await self._cleanup_position_tracking(copy_order, result)
                await send_telegram_alert(f"🛑 Trailing stop triggered: {symbol} closed at ${trigger_price:.6f}")
                logger.info(f"Trailing stop executed for {symbol} at ${trigger_price:.6f}")
            else:
                logger.error(f"Trailing stop execution failed for {symbol}")
                await send_telegram_alert(f"❌ Trailing stop execution failed for {symbol}")
            
        except Exception as e:
            logger.error(f"Trailing stop execution error: {e}")
    
    def set_copy_mode(self, mode: CopyMode):
        """Установка режима копирования"""
        self.copy_mode = mode
        logger.info(f"Copy mode set to: {mode.value}")
    
    def get_copy_stats(self) -> Dict[str, Any]:
        """Получение статистики копирования"""
        stats = self.copy_stats.copy()
        stats.update({
            'active_positions': len(self.active_positions),
            'queue_size': self.copy_queue.qsize(),
            'processing_active': self.processing_active,
            'copy_mode': self.copy_mode.value,
            'trailing_stops_active': len(self.trailing_manager.get_all_stops())
        })
        return stats

    async def _handle_position_modify(self, copy_order: CopyOrder, execution_result: Dict[str, Any]):
        """Handles position modification tracking and DB logging."""
        try:
            symbol = copy_order.target_symbol
            if symbol not in self.active_positions:
                logger.warning(f"Cannot log MODIFY for untracked position: {symbol}")
                await self._setup_position_tracking(copy_order, execution_result)
                return

            position = self.active_positions[symbol]

            qty_change = safe_float(copy_order.target_quantity)
            if copy_order.target_side == 'Sell':
                qty_change = -qty_change

            new_qty = safe_float(position['quantity']) + qty_change

            logger.info(f"Modifying position for {symbol}. Old qty: {position['quantity']}, Change: {qty_change}, New qty: {new_qty}")

            position['quantity'] = new_qty
            position['last_modified_price'] = execution_result.get('avg_price', copy_order.target_price)
            position['last_modified_time'] = time.time()
            position['raw'] = execution_result

            # Log modification to positions_db
            await positions_writer.update_position({
                "account_id": 1,
                "symbol": symbol,
                "side": position['side'],
                "qty": new_qty,
                "entry_price": position['entry_price'], # Entry price doesn't change on modify
                "leverage": position['leverage'],
                "position_idx": position['position_idx'],
                "raw": execution_result,
                "updated_at": datetime.fromtimestamp(position['last_modified_time'])
            })
            logger.info(f"DB_LOG_MODIFY: Position {symbol} logged to DB.")
        except Exception as e:
            logger.error(f"Position modify handling or DB logging error: {e}", exc_info=True)

# ================================
# СИСТЕМА КОНТРОЛЯ РИСКОВ
# ================================

class DrawdownController:
    """
    Комплексная система контроля просадки и управления рисками
    
    Функции:
    - Мониторинг просадки в реальном времени
    - Автоматическая остановка при превышении лимитов
    - Градуированные уровни предупреждений
    - Режим восстановления после просадки
    """
    
    def __init__(self):
        self.peak_balance = 0.0
        self.daily_start_balance = 0.0
        self.daily_reset_time = 0
    
        # ✅ ДОБАВЛЕНО: Лимиты риска для метода check_risk_limits
        self.max_daily_drawdown = 0.05  # 5% максимальная дневная просадка
        self.max_total_drawdown = 0.15  # 15% максимальная общая просадка
    
        # Уровни предупреждений (градуированные)
        self.alert_levels = [0.02, 0.035, 0.05, 0.08, 0.12]  # 2%, 3.5%, 5%, 8%, 12%
        self.alerts_sent = {}
    
        # Статистика рисков
        self.risk_stats = {
            'max_daily_drawdown': 0.0,
            'max_total_drawdown': 0.0,
            'emergency_stops_triggered': 0,
            'recovery_mode_activations': 0,
            'risk_alerts_sent': 0
        }
    
        self.emergency_stop_active = False
        self.recovery_mode_active = False
    
        # ✅ ДОБАВЛЕНО: Дополнительные атрибуты для расширенного контроля рисков
        self.current_balance = 0.0  # Текущий баланс для расчетов
        self.daily_pnl = 0.0  # Текущий дневной P&L

                # === SAFE-MODE / контекст риска (не ломает старые конфиги) ===
        # Собираем минимальный конфиг для RiskDataContext из твоего RISK_CONFIG
        _safe_mode_cfg = {
            'enabled': True,
            'data_stale_ttl_sec': 10.0,   # сколько считаем данные «устаревшими»
            'risk_confirm_reads': 2,      # сколькими чтениями подтверждать DD
            'risk_hysteresis': 0.01       # гистерезис 1%
        }
        _risk_cfg_wrapper = {
            'RISK': {'drawdown_limit': RISK_CONFIG.get('max_total_drawdown', 0.15)},
            'SAFE_MODE': _safe_mode_cfg
        }

        self.risk_ctx = RiskDataContext(_risk_cfg_wrapper)
        # notifier используем твой send_telegram_alert, он уже импортирован сверху
        self.supervisor = HealthSupervisor(_risk_cfg_wrapper, self.risk_ctx, notifier=send_telegram_alert)

        # high-watermark для дневной DD (дневной «пик», от которого считать дневную просадку)
        self.daily_high = 0.0


    async def check_risk_limits(self, current_balance: float = None, 
                                daily_pnl: float = None, 
                                max_drawdown: float = None) -> Dict[str, Any]:
        """
        Проверка лимитов риска для возможности открытия позиций (safe-gate).
        При недостоверных данных НЕ блокируем открытия (возвращаем ok с reason).
        """
        risk_check = {
            'can_trade': True,
            'can_open_position': True,
            'warning': False,
            'emergency_stop': False,
            'reason': 'Normal trading conditions',
            'recommended_position_size_multiplier': 1.0
        }
        try:
            # 1) Если баланс не передан – пробуем использовать последний известный
            if current_balance is None:
                current_balance = self.current_balance if self.current_balance > 0 else None

            # 2) Данные недостоверны → НЕ блокируем открытие (важно для copy-сигналов)
            if current_balance is None or not self.risk_ctx.is_data_reliable():
                risk_check['reason'] = 'Risk data not reliable — skip DD gate'
                return risk_check

            # 3) Обновим контексты риска
            await self.supervisor.on_api_success()
            self.risk_ctx.update_equity(current_balance)
            if self.daily_high == 0.0:
                self.daily_high = current_balance
            self.daily_high = max(self.daily_high, current_balance)

            if daily_pnl is None:
                daily_pnl = self.daily_pnl

            # 4) Считаем DD локально (как в твоей логике)
            if self.peak_balance <= 0 or current_balance > self.peak_balance:
                self.peak_balance = current_balance

            daily_drawdown = abs(float(daily_pnl)) / current_balance if current_balance > 0 else 0.0
            total_drawdown = (self.peak_balance - current_balance) / self.peak_balance if self.peak_balance > 0 else 0.0

            daily_drawdown = max(0.0, daily_drawdown)
            total_drawdown = max(0.0, total_drawdown)

            # 5) Обновим сохранённые значения
            self.current_balance = current_balance
            self.daily_pnl = daily_pnl

            # 6) Критические случаи — только по подтверждённой DD
            critical_daily = daily_drawdown > (self.max_daily_drawdown * 1.5)
            critical_total = total_drawdown > (self.max_total_drawdown * 1.2)
            if (critical_daily or critical_total) and self.risk_ctx.dd_confirmed():
                self.emergency_stop_active = True
                self.risk_stats['emergency_stops_triggered'] += 1
                risk_check.update({
                    'can_trade': False,
                    'can_open_position': False,
                    'emergency_stop': True,
                    'reason': f'Confirmed critical drawdown: total {total_drawdown:.2%}, daily {daily_drawdown:.2%}'
                })
                return risk_check

            # 7) Мягкие ограничения → предупреждаем/снижаем размер
            if total_drawdown >= self.max_total_drawdown or daily_drawdown >= self.max_daily_drawdown:
                risk_check.update({
                    'warning': True,
                    'can_open_position': False,
                    'reason': f'Recovery mode gating: total {total_drawdown:.2%}, daily {daily_drawdown:.2%}',
                    'recommended_position_size_multiplier': 0.5
                })

            return risk_check

        except Exception as e:
            logger.error(f"check_risk_limits error: {e}")
            return risk_check

    def can_open_positions(self) -> bool:
        """Пускать новые сделки, если не активирован стоп и не остановлен супервизором.
        ВАЖНО: недостоверность risk-данных НЕ блокирует открытия, но логируем предупреждение."""
        # Жёсткий стоп – всегда «нельзя»
        if getattr(self, "emergency_stop_active", False):
            return False

        # Супервизор может остановить открытия (HALTED / copy_enabled=False)
        if hasattr(self, "supervisor"):
            if not self.supervisor.can_open_positions():
                return False

        # Недостоверные risk-данные не должны парализовать систему принудительно
        if hasattr(self, "risk_ctx") and not self.risk_ctx.is_data_reliable():
            logger.warning("Risk data not reliable — skipping DD gate for openings")
            return True

        # По умолчанию пускаем
        return True



    async def check_drawdown_limits(self, current_balance: float) -> Dict[str, Any]:
        """Комплексная проверка лимитов просадки с защитой от ложных срабатываний"""
        # Значения по умолчанию для безопасного возврата
        result = {
            'total_drawdown': 0.0,
            'daily_drawdown': 0.0,
            'alerts_triggered': [],
            'emergency_stop_required': False,
            'recovery_mode_required': False
        }

        try:
            # 1) Если баланс недоступен/некорректен — НЕ эскалируем DD, уходим мягко
            if current_balance is None or current_balance <= 0:
                sys_logger.log_warning(
                    "DrawdownController",
                    "Invalid balance for drawdown check",
                    {"current_balance": current_balance}
                )
                await self.supervisor.on_api_failure("equity unavailable")
                # Никаких алёртов и ES при недостоверных данных
                return result

            # 2) Данные валидны → обновляем контексты
            await self.supervisor.on_api_success()
            self.risk_ctx.update_equity(current_balance)

            # high watermark для дневной DD
            if self.daily_high == 0.0:
                self.daily_high = current_balance
            self.daily_high = max(self.daily_high, current_balance)
            self.risk_ctx.update_daily_dd(current_balance, self.daily_high)

            # 3) Поддержка пиков и суточного сброса (сохраняем твою логику)
            if current_balance > self.peak_balance:
                old_peak = self.peak_balance
                self.peak_balance = current_balance
                self.alerts_sent.clear()  # сброс алёртов при новом пике
        
                if old_peak > 0:
                    sys_logger.log_event(
                        "INFO",
                        "DrawdownController",
                        "New peak balance reached",
                        {
                            "old_peak": old_peak,
                            "new_peak": self.peak_balance,
                            "increase": self.peak_balance - old_peak
                        }
                    )

            current_day = int(time.time() / 86400)
            if current_day != int(self.daily_reset_time / 86400):
                self.daily_start_balance = current_balance
                self.daily_reset_time = time.time()
                self.daily_high = current_balance  # сбрасываем дневной high
        
                sys_logger.log_event(
                    "INFO",
                    "DrawdownController",
                    "Daily drawdown reset",
                    {"new_daily_start": self.daily_start_balance}
                )

            # 4) Расчёт DD (жёстко ≥ 0)
            total_drawdown = 0.0
            daily_drawdown = 0.0

            if self.peak_balance > 0:
                total_drawdown = (self.peak_balance - current_balance) / self.peak_balance
            if self.daily_start_balance > 0:
                daily_drawdown = (self.daily_start_balance - current_balance) / self.daily_start_balance

            total_drawdown = max(0.0, float(total_drawdown))
            daily_drawdown = max(0.0, float(daily_drawdown))

            # 5) Обновляем статистику
            self.risk_stats['max_total_drawdown'] = max(self.risk_stats['max_total_drawdown'], total_drawdown)
            self.risk_stats['max_daily_drawdown'] = max(self.risk_stats['max_daily_drawdown'], daily_drawdown)

            # 6) Градуированные предупреждения — ТОЛЬКО при валидных данных
            max_dd = max(total_drawdown, daily_drawdown)
            alerts = self._check_warning_levels(max_dd, total_drawdown, daily_drawdown)
            result['alerts_triggered'] = alerts
    
            # Логируем алерты
            for alert_level in alerts:
                if alert_level not in self.alerts_sent:
                    sys_logger.log_event(
                        "WARNING",
                        "DrawdownController",
                        f"Drawdown alert level {alert_level:.1%} reached",
                        {
                            "alert_level": alert_level,
                            "total_drawdown": round(total_drawdown, 4),
                            "daily_drawdown": round(daily_drawdown, 4),
                            "current_balance": current_balance
                        }
                    )
                
                    # НОВОЕ: Логируем в risk_events для warning уровней
                    if alert_level >= 0.05:  # Логируем только значимые уровни (5%+)
                        risk_events_logger.log_drawdown_event(
                            account_id=2,
                            drawdown_percent=max_dd,
                            event_type=RiskEventType.DRAWDOWN_WARNING,
                            current_balance=current_balance,
                            peak_balance=self.peak_balance
                        )
                    
                    self.alerts_sent.add(alert_level)

            # 7) Режим восстановления / Экстренная остановка (только по подтвержденной просадке)
            #   - крит: 1.5× дневного лимита или 1.2× общего лимита
            critical_daily = daily_drawdown > (self.max_daily_drawdown * 1.5)
            critical_total = total_drawdown > (self.max_total_drawdown * 1.2)

            if (critical_daily or critical_total) and self.risk_ctx.dd_confirmed():
                if not self.emergency_stop_active:
                    # НОВОЕ: Логируем в risk_events
                    risk_events_logger.log_risk_event(
                        account_id=2,
                        event=RiskEventType.EMERGENCY_STOP,
                        reason=f"Critical DD confirmed: total={total_drawdown:.2%}, daily={daily_drawdown:.2%}",
                        value=max(total_drawdown, daily_drawdown)
                    )
                
                    sys_logger.log_warning(
                        "DrawdownController",
                        "EMERGENCY STOP ACTIVATED",
                        {
                            "total_drawdown": round(total_drawdown, 4),
                            "daily_drawdown": round(daily_drawdown, 4),
                            "critical_daily": critical_daily,
                            "critical_total": critical_total,
                            "current_balance": current_balance,
                            "peak_balance": self.peak_balance
                        }
                    )
        
                self.emergency_stop_active = True
                self.risk_stats['emergency_stops_triggered'] += 1
                result['emergency_stop_required'] = True
        
            elif (total_drawdown >= self.max_total_drawdown) or (daily_drawdown >= self.max_daily_drawdown):
                result['recovery_mode_required'] = True
                if not self.recovery_mode_active:
                    # НОВОЕ: Логируем в risk_events
                    risk_events_logger.log_risk_event(
                        account_id=2,
                        event=RiskEventType.RECOVERY_MODE_ON,
                        reason=f"Threshold exceeded: total={total_drawdown:.2%} (limit {self.max_total_drawdown}), daily={daily_drawdown:.2%} (limit {self.max_daily_drawdown})",
                        value=max(total_drawdown, daily_drawdown)
                    )
                
                    sys_logger.log_warning(
                        "DrawdownController",
                        "Recovery mode activated",
                        {
                            "total_drawdown": round(total_drawdown, 4),
                            "daily_drawdown": round(daily_drawdown, 4),
                            "total_limit": self.max_total_drawdown,
                            "daily_limit": self.max_daily_drawdown
                        }
                    )
                    await self._activate_recovery_mode(total_drawdown)

            # 8) Возвращаем фактические значения для репортов
            result['total_drawdown'] = total_drawdown
            result['daily_drawdown'] = daily_drawdown
            return result

        except Exception as e:
            sys_logger.log_error(
                "DrawdownController",
                f"Drawdown check error: {str(e)}",
                {"error": str(e), "current_balance": current_balance}
            )
            logger.error(f"Drawdown check error: {e}")
            return result

    
    def _assess_risk_level(self, total_dd: float, daily_dd: float) -> RiskLevel:
        """Оценка уровня риска"""
        max_dd = max(total_dd, daily_dd)
        
        if max_dd >= RISK_CONFIG['emergency_stop_threshold']:
            return RiskLevel.CRITICAL
        elif max_dd >= RISK_CONFIG['recovery_mode_threshold']:
            return RiskLevel.HIGH
        elif max_dd >= 0.05:  # 5%
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    async def _trigger_emergency_stop(self, total_dd: float, daily_dd: float):
        """Экстренная остановка торговли"""
        try:
            self.emergency_stop_active = True
            self.risk_stats['emergency_stops_triggered'] += 1
            
            reason = f"Emergency stop: Total DD={total_dd:.2%}, Daily DD={daily_dd:.2%}"
            logger.critical(reason)
            
            # Отправляем критическое уведомление
            await send_telegram_alert(
                f"🚨 ЭКСТРЕННАЯ ОСТАНОВКА ТОРГОВЛИ!\n"
                f"Общая просадка: {total_dd:.2%}\n"
                f"Дневная просадка: {daily_dd:.2%}\n"
                f"Система остановлена для защиты капитала."
            )
            
        except Exception as e:
            logger.error(f"Emergency stop trigger error: {e}")
    
    async def _activate_recovery_mode(self, total_dd: float):
        """Активация режима восстановления"""
        try:
            self.recovery_mode_active = True
            self.risk_stats['recovery_mode_activations'] += 1
            
            logger.warning(f"Recovery mode activated: Total DD={total_dd:.2%}")
            
            await send_telegram_alert(
                f"⚠️ РЕЖИМ ВОССТАНОВЛЕНИЯ АКТИВИРОВАН\n"
                f"Общая просадка: {total_dd:.2%}\n"
                f"Размеры позиций будут снижены для восстановления."
            )
            
        except Exception as e:
            logger.error(f"Recovery mode activation error: {e}")
    
    def _check_warning_levels(self, max_dd: float, total_dd: float, daily_dd: float) -> List[str]:
        """Проверка уровней предупреждений (только при валидных данных)"""
        triggered_alerts: List[str] = []
        try:
            # не тревожим, если данные ненадёжны
            if not self.risk_ctx.is_data_reliable():
                return triggered_alerts

            for i, level in enumerate(self.alert_levels):
                alert_key = f'level_{i}'
                if max_dd >= level and alert_key not in self.alerts_sent:
                    alert_message = (
                        f"⚠️ ПРЕДУПРЕЖДЕНИЕ О ПРОСАДКЕ {level:.1%}\n"
                        f"Общая просадка: {total_dd:.2%}\n"
                        f"Дневная просадка: {daily_dd:.2%}\n"
                        f"Уровень риска: {self._assess_risk_level(total_dd, daily_dd).value}"
                    )

                    # отправляем в Telegram из sync-контекста (без await)
                    try:
                        asyncio.create_task(send_telegram_alert(alert_message))
                    except RuntimeError:
                        # если текущий цикл не активен в контексте
                        asyncio.get_event_loop().create_task(send_telegram_alert(alert_message))

                    self.alerts_sent[alert_key] = time.time()
                    self.risk_stats['risk_alerts_sent'] += 1
                    triggered_alerts.append(f"Level {level:.1%}")

                    logger.warning(f"Risk alert triggered: {level:.1%}")

            return triggered_alerts

        except Exception as e:
            logger.error(f"Warning levels check error: {e}")
            return []


    
    def calculate_recovery_parameters(self, current_drawdown: float) -> Dict[str, float]:
        """Расчет параметров восстановления после просадки"""
        try:
            if current_drawdown <= 0.05:  # До 5% - нормальные параметры
                return {
                    'position_size_multiplier': 1.0,
                    'max_concurrent_positions': COPY_CONFIG['max_concurrent_positions'],
                    'risk_per_trade': KELLY_CONFIG['max_kelly_fraction']
                }
            
            # Агрессивное снижение размеров при просадке
            recovery_factor = max(0.3, 1 - (current_drawdown * 2))
            
            return {
                'position_size_multiplier': recovery_factor,
                'max_concurrent_positions': max(1, int(COPY_CONFIG['max_concurrent_positions'] * recovery_factor)),
                'risk_per_trade': KELLY_CONFIG['max_kelly_fraction'] * recovery_factor
            }
            
        except Exception as e:
            logger.error(f"Recovery parameters calculation error: {e}")
            return {
                'position_size_multiplier': 0.5,
                'max_concurrent_positions': 1,
                'risk_per_trade': 0.01
            }
    
    def reset_emergency_stop(self):
        """Сброс экстренной остановки (только вручную)"""
        self.emergency_stop_active = False
        self.recovery_mode_active = False
        logger.info("Emergency stop and recovery mode reset")
    
    def get_risk_stats(self) -> Dict[str, Any]:
        """Получение статистики рисков"""
        stats = self.risk_stats.copy()
        stats.update({
            'peak_balance': self.peak_balance,
            'daily_start_balance': self.daily_start_balance,
            'emergency_stop_active': self.emergency_stop_active,
            'recovery_mode_active': self.recovery_mode_active,
            'alerts_sent_count': len(self.alerts_sent)
        })
        return stats

# ================================
# ОСНОВНОЙ КЛАСС СИСТЕМЫ КОПИРОВАНИЯ (ЭТАП 2)
# ================================

class Stage2CopyTradingSystem:
    """
    Основной класс системы копирования торговли - Этап 2
    
    Интегрирует все компоненты:
    - FinalTradingMonitor (Этап 1) для мониторинга источника
    - PositionCopyManager для копирования позиций
    - DrawdownController для управления рисками
    - Kelly Criterion для оптимального размера позиций
    - Trailing Stop-Loss для защиты прибыли
    """
    
    def __init__(
        self,
        source_client=None,
        main_client=None,
        base_monitor: Optional[FinalTradingMonitor] = None,
        main_api_key: Optional[str] = None,
        main_api_secret: Optional[str] = None,
        main_api_url: Optional[str] = None,
        copy_config: Optional[dict] = None,
        kelly_config: Optional[dict] = None,
        trailing_config: Optional[dict] = None,
        risk_config: Optional[dict] = None,
    ):
        # 1) Используем внешний монитор, если он передан
        self.base_monitor = base_monitor or FinalTradingMonitor(
            source_client=source_client,
            main_client=main_client,
        )

        # 2) Единообразные ссылки на клиентов (не зависят от внутренностей монитора)
        self.source_client = getattr(self.base_monitor, "source_client", source_client)
        self.main_client   = getattr(self.base_monitor, "main_client",   main_client)

        # 3) Флаги идемпотентности для корректного запуска/регистрации
        self._started = False
        self._handlers_registered = False

        # 4) Конфигурации
        self.copy_config = copy_config or COPY_CONFIG
        self.kelly_config = kelly_config or KELLY_CONFIG
        self.trailing_config = trailing_config or TRAILING_CONFIG
        self.risk_config = risk_config or RISK_CONFIG


        # 5) Основные компоненты Этапа 2
        self.mode_detector = DonorPositionModeDetector(self.source_client)
        self.copy_manager = PositionCopyManager(
            self.source_client,
            self.main_client,
            self.trailing_config
        )
        self.drawdown_controller = DrawdownController()

        # ✅ Инициализация и алиас для Kelly Calculator (совместимость с Telegram)
        self.kelly_calculator = self.copy_manager.kelly_calculator
        self.kelly_calculator.apply_config(KELLY_CONFIG)

        # ✅ Экспонируем trailing_manager для внешнего доступа (например, для телеметрии)
        self.trailing_manager = self.copy_manager.trailing_manager

        # 5) Состояние системы
        self.system_active = False
        self.copy_enabled = True
        self._copy_ready = False  # Unified readiness flag
        self.copy_state = CopyState()
        if self.main_client.api_key and self.main_client.api_secret:
            self.copy_state.keys_loaded = True
        self.demo_mode = False
        self.last_balance_check = 0
        self.balance_check_interval = 60  # Проверяем баланс каждую минуту

        # B. Stage2 Executor Flag
        self.trade_executor_connected = False

        # 6) Статистика системы
        self.system_stats = {
            "start_time": 0,
            "uptime": 0,
            "total_signals_processed": 0,
            "successful_copies": 0,
            "failed_copies": 0,
            "emergency_stops": 0,
            "recovery_activations": 0,
        }

        # 7) Доп. поля
        self._last_stage2_report_ts = 0.0

        # Idempotency cache to prevent duplicate orders
        self._recent_actions = {}
        self._idempotency_window_sec = self.copy_config.get('idempotency_window_sec', 5)
        self._idempotency_lock = asyncio.Lock()

        # Cache for leverage sync to avoid redundant API calls
        self._last_synced_leverage = {}
        self._leverage_sync_locks = defaultdict(asyncio.Lock)

        # State for isolated margin mirroring
        self._known_positions_margin = {}
        self._last_margin_sync_time = {}

        # Deferred modification queue
        self._pending_modify = deque()
        self._max_pending_modify = 100
        self.copy_connected = False

        # ВАЖНО: не регистрируем обработчики здесь, чтобы не плодить дубли.
        # Регистрация произойдёт один раз в start_system() через
        # await self.copy_manager.start_copying()

        logger.info(
            "Stage 2 Copy Trading System initialized (monitor=%s)",
            type(self.base_monitor).__name__,
        )
    
    async def enqueue_signal(self, signal: TradingSignal) -> None:
        """
        A stable, documented entry point for receiving signals from Stage-1.
        This method routes the signal to the main position handler for processing.
        """
        try:
            logger.info(f"STAGE2_ENQUEUE_SIGNAL: symbol={signal.symbol}, type={signal.signal_type.value}")

            # The `on_position_item` method is the new central handler. We need to
            # convert the TradingSignal into the dictionary format it expects and wrap it in a list.
            item = {
                'symbol': signal.symbol,
                'side': signal.side,
                'size': str(signal.size),
                'entryPrice': str(signal.price),
                'positionIdx': signal.metadata.get('position_idx', 0),
            }
            await self.on_position_item([item])

        except Exception as e:
            logger.error(f"Failed to enqueue signal for {signal.symbol}: {e}", exc_info=True)

    async def initialize(self):
        """Initializes the copy trading system and registers handlers."""
        try:
            logger.info("Initializing Stage 2 Copy Trading System...")

            # Component initialization is now handled in __init__ to ensure correct dependency injection.
            # This method is for registering handlers and activating the system.

            if hasattr(self.base_monitor, 'websocket_manager'):
                self.base_monitor.websocket_manager.register_handler(
                    'position',
                    self.on_position_item
                )
                self.copy_state.source_ws_ok = True
                logger.info("Stage 2 'on_position_item' handler registered for 'position' topic.")
            
            self.system_active = True
            logger.info("✅ Stage 2 Copy Trading System initialized successfully")

        except Exception as e:
            logger.error(f"Stage 2 initialization error: {e}", exc_info=True)
            raise

    def register_ws_handlers(self, ws_manager):
        """Регистрирует обработчики WS. Теперь 'position' вместо 'position_update'."""
        if getattr(self, "_position_handler_registered", False):
            return
        # Важно: подписываемся на 'position', как и в WS менеджере.
        ws_manager.register_handler("position", self.on_position_item)
        self._position_handler_registered = True
        logger.info("Stage 2 WS handler 'on_position_item' registered for 'position' topic.")

    def get_integration_status(self):
        """Получение статуса интеграции систем"""
        try:
            return {
                'system_active': self.system_active,
                'copy_enabled': self.copy_enabled,
                'base_monitor_connected': hasattr(self, 'base_monitor') and self.base_monitor is not None,
                'copy_manager_ready': hasattr(self, 'copy_manager') and self.copy_manager is not None,
                'signal_integration_active': hasattr(self.base_monitor, 'signal_processor') and 
                                           hasattr(self.base_monitor.signal_processor, '_copy_system_callback'),
                'demo_mode': getattr(self, 'demo_mode', True),
                'total_signals_processed': self.system_stats.get('total_signals_processed', 0),
                'successful_copies': self.system_stats.get('successful_copies', 0),
                'failed_copies': self.system_stats.get('failed_copies', 0)
            }
        except Exception as e:
            logger.error(f"Integration status error: {e}")
            return {'error': str(e)}

    async def _mirror_margin_adjustment(self, symbol: str, donor_margin_change: float, position_idx: int):
        """Calculates and applies a proportional margin adjustment to the main account."""
        if not MARGIN_CONFIG['enabled']:
            return

        # Debounce check
        now = time.time()
        if now - self._last_margin_sync_time.get(symbol, 0) < MARGIN_CONFIG['debounce_sec']:
            logger.info(f"MARGIN_MIRROR_DEBOUNCED: Skipping margin sync for {symbol}.")
            return

        try:
            # Get balances for proportional calculation
            main_balance_raw = await self.main_client.get_balance()
            source_balance_raw = await self.source_client.get_balance()

            main_balance = safe_float(main_balance_raw.get("equity") if isinstance(main_balance_raw, dict) else main_balance_raw)
            source_balance = safe_float(source_balance_raw.get("equity") if isinstance(source_balance_raw, dict) else source_balance_raw)

            if not all([main_balance, source_balance]) or source_balance <= 0:
                logger.warning(f"MARGIN_MIRROR_SKIP: Invalid balances for {symbol}.")
                return

            # Calculate proportional margin change
            proportional_margin_change = donor_margin_change * (main_balance / source_balance)

            # Check against min delta
            if abs(proportional_margin_change) < MARGIN_CONFIG['min_usdt_delta']:
                logger.info(f"MARGIN_MIRROR_SKIP: Proportional delta ${proportional_margin_change:.2f} for {symbol} is below min threshold.")
                return

            # For now, we only handle adding margin, not reducing it.
            if proportional_margin_change < 0:
                logger.info(f"MARGIN_MIRROR_SKIP: Reducing margin is not currently supported for {symbol}.")
                return

            # TODO: Add capping logic based on MARGIN_MAX_PCT_OF_EQUITY

            margin_str = f"{proportional_margin_change:.4f}" # Format to string with precision

            logger.info(f"MARGIN_MIRROR_APPLY: donorΔIM=${donor_margin_change:+.2f}, mainΔIM=${proportional_margin_change:+.2f} for {symbol}")

            result = await self.main_client.add_margin(
                symbol=symbol,
                margin=margin_str,
                position_idx=position_idx
            )

            if result.get("success"):
                self._last_margin_sync_time[symbol] = now
                logger.info(f"MARGIN_MIRROR_OK: Successfully added margin for {symbol}.")
            else:
                logger.error(f"MARGIN_MIRROR_FAIL: Failed to add margin for {symbol}. Reason: {result.get('error')}")

        except Exception as e:
            logger.error(f"MARGIN_MIRROR_EXCEPTION: for {symbol}", exc_info=True)

    async def _sync_leverage_non_blocking(self, symbol: str, leverage: str):
        """
        Sets leverage for a symbol without blocking the caller.
        Checks current leverage first and handles '110043' (not modified) as a success.
        """
        async with self._leverage_sync_locks[symbol]:
            # Quick check from local cache
            if self._last_synced_leverage.get(symbol) == leverage:
                return

            try:
                # 1. Pre-check current leverage from the exchange to avoid unnecessary API calls.
                positions = await self.main_client.get_positions(category="linear", symbol=symbol)
                if positions:
                    # Leverage is set per symbol, so we can take it from the first position entry.
                    current_leverage_str = positions[0].get('leverage')
                    if current_leverage_str and current_leverage_str == leverage:
                        logger.info(f"LEVERAGE_SKIP_SAME: symbol={symbol} current={current_leverage_str} target={leverage}")
                        self._last_synced_leverage[symbol] = leverage
                        return

                # 2. If leverage differs or is unknown, attempt to set it.
                logger.info(f"LEVERAGE_SYNC_START: Attempting to set leverage for {symbol} to {leverage}x.")

                # Pass the callback to remember the leverage upon success
                leverage_result = await self.main_client.set_leverage(
                    category="linear",
                    symbol=symbol,
                    leverage=leverage,
                    on_success_callback=self.base_monitor.signal_processor._remember_leverage
                )

                # 3. Handle the response based on Bybit's retCode.
                ret_code = (leverage_result or {}).get("retCode")
                if ret_code == 0:
                    logger.info(f"LEVERAGE_SYNC_SUCCESS: Leverage for {symbol} set to {leverage}x.")
                    self._last_synced_leverage[symbol] = leverage
                elif ret_code == 110043:
                    logger.info(f"LEVERAGE_NOOP: symbol={symbol} ret=110043")
                    # The state is confirmed to be the target state, so we update the cache.
                    self._last_synced_leverage[symbol] = leverage
                else:
                    error_msg = (leverage_result or {}).get('retMsg', 'Unknown error')
                    logger.error(f"LEVERAGE_SYNC_FAILED: for {symbol} to {leverage}x. Reason: {error_msg} (retCode: {ret_code})")

            except Exception as e:
                logger.error(f"LEVERAGE_SYNC_EXCEPTION: for {symbol} to {leverage}x.", exc_info=True)

    async def on_position_item(self, positions: Union[list, dict]):
        """
        Main entry point for position updates. Normalizes input, detects position mode,
        and routes to the appropriate handler (Hedge or One-Way).
        """
        if isinstance(positions, dict):
            positions = [positions]

        if not isinstance(positions, list) or not positions:
            return

        symbol = positions[0].get('symbol')
        if not symbol:
            return

        self.mode_detector.update_from_ws(symbol, positions, category="linear")
        mode = self.mode_detector.get_mode(symbol)

        if mode is None:
            asyncio.create_task(self.mode_detector.ensure_rest_probe(symbol))
            logger.info(f"MODE_PENDING: Position mode for {symbol} is unknown. Scheduling probe.")
            return

        if not self.trade_executor_connected:
            logger.warning("COPY_GATE: System not connected, skipping position processing.")
            return

        if mode == PositionMode.HEDGE:
            for item in positions:
                await self._process_single_position_update(item, mode)
        elif mode == PositionMode.ONEWAY:
            await self._process_aggregate_position_update(positions, mode)

    async def _check_and_record_action(self, action_key: tuple) -> bool:
        """
        Centralized idempotency check. Returns True if the action should be skipped.
        Includes a probabilistic cleanup of the cache to prevent memory leaks.
        """
        async with self._idempotency_lock:
            # 1. Check if the action is a recent duplicate
            if time.time() - self._recent_actions.get(action_key, 0) < self._idempotency_window_sec:
                logger.info(f"IDEMPOTENCY_SKIP: Skipping duplicate action for key {action_key}")
                return True  # True means "skip this action"

            # 2. Record the new action
            self._recent_actions[action_key] = time.time()

            # 3. Probabilistic cleanup (e.g., 5% chance on each new action)
            if random.random() < 0.05:
                now = time.time()
                # Clean keys older than 10x the window to be safe
                cleanup_threshold = now - (self._idempotency_window_sec * 10)
                keys_to_delete = [k for k, ts in self._recent_actions.items() if ts < cleanup_threshold]
                for k in keys_to_delete:
                    del self._recent_actions[k]
                if keys_to_delete:
                    logger.debug(f"IDEMPOTENCY_CLEANUP: Removed {len(keys_to_delete)} old action keys.")

            return False # False means "do not skip"

    async def _process_single_position_update(self, item: dict, mode: PositionMode):
        """
        Self-contained handler for a single position update in HEDGE mode.
        Calculates delta, determines order parameters, and queues the order.
        Includes a first-class branch for force-closing positions when the donor closes.
        """
        try:
            symbol = item.get('symbol')
            pos_idx = int(item.get('positionIdx', 0))
            donor_size = float(item.get('size', 0) or 0)

            target_positions = await self.main_client.get_positions(category="linear", symbol=symbol)
            main_pos = next((p for p in target_positions if int(p.get('positionIdx', 0)) == pos_idx), None)
            main_size = safe_float(main_pos.get('size')) if main_pos else 0.0

            # --- HEDGE FULL CLOSE ---
            # This branch handles donor->0 full closes, bypassing normal delta logic and min_qty gates.
            if donor_size <= 0 and main_size > 0:
                main_side = (main_pos.get('side') or '').strip()
                close_side = 'Sell' if main_side == 'Buy' else 'Buy'

                # Resolve the MAIN position index for the close order.
                pos_idx_main = await self._resolve_main_pos_idx(symbol, close_side)

                # Format quantity and log intent
                formatted_qty = await format_quantity_for_symbol_live(self.main_client, symbol, main_size, None)
                logger.info(f"HEDGE_CLOSE_SYNC: symbol={symbol}, idx={pos_idx_main}, qty={formatted_qty}")

                # Create and place the order directly, awaiting the result.
                close_signal = TradingSignal(
                    signal_type=SignalType.POSITION_CLOSE, symbol=symbol, side=close_side, size=main_size,
                    price=0, timestamp=time.time(),
                    metadata={'reason': 'hedge_full_close', 'reduceOnly': True, 'position_idx': pos_idx_main}
                )
                close_order = CopyOrder(
                    source_signal=close_signal, target_symbol=symbol, target_side=close_side,
                    target_quantity=main_size, target_price=None,
                    order_strategy=OrderStrategy.MARKET, kelly_fraction=0.0, priority=0,
                    metadata={'reason': 'hedge_full_close', 'reduceOnly': True, 'position_idx': pos_idx_main}
                )

                result = await self.copy_manager.order_manager.place_adaptive_order(close_order)

                if result and result.get("success"):
                    logger.info(f"HEDGE_CLOSE_OK: Market close for {symbol} idx={pos_idx_main} successful.")
                    # Immediately clear the trailing stop for the now-closed position.
                    await self.copy_manager.order_manager.place_trailing_stop(
                        symbol=symbol, trailing_stop_price="0", position_idx=pos_idx_main
                    )
                else:
                    logger.error(f"HEDGE_CLOSE_FAIL: Market close for {symbol} idx={pos_idx_main} failed. Reason: {result.get('error')}")
                return # Exit after handling full close

            if donor_size <= 0 and main_size <= 0:
                logger.info(f"NOOP_ZERO: donor_size=0 and MAIN is empty for {symbol} (pos_idx={pos_idx}) - ignoring snapshot.")
                return

            # --- Standard Incremental Update Logic ---
            main_balance = await self.main_client.get_balance()
            source_balance = await self.source_client.get_balance()
            main_equity = safe_float(main_balance.get("equity") if isinstance(main_balance, dict) else main_balance)
            src_equity  = safe_float(source_balance.get("equity") if isinstance(source_balance, dict) else source_balance)
            if main_equity <= 0 or src_equity <= 0:
                logger.warning("Skipping copy due to invalid or zero balance.")
                return

            donor_side = (item.get('side') or "").strip()
            entry_price = float(item.get('entryPrice') or item.get('markPrice') or 0)
            main_side = (main_pos.get('side') or '').strip() if main_pos else ''
            main_signed_qty = main_size if main_side == 'Buy' else -main_size if main_side == 'Sell' else 0.0

            target_size = 0.0
            kelly_fraction = 0.0
            if donor_size > 0 and donor_side:
                proportional_size = donor_size * (main_equity / src_equity)
                kelly_result = await self.copy_manager.kelly_calculator.calculate_optimal_size(symbol=symbol, current_size=proportional_size, price=entry_price, balance=main_equity, source_balance=src_equity)
                target_size = kelly_result.get('recommended_size', proportional_size)
                kelly_fraction = kelly_result.get('kelly_fraction', 0.0)

            min_qty = await self.copy_manager.order_manager.get_min_order_qty(symbol, price=entry_price)
            if 0 < target_size < min_qty:
                target_size = min_qty

            donor_signed = target_size if donor_side == 'Buy' else -target_size if donor_side == 'Sell' else 0.0
            delta = donor_signed - main_signed_qty

            if abs(delta) < min_qty and donor_signed != 0:
                logger.info(f"HEDGE_SKIP_MIN_DELTA: Calculated delta {delta} for {symbol} is below min_qty {min_qty}. No action taken.")
                return

            side = 'Buy' if delta > 0 else 'Sell'
            qty = abs(delta)

            reduce_only = (_sign(delta) != _sign(main_signed_qty)) and (abs(delta) <= abs(main_signed_qty))
            final_position_idx = pos_idx # In Hedge mode, final pos_idx is the same as received.

            is_opening = (main_signed_qty == 0 and donor_signed != 0)
            is_full_close = (main_signed_qty != 0 and donor_signed == 0)

            if is_full_close:
                logger.info(f"HEDGE_CLOSE_SYNC: Donor closed position for {symbol} idx={final_position_idx}. Syncing MAIN to zero.")
                qty = abs(main_signed_qty)
                side = 'Sell' if main_signed_qty > 0 else 'Buy'
                reduce_only = True

                cleanup_tasks = [
                    self.trailing_manager.remove_trailing_stop({'symbol': symbol, 'position_idx': final_position_idx}),
                    self.copy_manager.order_manager.cancel_all_symbol_orders(symbol)
                ]
                await asyncio.gather(*cleanup_tasks)

            action_key = (symbol, final_position_idx, side, round(qty, 6))
            if await self._check_and_record_action(action_key):
                return

            await self._queue_copy_order(
                symbol, entry_price, side, qty, reduce_only, final_position_idx, is_opening, is_full_close,
                kelly_fraction, mode, delta=delta, main_qty=main_signed_qty, target_qty=donor_signed
            )

        except Exception as e:
            logger.error(f"HEDGE_FAIL: Processing single position update for {item.get('symbol')} failed: {e}", exc_info=True)

    async def _process_aggregate_position_update(self, positions: list, mode: PositionMode):
        """
        Self-contained handler for an aggregated position update in ONE-WAY mode.
        Calculates net delta, determines order params, and queues the order.
        """
        try:
            symbol = positions[0].get('symbol')
            donor_net = sum(float(p.get('size', 0)) * (1 if p.get('side') == 'Buy' else -1) for p in positions)

            target_positions = await self.main_client.get_positions(category="linear", symbol=symbol)
            main_pos = next((p for p in target_positions if int(p.get('positionIdx', 0)) == 0), None)
            main_net = (safe_float(main_pos.get('size')) if main_pos else 0.0) * (1 if (main_pos and (main_pos.get('side') or '').strip() == 'Buy') else -1 if main_pos else 0)

            if donor_net == 0 and main_net == 0:
                logger.info(f"NOOP_ZERO: donor_net=0 and MAIN is empty for {symbol} (one-way) - ignoring snapshot.")
                return

            main_balance = await self.main_client.get_balance()
            source_balance = await self.source_client.get_balance()
            main_equity = safe_float(main_balance.get("equity") if isinstance(main_balance, dict) else main_balance)
            src_equity  = safe_float(source_balance.get("equity") if isinstance(source_balance, dict) else source_balance)
            if main_equity <= 0 or src_equity <= 0:
                logger.warning("Skipping copy due to invalid or zero balance.")
                return

            entry_price = float(positions[0].get('markPrice') or positions[0].get('entryPrice') or 0)

            target_size = 0.0
            kelly_fraction = 0.0
            min_qty = await self.copy_manager.order_manager.get_min_order_qty(symbol, price=entry_price)

            if abs(donor_net) > 0:
                proportional = abs(donor_net) * (main_equity / src_equity)
                kelly_result = await self.copy_manager.kelly_calculator.calculate_optimal_size(symbol=symbol, current_size=proportional, price=entry_price, balance=main_equity, source_balance=src_equity)
                target_size = kelly_result.get('recommended_size', proportional)
                kelly_fraction = kelly_result.get('kelly_fraction', 0.0)

                if 0 < target_size < min_qty:
                    target_size = min_qty

            final_target_net = target_size * _sign(donor_net)
            delta = final_target_net - main_net

            if abs(delta) < min_qty and final_target_net != 0:
                logger.info(f"ONEWAY_SKIP_MIN_DELTA: Calculated delta {delta} for {symbol} is below min_qty {min_qty}. No action taken.")
                return

            side = 'Buy' if delta > 0 else 'Sell'
            qty = abs(delta)

            reduce_only = (_sign(delta) != _sign(main_net) and abs(delta) <= abs(main_net))
            is_opening = (main_net == 0 and final_target_net != 0)
            is_full_close = (main_net != 0 and final_target_net == 0)

            # Always resolve main pos_idx AFTER the final side is determined.
            final_position_idx = await self._resolve_main_pos_idx(symbol, side)
            logger.info(f"IDX_MAP_MAIN: symbol={symbol}, main_idx={final_position_idx}, side={side}, source_mode=ONEWAY")

            action_key = (symbol, final_position_idx, side, round(qty, 6))
            if await self._check_and_record_action(action_key):
                return

            await self._queue_copy_order(
                symbol, entry_price, side, qty, reduce_only, final_position_idx, is_opening, is_full_close,
                kelly_fraction, mode, delta=delta, main_qty=main_net, target_qty=final_target_net
            )

        except Exception as e:
            logger.error(f"ONEWAY_FAIL: Processing aggregate update for {positions[0].get('symbol')} failed: {e}", exc_info=True)

    async def _queue_copy_order(self, symbol, entry_price, order_side, order_qty, reduce_only_flag, final_position_idx, is_opening, is_full_close, kelly_fraction, mode, delta: float = 0.0, main_qty: float = 0.0, target_qty: float = 0.0):
        """Helper to create and queue a copy order."""
        reason = "ws_open" if is_opening else "ws_full_close" if is_full_close else "ws_modify"
        signal_type_enum = SignalType.POSITION_OPEN if is_opening else SignalType.POSITION_CLOSE if is_full_close else SignalType.POSITION_MODIFY

        log_main_qty_field = 'main_net' if mode == PositionMode.ONEWAY else 'main_signed'
        log_target_qty_field = 'final_target_net' if mode == PositionMode.ONEWAY else 'target_signed'

        logger.info(
            f"COPY_ACTION ({mode.name}): {reason.upper()} symbol={symbol} "
            f"delta={delta:+.4f} {log_main_qty_field}={main_qty:+.4f} {log_target_qty_field}={target_qty:+.4f} "
            f"-> qty={order_qty:.4f} side={order_side} reduceOnly={reduce_only_flag} position_idx={final_position_idx}"
        )

        synthetic_signal = TradingSignal(
            signal_type=signal_type_enum, symbol=symbol, side=order_side, size=order_qty,
            price=entry_price, timestamp=time.time(),
            metadata={'reason': reason, 'reduceOnly': reduce_only_flag, 'position_idx': final_position_idx}
        )
        order = CopyOrder(
            source_signal=synthetic_signal, target_symbol=symbol, target_side=order_side,
            target_quantity=order_qty, target_price=entry_price, order_strategy=OrderStrategy.MARKET,
            kelly_fraction=kelly_fraction, priority=0 if is_full_close else (1 if is_opening else 2),
            metadata={'reason': reason, 'reduceOnly': reduce_only_flag, 'position_idx': final_position_idx}
        )
        await self.copy_manager.copy_queue.put((order.priority, order.created_at, order))


    
    async def _resolve_main_pos_idx(self, symbol: str, side: str) -> int:
        """
        Return correct Bybit v5 positionIdx for the MAIN account state.
        HEDGE: Buy->1, Sell->2; ONEWAY: 0
        """
        main_positions = await self.main_client.get_positions(category="linear", symbol=symbol) or []
        is_hedge = any(int(p.get("positionIdx", 0)) in (1, 2) for p in main_positions)
        if is_hedge:
            return 1 if (side or "").strip() == "Buy" else 2
        return 0

    async def start_system(self):
        """Идемпотентный запуск Stage-2 без повторного старта Stage-1"""
        if getattr(self, "_started", False):
            logger.info("Stage2.start_system() called again — ignored (idempotent)")
            return
        self._started = True

        try:
            logger.info("🚀 Starting Stage 2 Copy Trading System...")
            self.system_stats['start_time'] = time.time()

            # ⚠️ НЕ стартуем Stage-1 здесь. Он запускается оркестратором.
            # if not getattr(self.base_monitor, "_started", False):
            #     await self.base_monitor.start()

            # Регистрируем обработчики копирования один раз
            if not self._handlers_registered:
                await self.copy_manager.start_copying()
                self._handlers_registered = True

            # Фоновые задачи Stage-2
            monitoring_task = asyncio.create_task(self._stage2_monitoring_loop())
            risk_task       = asyncio.create_task(self._risk_monitoring_loop())
            trailing_task   = asyncio.create_task(self._trailing_stop_loop())

            self.system_active = True
            # Set readiness flags
            self._copy_ready = True
            self.copy_state.main_rest_ok = True
            self.copy_state.limits_checked = True # Assume checked during startup

            # B. Stage2 Executor Flag
            self.trade_executor_connected = self.copy_state.ready and not dry_run
            logger.info(
                f"COPY_EXECUTOR_READY={self.trade_executor_connected} "
                f"(dry_run={dry_run}, main_client.ready={self.copy_state.ready})"
            )

            # --- Deferred Modify Flush ---
            self.copy_connected = True
            logger.info("✅ Copy system is now connected.")
            if self._pending_modify:
                logger.info(f"▶️ MODIFY_FLUSHED: count={len(self._pending_modify)}")
                while self._pending_modify:
                    item = self._pending_modify.popleft()
                    await self.on_position_item(item)
            # -----------------------------

            logger.info("✅ Copy system is ready and accepting signals. State: %s", self.copy_state)

            # Единичное уведомление о старте Stage-2
            await send_telegram_alert(
                "🚀 ЭТАП 2: СИСТЕМА КОПИРОВАНИЯ ЗАПУЩЕНА!\n"
                "✅ Мониторинг источника активен\n"
                "✅ Копирование позиций включено\n"
                "✅ Kelly Criterion активирован\n"
                "✅ Trailing Stop-Loss включен\n"
                "✅ Контроль рисков активен"
            )

            await asyncio.gather(
                monitoring_task, risk_task, trailing_task,
                return_exceptions=True
            )

        except Exception as e:
            logger.error(f"System startup error: {e}")
            try:
                await send_telegram_alert(f"❌ System startup failed: {e}")
            except Exception:
                pass
            raise


    
    async def _stage2_monitoring_loop(self):
        """Основной цикл мониторинга Этапа 2"""
        try:
            while self.system_active:
                current_time = time.time()
                self.system_stats['uptime'] = current_time - self.system_stats['start_time']
                
                # Периодическая отчетность (каждые 15 минут)
                if int(current_time) % 900 == 0:
                    await self._generate_stage2_report()
                
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            logger.debug("Stage 2 monitoring loop cancelled")
        except Exception as e:
            logger.error(f"Stage 2 monitoring loop error: {e}")
    
    async def _risk_monitoring_loop(self):
        """Цикл мониторинга рисков"""
        try:
            while self.system_active:
                current_time = time.time()
                
                # Проверяем баланс и просадку каждую минуту
                if current_time - self.last_balance_check > self.balance_check_interval:
                    await self._check_risk_levels()
                    self.last_balance_check = current_time
                
                await asyncio.sleep(10)
                
        except asyncio.CancelledError:
            logger.debug("Risk monitoring loop cancelled")
        except Exception as e:
            logger.error(f"Risk monitoring loop error: {e}")
    
    async def _trailing_stop_loop(self):
        """Цикл обновления Trailing Stop-Loss"""
        try:
            while self.system_active:
                await self.copy_manager.update_trailing_stops()
                await asyncio.sleep(5)  # Обновляем каждые 5 секунд
                
        except asyncio.CancelledError:
            logger.debug("Trailing stop loop cancelled")
        except Exception as e:
            logger.error(f"Trailing stop loop error: {e}")
    
    async def _check_risk_levels(self):
        """Проверка уровней риска с логированием снимков баланса"""
        try:
            # Пробуем получить детальную информацию о балансе
            try:
                # ✅ Используем специализированный метод клиента (Bybit v5)
                response = await self.base_monitor.main_client.get_wallet_balance("UNIFIED")

                if response and response.get('retCode') == 0:
                    # Парсим детальную информацию
                    for account in response.get('result', {}).get('list', []):
                        for coin_info in account.get('coin', []):
                            if coin_info.get('coin') == 'USDT':
                                # Извлекаем детальные данные
                                free = float(coin_info.get('walletBalance', 0))       # Свободный баланс
                                locked = float(coin_info.get('totalOrderIM', 0))       # Заблокировано в ордерах
                                unrealized_pnl = float(coin_info.get('unrealisedPnl', 0))
                                equity = float(coin_info.get('equity', free + locked + unrealized_pnl))

                                # Логируем снимок баланса
                                balance_logger.log_balance_snapshot(
                                    account_id=2,  # Основной аккаунт
                                    asset='USDT',
                                    free=free,
                                    locked=locked,
                                    equity=equity
                                )

                                # Проверяем drawdown с equity
                                risk_result = await self.drawdown_controller.check_drawdown_limits(equity)

                                if risk_result.get('emergency_stop_required'):
                                    await self._handle_emergency_stop()
                                elif risk_result.get('recovery_mode_required'):
                                    await self._handle_recovery_mode(risk_result['total_drawdown'])

                                return  # Обработали USDT, выходим

                else:
                    # Если Bybit вернул ошибку — даём предупреждение и идём в fallback
                    ret_code = response.get('retCode') if isinstance(response, dict) else 'N/A'
                    ret_msg = response.get('retMsg') if isinstance(response, dict) else 'N/A'
                    logger.warning(f"Wallet-balance retCode={ret_code}, retMsg={ret_msg}; using simple balance fallback")

            except Exception as e:
                logger.warning(f"Failed to get detailed balance, falling back to simple method: {e}")

            # Fallback: используем простой метод если детальный не сработал
            current_balance = await self.base_monitor.main_client.get_balance()

            if current_balance and current_balance > 0:
                # Логируем упрощенный снимок
                balance_logger.log_balance_snapshot(
                    account_id=2,
                    asset='USDT',
                    free=current_balance,
                    locked=0,
                    equity=current_balance
                )

                risk_result = await self.drawdown_controller.check_drawdown_limits(current_balance)

                if risk_result.get('emergency_stop_required'):
                    await self._handle_emergency_stop()
                elif risk_result.get('recovery_mode_required'):
                    await self._handle_recovery_mode(risk_result['total_drawdown'])

        except Exception as e:
            logger.error(f"Risk levels check error: {e}")

    
    async def _handle_emergency_stop(self):
        """Обработка экстренной остановки"""
        try:
            logger.critical("EMERGENCY STOP TRIGGERED - Stopping all trading")
            
            # Останавливаем копирование
            self.copy_enabled = False
            await self.copy_manager.stop_copying()
            
            # Пытаемся закрыть все открытые позиции
            await self._close_all_positions()
            
            self.system_stats['emergency_stops'] += 1
            
        except Exception as e:
            logger.error(f"Emergency stop handling error: {e}")
    
    async def _handle_recovery_mode(self, drawdown: float):
        """Обработка режима восстановления"""
        try:
            logger.warning(f"RECOVERY MODE ACTIVATED - Drawdown: {drawdown:.2%}")
            
            # Получаем параметры восстановления
            recovery_params = self.drawdown_controller.calculate_recovery_parameters(drawdown)
            
            # Применяем ограничения (здесь можно расширить логику)
            logger.info(f"Recovery parameters: {recovery_params}")
            
            self.system_stats['recovery_activations'] += 1
            
        except Exception as e:
            logger.error(f"Recovery mode handling error: {e}")
    
    async def _close_all_positions(self):
        """Закрытие всех открытых позиций (экстренная мера)"""
        try:
            active_positions = await self.base_monitor.main_client.get_positions()
            
            for position in active_positions:
                symbol = position.get('symbol')
                side = position.get('side')
                size = safe_float(position.get('size', 0))
                
                if size > 0:
                    close_side = "Sell" if side == "Buy" else "Buy"
                    
                    # Market ордер для быстрого закрытия
                    order_data = {
                        "category": "linear",
                        "symbol": symbol,
                        "side": close_side,
                        "orderType": "Market",
                        "qty": str(size),
                        "timeInForce": "IOC"
                    }
                    
                    result = await self.base_monitor.main_client._make_request_with_retry(
                        "POST", "order/create", data=order_data
                    )
                    
                    if result and result.get('retCode') == 0:
                        logger.info(f"Emergency close: {symbol} {close_side} {size}")
                    else:
                        logger.error(f"Emergency close failed: {symbol}")
            
        except Exception as e:
            logger.error(f"Emergency position closing error: {e}")
    
    async def _generate_stage2_report(self):
        """Генерация отчета Этапа 2 (устойчиво к сетевым сбоям и без await в спорном месте)"""
        try:
            # 1) Собираем статистику
            copy_stats = self.copy_manager.get_copy_stats()
            risk_stats = self.drawdown_controller.get_risk_stats()
            execution_stats = self.copy_manager.order_manager.get_execution_stats()

            # 2) Баланс может быть None при сетевых сбоях -> не падаем на форматировании
            current_balance = await self.base_monitor.main_client.get_balance()
            if current_balance is None:
                balance_str = "N/A"
            else:
                balance_str = f"${current_balance:.2f}"

            uptime_hours = float(self.system_stats.get('uptime', 0.0)) / 3600.0

            # 3) Формируем отчёт (безопасно)
            report = (
                "📊 ОТЧЕТ ЭТАПА 2: СИСТЕМА КОПИРОВАНИЯ\n"
                + "=" * 50 + "\n"
                + f"Время работы: {uptime_hours:.1f}ч\n"
                + f"Текущий баланс: {balance_str}\n\n"

                "🔄 КОПИРОВАНИЕ ПОЗИЦИЙ:\n"
                f"Режим: {copy_stats.get('copy_mode')}\n"
                f"Позиций скопировано: {copy_stats.get('positions_copied', 0)}\n"
                f"Позиций закрыто: {copy_stats.get('positions_closed', 0)}\n"
                f"Активных позиций: {copy_stats.get('active_positions', 0)}\n"
                f"Успешность: {float(copy_stats.get('copy_success_rate', 0.0)):.1f}%\n"
                f"Средняя задержка: {float(copy_stats.get('avg_sync_delay', 0.0)):.2f}s\n"
                f"Общий объем: ${float(copy_stats.get('total_volume_copied', 0.0)):.2f}\n\n"

                "🛡️ УПРАВЛЕНИЕ РИСКАМИ:\n"
                f"Макс. общая просадка: {float(risk_stats.get('max_total_drawdown', 0.0)):.2%}\n"
                f"Макс. дневная просадка: {float(risk_stats.get('max_daily_drawdown', 0.0)):.2%}\n"
                f"Экстренных остановок: {int(risk_stats.get('emergency_stops_triggered', 0))}\n"
                f"Режим восстановления: {'Активен' if risk_stats.get('recovery_mode_active') else 'Неактивен'}\n\n"

                "📈 TRAILING STOP-LOSS:\n"
                f"Активных стопов: {int(copy_stats.get('trailing_stops_active', 0))}\n\n"

                "🎯 KELLY CRITERION:\n"
                f"Корректировок Kelly: {int(copy_stats.get('kelly_adjustments', 0))}\n\n"

                "⚡ ИСПОЛНЕНИЕ ОРДЕРОВ:\n"
            )

            # 4) Топ-5 символов, безопасно берём поля
            for symbol, stats in list(execution_stats.items())[:5]:
                succ = int(stats.get('success', 0))
                fail = int(stats.get('failed', 0))
                total_orders = succ + fail
                success_rate = (succ / total_orders * 100.0) if total_orders > 0 else 0.0
                avg_time = float(stats.get('avg_time', 0.0))
                report += f"{symbol}: {success_rate:.1f}% ({avg_time:.2f}s)\n"

            logger.info(report)

            # 5) Отправляем в Telegram не чаще 1 раза в час и в «минуту после часа»
            now = time.time()
            if (now - getattr(self, "_last_stage2_report_ts", 0.0) >= 3600.0) and (int(now) % 3600 < 60):
                # Не блокируем текущий поток: отправка в фоне
                try:
                    asyncio.create_task(send_telegram_alert(report))
                except RuntimeError:
                    # если цикл ещё не поднят/другой контекст — берём текущий
                    asyncio.get_event_loop().create_task(send_telegram_alert(report))
                self._last_stage2_report_ts = now

        except Exception as e:
            logger.error(f"Stage 2 report generation error: {e}")

    
    async def stop_system(self):
        """Остановка системы копирования"""
        try:
            logger.info("Stopping Stage 2 Copy Trading System...")
            
            self.system_active = False
            self.copy_enabled = False
            
            # Останавливаем копирование
            await self.copy_manager.stop_copying()
            
            # Останавливаем базовую систему
            self.base_monitor.should_stop = True
            
            await send_telegram_alert("🛑 ЭТАП 2: Система копирования остановлена")
            
            logger.info("Stage 2 system stopped successfully")
            
        except Exception as e:
            logger.error(f"System stop error: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Получение полного статуса системы"""
        try:
            copy_stats = self.copy_manager.get_copy_stats()
            risk_stats = self.drawdown_controller.get_risk_stats()
            base_stats = {
                'websocket_status': self.base_monitor.websocket_manager.status.value,
                'signal_processor_active': self.base_monitor.signal_processor.processing_active
            }
            
            return {
                'system_active': self.system_active,
                'copy_enabled': self.copy_enabled,
                'uptime': self.system_stats['uptime'],
                'copy_stats': copy_stats,
                'risk_stats': risk_stats,
                'base_system': base_stats,
                'system_stats': self.system_stats
            }
            
        except Exception as e:
            logger.error(f"System status error: {e}")
            return {'error': str(e)}

# ================================
# ТОЧКА ВХОДА ЭТАПА 2
# ================================

async def main_stage2():
    """Главная функция запуска Этапа 2"""
    try:
        print("🚀 Запуск Этапа 2: Система Копирования Торговли")
        print("=" * 80)
        print("КОМПОНЕНТЫ ЭТАПА 2:")
        print("✅ Kelly Criterion для оптимального размера позиций")
        print("✅ Adaptive Order Manager для исполнения ордеров")
        print("✅ Dynamic Trailing Stop-Loss для защиты прибыли")
        print("✅ Position Copy Manager для копирования сделок")
        print("✅ Drawdown Controller для управления рисками")
        print("✅ Интеграция с системой мониторинга Этапа 1")
        print("=" * 80)
        
        # Создаем и запускаем систему копирования
        copy_system = Stage2CopyTradingSystem()
        await copy_system.start_system()
        
    except KeyboardInterrupt:
        logger.info("Stage 2 system stopped by user")
        print("\n🛑 Этап 2 остановлен пользователем")
    except Exception as e:
        logger.error(f"Critical Stage 2 error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        await send_telegram_alert(f"Critical Stage 2 error: {e}")
        print(f"\n💥 Критическая ошибка Этапа 2: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main_stage2())
    except KeyboardInterrupt:
        print("\n🛑 Программа завершена пользователем")
    except Exception as e:
        print(f"\n💥 Ошибка запуска: {e}")
        print("Убедитесь что:")
        print("1. Файл enhanced_trading_system_final_fixed.py находится в той же директории")
        print("2. Все зависимости установлены: pip install scipy")
        print("3. API ключи корректно настроены в конфигурационных файлах")
