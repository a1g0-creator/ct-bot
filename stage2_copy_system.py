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
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict, namedtuple
import math
import statistics
from scipy.optimize import minimize_scalar
from scipy.stats import norm
import traceback
import uuid
from decimal import Decimal
import aiohttp

from sys_events_logger import sys_logger
from orders_logger import orders_logger, OrderStatus
from risk_events_logger import risk_events_logger, RiskEventType
from balance_snapshots_logger import balance_logger

logger = logging.getLogger(__name__)

# Импортируем все компоненты из Этапа 1
try:
    from enhanced_trading_system_final_fixed import (
        FinalTradingMonitor, ProductionSignalProcessor, TradingSignal, SignalType,
        EnhancedBybitClient, FinalFixedWebSocketManager, 
        safe_float, send_telegram_alert, logger, MAIN_API_KEY, MAIN_API_SECRET,
        MAIN_API_URL, SOURCE_API_KEY, SOURCE_API_SECRET, SOURCE_API_URL
    )
    print("✅ Успешно импортированы все компоненты Этапа 1")
except ImportError as e:
    print(f"❌ Не удалось импортировать компоненты Этапа 1: {e}")
    print("Убедитесь, что файл enhanced_trading_system_final_fixed.py находится в той же директории")
    raise

from risk_state_classes import RiskDataContext, HealthSupervisor


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
    'kelly_max_mult': 2.0                   # и не увеличивает более чем в 2 раза
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

# Trailing Stop-Loss настройки
TRAILING_CONFIG = {
    'enabled': True,
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

async def format_quantity_for_symbol_live(bybit_client, symbol: str, quantity: float, price: float = None) -> str:
    """
    Форматирование количества на основе реальных биржевых фильтров Bybit.
    - Если не дотягиваем до минимальной стоимости (min_notional) — поднимаем количество ВВЕРХ.
    - Во всех остальных случаях округляем ВНИЗ к шагу, чтобы не завышать риск.
    """
    try:
        filters = await bybit_client.get_symbol_filters(symbol, category="linear")
        # Фоллбеки на случай, если биржа не вернула значения
        qty_step     = float(filters.get("qty_step") or 0.001)
        min_qty      = float(filters.get("min_qty") or 0.001)
        min_notional = float(filters.get("min_notional") or 10.0)

        decimals = 0
        # оценим количество знаков после запятой по шагу
        if qty_step < 1:
            s = f"{qty_step:.12f}".rstrip('0')
            decimals = len(s.split('.')[-1]) if '.' in s else 0

        need_bump_to_min = False
        if price and price > 0:
            min_qty_by_value = float(min_notional) / float(price)
            effective_min    = max(min_qty, min_qty_by_value)
            if quantity < effective_min:
                quantity = effective_min
                need_bump_to_min = True

        steps = quantity / qty_step
        rounded_qty = (math.ceil(steps) * qty_step) if need_bump_to_min else (math.floor(steps) * qty_step)
        if rounded_qty < min_qty:
            rounded_qty = min_qty

        formatted = f"{rounded_qty:.{decimals}f}".rstrip('0').rstrip('.')
        logger.info(
            f"[live-format] {symbol}: qty_in={quantity:.8f} step={qty_step} "
            f"min_qty={min_qty} min_notional={min_notional} → qty_out={formatted}"
        )
        return formatted or "0"
    except Exception as e:
        logger.warning(f"format_quantity_for_symbol_live fallback due to: {e}")
        # Фоллбек: используем прежнюю статическую функцию, чтобы не падать
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
        Сбрасывает кэши и обновляет параметры мгновенно.

        Args:
            cfg: Словарь с параметрами Kelly (из KELLY_CONFIG)
        """
        try:
            # Сохраняем старую конфигурацию для логирования
            old_config = {
                'kelly_multiplier': self.kelly_multiplier,
                'max_position_size': self.max_position_size,
                'min_position_size': self.min_position_size,
                'max_drawdown_threshold': self.max_drawdown_threshold,
                'lookback_window': getattr(self.trade_history, 'maxlen', 100)
            }
        
            # Обновляем множители и лимиты
            self.kelly_multiplier = float(cfg.get('conservative_factor', self.kelly_multiplier))
            self.max_position_size = float(cfg.get('max_kelly_fraction', self.max_position_size))
            self.min_position_size = float(cfg.get('min_position_size', self.min_position_size))
            self.default_position_size = max(self.min_position_size, 0.001)
    
            # Обновляем пороги
            self.max_drawdown_threshold = float(cfg.get('max_drawdown_threshold', 0.15))
    
            # Обновляем размер окна истории если изменился
            new_lookback = int(cfg.get('lookback_window', 100))
            cache_cleared = False
            if new_lookback != getattr(self.trade_history, 'maxlen', new_lookback):
                from collections import deque
                # Сохраняем существующие данные при изменении окна
                self.trade_history = deque(list(self.trade_history), maxlen=new_lookback)
                cache_cleared = True
        
            # Сбрасываем кэш для мгновенного эффекта
            self.kelly_cache.clear()
        
            # Логируем изменение конфигурации
            sys_logger.log_event(
                "INFO",
                "KellyCalculator",
                "Kelly configuration updated",
                {
                    "old_config": old_config,
                    "new_config": {
                        'kelly_multiplier': self.kelly_multiplier,
                        'max_position_size': self.max_position_size,
                        'min_position_size': self.min_position_size,
                        'max_drawdown_threshold': self.max_drawdown_threshold,
                        'lookback_window': new_lookback
                    },
                    "cache_cleared": cache_cleared
                }
            )
    
            logger.info(f"Kelly config applied: multiplier={self.kelly_multiplier}, "
                       f"max_size={self.max_position_size}, lookback={new_lookback}")
                   
        except Exception as e:
            sys_logger.log_error(
                "KellyCalculator",
                f"Failed to apply config: {str(e)}",
                {"config": cfg, "error": str(e)}
            )
            logger.error(f"Failed to apply Kelly config: {e}")
            raise

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
            avg_win = np.mean(wins)
            avg_loss = np.mean(losses)
        
            # Дополнительные метрики
            profit_factor = (avg_win * len(wins)) / (avg_loss * len(losses))
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
        try:
            cumulative = np.cumprod([1 + r for r in returns])
            running_max = np.maximum.accumulate(cumulative)
            drawdown = (cumulative - running_max) / running_max
            return abs(np.min(drawdown))
        except:
            return 0.0
    
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Расчет коэффициента Шарпа"""
        try:
            if len(returns) < 2:
                return 0.0
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            if std_return == 0:
                return 0.0
            return mean_return / std_return
        except:
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

        logger.info(f"DynamicTrailingStopManager initialized with config: {self.cfg}")

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
        self.default_distance_percent = self.cfg.get("activation_pct")
        self.min_trail_step = self.cfg.get("step_pct")
        self.max_distance_percent = self.cfg.get("max_pct")
        self.aggressive_mode = (self.cfg.get("mode") == "aggressive")
        self.atr_period = self.cfg.get("atr_period")
        self.atr_multiplier = self.cfg.get("atr_multiplier")

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
        Returns a snapshot of the current config, built strictly from self.cfg
        with sane defaults to prevent errors.
        """
        return {
            "enabled":        bool(self.cfg.get("enabled", True)),
            "mode":           self.cfg.get("mode", "conservative"),
            "activation_pct": float(self.cfg.get("activation_pct", 0.02)),
            "step_pct":       float(self.cfg.get("step_pct", 0.003)),
            "max_pct":        float(self.cfg.get("max_pct", 0.05)),
            "atr_period":     int(self.cfg.get("atr_period", 14)),
            "atr_multiplier": float(self.cfg.get("atr_multiplier", 1.5)),
        }

    async def create_or_update_trailing_stop(self, symbol: str, side: str, position_qty: float, position_value: float, entry_price: float):
        """
        Creates or updates a native trailing stop order on the exchange using the current config.
        """
        if not self.cfg.get('enabled'):
            logger.info(f"TS_SKIP: Trailing stops are disabled globally. Skipping for {symbol}.")
            return

        if position_value < self.cfg.get('min_notional_for_ts', 0):
            logger.info(f"TS_SKIP: Position value ${position_value:.2f} is below min_notional_for_ts of ${self.cfg.get('min_notional_for_ts', 0)}. Skipping.")
            return

        await self.order_manager.cancel_all_symbol_orders(symbol, "Stop")
        logger.info(f"TS_CANCEL_BEFORE_CREATE: Canceled existing stop orders for {symbol} before creating new one.")

        distance_value = 0.0
        use_atr = self.cfg.get('use_atr', self.cfg.get('mode') == 'atr') # Compatibility

        if use_atr:
            try:
                kline_data = await self.main_client.get_kline(symbol, '15m', self.cfg.get('atr_period', 14))
                if kline_data and len(kline_data) >= self.cfg.get('atr_period', 14):
                    highs = [float(k[2]) for k in kline_data]
                    lows = [float(k[3]) for k in kline_data]
                    closes = [float(k[4]) for k in kline_data]
                    true_ranges = [highs[0] - lows[0]]
                    for i in range(1, len(kline_data)):
                        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
                        true_ranges.append(tr)
                    atr_val = sum(true_ranges) / len(true_ranges)
                    distance_value = atr_val * self.cfg.get('atr_multiplier', 1.5)
                    logger.info(f"TS_ATR_CALC: ATR for {symbol} is {atr_val:.4f}. Distance: ${distance_value:.4f}")
                else:
                    logger.warning(f"Not enough kline data for ATR on {symbol}, falling back to percentage.")
                    distance_value = entry_price * self.cfg.get('activation_pct', 0.015)
            except Exception as e:
                logger.warning(f"Could not fetch kline for ATR, falling back. Error: {e}")
                distance_value = entry_price * self.cfg.get('activation_pct', 0.015)
        else:
            distance_value = entry_price * self.cfg.get('activation_pct', 0.015)
            logger.info(f"TS_FIXED_CALC: Using fixed distance for {symbol}. Pct: {self.cfg.get('activation_pct', 0.015):.2%}, Resulting Dist: ${distance_value:.4f}")

        max_dist_val = entry_price * self.cfg.get('max_pct', 0.1)
        distance_value = min(distance_value, max_dist_val)
        
        ts_side = 'Sell' if side == 'Buy' else 'Buy'
        
        order_result = await self.order_manager.place_trailing_stop(
            symbol=symbol, side=ts_side, qty=position_qty, trailing_distance=distance_value
        )

        if order_result and order_result.get('success'):
            logger.info(
                f"TS_CREATE: style={'atr' if use_atr else 'fixed'} "
                f"use_atr={use_atr} dist=${distance_value:.4f} "
                f"stop_price=N/A symbol={symbol} qty={position_qty}"
            )
            orders_logger.log_order(
                account_id=2, symbol=symbol, side=ts_side, qty=position_qty, price=None,
                status=OrderStatus.PLACED.value, order_type='TRAILING_STOP',
                reason='ts_create', exchange_order_id=order_result.get('order_id')
            )
        else:
            logger.error(f"TS_CREATE_FAILED: Failed to create trailing stop for {symbol}. Reason: {order_result.get('error')}")

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
                "orderLinkId": link_id
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

    async def place_trailing_stop(self, symbol: str, side: str, qty: float, trailing_distance: float) -> Dict[str, Any]:
        """Places a native trailing stop order."""
        try:
            link_id = f"ts:{symbol}:{int(time.time()*1000)}"
            distance_str = f"{trailing_distance:.4f}".rstrip('0').rstrip('.')

            order_data = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": str(qty),
                "tpslMode": "Partial",
                "trailingStop": distance_str,
                "orderLinkId": link_id,
                "reduceOnly": True,
            }

            self.logger.info(f"Placing TRAILING_STOP: {symbol} {side} qty={qty} distance=${distance_str}")
            result = await self.main_client._make_request_with_retry("POST", "order/create", data=order_data)

            if result and result.get("retCode") == 0:
                order_id = result.get("result", {}).get("orderId")
                self.logger.info(f"Trailing stop placed successfully: {order_id} link={link_id}")
                return {"success": True, "order_id": order_id, "order_link_id": link_id}

            err = (result or {}).get("retMsg") or "No response"
            self.logger.error(f"Trailing stop order failed: {err}")
            return {"success": False, "error": err}

        except Exception as e:
            self.logger.exception(f"Trailing stop placement error: {e}")
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
            self.system_stats['successful_copies'] = self.system_stats.get('successful_copies', 0) + 1
            if hasattr(self, 'copy_stats'):
                self.copy_stats['positions_copied'] = self.copy_stats.get('positions_copied', 0) + 1
            return True


    async def _execute_copy_order(self, copy_order: CopyOrder, queue_timestamp: float):
        """Выполнение ордера копирования с логированием в orders_log"""
        execution_id = str(uuid.uuid4())[:8]
    
        try:
            sync_delay = time.time() - queue_timestamp
        
            # Логируем начало выполнения в sys_events
            sys_logger.log_event(
                "INFO",
                "PositionCopyManager",
                f"Executing copy order for {copy_order.target_symbol}",
                {
                    "execution_id": execution_id,
                    "symbol": copy_order.target_symbol,
                    "side": copy_order.target_side,
                    "quantity": float(copy_order.target_quantity),
                    "kelly_fraction": float(copy_order.kelly_fraction),
                    "sync_delay_ms": round(sync_delay * 1000, 2),
                    "order_strategy": copy_order.order_strategy.value,
                    "retry_count": copy_order.retry_count
                }
            )
        
            logger.info(f"Executing copy order for {copy_order.target_symbol} (delay: {sync_delay:.3f}s)")
        
            # НОВОЕ: Логируем начало размещения ордера в orders_log
            orders_logger.log_order(
                account_id=2,  # Основной аккаунт
                symbol=copy_order.target_symbol,
                side=copy_order.target_side,
                qty=copy_order.target_quantity,
                status=OrderStatus.PENDING.value,
                exchange_order_id=execution_id,
                attempt=copy_order.retry_count + 1
            )

            # 1) Размещаем ордер (AdaptiveOrderManager уже возвращает order_link_id)
            start_time = time.time()
            result = await self.order_manager.place_adaptive_order(copy_order)
            execution_time = time.time() - start_time
            latency_ms = int(execution_time * 1000)

            if result.get('success'):
                # НОВОЕ: Обновляем статус ордера на FILLED/PLACED
                orders_logger.update_order_status(
                    exchange_order_id=execution_id,
                    new_status=OrderStatus.FILLED.value if result.get('filled') else OrderStatus.PLACED.value,
                    latency_ms=latency_ms
                )
            
                # Логируем успешное выполнение в sys_events
                sys_logger.log_event(
                    "INFO",
                    "PositionCopyManager",
                    f"Order executed successfully: {copy_order.target_symbol}",
                    {
                        "execution_id": execution_id,
                        "order_id": result.get('order_id'),
                        "order_link_id": result.get('order_link_id'),
                        "symbol": copy_order.target_symbol,
                        "executed_qty": float(result.get('executed_qty', copy_order.target_quantity)),
                        "avg_price": float(result.get('avg_price', 0)),
                        "execution_time_ms": round(execution_time * 1000, 2),
                        "status": "success"
                    }
                )
            
                # 2) СЧЁТЧИКИ — только через идемпотентную точку
                accounted = await self._account_success_once(result, copy_order)
                if not accounted:
                    logger.warning(
                        f"Duplicate success ignored for link={result.get('order_link_id') or result.get('order_id')}"
                    )

                # 3) Позиционный трекинг
                if copy_order.source_signal.signal_type == SignalType.POSITION_OPEN:
                    await self._setup_position_tracking(copy_order, result)
                elif copy_order.source_signal.signal_type == SignalType.POSITION_CLOSE:
                    await self._cleanup_position_tracking(copy_order)

                # 4) Kelly — запись результата (если есть средняя цена)
                if 'avg_price' in result:
                    pnl_estimate = self._estimate_trade_pnl(copy_order, result)
                    self.kelly_calculator.add_trade_result(
                        copy_order.target_symbol,
                        pnl_estimate,
                        {'copy_order': copy_order, 'result': result}
                    )

                # 5) Уведомление
                await send_telegram_alert(
                    f"✅ Position copied: {copy_order.target_symbol} {copy_order.target_side} "
                    f"{copy_order.target_quantity:.6f} (Kelly: {copy_order.kelly_fraction:.3f})"
                )

            else:
                # НОВОЕ: Логируем неудачу в orders_log
                orders_logger.update_order_status(
                    exchange_order_id=execution_id,
                    new_status=OrderStatus.FAILED.value,
                    latency_ms=latency_ms,
                    reason=result.get('error', 'Unknown error')
                )
            
                # Логируем неудачное выполнение в sys_events
                sys_logger.log_warning(
                    "PositionCopyManager",
                    f"Order execution failed: {copy_order.target_symbol}",
                    {
                        "execution_id": execution_id,
                        "symbol": copy_order.target_symbol,
                        "error": result.get('error', 'Unknown error'),
                        "retry_count": copy_order.retry_count,
                        "execution_time_ms": round(execution_time * 1000, 2),
                        "status": "failed"
                    }
                )
            
                # Неуспех — считаем только ошибки/задержки (без инкремента успехов)
                self._update_copy_stats(False, sync_delay, copy_order)

                # Повторные попытки
                if copy_order.retry_count < COPY_CONFIG['order_retry_attempts']:
                    copy_order.retry_count += 1
                    await asyncio.sleep(COPY_CONFIG['order_retry_delay'] * copy_order.retry_count)
                    await self.copy_queue.put((copy_order.priority, time.time(), copy_order))
                    logger.warning(
                        f"Retrying copy order for {copy_order.target_symbol} (attempt {copy_order.retry_count})"
                    )
                else:
                    logger.error(
                        f"Copy order failed after {COPY_CONFIG['order_retry_attempts']} attempts: "
                        f"{copy_order.target_symbol}"
                    )
                    await send_telegram_alert(
                        f"❌ Copy order failed: {copy_order.target_symbol} {copy_order.target_side} "
                        f"{copy_order.target_quantity:.6f}"
                    )

        except Exception as e:
            # НОВОЕ: В случае исключения также обновляем статус в orders_log
            try:
                orders_logger.update_order_status(
                    exchange_order_id=execution_id,
                    new_status=OrderStatus.FAILED.value,
                    reason=str(e)
                )
            except:
                pass  # Не прерываем основную обработку ошибки
            
            sys_logger.log_error(
                "PositionCopyManager",
                f"Copy order execution error: {str(e)}",
                {
                    "execution_id": execution_id,
                    "symbol": copy_order.target_symbol,
                    "error_type": type(e).__name__
                }
            )
            logger.error(f"Copy order execution error: {e}")



    
    async def _setup_position_tracking(self, copy_order: CopyOrder, execution_result: Dict[str, Any]):
        """Настройка отслеживания позиции после открытия"""
        try:
            symbol = copy_order.target_symbol
            
            # Сохраняем информацию о позиции
            position_data = {
                'symbol': symbol,
                'side': copy_order.target_side,
                'quantity': copy_order.target_quantity,
                'entry_price': execution_result.get('avg_price', copy_order.target_price),
                'kelly_fraction': copy_order.kelly_fraction,
                'open_time': time.time(),
                'order_id': execution_result.get('order_id')
            }
            
            self.active_positions[symbol] = position_data
            
            # Создаем trailing stop
            market_conditions = await self.order_manager.get_market_analysis(symbol)
            current_price = execution_result.get('avg_price', copy_order.target_price)
            position_value = copy_order.target_quantity * current_price
            
            trailing_stop = self.trailing_manager.create_trailing_stop(
                symbol, copy_order.target_side, current_price, position_value, market_conditions
            )
            
            logger.info(f"Position tracking setup for {symbol}: "
                       f"entry=${current_price:.6f}, trailing_stop=${trailing_stop.stop_price:.6f}")
            
        except Exception as e:
            logger.error(f"Position tracking setup error: {e}")
    
    async def _cleanup_position_tracking(self, copy_order: CopyOrder):
        """Очистка отслеживания позиции после закрытия с записью в БД"""
        try:
            symbol = copy_order.target_symbol
        
            if symbol in self.active_positions:
                position = self.active_positions[symbol]
            
                # Рассчитываем P&L для Kelly анализа
                entry_price = position['entry_price']
                exit_price = copy_order.target_price
                quantity = position['quantity']
            
                if position['side'].lower() == 'buy':
                    pnl = (exit_price - entry_price) * quantity
                else:
                    pnl = (entry_price - exit_price) * quantity
            
                # ===== НОВЫЙ КОД: ЗАПИСЬ В БД =====
                try:
                    api_payload = {
                        "account_id": 1,
                        "symbol": symbol,
                        "side": position['side'],
                        "qty": str(quantity),
                        "position_idx": position.get('position_idx', 0),
                        "entry_price": str(entry_price),
                        "exit_price": str(exit_price),
                        "realized_pnl": str(pnl),
                        "leverage": position.get('leverage'),
                        "margin_mode": position.get('margin_mode', 'Cross'),
                        "opened_at": position.get('opened_at'),
                        "closed_at": time.time() * 1000,
                        "raw_open": position.get('raw_data', {}),
                        "raw_close": {
                            "exit_price": str(exit_price),
                            "pnl": str(pnl),
                            "cleanup_time": time.time()
                        }
                    }
                
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            "http://localhost:8080/api/positions/close",
                            json=api_payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as response:
                            if response.status == 200:
                                logger.info(f"Position cleanup recorded in DB for {symbol}")
                            else:
                                logger.warning(f"Failed to record cleanup in DB: {response.status}")
                            
                except Exception as e:
                    logger.warning(f"Non-critical: Failed to record position cleanup in DB: {e}")
            
                # ===== КОНЕЦ НОВОГО КОДА =====
            
                # Добавляем результат в Kelly анализ
                self.kelly_calculator.add_trade_result(
                    symbol, pnl, 
                    {'position': position, 'exit_price': exit_price}
                )
            
                # Удаляем из активных позиций
                del self.active_positions[symbol]
            
                # Удаляем trailing stop
                self.trailing_manager.remove_trailing_stop(symbol)
            
                logger.info(f"Position tracking cleanup for {symbol}: P&L=${pnl:.2f}")
        
        except Exception as e:
            logger.error(f"Position tracking cleanup error: {e}")
    
    def _estimate_trade_pnl(self, copy_order: CopyOrder, execution_result: Dict[str, Any]) -> float:
        """Оценка P&L сделки (приблизительная для открывающих позиций)"""
        try:
            # Для открывающих позиций возвращаем 0 (P&L будет рассчитан при закрытии)
            if copy_order.source_signal.signal_type == SignalType.POSITION_OPEN:
                return 0.0
            
            # Для закрывающих позиций рассчитываем реальный P&L
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
            
            # Обновляем среднюю задержку синхронизации
            if self.copy_stats['avg_sync_delay'] == 0:
                self.copy_stats['avg_sync_delay'] = sync_delay
            else:
                self.copy_stats['avg_sync_delay'] = (
                    self.copy_stats['avg_sync_delay'] * 0.9 + sync_delay * 0.1
                )
            
            # Обновляем коэффициент успешности
            total_attempts = self.copy_stats['positions_copied'] + self.copy_stats['positions_closed']
            if total_attempts > 0:
                self.copy_stats['copy_success_rate'] = total_attempts / (total_attempts + 1) * 100
            
        except Exception as e:
            logger.error(f"Stats update error: {e}")
    
    async def update_trailing_stops(self):
        """Обновление всех trailing stops с валидацией и обработкой ошибок"""
        try:
            # Собираем уникальные символы из активных позиций
            symbols_to_check = set()

            for position_key, position_data in list(self.active_positions.items()):
                symbol = None
                if isinstance(position_data, dict):
                    symbol = position_data.get('symbol')

                if not symbol:
                    key_str = str(position_key)

                    # Убираем суффиксы направления в разных вариантах
                    for suffix in ('*Buy', '*Sell', '_Buy', '_Sell', ':Buy', ':Sell', 'Buy', 'Sell'):
                        if key_str.endswith(suffix):
                            key_str = key_str[:-len(suffix)]
                            break

                    # Если в конце ключа был таймстамп вида _1234567890 — уберём его
                    if '_' in key_str:
                        left, right = key_str.rsplit('_', 1)
                        if right.isdigit():
                            key_str = left

                    symbol = key_str or None

                    # Сохраняем символ в данные позиции, чтобы в следующий раз не парсить ключ
                    if symbol and isinstance(position_data, dict):
                        position_data['symbol'] = symbol

                if symbol:
                    symbols_to_check.add(symbol)

            if not symbols_to_check:
                logger.debug("No symbols to update trailing stops")
                return

            # Получаем цены (последовательно, без изменения логики ретраев)
            prices_cache = {}
            for symbol in symbols_to_check:
                try:
                    ticker_params = {
                        "category": "linear",
                        "symbol": symbol,
                    }
                    ticker_result = await self.main_client._make_request_with_retry(
                        "GET", "market/tickers", ticker_params
                    )
                    if ticker_result and ticker_result.get('retCode') == 0:
                        lst = ticker_result.get('result', {}).get('list', [])
                        if lst:
                            current_price = safe_float(lst[0].get('lastPrice', 0))
                            if current_price > 0:
                                prices_cache[symbol] = current_price
                            else:
                                logger.warning(f"Invalid price for {symbol}: {current_price}")
                    else:
                        logger.warning(f"Failed to get ticker for {symbol}: {ticker_result}")
                except Exception as e:
                    logger.error(f"Error fetching price for {symbol}: {e}")

            # Обновляем/проверяем trailing stops только для валидных цен
            for symbol, current_price in prices_cache.items():
                try:
                    self.trailing_manager.update_trailing_stop(symbol, current_price)
                    if self.trailing_manager.check_stop_triggered(symbol, current_price):
                        await self._execute_trailing_stop_exit(symbol, current_price)
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
            
            # Создаем экстренный ордер закрытия
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
            
            # Немедленно исполняем
            result = await self.order_manager.place_adaptive_order(copy_order)
            
            if result.get('success'):
                await self._cleanup_position_tracking(copy_order)
                
                await send_telegram_alert(
                    f"🛑 Trailing stop triggered: {symbol} closed at ${trigger_price:.6f}"
                )
                
                logger.info(f"Trailing stop executed for {symbol} at ${trigger_price:.6f}")
            else:
                logger.error(f"Trailing stop execution failed for {symbol}")
                await send_telegram_alert(
                    f"❌ Trailing stop execution failed for {symbol}"
                )
            
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
        self.copy_manager = PositionCopyManager(
            self.source_client,
            self.main_client,
            self.trailing_config
        )
        self.drawdown_controller = DrawdownController()

        # 5) Состояние системы
        self.system_active = False
        self.copy_enabled = True
        self.demo_mode = False
        self.last_balance_check = 0
        self.balance_check_interval = 60  # Проверяем баланс каждую минуту

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
        self._idempotency_window_sec = 5  # 5-second window
        self._idempotency_lock = asyncio.Lock()

        # ВАЖНО: не регистрируем обработчики здесь, чтобы не плодить дубли.
        # Регистрация произойдёт один раз в start_system() через
        # await self.copy_manager.start_copying()

        logger.info(
            "Stage 2 Copy Trading System initialized (monitor=%s)",
            type(self.base_monitor).__name__,
        )
    
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

    async def on_position_item(self, item: dict):
        """
        Refactored handler for all position updates (OPEN, MODIFY, CLOSE).
        Calculates the final target size based on all rules and then executes the required delta.
        """
        try:
            # 1. NORMALIZE & VALIDATE INPUT
            if not isinstance(item, dict):
                logger.warning(f"on_position_item received non-dict data: {type(item)}")
                return

            symbol = item.get('symbol')
            if not symbol:
                logger.warning("on_position_item: received item with no symbol.")
                return

            pos_idx = int(item.get('positionIdx', 0))
            donor_side = (item.get('side') or "").strip()
            donor_size = float(item.get('size', 0) or 0)
            entry_price = float(item.get('entryPrice') or item.get('markPrice') or 0)

            # Idempotency check
            action_key = (symbol, pos_idx)
            async with self._idempotency_lock:
                if time.time() - self._recent_actions.get(action_key, 0) < self._idempotency_window_sec:
                    logger.debug(f"IDEMPOTENCY_SKIP: Action for {action_key} recently processed.")
                    return
                self._recent_actions[action_key] = time.time()

            logger.info(f"COPY_HANDLER_IN: symbol={symbol} side='{donor_side}' size={donor_size} entry={entry_price} idx={pos_idx}")

            # 2. FETCH CURRENT STATE (BALANCES & TARGET POSITION)
            main_balance = await self.main_client.get_balance()
            source_balance = await self.source_client.get_balance()

            if not all([main_balance, source_balance]) or main_balance <= 0 or source_balance <= 0:
                logger.error(f"Invalid balances for calculation: MAIN={main_balance}, SOURCE={source_balance}. Skipping.")
                return

            target_positions = await self.main_client.get_positions(category="linear", symbol=symbol)
            target_pos = next((p for p in target_positions if p.get('symbol') == symbol and int(p.get('positionIdx', 0)) == pos_idx), None)
            current_target_size = safe_float(target_pos.get('size')) if target_pos else 0.0
            current_target_side = (target_pos.get('side') or "").strip() if target_pos else ""

            # 3. CALCULATE FINAL TARGET SIZE (The core logic)
            final_target_size = 0
            kelly_fraction = 0.0

            if donor_size > 0 and donor_side:
                # a. Proportional size based on balance ratio
                proportional_size = donor_size * (main_balance / source_balance)

                # b. Kelly Criterion adjustment
                kelly_result = await self.copy_manager.kelly_calculator.calculate_optimal_size(
                    symbol=symbol, current_size=proportional_size, price=entry_price,
                    balance=main_balance, source_balance=source_balance
                )
                kelly_recommended_size = kelly_result.get('recommended_size', proportional_size)
                kelly_fraction = kelly_result.get('kelly_fraction', 0.0)

                # c. Apply master copy_config limits
                final_target_size = kelly_recommended_size * COPY_CONFIG['default_copy_ratio']
                final_target_size = max(COPY_CONFIG['min_copy_size'], final_target_size)
                final_target_size = min(COPY_CONFIG['max_copy_size'], final_target_size)

                logger.info(
                    f"TARGET_CALC: Donor Size={donor_size:.4f} -> Proportional={proportional_size:.4f} "
                    f"-> Kelly Rec={kelly_recommended_size:.4f} -> Final Target={final_target_size:.4f}"
                )

            # 4. ANTI-CLOSURE & PIN-TO-MIN LOGIC
            min_qty = await self.copy_manager.order_manager.get_min_order_qty(symbol, price=entry_price)

            if donor_size > 0 and 0 < final_target_size < min_qty:
                logger.info(f"PIN_TO_MIN: target {final_target_size:.4f} < min_qty {min_qty} for {symbol} -> use min_qty")
                final_target_size = min_qty

            # 5. CALCULATE DELTA AND EXECUTE
            is_flip = current_target_side and donor_side and current_target_side != donor_side

            if is_flip:
                logger.info(f"FLIP DETECTED: {symbol} from {current_target_side} to {donor_side}")

                # Step 1: Close existing position
                close_side = 'Sell' if current_target_side == 'Buy' else 'Buy'
                logger.info(f"FLIP_CLOSE: Closing {current_target_size} of {symbol} with a {close_side} order.")
                close_order = CopyOrder(
                    source_signal=None,
                    target_symbol=symbol,
                    target_side=close_side,
                    target_quantity=current_target_size,
                    target_price=entry_price,
                    order_strategy=OrderStrategy.MARKET,
                    kelly_fraction=0, # Not applicable for closing
                    priority=0,
                    metadata={'reason': 'ws_flip_close', 'reduceOnly': True}
                )
                close_result = await self.copy_manager.order_manager.place_adaptive_order(close_order)

                if not close_result.get('success'):
                    logger.error(f"FLIP_CLOSE FAILED for {symbol}. Aborting flip.")
                    return

                logger.info(f"FLIP_CLOSE for {symbol} successful.")
                await asyncio.sleep(1) # Small delay to allow position to update

                # Step 2: Open new position
                logger.info(f"FLIP_OPEN: Opening {final_target_size} of {symbol} with a {donor_side} order.")
                open_order = CopyOrder(
                    source_signal=None,
                    target_symbol=symbol,
                    target_side=donor_side,
                    target_quantity=final_target_size,
                    target_price=entry_price,
                    order_strategy=OrderStrategy.MARKET,
                    kelly_fraction=kelly_fraction,
                    priority=1,
                    metadata={'reason': 'ws_flip_open', 'reduceOnly': False}
                )
                order_result = await self.copy_manager.order_manager.place_adaptive_order(open_order)

            else:
                # Original delta logic for non-flip scenarios
                size_delta = final_target_size - current_target_size

                if abs(size_delta) < min_qty:
                    logger.info(f"HYSTERESIS_SKIP: Delta {size_delta:.6f} is smaller than min_qty {min_qty} for {symbol}. No action needed.")
                    return

                order_side = 'Buy' if size_delta > 0 else 'Sell'
                order_qty = abs(size_delta)

                is_opening = current_target_size == 0 and final_target_size > 0
                is_closing = final_target_size == 0 and current_target_size > 0
                is_modifying = not is_opening and not is_closing

                reason = "ws_open" if is_opening else "ws_close" if is_closing else "ws_modify"

                logger.info(f"COPY_ACTION: {reason.upper()} for {symbol} | Delta: {size_delta:.4f} | Qty: {order_qty:.4f} | Side: {order_side}")

                order = CopyOrder(
                    source_signal=None,
                    target_symbol=symbol,
                    target_side=order_side,
                    target_quantity=order_qty,
                    target_price=entry_price,
                    order_strategy=OrderStrategy.MARKET,
                    kelly_fraction=kelly_fraction,
                    priority=1 if is_opening or is_closing else 0,
                    metadata={'reason': reason, 'reduceOnly': size_delta < 0}
                )

                order_result = await self.copy_manager.order_manager.place_adaptive_order(order)

            # 6. POST-ORDER ACTIONS (Trailing Stops Lifecycle)
            if order_result and order_result.get('success'):
                ts_manager = self.copy_manager.trailing_manager
                ts_cfg = ts_manager.cfg

                if not ts_cfg.get('enabled'):
                    logger.info(f"TS_LIFECYCLE: TS is disabled, no action for {symbol}.")
                    return

                # For modifications, the position value is based on the new total size
                avg_price = order_result.get('price', entry_price)
                position_value = final_target_size * avg_price

                # Opening a new position
                if is_opening:
                    logger.info(f"TS_LIFECYCLE (OPEN): Creating TS for new {symbol} position.")
                    await ts_manager.create_or_update_trailing_stop(symbol, donor_side, final_target_size, position_value, avg_price)

                # Adding to an existing position
                elif is_modifying and size_delta > 0:
                    if ts_cfg.get('update_on_add'):
                        logger.info(f"TS_LIFECYCLE (ADD): Updating TS for {symbol} position.")
                        await ts_manager.create_or_update_trailing_stop(symbol, donor_side, final_target_size, position_value, avg_price)
                    else:
                        logger.info(f"TS_LIFECYCLE (ADD): update_on_add is false, TS for {symbol} not updated.")

                # Reducing an existing position
                elif is_modifying and size_delta < 0:
                    logger.info(f"TS_LIFECYCLE (REDUCE): Modifying TS for {symbol} position.")
                    if ts_cfg.get('rearm_on_modify'):
                        await ts_manager.create_or_update_trailing_stop(symbol, donor_side, final_target_size, position_value, avg_price)
                    else:
                        await self.copy_manager.order_manager.cancel_all_symbol_orders(symbol, "Stop")
                        logger.info(f"TS_LIFECYCLE (REDUCE): rearm_on_modify is false, existing TS for {symbol} cancelled.")

                # Closing a position
                elif is_closing:
                    logger.info(f"TS_LIFECYCLE (CLOSE): Cancelling TS for closing {symbol} position.")
                    await self.copy_manager.order_manager.cancel_all_symbol_orders(symbol, "Stop")

            # Handle TS on FLIP
            elif is_flip:
                ts_manager = self.copy_manager.trailing_manager
                logger.info(f"TS_LIFECYCLE (FLIP): Handling TS for {symbol} flip.")
                # Cancel before close is already handled by create_or_update_trailing_stop
                # Create after open
                if order_result and order_result.get('success'):
                    avg_price = order_result.get('price', entry_price)
                    position_value = final_target_size * avg_price
                    await ts_manager.create_or_update_trailing_stop(symbol, donor_side, final_target_size, position_value, avg_price)


        except Exception as e:
            logger.error(f"on_position_item failed: {e}", exc_info=True)


    async def process_copy_signal(self, signal: TradingSignal):
        """
        Compatibility wrapper: старый pipeline -> новый on_position_item().
        Не удаляем метод, чтобы сохранить обратную совместимость.
        """
        try:
            handler = getattr(self, "on_position_item", None)
            if handler is None:
                logger.warning("process_copy_signal: on_position_item() is missing; signal ignored: %s", signal)
                return

            # Нормализация: собрать минимальный item, который ожидает on_position_item(...)
            side = getattr(signal, "side", "Buy")
            side = side.name if hasattr(side, "name") else str(side)

            item = {
                "symbol": getattr(signal, "symbol", "") or "",
                "side": side or "",
                "size": str(getattr(signal, "size", 0) or 0),
                "entryPrice": str(getattr(signal, "price", 0) or 0),
            "positionIdx": int(signal.metadata.get("position_idx", 0) if hasattr(signal, 'metadata') and signal.metadata else 0),
            }

            return await handler(item)

        except Exception as e:
            logger.error("process_copy_signal failed: %s", e, exc_info=True)


    async def process_copy_signal_legacy(self, signal: TradingSignal):
        """
        Обработчик сигналов копирования для Stage2CopyTradingSystem.
        Добавлен реальный bypass для /force_copy (metadata.force_copy=True),
        глобальный gate от контроллера риска применяется только если НЕ force_copy.
        Делегирует выполнение в PositionCopyManager.
        """
        try:
            # ---- извлекаем основные поля и флаг форс-режима
            symbol = getattr(signal, 'symbol', None)
            side   = getattr(signal, 'side', None)
            size   = float(getattr(signal, 'size', 0) or 0)
            is_force = bool((getattr(signal, "metadata", {}) or {}).get("force_copy", False))

            logger.info("🔄 COPY SIGNAL RECEIVED: %s %s %s", symbol, side, size)

            # 0) Проверяем готовность системы
            if not self.system_active:
                logger.warning("Copy system not active - ignoring signal")
                await send_telegram_alert(
                    f"⚠️ **СИСТЕМА КОПИРОВАНИЯ НЕАКТИВНА**\n"
                    f"Пропущен сигнал: {symbol} {side}\n"
                    "Активируйте систему для копирования"
                )
                return

            if not self.copy_enabled:
                logger.warning("Copy disabled - ignoring signal")
                await send_telegram_alert(
                    f"⚠️ **КОПИРОВАНИЕ ОТКЛЮЧЕНО**\n"
                    f"Пропущен сигнал: {symbol} {side}\n"
                    "Включите копирование в настройках"
                )
                return

            # 1) Глобальный gate от контроллера риска (только для открытия позиций)
            #    Применяем ТОЛЬКО если НЕ force_copy.
            if signal.signal_type == SignalType.POSITION_OPEN and not is_force:
                if hasattr(self, 'drawdown_controller') and hasattr(self.drawdown_controller, 'can_open_positions'):
                    if not self.drawdown_controller.can_open_positions():
                        mode = getattr(getattr(self.drawdown_controller, 'supervisor', None), 'mode', None)
                        mode_name = getattr(mode, 'value', str(mode)) if mode is not None else 'unknown'
                    
                        # НОВОЕ: Логируем в risk_events
                        risk_events_logger.log_position_rejection(
                            account_id=2,
                            symbol=symbol,
                            requested_size=size,
                            reason=f"Blocked by risk manager: {mode_name} mode"
                        )
                    
                        logger.warning("Cannot open new positions: system in %s mode", mode_name)
                        await send_telegram_alert(
                            "🛡️ **РИСК-МЕНЕДЖМЕНТ: БЛОКИРОВКА ОТКРЫТИЯ**\n"
                            f"Сигнал: {symbol} {side}\n"
                            f"Режим: {mode_name}"
                        )
                        return

            # Если пришёл принудительный сигнал — явно логируем и уведомляем
            if is_force:
                logger.warning("FORCED COPY OVERRIDE: proceeding to open %s %s %s", symbol, side, size)
                try:
                    await send_telegram_alert(
                        f"⚡️ **FORCED COPY**: исполняем {symbol} {side} {size} (обход DD-гейта)"
                    )
                except Exception:
                    pass

            # 2) Подробная проверка лимитов риска (твоя существующая логика)
            #    Блокируем только если НЕ force_copy.
            if hasattr(self, 'drawdown_controller'):
                try:
                    risk_check = await self.drawdown_controller.check_risk_limits()
                    if signal.signal_type == SignalType.POSITION_OPEN and not is_force \
                        and not risk_check.get('can_open_position', True):
                    
                        # НОВОЕ: Логируем в risk_events
                        risk_events_logger.log_position_rejection(
                            account_id=2,
                            symbol=symbol,
                            requested_size=size,
                            reason=risk_check.get('reason', 'Risk limits exceeded')
                        )
                    
                        logger.warning("Risk limits prevent copying: %s", risk_check.get('reason', 'Unknown'))
                        await send_telegram_alert(
                            "🛡️ **РИСК-МЕНЕДЖМЕНТ БЛОКИРОВАЛ КОПИРОВАНИЕ**\n"
                            f"Сигнал: {symbol} {side}\n"
                            f"Причина: {risk_check.get('reason', 'Risk limits')}"
                        )
                        return
                except Exception as e:
                    logger.warning(f"Risk check error: {e}")
                    # продолжаем с предупреждением

            # 3) Делегируем в copy_manager, где находятся нужные методы
            if signal.signal_type == SignalType.POSITION_OPEN:
                await self._handle_position_open_for_copy(signal)
            elif signal.signal_type == SignalType.POSITION_CLOSE:
                await self._handle_position_close_for_copy(signal)
            elif signal.signal_type == SignalType.POSITION_MODIFY:
                await self._handle_position_modify_for_copy(signal)
            else:
                logger.warning(f"Unknown signal type: {signal.signal_type}")
                return

            # 4) Обновляем статистику
            self.system_stats['total_signals_processed'] += 1
            self.system_stats['successful_copies'] += 1

            # 5) Отправляем уведомление об обработке
            forced_note = " (FORCED)" if is_force else ""
            await send_telegram_alert(
                "✅ **СИГНАЛ ОБРАБОТАН**\n"
                f"Action: {signal.signal_type.value}{forced_note}\n"
                f"Symbol: {symbol}\n"
                f"Size: {size:.6f}\n"
                "Status: Делегировано в PositionCopyManager"
            )

        except Exception as e:
            logger.error(f"Copy signal processing error: {e}")
            self.system_stats['failed_copies'] += 1
            await send_telegram_alert(f"❌ **ОШИБКА КОПИРОВАНИЯ**: {str(e)}")

    async def _handle_position_open_for_copy(self, signal):
        """Обработка открытия позиции для копирования - ПОЛНАЯ ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            logger.info(f"🟢 COPYING POSITION OPEN: {signal.symbol}")
    
            # Получаем балансы для расчета размера
            source_balance = 0.0
            main_balance = 0.0
    
            try:
                source_balance = await self.base_monitor.source_client.get_balance()
                main_balance = await self.base_monitor.main_client.get_balance()
            except Exception as e:
                logger.error(f"Balance retrieval error: {e}")
                await send_telegram_alert(f"❌ **ОШИБКА ПОЛУЧЕНИЯ БАЛАНСОВ**: {str(e)}")
                return
        
            if source_balance <= 0 or main_balance <= 0:
                logger.error("Invalid balances for copying")
                await send_telegram_alert("❌ **НЕДОПУСТИМЫЕ БАЛАНСЫ ДЛЯ КОПИРОВАНИЯ**")
                return
    
            # Рассчитываем размер позиции
            try:
                # Процент от баланса источника
                source_position_value = signal.size * signal.price
                source_percentage = source_position_value / source_balance
        
                # Применяем к основному балансу
                target_value = main_balance * source_percentage
                target_size = target_value / signal.price
        
                # ✅ KELLY CRITERION: Если доступен, оптимизируем размер
                # ✅ KELLY CRITERION: ограничиваем влияние в безопасном коридоре
                if hasattr(self, 'kelly_calculator'):
                    try:
                        base_target_size = target_size  # запомним пропорциональную копию
                        kelly_data = await self.kelly_calculator.calculate_optimal_size(
                            signal.symbol, target_size, signal.price
                        )
                        if kelly_data['recommended_size'] > 0:
                            ksize = kelly_data['recommended_size']
                            k_min = COPY_CONFIG.get('kelly_min_mult', 0.5)
                            k_max = COPY_CONFIG.get('kelly_max_mult', 2.0)
                            target_size = min(max(ksize, base_target_size * k_min), base_target_size * k_max)
                            logger.info(f"Kelly adjustment: {ksize:.6f} -> bounded {target_size:.6f} "
                                        f"(bounds {k_min}x..{k_max}x of {base_target_size:.6f})")
                    except Exception as e:
                        logger.warning(f"Kelly calculation error: {e}")

        
                # Ограничиваем размер позиции (безопасность)
                max_position_value = main_balance * 0.1  # Максимум 10% от баланса
                if target_value > max_position_value:
                    target_size = max_position_value / signal.price
                    logger.info(f"Position size limited for safety: {target_size:.6f}")
            
                logger.info(f"📊 COPY CALCULATION: Source ${source_position_value:.2f} ({source_percentage:.2%}) -> Target ${target_size * signal.price:.2f}")
        
                # ✅ РЕАЛЬНОЕ РАЗМЕЩЕНИЕ ОРДЕРА
                if hasattr(self, 'copy_manager') and hasattr(self.copy_manager, 'order_manager'):
                    try:
                        # В демо режиме только логируем БЕЗ обновления статистики
                        if getattr(self, 'demo_mode', True):
                            logger.info(f"🔄 DEMO MODE: Would place order {signal.symbol} {signal.side} {target_size:.6f}")
                        
                            # Сохраняем в активные позиции для отслеживания
                            if hasattr(self.copy_manager, 'active_positions'):
                                self.copy_manager.active_positions[signal.symbol] = {
                                    'symbol': signal.symbol,
                                    'side': signal.side,
                                    'size': target_size,
                                    'entry_price': signal.price,
                                    'timestamp': time.time(),
                                    'source_signal_id': getattr(signal, 'id', None)
                                }
                        
                            # НЕ обновляем статистику в DEMO режиме
                        
                        else:
                            # LIVE режим - реальное размещение
                            from stage2_copy_system import CopyOrder, OrderStrategy
                        
                            copy_order = CopyOrder(
                                source_signal=signal,
                                target_symbol=signal.symbol,
                                target_side=signal.side,
                                target_quantity=target_size,
                                target_price=signal.price,
                                kelly_fraction=target_size / signal.size if signal.size > 0 else 1.0,
                                priority=3,
                                order_strategy=OrderStrategy.MARKET
                            )
                        
                            order_result = await self.copy_manager.order_manager.place_adaptive_order(copy_order)
                    
                            if order_result.get('success'):
                                logger.info(f"✅ Order placed successfully: {order_result.get('order_id', 'N/A')}")
                            
                                # Сохраняем в активные позиции
                                if hasattr(self.copy_manager, 'active_positions'):
                                    self.copy_manager.active_positions[signal.symbol] = {
                                        'symbol': signal.symbol,
                                        'side': signal.side,
                                        'size': target_size,
                                        'entry_price': signal.price,
                                        'timestamp': time.time(),
                                        'source_signal_id': getattr(signal, 'id', None),
                                        'order_id': order_result.get('order_id')
                                    }
                            
                                # Обновляем статистику ТОЛЬКО в LIVE режиме
                                if hasattr(self.copy_manager, 'copy_stats'):
                                    ##self.copy_manager.copy_stats['positions_copied'] += 1
                                    self.copy_manager.copy_stats['total_volume_copied'] += target_size * signal.price
                            
                                # Создаем trailing stop
                                if hasattr(self.copy_manager, 'trailing_manager'):
                                    try:
                                        market_conditions = await self.copy_manager.order_manager.get_market_analysis(signal.symbol)
                                    
                                        trailing_stop = self.copy_manager.trailing_manager.create_trailing_stop(
                                            symbol=signal.symbol,
                                            side=signal.side,
                                            current_price=signal.price,
                                            position_size=target_size * signal.price,
                                            market_conditions=market_conditions
                                        )
                                    
                                        if trailing_stop:
                                            logger.info(f"✅ Trailing stop created for {signal.symbol}: "
                                                      f"stop_price={trailing_stop.stop_price:.2f}, "
                                                      f"distance={trailing_stop.distance_percent:.2%}")
                                        
                                    except Exception as e:
                                        logger.error(f"Trailing stop creation error: {e}")
                            
                            else:
                                error_msg = order_result.get('error', 'Unknown error')
                                raise Exception(f"Order failed: {error_msg}")
                        
                    except Exception as e:
                        logger.error(f"Order placement error: {e}")
                        await send_telegram_alert(f"❌ **ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА**: {str(e)}")
                        return
        
                # Успешное уведомление
                await send_telegram_alert(
                    f"✅ **ПОЗИЦИЯ СКОПИРОВАНА**\n"
                    f"Symbol: {signal.symbol}\n"
                    f"Side: {signal.side}\n"
                    f"Original: {signal.size:.6f} (${source_position_value:.2f})\n"
                    f"Copy: {target_size:.6f} (${target_size * signal.price:.2f})\n"
                    f"Ratio: {source_percentage:.2%}\n"
                    f"Mode: {'DEMO' if getattr(self, 'demo_mode', True) else 'LIVE'}"
                )
        
                # Обновляем статистику успеха
                ##self.system_stats['successful_copies'] += 1
        
            except Exception as e:
                logger.error(f"Position size calculation error: {e}")
                await send_telegram_alert(f"❌ **ОШИБКА РАСЧЕТА РАЗМЕРА**: {str(e)}")
        
        except Exception as e:
            logger.error(f"Position copy open error: {e}")
            await send_telegram_alert(f"❌ **ОШИБКА КОПИРОВАНИЯ ОТКРЫТИЯ**: {str(e)}")

    async def _handle_position_close_for_copy(self, signal):
        """Обработка закрытия позиции для копирования с записью в БД"""
        try:
            logger.info(f"🔴 COPYING POSITION CLOSE: {signal.symbol}")
        
            # Логирование входящего сигнала
            logger.info(f"[DEBUG] Incoming signal data: symbol={signal.symbol}, price={signal.price}, timestamp={signal.timestamp}")
        
            # ИСПРАВЛЕНИЕ: используем правильный ключ с учетом side
            position_key = f"{signal.symbol}_{signal.side}"
            position_to_close = None
            if hasattr(self.copy_manager, 'active_positions'):
                # Сначала пробуем с составным ключом
                position_to_close = self.copy_manager.active_positions.get(position_key)
                if not position_to_close:
                    # Fallback на простой ключ для обратной совместимости
                    position_to_close = self.copy_manager.active_positions.get(signal.symbol)
            
                # Детальное логирование найденной позиции
                if position_to_close:
                    logger.info(f"[DEBUG] Found position_to_close: {position_to_close}")
                    logger.info(f"[DEBUG] Position fields: side={position_to_close.get('side')}, size={position_to_close.get('size')}, entry_price={position_to_close.get('entry_price')}, leverage={position_to_close.get('leverage')}")
                else:
                    logger.warning(f"[DEBUG] No position found in active_positions for {signal.symbol} (tried keys: {position_key}, {signal.symbol})")

            if not position_to_close:
                logger.warning(f"No active position found to close: {signal.symbol}")
                await send_telegram_alert(
                    f"⚠️ **НЕТ ПОЗИЦИИ ДЛЯ ЗАКРЫТИЯ**\n"
                    f"Symbol: {signal.symbol}\n"
                    "Возможно позиция уже была закрыта или не была скопирована"
                )
                return

            # ✅ РЕАЛЬНОЕ ЗАКРЫТИЕ ПОЗИЦИИ
            if hasattr(self, 'copy_manager') and hasattr(self.copy_manager, 'order_manager'):
                try:
                    if getattr(self, 'demo_mode', True):
                        logger.info(f"🔄 DEMO MODE: Would close position {signal.symbol}")
            
                        # Удаляем из активных позиций (пробуем оба ключа)
                        if hasattr(self.copy_manager, 'active_positions'):
                            if position_key in self.copy_manager.active_positions:
                                del self.copy_manager.active_positions[position_key]
                            elif signal.symbol in self.copy_manager.active_positions:
                                del self.copy_manager.active_positions[signal.symbol]
                
                        # Обновляем статистику
                        if hasattr(self.copy_manager, 'copy_stats'):
                            self.copy_manager.copy_stats['positions_closed'] += 1
                
                    else:
                        # Реальное закрытие
                        close_side = 'Sell' if position_to_close['side'] == 'Buy' else 'Buy'
                        close_result = await self.copy_manager.order_manager.place_order(
                            symbol=signal.symbol,
                            side=close_side,
                            size=position_to_close['size'],
                            order_type='Market'
                        )
                
                        if close_result['success']:
                            logger.info(f"✅ Position closed successfully: {close_result['order_id']}")
                            # Удаляем из активных позиций после успешного закрытия
                            if hasattr(self.copy_manager, 'active_positions'):
                                if position_key in self.copy_manager.active_positions:
                                    del self.copy_manager.active_positions[position_key]
                                elif signal.symbol in self.copy_manager.active_positions:
                                    del self.copy_manager.active_positions[signal.symbol]
                        else:
                            raise Exception(f"Close failed: {close_result['error']}")
                
                except Exception as e:
                    logger.error(f"Position close error: {e}")
                    await send_telegram_alert(f"❌ **ОШИБКА ЗАКРЫТИЯ ПОЗИЦИИ**: {str(e)}")
                    return
    
            # ===== ЗАПИСЬ В БД ЧЕРЕЗ WEB API С ПОЛУЧЕНИЕМ ПОЛНЫХ ДАННЫХ =====
            try:
                import aiohttp
                import json
                import psycopg2
            
                # Получаем полные данные из БД
                logger.info(f"[DEBUG] Fetching position data from DB for {signal.symbol}")
            
                entry_price = None
                leverage = None
                margin_mode = 'Cross'
                raw_open = {}
            
                try:
                    conn = psycopg2.connect("postgresql://sa:1Qaz2wsX@127.0.0.1:5432/trading_bot")
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT entry_price, leverage, margin_mode, raw::text, qty, side
                        FROM positions_open 
                        WHERE account_id = 1 AND symbol = %s 
                        LIMIT 1
                    """, (signal.symbol,))
                    db_position = cur.fetchone()
                
                    if db_position:
                        logger.info(f"[DEBUG] Found DB position: entry_price={db_position[0]}, leverage={db_position[1]}, margin_mode={db_position[2]}")
                        entry_price = str(db_position[0]) if db_position[0] else None
                        leverage = int(db_position[1]) if db_position[1] else None
                        margin_mode = db_position[2] or 'Cross'
                        if db_position[3]:
                            raw_open = json.loads(db_position[3])
                            logger.info(f"[DEBUG] Raw open data keys: {list(raw_open.keys())[:10]}")
                    else:
                        logger.warning(f"[DEBUG] No position found in DB for {signal.symbol}")
                    
                    cur.close()
                    conn.close()
                except Exception as db_error:
                    logger.error(f"[DEBUG] DB fetch error: {db_error}")
                    # Используем данные из position_to_close как fallback
                    entry_price = str(position_to_close.get('entry_price', 0)) if position_to_close.get('entry_price') else None
                    leverage = position_to_close.get('leverage')
                    margin_mode = position_to_close.get('margin_mode', 'Cross')
            
                # Подготовка данных для API
                api_payload = {
                    "account_id": 1,
                    "symbol": signal.symbol,
                    "side": position_to_close.get('side', signal.side),  # Используем side из позиции или сигнала
                    "qty": str(position_to_close.get('size', 0)),
                    "position_idx": 0,
                    "entry_price": entry_price,
                    "exit_price": str(signal.price) if signal.price else None,
                    "mark_price": str(signal.price) if signal.price else None,
                    "leverage": leverage,
                    "margin_mode": margin_mode,
                    "liq_price": str(position_to_close.get('liq_price')) if position_to_close.get('liq_price') else None,
                    "realized_pnl": None,  # Будет рассчитан в API
                    "fees": None,
                    "exchange_position_id": position_to_close.get('exchange_position_id'),
                    "opened_at": position_to_close.get('opened_at') or position_to_close.get('timestamp'),
                    "closed_at": signal.timestamp * 1000 if signal.timestamp else None,
                    "raw_open": raw_open or position_to_close.get('raw_open', {}),
                    "raw_close": {
                        "symbol": signal.symbol,
                        "side": position_to_close.get('side', signal.side),
                        "markPrice": str(signal.price) if signal.price else None,
                        "size": "0",
                        "closedTime": str(int(signal.timestamp * 1000)) if signal.timestamp else None,
                        "signal_metadata": signal.metadata if hasattr(signal, 'metadata') else {}
                    }
                }
            
                logger.info(f"[DEBUG] API payload prepared: entry_price={api_payload['entry_price']}, exit_price={api_payload['exit_price']}, leverage={api_payload['leverage']}")
        
                # Асинхронный вызов Web API
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "http://localhost:8080/api/positions/close",
                        json=api_payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            if result.get("success"):
                                logger.info(f"✅ Position close recorded in DB: position_id={result.get('position_id')}")
                            else:
                                logger.warning(f"DB recording issue: {result.get('message')}")
                        else:
                            error_text = await response.text()
                            logger.error(f"Failed to record position close in DB: {response.status} - {error_text}")
                    
            except asyncio.TimeoutError:
                logger.warning("Timeout recording position close to DB (non-critical)")
            except Exception as e:
                logger.error(f"Error recording position close to DB: {e}")
                import traceback
                logger.error(f"[DEBUG] Full traceback: {traceback.format_exc()}")
    
            # ===== КОНЕЦ КОДА ЗАПИСИ =====
    
            await send_telegram_alert(
                f"🔄 **ПОЗИЦИЯ ЗАКРЫТА**\n"
                f"Symbol: {signal.symbol}\n"
                f"Original Side: {position_to_close.get('side', 'Unknown')}\n"
                f"Size: {position_to_close.get('size', 0):.6f}\n"
                f"Mode: {'DEMO' if getattr(self, 'demo_mode', True) else 'LIVE'}"
            )

            # Обновляем статистику успеха
            ##self.system_stats['successful_copies'] += 1

        except Exception as e:
            logger.error(f"Position copy close error: {e}")
            await send_telegram_alert(f"❌ **ОШИБКА КОПИРОВАНИЯ ЗАКРЫТИЯ**: {str(e)}")

    async def _handle_position_modify_for_copy(self, signal):
        """Обработка изменения позиции для копирования"""
        try:
            logger.info(f"🟡 COPYING POSITION MODIFY: {signal.symbol}")

            # Находим активную позицию
            current_position = None
            if hasattr(self.copy_manager, 'active_positions'):
                current_position = self.copy_manager.active_positions.get(signal.symbol)

            if not current_position:
                logger.warning(f"No position to modify: {signal.symbol}")
                await send_telegram_alert(
                    f"⚠️ **НЕТ ПОЗИЦИИ ДЛЯ ИЗМЕНЕНИЯ**\n"
                    f"Symbol: {signal.symbol}\n"
                    "Позиция может быть еще не скопирована"
                )
                return

            # Рассчитываем новый размер (аналогично открытию)
            try:
                source_balance = await self.base_monitor.source_client.get_balance()
                main_balance   = await self.base_monitor.main_client.get_balance()

                source_position_value = signal.size * signal.price
                source_percentage     = source_position_value / source_balance
                target_value          = main_balance * source_percentage
                new_target_size       = target_value / signal.price

                old_size = float(current_position.get('size', 0))
                logger.info(f"🟡 MODIFY CALCULATION: Current {old_size:.6f} -> New {new_target_size:.6f}")

                # DEMO: просто обновляем локальные данные и НИЧЕГО НЕ считаем как "скопировано"
                if getattr(self, 'demo_mode', True):
                    current_position['size'] = new_target_size
                    current_position['last_modified'] = time.time()
                    result_success = False  # нет реального ордера
                else:
                    # TODO: здесь должна быть реальная логика доведения позиции до new_target_size
                    # пример: self.copy_manager.order_manager.place_adaptive_order(...)
                    logger.info(f"🔄 LIVE MODE: would modify position to {new_target_size:.6f}")
                    result_success = False  # пока реальный modify не реализован

            except Exception as e:
                logger.error(f"Position modify calculation error: {e}")
                await send_telegram_alert(f"❌ **ОШИБКА РАСЧЕТА ИЗМЕНЕНИЯ**: {str(e)}")
                return

            await send_telegram_alert(
                f"🔄 **ПОЗИЦИЯ ИЗМЕНЕНА**\n"
                f"Symbol: {signal.symbol}\n"
                f"Old Size: {old_size:.6f}\n"
                f"New Size: {new_target_size:.6f}\n"
                f"Mode: {'DEMO' if getattr(self, 'demo_mode', True) else 'LIVE'}"
            )

            # ✔ Считаем "скопировано" ТОЛЬКО при реальном успешном изменении (когда появится ордер)
            ##if result_success:
                ##self.system_stats['successful_copies'] += 1

        except Exception as e:
            logger.error(f"Position copy modify error: {e}")
            await send_telegram_alert(f"❌ **ОШИБКА КОПИРОВАНИЯ ИЗМЕНЕНИЯ**: {str(e)}")


    async def handle_position_signal(self, position_data):
        """
        ИСПРАВЛЕННЫЙ обработчик сигналов позиций для системы копирования
        """
        try:
            # ✅ ДОБАВЛЕНО: Детальное логирование для диагностики
            logger.info(f"🔄 Stage2 received position signal: {position_data}")
        
            if not self.system_active or not self.copy_enabled:
                logger.info(f"Stage2 not ready: active={self.system_active}, enabled={self.copy_enabled}")
                return
    
            symbol = position_data.get('symbol', '')
            current_size = float(position_data.get('size', '0'))
            side = position_data.get('side', '')
            price = float(position_data.get('markPrice', '0'))
    
            logger.info(f"📊 Position signal details: {symbol} {side} size={current_size} price={price}")
        
            # Игнорируем тестовые сигналы
            if symbol == 'TEST':
                return
    
            # Проверяем изменения позиции
            position_key = f"{symbol}_{side}"
    
            # ✅ ИСПРАВЛЕНО: Используем copy_manager.active_positions вместо self.active_positions
            if position_key in self.copy_manager.active_positions:
                # Существующая позиция - проверяем изменения
                prev_size = self.copy_manager.active_positions[position_key].get('size', 0)
                size_delta = current_size - prev_size
            
                logger.info(f"📈 Position change: {symbol} {prev_size:.6f} -> {current_size:.6f} (delta: {size_delta:.6f})")
        
                if abs(size_delta) > 0.001:  # Минимальное изменение 0.001
                    if size_delta > 0:
                        # Увеличение позиции
                        signal = TradingSignal(
                            signal_type=SignalType.POSITION_MODIFY,
                            symbol=symbol,
                            side=side,
                            size=abs(size_delta),
                            price=price,
                            timestamp=time.time(),
                            metadata={
                                'action': 'increase',
                                'prev_size': prev_size,
                                'new_size': current_size,
                                'source': 'websocket'
                            }
                        )
                        logger.info(f"🟡 Generated MODIFY signal for increase: {symbol}")
                        await self.process_copy_signal(signal)
                
                    elif current_size == 0:
                        # Закрытие позиции
                        signal = TradingSignal(
                            signal_type=SignalType.POSITION_CLOSE,
                            symbol=symbol,
                            side=side,
                            size=prev_size,
                            price=price,
                            timestamp=time.time(),
                            metadata={
                                'action': 'close',
                                'prev_size': prev_size,
                                'source': 'websocket'
                            }
                        )
                        logger.info(f"🔴 Generated CLOSE signal: {symbol}")
                        await self.process_copy_signal(signal)
                
                    else:
                        # Уменьшение позиции
                        signal = TradingSignal(
                            signal_type=SignalType.POSITION_MODIFY,
                            symbol=symbol,
                            side=side,
                            size=abs(size_delta),
                            price=price,
                            timestamp=time.time(),
                            metadata={
                                'action': 'decrease',
                                'prev_size': prev_size,
                                'new_size': current_size,
                                'source': 'websocket'
                            }
                        )
                        logger.info(f"🟡 Generated MODIFY signal for decrease: {symbol}")
                        await self.process_copy_signal(signal)
            else:
                # Новая позиция
                if current_size > 0:
                    signal = TradingSignal(
                        signal_type=SignalType.POSITION_OPEN,
                        symbol=symbol,
                        side=side,
                        size=current_size,
                        price=price,
                        timestamp=time.time(),
                        metadata={
                            'action': 'open',
                            'source': 'websocket'
                        }
                    )
                    logger.info(f"🟢 Generated OPEN signal: {symbol}")
                    await self.process_copy_signal(signal)
    
            # ✅ ИСПРАВЛЕНО: Обновляем состояние позиций в правильном месте
            if current_size > 0:
                self.copy_manager.active_positions[position_key] = {
                    'symbol': symbol,
                    'side': side,
                    'size': current_size,
                    'price': price,
                    'last_update': time.time()
                }
                logger.debug(f"Updated position state: {position_key}")
            elif position_key in self.copy_manager.active_positions:
                del self.copy_manager.active_positions[position_key]
                logger.debug(f"Removed position state: {position_key}")
    
            # Обновляем статистику
            self.system_stats['total_signals_processed'] += 1
    
        except Exception as e:
            logger.error(f"Position signal handling error: {e}")
            logger.error(f"Position data: {position_data}")

    def _register_copy_signal_handler(self):
        """Регистрация обработчика сигналов для копирования"""
        try:
            # Расширяем signal processor из Этапа 1 для обработки копирования
            original_execute_signal = self.base_monitor.signal_processor._execute_signal_processing
            
            async def enhanced_signal_processing(signal: TradingSignal):
                """Расширенная обработка сигналов с копированием"""
                # Сначала выполняем базовую обработку
                await original_execute_signal(signal)
                
                # Затем обрабатываем копирование
                if self.copy_enabled and not self.drawdown_controller.emergency_stop_active:
                    await self.process_copy_signal(signal)
                    self.system_stats['total_signals_processed'] += 1
            
            # Подменяем обработчик
            self.base_monitor.signal_processor._execute_signal_processing = enhanced_signal_processing
            
            logger.info("Copy signal handler registered")
            
        except Exception as e:
            logger.error(f"Copy signal handler registration error: {e}")
    
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
        print("2. Все зависимости установлены: pip install numpy pandas scipy")
        print("3. API ключи корректно настроены в конфигурационных файлах")
