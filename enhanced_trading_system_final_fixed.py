#!/usr/bin/env python3
"""
BYBIT COPY TRADING SYSTEM - ЭТАП 1 (ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ ВЕРСИЯ)
Версия 5.0 - ВСЕ WEBSOCKET ПРОБЛЕМЫ РЕШЕНЫ!

🎯 КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ НА ОСНОВЕ ДИАГНОСТИЧЕСКОГО ТЕСТИРОВАНИЯ:
- ✅ ИНТЕГРИРОВАНЫ исправления из websocket_fixed_functions.py
- ✅ ЗАМЕНЕНА функция is_websocket_open() на основе результатов тестов
- ✅ ЗАМЕНЕНА функция close_websocket_safely() на основе результатов тестов  
- ✅ ИСПРАВЛЕНО свойство closed в FixedWebSocketManager
- ✅ ДОБАВЛЕНА диагностическая функция diagnose_websocket_issue()
- ✅ ws.state.name = "OPEN" - РАБОЧИЙ МЕТОД для websockets 15.0.1
- ✅ ws.closed НЕ СУЩЕСТВУЕТ в websockets 15.0.1 - ИСПРАВЛЕНО

🔧 РЕЗУЛЬТАТ: Система полностью совместима с websockets 15.0.1!
"""

import asyncio, random, functools
import time
import json
import hmac
import numpy as np
import hashlib
import logging, logging.handlers
import requests
import websockets
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union, Callable
from urllib.parse import urlencode
from dataclasses import dataclass
from enum import Enum, auto
import psutil
import gc
from collections import deque, defaultdict, OrderedDict
import traceback
import sys
import weakref
import signal
import os
import statistics
import uuid
import os, socket
from decimal import Decimal
import asyncio
from datetime import timezone

from sys_events_logger import sys_logger
from signals_logger import signals_logger
from positions_db_writer import positions_writer
# from positions_store import positions_store

# Единый системный логгер для всех модулей проекта
SYSTEM_LOGGER_NAME = "bybit_trading_system"
system_logger = logging.getLogger(SYSTEM_LOGGER_NAME)

# Конфигурируем ОДИН раз и не даём сообщениям «утекать» вверх
if not getattr(system_logger, "_configured", False):
    system_logger.setLevel(logging.INFO)
    system_logger.propagate = False  # ключ: не передавать сообщения родителю

    # На всякий случай уберём любые ранее добавленные хендлеры
    for h in list(system_logger.handlers):
        system_logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    system_logger.addHandler(sh)

    # помечаем, что уже настроили, чтобы не конфигурировать повторно
    system_logger._configured = True

# Локальный логгер текущего файла переиспользует тот же хендлер и тоже не пропагирует
logger = logging.getLogger("enhanced_trading_system_final_fixed")
# КРИТИЧНО: Устанавливаем DEBUG для детальной диагностики API и WS
logger.setLevel(logging.DEBUG)
logger.propagate = False
if not logger.handlers:
    for h in system_logger.handlers:
        logger.addHandler(h)


# ================================
# УНИВЕРСАЛЬНЫЕ ИМПОРТЫ И КОНФИГУРАЦИЯ
# ================================
# 1) безопасные импорты: берём recv_window из старого конфига,
#    а ключи — только из БД через CredentialsStore (обёртка в config.py)
from config import get_api_credentials, BYBIT_RECV_WINDOW, DEFAULT_TRADE_ACCOUNT_ID, BALANCE_ACCOUNT_TYPE, TARGET_ACCOUNT_ID

log = logging.getLogger(__name__)

# 2) Telegram-конфиг (как и был)
try:
    from telegram_cfg import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
except Exception as e:
    print(f"⚠️ Не удалось импортировать telegram_cfg.py: {e}")
    TELEGRAM_TOKEN = None
    TELEGRAM_CHAT_ID = None

# 3) Какими account_id пользоваться.
#    TARGET_ACCOUNT_ID — аккаунт, на который выставляем ордера (демо/прод).
#    DONOR_ACCOUNT_ID  — источник read-only с mainnet (если используете донорский поток/сигналы).
TARGET_ACCOUNT_ID = int(os.getenv("TARGET_ACCOUNT_ID", str(DEFAULT_TRADE_ACCOUNT_ID)))
DONOR_ACCOUNT_ID  = int(os.getenv("DONOR_ACCOUNT_ID", "2"))  # можно переопределить через .env

# 4) Подтягиваем ключи из БД (AES-GCM).
#    Важно: БОЛЬШЕ НЕ ПАДАЕМ на импорте модуля, если ключей нет.
#    Даём модулю загрузиться и переходим в SETUP MODE, чтобы их ввести через /keys.
_creds_target = get_api_credentials(TARGET_ACCOUNT_ID)
if not _creds_target:
    os.environ["SETUP_MODE_NO_KEYS"] = "1"
    MAIN_API_KEY = None
    MAIN_API_SECRET = None
    log.warning(
        "Ключи в БД для TARGET_ACCOUNT_ID=%s не найдены. Запуск в SETUP MODE. "
        "Введите их через /keys в Telegram и нажмите «Применить без рестарта».",
        TARGET_ACCOUNT_ID
    )
else:
    MAIN_API_KEY, MAIN_API_SECRET = _creds_target  # ← имена оставлены без изменений

# Донорские ключи (опционально)
_creds_donor = get_api_credentials(DONOR_ACCOUNT_ID)
if _creds_donor:
    SOURCE_API_KEY, SOURCE_API_SECRET = _creds_donor
else:
    # если донор не используется — оставляем None, остальной код сможет это учитывать
    SOURCE_API_KEY = None
    SOURCE_API_SECRET = None

# 5) Адреса/эндпойнты — можно переопределить через .env, но дефолты те же, что были
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()
IS_PROD = (ENVIRONMENT == "prod")

# Основной URL для API
if IS_PROD:
    # В проде всегда используем основной API
    MAIN_API_URL = os.getenv("MAIN_API_URL", "https://api.bybit.com")
    SOURCE_API_URL = os.getenv("SOURCE_API_URL", "https://api.bybit.com")
    logger.info("ENVIRONMENT=prod. Using PRODUCTION API endpoints: %s", MAIN_API_URL)
else:
    # В dev/test используем демо по умолчанию
    MAIN_API_URL = os.getenv("MAIN_API_URL", "https://api-demo.bybit.com")
    SOURCE_API_URL = os.getenv("SOURCE_API_URL", "https://api.bybit.com") # донор может оставаться на проде
    logger.info("ENVIRONMENT=%s. Using DEMO API endpoint: %s", ENVIRONMENT, MAIN_API_URL)


# WebSocket URLs (Bybit v5)
if IS_PROD:
    SOURCE_WS_URL = os.getenv("SOURCE_WS_URL", "wss://stream.bybit.com/v5/private")
    PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "wss://stream.bybit.com/v5/public/linear")
    logger.info("ENVIRONMENT=prod. Using PRODUCTION WebSocket endpoints.")
else:
    # Для разработки можно использовать демо-WS, если они есть
    SOURCE_WS_URL = os.getenv("SOURCE_WS_URL", "wss://stream-demo.bybit.com/v5/private")
    PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "wss://stream-demo.bybit.com/v5/public/linear")
    logger.info("ENVIRONMENT=%s. Using DEMO WebSocket endpoints.", ENVIRONMENT)

# дальше идёт твой существующий код: RATE_LIMITS, тайминги, константы и т.д.

# Rate limiting конфигурация
RATE_LIMITS = {
    'rest_per_minute': 120,
    'rest_per_second': 10,
    'websocket_per_second': 10,
    'websocket_connections_max': 5
}

# ✅ ИСПРАВЛЕНО: Обновленные константы для ping/pong
WEBSOCKET_PING_INTERVAL = 20  # секунд (согласно документации Bybit)
WEBSOCKET_PONG_TIMEOUT = 30   # ✅ УВЕЛИЧЕНО для надежности
WEBSOCKET_TIMEOUT = 15        # секунд для операций
RECONNECT_DELAYS = [1, 2, 5, 10, 30, 60]  # Exponential backoff
MAX_RECONNECT_ATTEMPTS = 10

# Production настройки
PRODUCTION_CONFIG = {
    'max_memory_mb': 1000,
    'max_queue_size': 500,
    'health_check_interval': 60,
    'stats_report_interval': 300,
    'cleanup_interval': 3600
}

class DataState(Enum):
    OK = auto()
    STALE = auto()
    UNAVAILABLE = auto()

class RiskDataContext:
    def __init__(self, cfg):
        self.cfg = cfg
        self.dd_percent = 0.0
        self.daily_dd_percent = 0.0
        self.high_watermark: Optional[float] = None
        self._last_valid_equity: Optional[float] = None
        self.last_equity_ok_ts = 0.0
        self.equity_state = DataState.UNAVAILABLE
        self._dd_hits = 0

    def update_equity(self, equity: Optional[float]) -> None:
        now = time.time()
        if equity is None:
            self.equity_state = (
                DataState.UNAVAILABLE if now - self.last_equity_ok_ts > self.cfg.SAFE_MODE["data_stale_ttl_sec"]
                else DataState.STALE
            )
            self._dd_hits = 0
            return
        self.equity_state = DataState.OK
        self.last_equity_ok_ts = now
        self._last_valid_equity = equity
        if self.high_watermark is None:
            self.high_watermark = equity
        self.high_watermark = max(self.high_watermark, equity)
        if self.high_watermark > 0:
            self.dd_percent = max(0.0, (self.high_watermark - equity) / self.high_watermark)
        else:
            self.dd_percent = 0.0

    def update_daily_dd(self, daily_equity: Optional[float], daily_high: Optional[float]) -> None:
        if daily_equity and daily_high and daily_high > 0:
            self.daily_dd_percent = max(0.0, (daily_high - daily_equity) / daily_high)

    def dd_confirmed(self, positional_dd: Optional[float] = None) -> bool:
        if self.equity_state is not DataState.OK:
            self._dd_hits = 0
            return False
        dd2 = self.dd_percent if positional_dd is None else positional_dd
        if self.dd_percent >= self.cfg.RISK["drawdown_limit"] and dd2 >= self.cfg.RISK["drawdown_limit"]:
            self._dd_hits += 1
        else:
            self._dd_hits = 0
        return self._dd_hits >= self.cfg.SAFE_MODE["risk_confirm_reads"]

    def is_data_reliable(self) -> bool:
        return self.equity_state is DataState.OK

class SystemMode(Enum):
    RUNNING = "RUNNING"
    SAFE_MODE = "SAFE_MODE"
    EMERGENCY_STOP = "EMERGENCY_STOP"

class HealthSupervisor:
    
    def __init__(self, cfg, risk_ctx: RiskDataContext, notifier=None):
        self.cfg = cfg
        self.risk = risk_ctx
        self.notifier = notifier
        self.mode = SystemMode.RUNNING
        self.fail_consecutive = 0
        self.copy_enabled = True

    async def on_api_failure(self, reason: str):
        self.fail_consecutive += 1
        if (self.cfg.SAFE_MODE["enabled"]
            and self.mode is SystemMode.RUNNING
            and (self.fail_consecutive >= self.cfg.SAFE_MODE["network_error_tolerance"]
                 or not self.risk.is_data_reliable())):
            await self.enter_safe_mode(f"API degraded: {reason}")
        if (self.fail_consecutive >= self.cfg.SAFE_MODE["api_retry_limit"]
            and self.risk.dd_confirmed()):
            await self.trigger_emergency_stop("API degraded + confirmed DD")

    async def on_api_success(self):
        if self.fail_consecutive:
            self.fail_consecutive = 0
        if self.mode is SystemMode.SAFE_MODE and self.risk.is_data_reliable():
            self.mode = SystemMode.RUNNING
            self.copy_enabled = True
            await self._notify("✅ SAFE MODE exited: API recovered, data OK.")

    async def enter_safe_mode(self, msg: str):
        if self.mode is SystemMode.SAFE_MODE:
            return
        self.mode = SystemMode.SAFE_MODE
        self.copy_enabled = False
        await self._notify(f"⚠️ SAFE MODE: {msg}. New entries paused, protection stays active.")

    async def trigger_emergency_stop(self, reason: str):
        if self.mode is SystemMode.EMERGENCY_STOP:
            return
        self.mode = SystemMode.EMERGENCY_STOP
        self.copy_enabled = False
        await self._notify(f"🛑 EMERGENCY STOP: {reason}")

    def can_open_positions(self) -> bool:
        return self.mode is SystemMode.RUNNING and self.copy_enabled

    async def _notify(self, text: str):
        try:
            logger.warning(text)
            if self.notifier:
                await self.notifier(text)
        except Exception:
            pass

    def async_retry(retries=None, base=None, factor=None, max_delay=None, retriable=(asyncio.TimeoutError, OSError)):
        # значения по умолчанию из config.SAFE_MODE
        from config import SAFE_MODE as _SM
        retries = _SM["api_retry_limit"] if retries is None else retries
        base = _SM["api_backoff_base"] if base is None else base
        factor = _SM["api_backoff_factor"] if factor is None else factor
        max_delay = _SM["api_backoff_max"] if max_delay is None else max_delay

        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*a, **kw):
                delay = base
                last = None
                for attempt in range(retries):
                    try:
                        return await func(*a, **kw)
                    except retriable as e:
                        last = e
                        if attempt == retries - 1:
                            logger.error("%s failed after %d attempts: %s", func.__name__, retries, e)
                            raise
                        jitter = random.random() * 0.2 * delay
                        sleep_sec = min(delay + jitter, max_delay)
                        logger.warning("%s attempt %d/%d failed: %s. Retry in %.2fs",
                                       func.__name__, attempt+1, retries, e, sleep_sec)
                        await asyncio.sleep(sleep_sec)
                        delay = min(delay * factor, max_delay)
                raise last
            return wrapper
        return decorator

# ================================
# 1.4 PRODUCTION LOGGING SYSTEM
# ================================

class ProductionLogger:
    """
    📝 ENTERPRISE-GRADE СИСТЕМА ЛОГИРОВАНИЯ
    
    Функции:
    - Structured JSON logging для machine parsing
    - Log rotation с compression
    - Centralized logging ready
    - Performance metrics integration
    - Real-time error alerting
    """
    
    def __init__(self, 
                 app_name: str = "bybit_trading_system",
                 log_level: str = "INFO",
                 enable_rotation: bool = True,
                 enable_json: bool = True,
                 enable_metrics: bool = True):
        
        self.app_name = app_name
        self.enable_json = enable_json
        self.enable_metrics = enable_metrics
        
        # Создаем директории
        self.log_dir = "logs"
        self.metrics_dir = "metrics" 
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)
        
        # Настраиваем основной logger
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Очищаем существующие handlers
        self.logger.handlers.clear()
        
        # Добавляем handlers
        self._setup_console_handler()
        if enable_rotation:
            self._setup_file_handlers()
        
        # Metrics tracking
        if enable_metrics:
            self.metrics = LogMetrics()
        
        # Error alerting
        self.error_alerter = ErrorAlerter()
        
        logger.info(f"Production logging initialized for {app_name}")
    
    def _setup_console_handler(self):
        """Настройка консольного вывода"""
        console_handler = logging.StreamHandler(sys.stdout)
        
        if self.enable_json:
            formatter = JSONFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)8s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)
    
    def _setup_file_handlers(self):
        """Настройка файловых handlers с rotation"""
        
        # Основной лог с rotation
        main_handler = logging.handlers.RotatingFileHandler(
            filename=f"{self.log_dir}/{self.app_name}.log",
            maxBytes=50*1024*1024,  # 50MB
            backupCount=10,
            encoding='utf-8'
        )
        
        # Лог ошибок
        error_handler = logging.handlers.RotatingFileHandler(
            filename=f"{self.log_dir}/{self.app_name}_errors.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        
        # Performance metrics лог
        perf_handler = logging.handlers.TimedRotatingFileHandler(
            filename=f"{self.log_dir}/{self.app_name}_performance.log",
            when='H',  # Hourly rotation
            interval=1,
            backupCount=24*7,  # Неделя данных
            encoding='utf-8'
        )
        
        # JSON форматтеры для structured logging
        if self.enable_json:
            json_formatter = JSONFormatter()
            main_handler.setFormatter(json_formatter)
            error_handler.setFormatter(json_formatter)
            perf_handler.setFormatter(json_formatter)
        else:
            standard_formatter = logging.Formatter(
                '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s'
            )
            main_handler.setFormatter(standard_formatter)
            error_handler.setFormatter(standard_formatter)
            perf_handler.setFormatter(standard_formatter)
        
        # Добавляем handlers
        self.logger.addHandler(main_handler)
        self.logger.addHandler(error_handler)
        
        # Отдельный logger для performance
        self.perf_logger = logging.getLogger(f"{self.app_name}.performance")
        self.perf_logger.addHandler(perf_handler)
        self.perf_logger.setLevel(logging.INFO)
    
    def log_performance(self, 
                       operation: str,
                       duration: float,
                       success: bool,
                       metadata: Dict[str, Any] = None):
        """Логирование performance метрик"""
        if not self.enable_metrics:
            return
        
        # Обновляем внутренние метрики
        self.metrics.record_operation(operation, duration, success)
        
        # Structured log entry
        log_entry = {
            'event_type': 'performance',
            'operation': operation,
            'duration_ms': round(duration * 1000, 2),
            'success': success,
            'timestamp': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        }
        
        if hasattr(self, 'perf_logger'):
            self.perf_logger.info(json.dumps(log_entry))
    
    def log_error(self, 
                  error: Exception,
                  context: Dict[str, Any] = None,
                  send_alert: bool = True):
        """Расширенное логирование ошибок с alerting"""
        
        error_info = {
            'event_type': 'error',
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'context': context or {},
            'timestamp': datetime.utcnow().isoformat(),
            'severity': self.error_alerter._classify_error_severity(error)
        }
        
        # Логируем
        self.logger.error(json.dumps(error_info) if self.enable_json else error_info['error_message'])
        
        # Отправляем alert для критичных ошибок
        if send_alert and error_info['severity'] in ['high', 'critical']:
            asyncio.create_task(self.error_alerter.send_alert(error_info))

class JSONFormatter(logging.Formatter):
    """JSON форматтер для structured logging"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Добавляем exception info если есть
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Добавляем extra fields если есть
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'getMessage', 'exc_info', 'exc_text']:
                log_entry[key] = value
        
        return json.dumps(log_entry, ensure_ascii=False)

class LogMetrics:
    """Сбор и анализ метрик логирования"""
    
    def __init__(self):
        self.operation_stats = defaultdict(lambda: {
            'count': 0,
            'success_count': 0,
            'total_duration': 0,
            'max_duration': 0,
            'min_duration': float('inf')
        })
        
        self.error_counts = defaultdict(int)
        self.performance_history = deque(maxlen=1000)
    
    def record_operation(self, operation: str, duration: float, success: bool):
        """Записать метрику операции"""
        stats = self.operation_stats[operation]
        stats['count'] += 1
        stats['total_duration'] += duration
        
        if success:
            stats['success_count'] += 1
        
        stats['max_duration'] = max(stats['max_duration'], duration)
        stats['min_duration'] = min(stats['min_duration'], duration)
        
        # История для анализа трендов
        self.performance_history.append({
            'operation': operation,
            'duration': duration,
            'success': success,
            'timestamp': time.time()
        })
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Получить сводку производительности"""
        summary = {}
        
        for operation, stats in self.operation_stats.items():
            if stats['count'] > 0:
                avg_duration = stats['total_duration'] / stats['count']
                success_rate = stats['success_count'] / stats['count']
                
                summary[operation] = {
                    'total_calls': stats['count'],
                    'success_rate': round(success_rate * 100, 2),
                    'avg_duration_ms': round(avg_duration * 1000, 2),
                    'max_duration_ms': round(stats['max_duration'] * 1000, 2),
                    'min_duration_ms': round(stats['min_duration'] * 1000, 2)
                }
        
        return summary

class ErrorAlerter:
    """Система отправки alerts для критичных ошибок"""
    
    def __init__(self):
        self.alert_cooldown = 300  # 5 минут между одинаковыми алертами
        self.recent_alerts = {}
    
    async def send_alert(self, error_info: Dict[str, Any]):
        """Отправка alert"""
        try:
            # Дедупликация алертов
            alert_key = f"{error_info['error_type']}:{error_info.get('operation', 'unknown')}"
            now = time.time()
            
            if alert_key in self.recent_alerts:
                if now - self.recent_alerts[alert_key] < self.alert_cooldown:
                    return  # Слишком рано для повторного алерта
            
            self.recent_alerts[alert_key] = now
            
            # Формируем сообщение
            alert_message = self._format_alert_message(error_info)
            
            # Отправляем в Telegram (если настроен)
            await self._send_telegram_alert(alert_message)
            
            # Здесь можно добавить другие каналы (email, Slack, etc.)
            
        except Exception as e:
            # Алерты не должны ломать основную логику
            logging.getLogger('error_alerter').error(f"Failed to send alert: {e}")
    
    def _format_alert_message(self, error_info: Dict[str, Any]) -> str:
        """Форматирование сообщения алерта"""
        severity_emoji = {
            'critical': '🚨',
            'high': '⚠️',
            'medium': '🔶',
            'low': 'ℹ️'
        }
        
        emoji = severity_emoji.get(error_info['severity'], '❗')
        
        message = f"""{emoji} **TRADING SYSTEM ALERT** {emoji}

**Severity:** {error_info['severity'].upper()}
**Error:** {error_info['error_type']}
**Message:** {error_info['error_message']}
**Time:** {error_info['timestamp']}

**Context:** {json.dumps(error_info.get('context', {}), indent=2)}
"""
        
        return message
    
    def _classify_error_severity(self, error: Exception) -> str:
        """Классификация серьезности ошибки"""
        try:
            error_type = type(error).__name__
            error_message = str(error).lower()
        
            # Критические ошибки - требуют немедленного внимания
            critical_errors = [
                'ConnectionError', 'TimeoutError', 'AuthenticationError',
                'ConnectionResetError', 'ConnectionRefusedError', 'OSError'
            ]
        
            # Высокоприоритетные ошибки - влияют на торговлю
            high_errors = [
                'RateLimitError', 'APIError', 'ValidationError', 'HTTPError',
                'InvalidSignatureError', 'InsufficientBalanceError'
            ]
        
            # Средние ошибки - требуют внимания но не критичны
            medium_errors = [
                'ValueError', 'KeyError', 'TypeError', 'AttributeError',
                'JSONDecodeError', 'ParseError'
            ]
        
            # Проверяем по типу ошибки
            if error_type in critical_errors:
                return 'critical'
            elif error_type in high_errors:
                return 'high'
            elif error_type in medium_errors:
                return 'medium'
        
            # Проверяем по содержанию сообщения
            critical_keywords = [
                'connection refused', 'connection reset', 'network unreachable',
                'authentication failed', 'invalid api key', 'signature verification failed',
                'server error', 'internal server error', 'service unavailable'
            ]
        
            high_keywords = [
                'rate limit', 'too many requests', 'quota exceeded',
                'insufficient balance', 'position not found', 'order failed',
                'market closed', 'trading suspended'
            ]
        
            medium_keywords = [
                'invalid parameter', 'missing field', 'validation error',
                'parse error', 'format error', 'data error'
            ]
        
            # Проверяем критические ключевые слова
            for keyword in critical_keywords:
                if keyword in error_message:
                    return 'critical'
        
            # Проверяем высокоприоритетные ключевые слова
            for keyword in high_keywords:
                if keyword in error_message:
                    return 'high'
        
            # Проверяем средние ключевые слова
            for keyword in medium_keywords:
                if keyword in error_message:
                    return 'medium'
        
            # Дополнительные проверки
            if 'network' in error_message or 'connection' in error_message:
                return 'high'
            elif 'websocket' in error_message or 'socket' in error_message:
                return 'high'
            elif 'bybit' in error_message or 'api' in error_message:
                return 'medium'
        
            # По умолчанию - низкий приоритет
            return 'low'
        
        except Exception as classification_error:
            # Если классификация не удалась, возвращаем средний уровень
            logger.debug(f"Error classification failed: {classification_error}")
            return 'medium'

    async def _send_telegram_alert(self, message: str):
        """Отправка в Telegram"""
        # Используем существующую функцию send_telegram_alert
        # которая уже есть в системе
        try:
            # Импорт здесь чтобы избежать circular import
            from enhanced_trading_system_final_fixed import send_telegram_alert
            await send_telegram_alert(message)
        except Exception as e:
            logger.debug(f"Telegram alert failed: {e}")

# ================================
# ГЛОБАЛЬНАЯ ИНИЦИАЛИЗАЦИЯ ЛОГИРОВАНИЯ
# ================================

try:
    # Создаем Production Logger
    prod_logger = ProductionLogger(
        app_name="bybit_trading_system",
        log_level="INFO",
        enable_rotation=True,
        enable_json=False,  # Для стабильности
        enable_metrics=True
    )
    
    # Создаем алиас для совместимости
    logger = prod_logger.logger
    
    logger.info("✅ Production logging system initialized successfully")
    
except Exception as init_error:
    # Fallback к стандартному логированию
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Заглушка для prod_logger
    class DummyProdLogger:
        def __init__(self, fallback_logger):
            self.logger = fallback_logger
            self.metrics = None
        def log_performance(self, *args, **kwargs): pass
        def log_error(self, error, context=None, send_alert=False):
            self.logger.error(f"Error: {error}")
    
    prod_logger = DummyProdLogger(logger)
    logger.warning(f"Fallback logging due to: {init_error}")


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit tripped, failing fast
    HALF_OPEN = "half_open"  # Testing if service recovered

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5          # Failures before opening
    recovery_timeout: int = 60          # Seconds before trying again
    expected_exception: Exception = Exception
    half_open_max_calls: int = 3        # Max calls in half-open state

class EnterpriseBybitConnector:
    """
    🏭 ENTERPRISE-GRADE CONNECTION MANAGER
    
    Solves API performance issues with optimized connection pooling
    and intelligent request distribution for Bybit API.
    
    Based on Bybit specifications:
    - 600 requests per 5 seconds (institutional limit)
    - 500 connections per 5 minutes maximum
    - AWS Singapore hosting optimization
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.TCPConnector] = None
        self._setup_enterprise_connector()
        
        # Performance tracking
        self.connection_stats = {
            'total_requests': 0,
            'reused_connections': 0,
            'new_connections': 0,
            'avg_response_time': 0.0,
            'connection_pool_hits': 0,
            'connection_pool_misses': 0,
            'active_connections': 0,
            'max_connections_used': 0
        }
        
        # Connection health monitoring
        self.health_metrics = {
            'successful_requests': 0,
            'failed_requests': 0,
            'timeout_errors': 0,
            'connection_errors': 0,
            'last_health_check': time.time()
        }
        
        logger.info("EnterpriseBybitConnector initialized with optimized settings")
    
    def _setup_enterprise_connector(self):
        """Setup production-grade TCP connector based on Bybit specifications"""
    
        try:
            # FIXED: Remove unsupported parameters for aiohttp TCPConnector
            self.connector = aiohttp.TCPConnector(
                limit=200,                    # Total connections
                limit_per_host=50,           # Per-host limit  
                ttl_dns_cache=300,           # DNS cache TTL (5 minutes)
                use_dns_cache=True,          # Enable DNS caching
                keepalive_timeout=30,        # Keep connections alive for 30s
                enable_cleanup_closed=True,  # Clean up closed connections
                force_close=False,           # Reuse connections (critical)
                #ssl=True                     # Enable SSL for HTTPS
            )
        
            logger.info("Enterprise TCP connector configured successfully for Bybit API")
        
        except Exception as e:
            logger.error(f"Failed to create enterprise TCP connector: {e}")
            # CRITICAL FIX: Set connector to None to trigger fallback
            self.connector = None

    async def create_session(self) -> aiohttp.ClientSession:
        """
        Create enterprise-grade session with optimized settings
    
        Returns:
            aiohttp.ClientSession: Optimized session for Bybit API
        """
    
        # CRITICAL FIX: Check if connector exists
        if self.connector is None:
            logger.warning("Enterprise connector is None, creating basic session")
            # Create basic session without connector
            timeout = aiohttp.ClientTimeout(
                total=30.0,
                connect=10.0,
                sock_read=20.0
            )
        
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={'User-Agent': 'Bybit-Trading-Bot-Basic/1.0'}
            )
        
            self.connection_stats['new_connections'] += 1
            self.connection_stats['connection_pool_misses'] += 1
        
            logger.warning("Created basic session without enterprise connector")
            return self.session
    
        if self.session and not self.session.closed:
            # Session reuse - update stats
            self.connection_stats['reused_connections'] += 1
            self.connection_stats['connection_pool_hits'] += 1
            return self.session
    
        # Create new session - update stats
        self.connection_stats['new_connections'] += 1
        self.connection_stats['connection_pool_misses'] += 1
    
        # CRITICAL FIX: Optimized timeout configuration for Bybit API
        timeout = aiohttp.ClientTimeout(
            total=60,           # Total request timeout (increased for stability)
            connect=10,         # Connection establishment timeout  
            sock_connect=5,     # Socket connection timeout
            sock_read=30        # Socket read timeout
        )
    
        try:
            # Create session with enterprise connector
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=timeout,
            
                # Performance optimizations
                headers={
                    'User-Agent': 'Bybit-Enterprise-Trading-Bot/2.0',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                },
            
                # Error handling
                raise_for_status=False,
                trust_env=True,
            )
        
            # CRITICAL FIX: Safe access to connector stats
            if self.connector and hasattr(self.connector, '_conns'):
                try:
                    active_connections = len(self.connector._conns)
                    self.connection_stats['active_connections'] = active_connections
                    self.connection_stats['max_connections_used'] = max(
                        self.connection_stats['max_connections_used'], 
                        active_connections
                    )
                except Exception as e:
                    logger.debug(f"Could not get connection stats: {e}")
        
            logger.info(f"Enterprise session created successfully")
            return self.session
        
        except Exception as e:
            logger.error(f"Failed to create enterprise session: {e}")
        
            # Fallback to basic session
            timeout = aiohttp.ClientTimeout(
                total=30.0,
                connect=10.0,
                sock_read=20.0
            )
        
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={'User-Agent': 'Bybit-Trading-Bot-Fallback/1.0'}
            )
        
            logger.warning("Created fallback session due to enterprise session failure")
            return self.session
    
    async def close(self):
        """Graceful cleanup of connections with proper resource management"""
        try:
            # Close session first
            if self.session and not self.session.closed:
                await self.session.close()
                logger.debug("Enterprise session closed")
            
            # Close connector
            if self.connector:
                await self.connector.close()
                logger.debug("Enterprise connector closed")
            
            # Give time for connections to close properly (important for cleanup)
            await asyncio.sleep(0.1)
            
            # Reset references
            self.session = None
            self.connector = None
            
            logger.info("Enterprise connector cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during enterprise connector cleanup: {e}")
            # Don't raise - cleanup should be fault-tolerant
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of the connector"""
        
        total_requests = (self.health_metrics['successful_requests'] + 
                         self.health_metrics['failed_requests'])
        
        success_rate = 0.0
        if total_requests > 0:
            success_rate = (self.health_metrics['successful_requests'] / total_requests) * 100
        
        connection_efficiency = 0.0
        if self.connection_stats['total_requests'] > 0:
            connection_efficiency = (self.connection_stats['reused_connections'] / 
                                   self.connection_stats['total_requests']) * 100
        
        return {
            'status': 'healthy' if success_rate > 90 else 'degraded' if success_rate > 50 else 'unhealthy',
            'success_rate_pct': success_rate,
            'connection_efficiency_pct': connection_efficiency,
            'active_connections': self.connection_stats['active_connections'],
            'max_connections_used': self.connection_stats['max_connections_used'],
            'total_requests': total_requests,
            'avg_response_time': self.connection_stats['avg_response_time'],
            'last_health_check': self.health_metrics['last_health_check']
        }
    
    def update_health_metrics(self, success: bool, response_time: float, error_type: str = None):
        """Update health metrics after each request"""
        
        if success:
            self.health_metrics['successful_requests'] += 1
        else:
            self.health_metrics['failed_requests'] += 1
            
            # Categorize error types
            if error_type:
                if 'timeout' in error_type.lower():
                    self.health_metrics['timeout_errors'] += 1
                elif 'connection' in error_type.lower():
                    self.health_metrics['connection_errors'] += 1
        
        # Update average response time (exponential moving average)
        alpha = 0.1
        if self.connection_stats['avg_response_time'] == 0:
            self.connection_stats['avg_response_time'] = response_time
        else:
            self.connection_stats['avg_response_time'] = (
                alpha * response_time + 
                (1 - alpha) * self.connection_stats['avg_response_time']
            )
        
        self.health_metrics['last_health_check'] = time.time()


class NetworkResilienceManager:
    """
    🛡️ NETWORK RESILIENCE MANAGER
    
    CRITICAL FIX: Implements circuit breaker pattern with intelligent retry logic
    for handling Bybit API connectivity issues. Solves Network Timeout failures.
    
    Features:
    - Circuit breaker pattern (Closed/Open/Half-Open states)
    - Adaptive timeout calculation based on network health
    - Exponential backoff with jitter
    - Network health tracking and monitoring
    - Intelligent failure categorization
    """
    
    def __init__(self, config: CircuitBreakerConfig = CircuitBreakerConfig()):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0
        
        # Network health tracking
        self.network_health = {
            'consecutive_successes': 0,
            'consecutive_failures': 0,
            'avg_response_time': 0.0,
            'total_requests': 0,
            'success_rate': 100.0,
            'last_success_time': time.time(),
            'last_failure_time': 0
        }
        
        # Failure analysis
        self.failure_analysis = {
            'timeout_errors': 0,
            'connection_errors': 0,
            'http_errors': 0,
            'api_errors': 0,
            'unknown_errors': 0
        }
        
        # Performance metrics
        self.performance_metrics = {
            'fastest_response': float('inf'),
            'slowest_response': 0.0,
            'response_times': deque(maxlen=100),  # Last 100 response times
            'hourly_success_rate': 100.0,
            'circuit_breaker_activations': 0
        }
        
        logger.info("Network Resilience Manager initialized with circuit breaker pattern")
    
    async def call_with_circuit_breaker(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection
    
        Args:
            func: Function to execute with protection
            *args: Function arguments
            **kwargs: Function keyword arguments
        
        Returns:
            Any: Result of function execution
        
        Raises:
            Exception: If circuit is open or function fails after retries
        """
    
        # Check circuit state
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time < self.config.recovery_timeout:
                self.performance_metrics['circuit_breaker_activations'] += 1
                raise Exception(f"Circuit breaker OPEN - failing fast. Recovery in {self.config.recovery_timeout - (time.time() - self.last_failure_time):.1f}s")
            else:
                # Transition to half-open for testing
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                logger.info("Circuit breaker transitioning to HALF_OPEN for testing")
    
        # Execute with timeout and retry logic
        start_time = time.time()
        last_exception = None
    
        for retry_attempt in range(3):  # Max 3 attempts
            try:
                # CRITICAL FIX: Implement graduated timeouts based on network health
                timeout = self._calculate_adaptive_timeout()
            
                # FIXED: Remove retry_attempt from kwargs - not all functions support it
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            
                # Record success
                response_time = time.time() - start_time
                self._record_success(response_time)
            
                return result
            
            except asyncio.TimeoutError as e:
                last_exception = e
                self._record_failure(e, 'timeout')
            
                if retry_attempt < 2:  # Don't wait after last attempt
                    await self._exponential_backoff(retry_attempt)
                
            # FIXED: Replace ClientTimeoutError with ServerTimeoutError
            except aiohttp.ServerTimeoutError as e:
                last_exception = e
                self._record_failure(e, 'server_timeout')
            
                if retry_attempt < 2:
                    await self._exponential_backoff(retry_attempt)
                
            except aiohttp.ClientConnectionError as e:
                last_exception = e
                self._record_failure(e, 'connection')
            
                if retry_attempt < 2:
                    await self._exponential_backoff(retry_attempt)
                
            except aiohttp.ClientResponseError as e:
                last_exception = e
                self._record_failure(e, 'http_response')
            
                # Don't retry on client errors (4xx)
                if hasattr(e, 'status') and 400 <= e.status < 500:
                    break
                
                if retry_attempt < 2:
                    await self._exponential_backoff(retry_attempt)
                
            # FIXED: Catch all aiohttp errors with general ClientError
            except aiohttp.ClientError as e:
                last_exception = e
                self._record_failure(e, 'client_error')
            
                if retry_attempt < 2:
                    await self._exponential_backoff(retry_attempt)
                
            except Exception as e:
                last_exception = e
                self._record_failure(e, 'unknown')
            
                if retry_attempt < 2:
                    await self._exponential_backoff(retry_attempt)
    
        # All retries failed
        total_time = time.time() - start_time
        logger.error(f"All retry attempts failed after {total_time:.2f}s")
        raise last_exception
    
    def _calculate_adaptive_timeout(self) -> float:
        """Calculate adaptive timeout based on network health and performance"""
        base_timeout = 10.0
        
        # Adjust based on average response time
        if self.network_health['avg_response_time'] > 5.0:
            # Slow network detected - increase timeout
            multiplier = min(self.network_health['avg_response_time'] / 5.0, 3.0)
            adaptive_timeout = base_timeout * multiplier
        elif self.network_health['consecutive_failures'] > 2:
            # Multiple failures - reduce timeout for faster failure detection
            adaptive_timeout = max(base_timeout * 0.5, 5.0)
        elif self.network_health['success_rate'] < 50.0:
            # Poor success rate - reduce timeout
            adaptive_timeout = max(base_timeout * 0.7, 5.0)
        else:
            adaptive_timeout = base_timeout
        
        # Ensure reasonable bounds
        return max(min(adaptive_timeout, 30.0), 5.0)
    
    def _record_success(self, response_time: float):
        """Record successful network operation with comprehensive metrics"""
        
        # Reset failure tracking
        self.failure_count = 0
        self.network_health['consecutive_successes'] += 1
        self.network_health['consecutive_failures'] = 0
        self.network_health['last_success_time'] = time.time()
        self.network_health['total_requests'] += 1
        
        # Update response time metrics
        self.performance_metrics['response_times'].append(response_time)
        self.performance_metrics['fastest_response'] = min(
            self.performance_metrics['fastest_response'], response_time
        )
        self.performance_metrics['slowest_response'] = max(
            self.performance_metrics['slowest_response'], response_time
        )
        
        # Update average response time (exponential moving average)
        alpha = 0.1
        if self.network_health['avg_response_time'] == 0:
            self.network_health['avg_response_time'] = response_time
        else:
            self.network_health['avg_response_time'] = (
                alpha * response_time + 
                (1 - alpha) * self.network_health['avg_response_time']
            )
        
        # Calculate success rate
        total_requests = self.network_health['total_requests']
        total_failures = sum(self.failure_analysis.values())
        if total_requests > 0:
            self.network_health['success_rate'] = ((total_requests - total_failures) / total_requests) * 100
        
        # State management for circuit breaker
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= self.config.half_open_max_calls:
                self.state = CircuitState.CLOSED
                logger.info("Circuit breaker CLOSED - service recovered")
        
        # Log performance milestones
        if self.network_health['consecutive_successes'] % 100 == 0:
            logger.info(f"Network health: {self.network_health['consecutive_successes']} consecutive successes, "
                       f"avg response time: {self.network_health['avg_response_time']:.3f}s")
    
    def _record_failure(self, exception: Exception, error_type: str):
        """Record failed network operation with detailed analysis"""
        
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.network_health['consecutive_failures'] += 1
        self.network_health['consecutive_successes'] = 0
        self.network_health['last_failure_time'] = time.time()
        self.network_health['total_requests'] += 1
        
        # Categorize failure type
        if error_type in self.failure_analysis:
            self.failure_analysis[error_type] += 1
        else:
            self.failure_analysis['unknown_errors'] += 1
        
        # Calculate success rate
        total_requests = self.network_health['total_requests']
        total_failures = sum(self.failure_analysis.values())
        if total_requests > 0:
            self.network_health['success_rate'] = ((total_requests - total_failures) / total_requests) * 100
        
        # Circuit breaker state management
        if self.failure_count >= self.config.failure_threshold:
            if self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                self.performance_metrics['circuit_breaker_activations'] += 1
                logger.warning(f"Circuit breaker OPEN - {self.failure_count} failures detected. Error type: {error_type}")
        
        # Log failure patterns
        if self.network_health['consecutive_failures'] % 5 == 0:
            logger.warning(f"Network degradation: {self.network_health['consecutive_failures']} consecutive failures, "
                          f"success rate: {self.network_health['success_rate']:.1f}%")
    
    async def _exponential_backoff(self, attempt: int):
        """Implement exponential backoff with jitter and adaptive delays"""
        
        # Base delay increases exponentially
        base_delay = 1.0 * (2 ** attempt)  # 1s, 2s, 4s, 8s...
        max_delay = 30.0
        
        # Adaptive delay based on network health
        if self.network_health['success_rate'] < 30.0:
            # Very poor network - longer delays
            base_delay *= 2.0
        elif self.network_health['success_rate'] > 80.0:
            # Network mostly good - shorter delays
            base_delay *= 0.5
        
        delay = min(base_delay, max_delay)
        
        # Add jitter (±25%) to prevent thundering herd
        jitter_range = delay * 0.25
        jitter = (hash(time.time()) % 100) / 100 * 2 - 1  # -1 to 1
        delay += jitter * jitter_range
        
        # Ensure minimum delay
        delay = max(delay, 0.5)
        
        logger.debug(f"Exponential backoff: waiting {delay:.2f}s (attempt {attempt + 1})")
        await asyncio.sleep(delay)
    
    def get_resilience_report(self) -> Dict[str, Any]:
        """Get comprehensive resilience and performance report"""
        
        # Calculate percentiles for response times
        response_times = list(self.performance_metrics['response_times'])
        percentiles = {}
        if response_times:
            percentiles = {
                'p50': np.percentile(response_times, 50),
                'p95': np.percentile(response_times, 95),
                'p99': np.percentile(response_times, 99)
            }
        
        return {
            'circuit_breaker': {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'activations': self.performance_metrics['circuit_breaker_activations']
            },
            'network_health': self.network_health,
            'failure_analysis': self.failure_analysis,
            'performance': {
                'fastest_response': self.performance_metrics['fastest_response'] if self.performance_metrics['fastest_response'] != float('inf') else 0,
                'slowest_response': self.performance_metrics['slowest_response'],
                'percentiles': percentiles,
                'total_samples': len(response_times)
            }
        }
    
    def reset_circuit_breaker(self):
        """Manually reset circuit breaker (for administrative purposes)"""
        
        previous_state = self.state
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
        
        logger.info(f"Circuit breaker manually reset from {previous_state.value} to CLOSED")

# ================================
# УНИВЕРСАЛЬНЫЕ УТИЛИТЫ
# ================================

class ConnectionMonitorPro:
    """
    📡 PRODUCTION-GRADE МОНИТОРИНГ СОЕДИНЕНИЙ
    
    Отслеживает здоровье всех сетевых соединений и автоматически восстанавливает
    """
    
    def __init__(self):
        self.connections = {}  # {id: ConnectionState}
        self.health_checkers = {}
        self.recovery_strategies = {}
        self.monitoring_active = False
        
        # Circuit breaker pattern
        self.circuit_breakers = {}
        
        # Статистика
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'failed_connections': 0,
            'recovery_attempts': 0,
            'successful_recoveries': 0
        }
    
    async def register_connection(self, 
                                connection_id: str,
                                connection_obj: Any,
                                health_check_func: callable,
                                recovery_func: callable,
                                check_interval: float = 30.0):
        """Регистрация соединения для мониторинга"""
        
        self.connections[connection_id] = {
            'object': weakref.ref(connection_obj),
            'health_check': health_check_func,
            'recovery_func': recovery_func,
            'last_check': 0,
            'check_interval': check_interval,
            'consecutive_failures': 0,
            'status': 'unknown'
        }
        
        # Создаем circuit breaker
        self.circuit_breakers[connection_id] = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=60,
            expected_exception=Exception
        )
        
        logger.info(f"Connection {connection_id} registered for monitoring")
        
        # Запускаем мониторинг если еще не активен
        if not self.monitoring_active:
            asyncio.create_task(self._monitoring_loop())
    
    async def _monitoring_loop(self):
        """✅ УЛУЧШЕННЫЙ основной цикл мониторинга с метриками"""
        self.monitoring_active = True
        logger.info("Connection monitoring started")
    
        # ✅ НОВОЕ: Инициализация метрик
        last_metrics_report = 0
        metrics_interval = 300  # 5 минут
    
        while self.monitoring_active:
            try:
                current_time = time.time()
            
                # Основная проверка соединений
                await self._check_all_connections()
            
                # ✅ НОВОЕ: Периодический отчет метрик (каждые 5 минут)
                if current_time - last_metrics_report > metrics_interval:
                    try:
                        await self._report_system_metrics()
                        last_metrics_report = current_time
                    except Exception as metrics_error:
                        logger.error(f"Metrics reporting error: {metrics_error}")
                        # ✅ НОВОЕ: Логируем ошибку метрик с Production Logger
                        if 'prod_logger' in globals():
                            prod_logger.log_error(metrics_error, {
                                'operation': 'system_metrics_reporting',
                                'context': 'monitoring_loop',
                                'component': 'connection_monitor'
                            }, send_alert=False)  # Не критичная ошибка
            
                await asyncio.sleep(10)  # Проверка каждые 10 секунд
            
            except Exception as e:
                logger.error(f"Connection monitoring error: {e}")
            
                # ✅ НОВОЕ: Production Logger для ошибок мониторинга
                if 'prod_logger' in globals():
                    try:
                        prod_logger.log_error(e, {
                            'operation': 'connection_monitoring',
                            'context': 'monitoring_loop_error',
                            'component': 'connection_monitor',
                            'monitoring_active': self.monitoring_active
                        }, send_alert=True)  # Критичная ошибка - отправляем alert
                    except:
                        pass  # Не ломаем основную логику
            
                await asyncio.sleep(30)
    
    async def _check_all_connections(self):
        """Проверка всех зарегистрированных соединений"""
        now = time.time()
        
        for conn_id, conn_info in list(self.connections.items()):
            try:
                # Проверяем нужно ли проверить это соединение
                if now - conn_info['last_check'] > conn_info['check_interval']:
                    await self._check_single_connection(conn_id, conn_info)
                    conn_info['last_check'] = now
                    
            except Exception as e:
                logger.error(f"Error checking connection {conn_id}: {e}")
    
    async def _check_single_connection(self, conn_id: str, conn_info: Dict):
        """Проверка одного соединения"""
        try:
            connection_obj = conn_info['object']()
            if connection_obj is None:
                # Объект был удален сборщиком мусора
                del self.connections[conn_id]
                return
            
            # Используем circuit breaker
            circuit_breaker = self.circuit_breakers[conn_id]
            
            if circuit_breaker.state == 'open':
                # Circuit открыт, не проверяем
                conn_info['status'] = 'circuit_open'
                return
            
            # Выполняем health check
            health_status = await conn_info['health_check'](connection_obj)
            
            if health_status:
                # Соединение здоровое
                conn_info['status'] = 'healthy'
                conn_info['consecutive_failures'] = 0
                circuit_breaker.record_success()
                
            else:
                # Соединение нездоровое
                conn_info['status'] = 'unhealthy'
                conn_info['consecutive_failures'] += 1
                circuit_breaker.record_failure()
                
                # Попытка восстановления
                if conn_info['consecutive_failures'] >= 2:
                    await self._attempt_recovery(conn_id, conn_info)
                    
        except Exception as e:
            logger.warning(f"Health check failed for {conn_id}: {e}")
            conn_info['status'] = 'error'
            self.circuit_breakers[conn_id].record_failure()

    async def _report_system_metrics(self):
        """✅ НОВЫЙ МЕТОД: Периодический отчет метрик системы"""
        try:
            # ✅ Performance metrics (если Production Logger доступен)
            perf_summary = {}
            if 'prod_logger' in globals() and hasattr(prod_logger, 'metrics'):
                try:
                    perf_summary = prod_logger.metrics.get_performance_summary()
                except:
                    perf_summary = {'status': 'metrics_unavailable'}
        
            # ✅ System metrics
            try:
                import psutil
                process = psutil.Process()
                system_metrics = {
                    'memory_mb': round(process.memory_info().rss / 1024 / 1024, 2),
                    'cpu_percent': process.cpu_percent(),
                    'uptime_hours': round((time.time() - self.start_time) / 3600, 2)
                }
            except ImportError:
                system_metrics = {
                    'memory_mb': 'psutil_not_available',
                    'cpu_percent': 'psutil_not_available',
                    'uptime_hours': round((time.time() - self.start_time) / 3600, 2)
                }
            except Exception as e:
                system_metrics = {'error': str(e)}
        
            # ✅ Connection Monitor stats (если доступен)
            connection_metrics = {}
            if hasattr(self, 'connection_monitor') and self.connection_monitor:
                try:
                    connection_metrics = {
                        'total_connections': len(self.connection_monitor.connections),
                        'active_connections': sum(1 for conn in self.connection_monitor.connections.values() 
                                                if conn.get('status') == 'healthy'),
                        'monitoring_stats': self.connection_monitor.stats
                    }
                except:
                    connection_metrics = {'status': 'connection_monitor_unavailable'}
        
            # ✅ Rate limiter stats
            rate_limiter_stats = {}
            try:
                if hasattr(self, 'source_client') and hasattr(self.source_client, 'rate_limiter'):
                    if hasattr(self.source_client.rate_limiter, 'stats'):
                        rate_limiter_stats['source'] = self.source_client.rate_limiter.stats
                    
                if hasattr(self, 'main_client') and hasattr(self.main_client, 'rate_limiter'):
                    if hasattr(self.main_client.rate_limiter, 'stats'):
                        rate_limiter_stats['main'] = self.main_client.rate_limiter.stats
            except:
                rate_limiter_stats = {'status': 'rate_limiter_unavailable'}
        
            # ✅ WebSocket stats
            websocket_stats = {}
            try:
                if hasattr(self, 'websocket_manager') and self.websocket_manager:
                    websocket_stats = self.websocket_manager.get_stats()
            except:
                websocket_stats = {'status': 'websocket_unavailable'}
        
            # ✅ Формируем полный отчет
            metrics_report = {
                'timestamp': datetime.utcnow().isoformat(),
                'system': system_metrics,
                'connections': connection_metrics,
                'rate_limiters': rate_limiter_stats,
                'websocket': websocket_stats,
                'performance': perf_summary
            }
        
            # ✅ Логируем метрики
            logger.info("📊 SYSTEM METRICS REPORT:")
            logger.info(f"System: Memory={system_metrics.get('memory_mb', 'N/A')}MB, "
                       f"CPU={system_metrics.get('cpu_percent', 'N/A')}%, "
                       f"Uptime={system_metrics.get('uptime_hours', 'N/A')}h")
        
            if connection_metrics:
                logger.info(f"Connections: {connection_metrics.get('active_connections', 'N/A')}/"
                           f"{connection_metrics.get('total_connections', 'N/A')} active")
        
            if websocket_stats:
                ws_status = websocket_stats.get('status', 'unknown')
                ws_messages = websocket_stats.get('messages_received', 0)
                logger.info(f"WebSocket: Status={ws_status}, Messages={ws_messages}")
        
            # ✅ Детальный JSON отчет для анализа
            logger.debug(f"DETAILED METRICS: {json.dumps(metrics_report, indent=2)}")
        
            # ✅ Production Logger metrics (если доступен)
            if 'prod_logger' in globals():
                try:
                    prod_logger.log_performance('system_metrics_report', 0.1, True, {
                        'metrics_summary': {
                            'memory_mb': system_metrics.get('memory_mb'),
                            'active_connections': connection_metrics.get('active_connections'),
                            'websocket_status': websocket_stats.get('status')
                        }
                    })
                except:
                    pass
        
        except Exception as e:
            logger.error(f"System metrics reporting error: {e}")

class CircuitBreaker:
    """Circuit Breaker pattern для защиты от каскадных сбоев"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0, expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half_open
    
    def record_success(self):
        """Записать успешную операцию"""
        self.failure_count = 0
        self.state = 'closed'
    
    def record_failure(self):
        """Записать неудачную операцию"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'open'
    
    def can_proceed(self) -> bool:
        """Проверить можно ли выполнить операцию"""
        if self.state == 'closed':
            return True
        elif self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half_open'
                return True
            return False
        elif self.state == 'half_open':
            return True
        
        return False


class SignalType(Enum):
    POSITION_OPEN = "position_open"
    POSITION_CLOSE = "position_close" 
    POSITION_MODIFY = "position_modify"
    ORDER_FILL = "order_fill"
    ORDER_CANCEL = "order_cancel"

class ConnectionStatus(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    ERROR = "error"

class HealthStatus(Enum):
    OK = "ok"
    WARNING = "warning" 
    CRITICAL = "critical"

@dataclass
class TradingSignal:
    signal_type: SignalType
    symbol: str
    side: str
    size: float
    price: float
    timestamp: float
    metadata: Dict[str, Any]
    priority: int = 1  # 1=low, 2=normal, 3=high

@dataclass 
class HealthCheck:
    component: str
    status: HealthStatus
    message: str
    details: Dict[str, Any] = None

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_float(value, default=0.0):
    """Безопасное преобразование в float"""
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

# Глобальная enterprise сессия для Telegram alerts
_telegram_enterprise_session: Optional[aiohttp.ClientSession] = None

async def get_telegram_enterprise_session() -> aiohttp.ClientSession:
    """
    CRITICAL FIX: Get or create optimized session for Telegram alerts
    Заменяет временные сессии на переиспользуемую enterprise сессию
    """
    global _telegram_enterprise_session
    
    if _telegram_enterprise_session is None or _telegram_enterprise_session.closed:
        # Создаем оптимизированную сессию для Telegram
        connector = aiohttp.TCPConnector(
            limit=10,                    # Небольшой pool для Telegram
            limit_per_host=5,           # Лимит для api.telegram.org
            ttl_dns_cache=300,          # DNS cache 5 минут
            use_dns_cache=True,         # Enable DNS caching
            keepalive_timeout=30,       # Keep connections alive
            enable_cleanup_closed=True, # Clean up closed connections
        )
        
        timeout = aiohttp.ClientTimeout(
            total=15,        # Увеличен timeout для Telegram
            connect=5,       # Connection timeout
            sock_read=10     # Read timeout
        )
        
        _telegram_enterprise_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Bybit-Trading-Bot/1.0'}  # Identify our bot
        )
        
        logger.debug("Telegram enterprise session created")
    
    return _telegram_enterprise_session

async def cleanup_telegram_session():
    """Cleanup функция для Telegram сессии"""
    global _telegram_enterprise_session
    
    if _telegram_enterprise_session and not _telegram_enterprise_session.closed:
        await _telegram_enterprise_session.close()
        _telegram_enterprise_session = None
        logger.debug("Telegram enterprise session closed")

async def send_telegram_alert(message: str) -> bool:
    """
    CRITICAL FIX: Отправка критических алертов в Telegram с enterprise connection management
    ЗАМЕНЯЕТ временные сессии на оптимизированную переиспользуемую сессию
    """
    try:
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            logger.debug("Telegram credentials not configured")
            return False
        
        # Ограничиваем длину сообщения
        if len(message) > 4000:
            message = message[:3900] + "\n\n[Сообщение обрезано]"
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"🚨 SYSTEM ALERT: {message}",
            "parse_mode": "HTML",
            "disable_web_page_preview": True  # Уменьшает размер ответа
        }
        
        # CRITICAL FIX: Используем enterprise сессию вместо временной
        session = await get_telegram_enterprise_session()
        
        start_time = time.time()
        
        async with session.post(url, json=data) as response:  # json вместо data для лучшей производительности
            response_time = time.time() - start_time
            success = response.status == 200
            
            if success:
                logger.debug(f"Telegram alert sent successfully in {response_time:.3f}s")
            else:
                response_text = await response.text()
                logger.warning(f"Telegram alert failed: HTTP {response.status}, {response_text}")
            
            return success
                
    except asyncio.TimeoutError:
        logger.error("Telegram alert timeout - network issue")
        return False
    except aiohttp.ClientError as e:
        logger.error(f"Telegram alert network error: {e}")
        return False
    except Exception as e:
        logger.error(f"Telegram alert unexpected error: {e}")
        return False

# ДОПОЛНИТЕЛЬНО: Добавить в cleanup функции системы
async def cleanup_all_enterprise_sessions():
    """
    CRITICAL: Cleanup всех enterprise сессий при завершении системы
    ДОБАВИТЬ ВЫЗОВ ЭТОЙ ФУНКЦИИ в cleanup методы FinalTradingMonitor
    """
    try:
        await cleanup_telegram_session()
        logger.info("All enterprise sessions cleaned up")
    except Exception as e:
        logger.error(f"Error cleaning up enterprise sessions: {e}")

# ================================
# ✅ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЕ WEBSOCKET ФУНКЦИИ (из диагностического тестирования)
# ================================

def get_websockets_version():
    """Получение версии websockets библиотеки"""
    try:
        import websockets
        return websockets.__version__
    except:
        return "unknown"

def is_websocket_open(ws) -> bool:
    """
    ✅ ИСПРАВЛЕННАЯ проверка состояния WebSocket для websockets 15.0.1
    
    НА ОСНОВЕ РЕЗУЛЬТАТОВ ДИАГНОСТИЧЕСКОГО ТЕСТИРОВАНИЯ:
    - ws.closed НЕ СУЩЕСТВУЕТ в ClientConnection
    - ws.state.name = "OPEN" - РАБОТАЕТ
    - ws.close_code = None - РАБОТАЕТ
    """
    if not ws:
        return False
        
    try:
        # ✅ МЕТОД 1: websockets 15.0.1 с state.name (ПРИОРИТЕТ)
        # Тест показал: ws.state.name = "OPEN" (str)
        if hasattr(ws, 'state') and hasattr(ws.state, 'name'):
            state_name = ws.state.name.upper()
            is_open = 'OPEN' in state_name
            logger.debug(f"WebSocket state check: {ws.state.name} -> {is_open}")
            return is_open
        
        # ✅ МЕТОД 2: fallback через close_code
        # Тест показал: ws.close_code = None (для открытых)
        if hasattr(ws, 'close_code'):
            is_open = ws.close_code is None
            logger.debug(f"WebSocket close_code check: {ws.close_code} -> {is_open}")
            return is_open
        
        # ✅ МЕТОД 3: проверка методов (всегда работает)
        # Тест показал: hasattr(ws, "send") = True, hasattr(ws, "recv") = True
        has_methods = hasattr(ws, 'send') and hasattr(ws, 'recv')
        logger.debug(f"WebSocket methods check: send={hasattr(ws, 'send')}, recv={hasattr(ws, 'recv')} -> {has_methods}")
        return has_methods
        
    except Exception as e:
        logger.debug(f"WebSocket state check error: {e}")
        return False

async def close_websocket_safely(ws):
    """
    ✅ ИСПРАВЛЕННОЕ безопасное закрытие WebSocket для websockets 15.0.1
    
    НА ОСНОВЕ РЕЗУЛЬТАТОВ ДИАГНОСТИЧЕСКОГО ТЕСТИРОВАНИЯ:
    - await ws.close() работает идеально (0.036s)
    - Все 5 методов закрытия работают без ошибок
    - Рекомендуется проверка состояния перед закрытием
    """
    if not ws:
        return
        
    try:
        # ✅ ИСПРАВЛЕНО: Проверяем состояние перед закрытием (новый метод)
        if is_websocket_open(ws):
            # Тест показал: await ws.close() работает за 0.036s
            await asyncio.wait_for(ws.close(), timeout=5.0)
            logger.debug("WebSocket closed successfully")
        else:
            logger.debug("WebSocket was already closed")
            
    except asyncio.TimeoutError:
        logger.warning("WebSocket close timeout")
    except Exception as e:
        # Тест показал: нет ошибок при закрытии, но на всякий случай логируем
        logger.debug(f"Error closing WebSocket: {e}")
        # Игнорируем ошибки закрытия - главное не упасть

async def diagnose_websocket_issue(ws, name="WebSocket"):
    """
    ✅ НОВАЯ диагностическая функция для отладки проблем с WebSocket
    
    ИСПОЛЬЗОВАНИЕ:
    await diagnose_websocket_issue(self.ws, "MyWebSocket")
    """
    print(f"\n🔍 ДИАГНОСТИКА WEBSOCKET: {name}")
    print(f"   Тип объекта: {type(ws).__name__}")
    print(f"   websockets версия: {get_websockets_version()}")
    
    # Проверяем доступные атрибуты
    checks = [
        ("ws.closed", lambda: getattr(ws, 'closed', 'NOT_FOUND')),
        ("ws.state", lambda: getattr(ws, 'state', 'NOT_FOUND')),
        ("ws.state.name", lambda: getattr(getattr(ws, 'state', None), 'name', 'NOT_FOUND') if hasattr(ws, 'state') else 'NO_STATE'),
        ("ws.close_code", lambda: getattr(ws, 'close_code', 'NOT_FOUND')),
    ]
    
    for check_name, check_func in checks:
        try:
            result = check_func()
            status = "✅" if result != 'NOT_FOUND' else "❌"
            print(f"   {status} {check_name}: {result}")
        except Exception as e:
            print(f"   ❌ {check_name}: ERROR - {e}")
    
    # Тестируем нашу исправленную функцию
    is_open = is_websocket_open(ws)
    print(f"   🔧 is_websocket_open(): {is_open}")
    
    return {
        'type': type(ws).__name__,
        'version': get_websockets_version(),
        'is_open': is_open
    }

# ================================
# ПРОДВИНУТАЯ СИСТЕМА API (БЕЗ ИЗМЕНЕНИЙ - РАБОТАЕТ КОРРЕКТНО)
# ================================


class AdvancedRateLimiterPro:
    """
    🚀 PRODUCTION-GRADE RATE LIMITING СИСТЕМА
    
    Основано на исследованиях Bybit API лимитов:
    - REST API: 600 запросов за 5 секунд = 120/мин
    - WebSocket: 10 подключений/сек
    - Институциональные лимиты с августа 2025
    """
    
    def __init__(self, 
                 requests_per_minute: int = 120,
                 requests_per_second: int = 10,
                 burst_allowance: int = 20,
                 adaptive_mode: bool = True):
        
        # Основные лимиты
        self.rpm_limit = requests_per_minute
        self.rps_limit = requests_per_second
        self.burst_allowance = burst_allowance
        self.adaptive_mode = adaptive_mode
        
        # Sliding window tracking
        self.requests_per_minute = deque()
        self.requests_per_second = deque()
        self.requests_per_5sec = deque()  # Bybit специфичный лимит
        
        # Advanced features
        self.priority_queue = {
            'critical': deque(),  # Экстренные запросы
            'high': deque(),     # Торговые операции
            'normal': deque(),   # Обычные запросы
            'low': deque()       # Статистика/мониторинг
        }
        
        # Adaptive throttling
        self.current_latency = 0
        self.error_rate = 0
        self.adaptive_factor = 1.0
        
        # Rate limit headers tracking (X-Bapi-Limit)
        self.server_limits = {}
        self.limit_reset_times = {}
        
        # Monitoring
        self.stats = {
            'total_requests': 0,
            'throttled_requests': 0,
            'burst_used': 0,
            'adaptive_adjustments': 0,
            'priority_bypasses': 0
        }
        
        self.lock = asyncio.Lock()
        logger.info(f"Advanced Rate Limiter initialized: {self.rpm_limit}/min, {self.rps_limit}/sec")
    
    async def acquire(self, priority: str = 'normal', endpoint: str = None) -> Dict[str, Any]:
        """
        🎯 ИНТЕЛЛЕКТУАЛЬНОЕ ПОЛУЧЕНИЕ РАЗРЕШЕНИЯ НА ЗАПРОС
        
        Args:
            priority: 'critical', 'high', 'normal', 'low'
            endpoint: Конкретный endpoint для per-endpoint лимитов
        """
        async with self.lock:
            now = time.time()
            
            # Очистка устаревших записей (sliding window)
            self._cleanup_old_requests(now)
            
            # Проверка server-side лимитов из headers
            if endpoint and endpoint in self.server_limits:
                if not self._check_server_limits(endpoint, now):
                    return await self._wait_for_server_limit_reset(endpoint)
            
            # Адаптивная корректировка лимитов
            if self.adaptive_mode:
                self._adjust_adaptive_limits()
            
            # Проверка приоритетных запросов
            if priority in ['critical', 'high']:
                bypass_reason = self._check_priority_bypass(priority, now)
                if bypass_reason:
                    return await self._execute_priority_request(now, bypass_reason)
            
            # Основная логика rate limiting
            wait_times = self._calculate_wait_times(now)
            
            if any(wait_times.values()):
                max_wait = max(wait_times.values())
                logger.warning(f"Rate limit hit, waiting {max_wait:.2f}s")
                await self._intelligent_wait(max_wait, priority)
            
            # Регистрируем запрос
            return self._register_request(now, priority, endpoint)
    
    def _check_priority_bypass(self, priority: str, now: float) -> Optional[str]:
        """Проверка возможности bypass для приоритетных запросов"""
        try:
            # Критические запросы всегда проходят
            if priority == 'critical':
                self.stats['priority_bypasses'] += 1
                return 'critical_bypass'
        
            # High priority проходит если система не перегружена
            if priority == 'high':
                current_rps = len(self.requests_per_second)
                if current_rps < (self.rps_limit * 0.8):  # Менее 80% лимита
                    self.stats['priority_bypasses'] += 1
                    return 'high_priority_bypass'
        
            return None
        
        except Exception as e:
            logger.debug(f"Priority bypass check error: {e}")
            return None

    async def _execute_priority_request(self, now: float, bypass_reason: str) -> Dict[str, Any]:
        """Выполнение приоритетного запроса с bypass"""
        try:
            # Регистрируем запрос
            self.requests_per_minute.append(now)
            self.requests_per_second.append(now)
            self.requests_per_5sec.append(now)
            self.stats['total_requests'] += 1
        
            return {
                'timestamp': now,
                'priority': 'bypassed',
                'bypass_reason': bypass_reason,
                'adaptive_factor': self.adaptive_factor,
                'requests_in_window': {
                    'rpm': len(self.requests_per_minute),
                    'rps': len(self.requests_per_second),
                    '5sec': len(self.requests_per_5sec)
                }
            }
        
        except Exception as e:
            logger.error(f"Priority request execution error: {e}")
            return self._register_request(now, 'high', None)

    def _check_server_limits(self, endpoint: str, now: float) -> bool:
        """Проверка server-side лимитов из headers"""
        try:
            if endpoint not in self.server_limits:
                return True
        
            server_limit = self.server_limits[endpoint]
            reset_time = self.limit_reset_times.get(endpoint, 0)
        
            # Если время сброса прошло, лимит обновился
            if now > reset_time:
                return True
        
            # Проверяем оставшиеся запросы
            # Это упрощенная логика, в реальности нужно отслеживать текущее использование
            return True
        
        except Exception as e:
            logger.debug(f"Server limits check error: {e}")
            return True

    async def _wait_for_server_limit_reset(self, endpoint: str) -> Dict[str, Any]:
        """Ожидание сброса server-side лимита"""
        try:
            reset_time = self.limit_reset_times.get(endpoint, 0)
            current_time = time.time()
        
            if reset_time > current_time:
                wait_time = reset_time - current_time
                logger.info(f"Waiting for server limit reset: {wait_time:.1f}s")
                await asyncio.sleep(min(wait_time, 60))  # Максимум минута
        
            return self._register_request(time.time(), 'normal', endpoint)
        
        except Exception as e:
            logger.error(f"Server limit wait error: {e}")
            return self._register_request(time.time(), 'normal', endpoint)

    def _cleanup_old_requests(self, now: float):
        """Очистка устаревших запросов из sliding windows"""
        # 1 минута window
        while self.requests_per_minute and now - self.requests_per_minute[0] > 60:
            self.requests_per_minute.popleft()
        
        # 1 секунда window  
        while self.requests_per_second and now - self.requests_per_second[0] > 1:
            self.requests_per_second.popleft()
            
        # 5 секунд window (Bybit специфичный)
        while self.requests_per_5sec and now - self.requests_per_5sec[0] > 5:
            self.requests_per_5sec.popleft()
    
    def _calculate_wait_times(self, now: float) -> Dict[str, float]:
        """Расчет времени ожидания для каждого лимита"""
        wait_times = {}
        
        # RPM check
        adjusted_rpm = int(self.rpm_limit * self.adaptive_factor)
        if len(self.requests_per_minute) >= adjusted_rpm:
            wait_times['rpm'] = 60 - (now - self.requests_per_minute[0])
        
        # RPS check
        adjusted_rps = int(self.rps_limit * self.adaptive_factor)
        if len(self.requests_per_second) >= adjusted_rps:
            wait_times['rps'] = 1 - (now - self.requests_per_second[0])
            
        # Bybit 5-sec window (600 requests per 5 seconds)
        if len(self.requests_per_5sec) >= 600:
            wait_times['5sec'] = 5 - (now - self.requests_per_5sec[0])
        
        return wait_times
    
    def _adjust_adaptive_limits(self):
        """Адаптивная корректировка лимитов на основе производительности"""
        # Увеличиваем лимиты при хорошей производительности
        if self.current_latency < 100 and self.error_rate < 0.01:
            self.adaptive_factor = min(1.2, self.adaptive_factor + 0.05)
            
        # Уменьшаем при проблемах с производительностью
        elif self.current_latency > 500 or self.error_rate > 0.05:
            self.adaptive_factor = max(0.5, self.adaptive_factor - 0.1)
            self.stats['adaptive_adjustments'] += 1
    
    async def _intelligent_wait(self, wait_time: float, priority: str):
        """Интеллектуальное ожидание с учетом приоритета"""
        self.stats['throttled_requests'] += 1
        
        # Критичные запросы ждут меньше
        if priority == 'critical':
            wait_time *= 0.5
        elif priority == 'high':
            wait_time *= 0.7
        elif priority == 'low':
            wait_time *= 1.5
            
        # Добавляем небольшой jitter для избежания thundering herd
        jitter = wait_time * 0.1 * (hash(asyncio.current_task()) % 100) / 100
        await asyncio.sleep(wait_time + jitter)
    
    def _register_request(self, now: float, priority: str, endpoint: str) -> Dict[str, Any]:
        """Регистрация выполненного запроса"""
        self.requests_per_minute.append(now)
        self.requests_per_second.append(now)
        self.requests_per_5sec.append(now)
        self.stats['total_requests'] += 1
        
        return {
            'timestamp': now,
            'priority': priority,
            'endpoint': endpoint,
            'adaptive_factor': self.adaptive_factor,
            'requests_in_window': {
                'rpm': len(self.requests_per_minute),
                'rps': len(self.requests_per_second),
                '5sec': len(self.requests_per_5sec)
            }
        }
    
    def update_from_response_headers(self, headers: Dict[str, str], endpoint: str):
        """Обновление лимитов на основе response headers от Bybit"""
        try:
            # X-Bapi-Limit: текущий лимит для endpoint
            if 'X-Bapi-Limit' in headers:
                self.server_limits[endpoint] = int(headers['X-Bapi-Limit'])
            
            # X-Bapi-Limit-Status: оставшиеся запросы
            if 'X-Bapi-Limit-Status' in headers:
                remaining = int(headers['X-Bapi-Limit-Status'])
                if remaining < 5:  # Близко к лимиту
                    logger.warning(f"Endpoint {endpoint} limit almost reached: {remaining} remaining")
            
            # X-Bapi-Limit-Reset-Timestamp: время сброса лимита
            if 'X-Bapi-Limit-Reset-Timestamp' in headers:
                reset_time = int(headers['X-Bapi-Limit-Reset-Timestamp']) / 1000
                self.limit_reset_times[endpoint] = reset_time
                
        except (ValueError, KeyError) as e:
            logger.debug(f"Error parsing rate limit headers: {e}")


class AWSTimeSyncPro:
    """
    🌏 ПРОФЕССИОНАЛЬНАЯ СИНХРОНИЗАЦИЯ ВРЕМЕНИ С СЕРВЕРАМИ BYBIT

    Требования Bybit: клиентский timestamp должен попадать в окно ±5s
    Реализация хранит базовую точку серверного времени вместе с monotonic(),
    чтобы исключить скачки из-за NTP/сна/ресинков VM.
    """

    def __init__(self):
        # Публичные поля (оставляем как в твоей версии)
        self.time_offset: float = 0.0      # смещение "клиент -> сервер" в мс (для метрик/логов)
        self.last_sync: float = 0.0        # unix seconds последней успешной синхронизации
        self.sync_interval: int = 300      # 5 минут
        self.sync_accuracy: float = 0.0    # точность последней синхронизации (±мс)

        # Источники времени
        if IS_PROD:
            logger.info("AWSTimeSyncPro: Using PRODUCTION time sources.")
            self.time_sources = [
                "https://api.bybit.com/v5/market/time",
                "https://api.bybit.com/v5/public/time",
            ]
        else:
            logger.info("AWSTimeSyncPro: Using DEMO and PRODUCTION time sources for dev.")
            self.time_sources = [
                "https://api-demo.bybit.com/v5/market/time",
                "https://api-demo.bybit.com/v5/public/time",
                "https://api.bybit.com/v5/market/time", # В dev можно использовать и прод для сравнения
                "https://api.bybit.com/v5/public/time",
            ]

        # Статистика синхронизации
        self.sync_stats = {
            "successful_syncs": 0,
            "failed_syncs": 0,
            "average_accuracy": 0.0,
            "max_drift": 0.0,
            "last_sync_source": None,
        }

        # История для сглаживания
        self.drift_samples = deque(maxlen=10)
        self.rtt_samples = deque(maxlen=10)

        # 🔧 Внутренняя "опора времени" (монолитная база)
        self._server_ms0: Optional[int] = None  # серверное время в момент синка (мс)
        self._mono0: Optional[float] = None     # time.monotonic() в момент синка (сек)

    # ------------------------- ПУБЛИЧНЫЙ ИНТЕРФЕЙС (как у тебя) -------------------------

    async def sync_server_time(self, api_url: str) -> bool:
        """
        🎯 ВЫСОКОТОЧНАЯ СИНХРОНИЗАЦИЯ ВРЕМЕНИ (алгоритм NTP с выбором лучшего замера)
        public: сохраняем старое имя и сигнатуру
        """
        best_accuracy = float("inf")
        best_offset = 0.0
        best_source = None
        successful_sources = 0
        old_offset = self.time_offset

        for source_url in self.time_sources:
            try:
                measurements = []
                # несколько замеров с короткой паузой
                for _ in range(3):
                    result = await self._single_time_measurement(source_url)
                    if result:
                        measurements.append(result)
                        await asyncio.sleep(0.1)

                if measurements:
                    # лучший по минимальному RTT
                    best_measurement = min(measurements, key=lambda x: x["rtt"])
                    if best_measurement["accuracy"] < best_accuracy:
                        best_accuracy = best_measurement["accuracy"]
                        best_offset = best_measurement["offset"]
                        best_source = source_url
                        successful_sources += 1

            except Exception as e:
                logger.warning(f"Time sync failed for {source_url}: {e}")
                self.sync_stats["failed_syncs"] += 1

        if successful_sources == 0:
            logger.error("All time sync sources failed")
            return False

        # Калибровка смещения по истории дрейфа
        calibrated_offset = self._apply_drift_calibration(best_offset)

        # Фиксируем базу: серверное время на момент синхронизации и монотонный якорь
        client_epoch_ms_now = int(time.time() * 1000)
        self._server_ms0 = int(client_epoch_ms_now + calibrated_offset)
        self._mono0 = time.monotonic()

        # Обновляем «старые» публичные поля для совместимости и метрик
        self.time_offset = float(calibrated_offset)
        if old_offset != self.time_offset:
            logger.info(f"TIME_OFFSET_UPDATE old={old_offset:.1f} new={self.time_offset:.1f}")
        self.sync_accuracy = float(best_accuracy)
        self.last_sync = time.time()
        self.sync_stats["successful_syncs"] += 1
        self.sync_stats["average_accuracy"] = (
            self.sync_stats["average_accuracy"] * 0.9 + best_accuracy * 0.1
        )
        self.sync_stats["last_sync_source"] = best_source

        logger.info(
            "Time sync successful: offset=%.1fms, accuracy=±%.1fms (source=%s)",
            self.time_offset,
            self.sync_accuracy,
            best_source,
        )
        return True

    async def ensure_time_sync(self, api_url: str) -> bool:
        """
        Гарантирует, что база времени актуальна (как в твоем коде).
        """
        need_sync = (
            self._server_ms0 is None
            or self._mono0 is None
            or (time.time() - self.last_sync) > self.sync_interval
        )
        if need_sync:
            return await self.sync_server_time(api_url)
        return True

    def get_server_time(self) -> int:
        """
        Получение серверного времени (ms) с компенсацией дрейфа.
        Сохраняем название и синхронность.
        """
        # Если уже есть монотонная база — используем её (устойчиво к ресинкам/сну)
        if self._server_ms0 is not None and self._mono0 is not None:
            elapsed_ms = (time.monotonic() - self._mono0) * 1000.0
            return int(self._server_ms0 + elapsed_ms)

        # Fallback: старый путь (epoch + time_offset), пригоден до первого успешного sync()
        current_local_time = time.time() * 1000.0
        time_since_sync = max(0.0, time.time() - self.last_sync)
        estimated_drift = time_since_sync * 0.0003  # ~0.3мс/ч — как было у тебя
        compensated_time = current_local_time + self.time_offset - estimated_drift
        return int(compensated_time)

    def get_server_timestamp(self) -> int:
        """
        ✅ Синхронный метод — именно такой ты вызываешь в коде:
            await self.time_sync.ensure_time_sync(self.api_url)
            timestamp = str(self.time_sync.get_server_timestamp())
        """
        return self.get_server_time()

    # ------------------------------- ВНУТРЕННИЕ МЕТОДЫ -------------------------------

    async def _single_time_measurement(self, url: str) -> Optional[Dict[str, Union[float, str]]]:
        """
        Один замер времени: расчёт offset по NTP-подобной схеме.
        Возвращает словарь с полями:
          offset, accuracy, rtt, server_time, source
        """
        try:
            # t1/t4 измеряем в epoch-мс, как у тебя (достаточно для точности окон Bybit)
            t1 = time.time() * 1000.0
            timeout = aiohttp.ClientTimeout(total=5)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    t4 = time.time() * 1000.0

                    if response.status != 200:
                        return None

                    data = await response.json(content_type=None)

                    # retCode=0 для market/public time — нормальный ответ
                    if (isinstance(data, dict) and data.get("retCode", 0) == 0
                            and isinstance(data.get("result"), dict)):

                        # Достаём серверное время в мс из разных вариантов полей
                        result = data["result"]
                        server_time = self._extract_server_time_ms(result)
                        if server_time is None:
                            return None

                        # RTT/2 — половина пути
                        rtt = float(t4 - t1)
                        network_delay = rtt / 2.0

                        # На сервере t2≈t3=server_time, берём компенсированное время получения
                        adjusted_server_time = server_time + network_delay

                        # Средняя точка клиентского времени
                        client_time_mid = (t1 + t4) / 2.0

                        # Offset и точность
                        offset = float(adjusted_server_time - client_time_mid)
                        accuracy = network_delay  # половина RTT

                        # Статистика
                        self.rtt_samples.append(rtt)

                        return {
                            "offset": float(offset),
                            "accuracy": float(accuracy),
                            "rtt": float(rtt),
                            "server_time": float(server_time),
                            "source": url,
                        }

        except Exception as e:
            logger.debug(f"Time measurement failed for {url}: {e}")
            return None

    @staticmethod
    def _extract_server_time_ms(result: Dict) -> Optional[int]:
        """
        Аккуратно вытаскиваем миллисекунды из разных ответов Bybit.
        Поддерживаются:
          - timeNano (наносекунды)
          - timeMicro / timeMillis / timeMilli (микро/миллисекунды)
          - timeSecond (секунды) — умножаем на 1000
        """
        try:
            if "timeNano" in result:
                # перевод в мс с округлением вниз
                return int(int(result["timeNano"]) / 1_000_000)
            if "timeMicro" in result:
                return int(int(result["timeMicro"]) / 1_000)
            if "timeMillis" in result:
                return int(result["timeMillis"])
            if "timeMilli" in result:
                return int(result["timeMilli"])
            if "timeSecond" in result:
                return int(result["timeSecond"]) * 1000
        except Exception:
            return None
        return None

    def _apply_drift_calibration(self, raw_offset: float) -> float:
        """
        Калибровка на основе анализа дрейфа часов. Сохраняем публичное поведение.
        """
        if self.time_offset != 0.0:
            drift = raw_offset - self.time_offset
            self.drift_samples.append(drift)

        if len(self.drift_samples) >= 5:
            avg_drift = sum(self.drift_samples) / len(self.drift_samples)
            corrected_offset = raw_offset - (avg_drift * 0.1)
            return float(corrected_offset)

        return float(raw_offset)


class EnhancedBybitClient:
    """Улучшенный клиент Bybit API с промышленной обработкой ошибок"""
    
    def __init__(self, api_key: str, api_secret: str, api_url: str, name: str = "client", copy_state=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_url = api_url
        self.name = name
        self.copy_state = copy_state
        
        # Системы управления
        self.rate_limiter = AdvancedRateLimiterPro(
            requests_per_minute=120,
            requests_per_second=10,
            adaptive_mode=True
        )
        self.time_sync = AWSTimeSyncPro()
        
        # Статистика и мониторинг
        self.request_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0,
            'last_error': None
        }
        
        # Retry конфигурация
        self.max_retries = 3
        self.retry_delays = [1, 2, 5]
        
        # CRITICAL FIX: Enterprise connection management
        self.enterprise_connector = EnterpriseBybitConnector()
        self.resilience_manager = NetworkResilienceManager()
        
        # Connection cleanup registration
        import atexit
        atexit.register(self._cleanup_on_exit)
        
        logger.info(f"{self.name} - Enhanced client initialized with enterprise connection management")

    async def get_wallet_balance(self, account_type: str = "UNIFIED"):
        """
        Обёртка над /v5/account/wallet-balance (возвращает JSON Bybit как есть).
        Использует общий низкоуровневый стек _make_request_with_retry → _make_single_request.
        """
        logger.info(f"[{self.name}] Fetching wallet balance for account_type='{account_type}', source=REST")
        params = {"accountType": account_type}
        return await self._make_request_with_retry("GET", "account/wallet-balance", params)

    def _cleanup_on_exit(self):
        """Cleanup on process exit"""
        try:
            if hasattr(self, 'enterprise_connector') and self.enterprise_connector:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.cleanup_connections())
                loop.close()
        except Exception as e:
            logger.error(f"Exit cleanup error: {e}")
        
    def _generate_signature(self, timestamp: str, recv_window: str, query_string: str = "", body: str = "") -> str:
        """V5: HMAC-SHA256(timestamp + api_key + recv_window + query/body) → hex"""
        if not isinstance(recv_window, str):
            recv_window = str(recv_window)
        signature_payload = f"{timestamp}{self.api_key}{recv_window}{query_string}{body}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signature_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        logger.debug(f"{self.name} - Signature payload length: {len(signature_payload)}")
        return signature

    
    async def _make_request_with_retry(self, method: str, endpoint: str, params: dict = None, data: dict = None) -> Optional[dict]:
        """Выполнение запроса с retry логикой и exponential backoff.

        ВАЖНО: статистику успешных запросов обновляет _make_single_request.
        Здесь не инкрементируем successful_requests, чтобы не удваивать метрики.
        """
        for attempt in range(self.max_retries + 1):
            try:
                # Все метрики total/success/fail ведёт _make_single_request
                return await self._make_single_request(method, endpoint, params, data)
            except Exception as e:
                if "API error 10002" in str(e):
                    req_timestamp = int(data.get('timestamp', 0)) if data else 0
                    req_delta = (time.time() * 1000) - req_timestamp if req_timestamp else -1
                    logger.warning(
                        f"API_10002_RESYNC req_delta={req_delta:.0f}ms offset_before={self.time_sync.time_offset:.1f}"
                    )
                    await self.time_sync.sync_server_time(self.api_url)
                else:
                    logger.warning(f"{self.name} - Request attempt {attempt + 1} failed: {e}")

                if attempt < self.max_retries:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    logger.info(f"{self.name} - Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"{self.name} - All retry attempts failed for {endpoint}")
                    self.request_stats['failed_requests'] += 1
                    self.request_stats['last_error'] = str(e)
                    return None


    async def get_symbol_filters(self, symbol: str, category: str = "linear") -> dict:
        """
        Кэшированный запрос v5/market/instruments-info: шаг лота и тика цены.
        Возвращает dict: {'min_qty','qty_step','tick_size','min_notional'}
        """
        try:
            if not hasattr(self, "instrument_cache"):
                self.instrument_cache = {}

            cache = self.instrument_cache.get(symbol)
            if cache and (time.time() - cache['ts'] < 3600):
                return cache['filters']

            params = {"category": category, "symbol": symbol}
            res = await self._make_request_with_retry("GET", "market/instruments-info", params)

            filters = {'min_qty': 0.0, 'qty_step': 0.001, 'tick_size': 0.01, 'min_notional': 0.0}
            item = (res or {}).get("result", {}).get("list", [])
            if item:
                item = item[0]
                lot   = item.get("lotSizeFilter", {})
                price = item.get("priceFilter", {})
                filters['min_qty']      = safe_float(lot.get("minOrderQty", 0.0))
                filters['qty_step']     = safe_float(lot.get("qtyStep", 0.001))
                filters['tick_size']    = safe_float(price.get("tickSize", 0.01))
                filters['min_notional'] = safe_float(lot.get("minNotionalValue", lot.get("minOrderValue", 0.0)))

            self.instrument_cache[symbol] = {'ts': time.time(), 'filters': filters}
            return filters

        except Exception as e:
            logger.warning(f"get_symbol_filters failed for {symbol}: {e}")
            return {'min_qty': 0.0, 'qty_step': 0.001, 'tick_size': 0.01, 'min_notional': 0.0}

    async def invalidate_caches(self):
        """Clears all internal caches to force re-fetching of data."""
        logger.info(f"[{self.name}] Invalidating all internal caches...")
        if hasattr(self, "instrument_cache"):
            self.instrument_cache.clear()
            logger.info(f"[{self.name}] Cleared instrument_cache (symbol filters).")
        # Add any other caches here in the future
        await asyncio.sleep(0) # Yield control to allow other tasks to run

    async def _make_single_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, allow_ret_codes: list = None) -> Optional[dict]:
        """
        CRITICAL FIX: Unified single request with enterprise connection management and detailed diagnostics.
        ЗАМЕНЯЕТ ОБА СТАРЫХ ПОДХОДА на один оптимизированный.
        """
        start_time = time.time()
        result = None
        response_data = None
        url = f"{self.api_url}/v5/{endpoint}" # Определяем url в начале для логирования ошибок
        safe_headers_for_log = {}
        body = ""

        try:
            # Rate limiting
            await self.rate_limiter.acquire()

            # Синхронизация времени
            await self.time_sync.ensure_time_sync(self.api_url)

            # CRITICAL FIX: Use enterprise session instead of temporary sessions
            session = await self.get_or_create_enterprise_session()

            # Подготовка запроса
            await self.time_sync.ensure_time_sync(self.api_url)
            if abs(self.time_sync.time_offset) > 400:
                logger.warning(f"Time offset {self.time_sync.time_offset}ms > 400ms, forcing resync.")
                await self.time_sync.sync_server_time(self.api_url)

            timestamp = str(self.time_sync.get_server_time())
            recv_window = str(BYBIT_RECV_WINDOW)

            query_string = ""

            if endpoint.startswith("v5/"):
                endpoint = endpoint[3:]

            if method == "GET" and params:
                sorted_params = dict(sorted(params.items()))
                query_string = urlencode(sorted_params)
                url += f"?{query_string}"
            elif method == "POST" and data:
                sorted_data = dict(sorted(data.items()))
                body = json.dumps(sorted_data, separators=(',', ':'))

            # Генерация подписи
            signature = self._generate_signature(timestamp, recv_window, query_string, body)

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
                "X-BAPI-SIGN": signature,
                "Content-Type": "application/json"
            }

            # --- DIAGNOSTICS: Логируем запрос ПЕРЕД отправкой ---
            safe_headers_for_log = headers.copy()
            safe_headers_for_log["X-BAPI-API-KEY"] = f"***{self.api_key[-4:]}"
            safe_headers_for_log["X-BAPI-SIGN"] = "***"
            logger.debug(f"[{self.name}] API REQ -> {method} {url}")
            logger.debug(f"[{self.name}] API REQ HEADERS: {safe_headers_for_log}")
            if body:
                logger.debug(f"[{self.name}] API REQ BODY: {body}")

            # CRITICAL FIX: Execute request with proper session reuse
            self.request_stats['total_requests'] += 1
            response = None
            if method == "GET":
                async with session.get(url, headers=headers) as resp:
                    response = resp
                    response_data = await response.json()
            else:
                async with session.post(url, headers=headers, data=body) as resp:
                    response = resp
                    response_data = await response.json()

            # --- DIAGNOSTICS: Логируем ответ ПОСЛЕ получения ---
            ret_code = response_data.get('retCode')
            ret_msg = response_data.get('retMsg')
            result_list = (response_data.get('result') or {}).get('list', [])
            items_count = len(result_list) if isinstance(result_list, list) else 0

            log_preview = ""
            if items_count > 0:
                log_preview = f", preview: {json.dumps(result_list[:2])}"

            logger.debug(
                f"[{self.name}] API RSP <- {method} {url} | retCode: {ret_code}, retMsg: '{ret_msg}', items: {items_count}{log_preview}"
            )


            # CRITICAL FIX: Process response headers for rate limiting
            if hasattr(self.rate_limiter, 'update_from_response_headers') and response.headers:
                try:
                    self.rate_limiter.update_from_response_headers(
                        dict(response.headers), endpoint)
                except Exception as e:
                    logger.debug(f"{self.name} - Error processing response headers: {e}")

            # Обновляем статистику времени ответа
            response_time = time.time() - start_time
            self._update_response_time_stats(response_time)

            # Обработка ответа
            if response.status == 200:
                if ret_code == 0 or (allow_ret_codes and ret_code in allow_ret_codes):
                    logger.debug(f"{self.name} - Request successful: {endpoint} (retCode: {ret_code})")
                    result = response_data
                    self.request_stats['successful_requests'] += 1
                    if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = True
                    return result
                else:
                    # Специальная обработка критических ошибок
                    if ret_code == 10003:  # Invalid signature
                        logger.critical(f"{self.name} - Invalid signature error!")
                        if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = False
                        raise Exception(f"Signature error: {ret_msg}")
                    elif ret_code == 10002: # Request expired
                            logger.warning(f"{self.name} - Timestamp error (10002): {ret_msg}")
                            raise Exception(f"API error 10002: {ret_msg}")
                    elif ret_code == 10006:  # Rate limit exceeded
                        logger.warning(f"{self.name} - Rate limit exceeded")
                        if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = False
                        raise Exception(f"Rate limit: {ret_msg}")
                    else:
                        if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = False
                        raise Exception(f"API error {ret_code}: {ret_msg}")
            else:
                if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = False
                raise Exception(f"HTTP {response.status}: {response_data}")

        except Exception as e:
            if self.copy_state and self.name == "MAIN": self.copy_state.main_rest_ok = False
            self.request_stats['failed_requests'] += 1
            self.request_stats['last_error'] = str(e)
            # Логируем ошибку с максимальной информацией
            logger.error(f"[{self.name}] API Request failed for {endpoint}: {e}")
            logger.debug(f"[{self.name}] Failed request details: URL={url}, Headers={safe_headers_for_log}, Body={body}")
            if response_data:
                logger.error(f"[{self.name}] Failed response data: {response_data}")
            raise

        return result

    async def get_or_create_enterprise_session(self) -> aiohttp.ClientSession:
        """
        CRITICAL FIX: Get or create optimized session with connection pooling
        ЗАМЕНЯЕТ старую систему на enterprise-grade управление сессиями
        """
    
        # Проверяем существует ли enterprise connector
        if not hasattr(self, 'enterprise_connector'):
            # Создаем enterprise connector если его нет
            self.enterprise_connector = EnterpriseBybitConnector()
    
        # Возвращаем optimized session
        return await self.enterprise_connector.create_session()

    async def cleanup_connections(self):
        """
        CRITICAL: Proper cleanup of all connections
        """
        try:
            # Cleanup enterprise connector
            if hasattr(self, 'enterprise_connector'):
                await self.enterprise_connector.close()
        
            # Cleanup old session if exists
            if hasattr(self, 'session') and not self.session.closed:
                await self.session.close()
            
            logger.info(f"{self.name} - All connections cleaned up")
        
        except Exception as e:
            logger.error(f"{self.name} - Error cleaning up connections: {e}")

    async def cleanup(self):
        """
        Alias: внешний “чистый” shutdown клиента.
        Делает то же самое, что cleanup_connections().
        """
        try:
            await self.cleanup_connections()
        except Exception as e:
            logger.error(f"{self.name} - Error in cleanup(): {e}")


    def get_connection_stats(self) -> Dict[str, Any]:
        """Get comprehensive connection statistics"""
        stats = {
            'request_stats': self.request_stats,
            'enterprise_connector': None,
            'rate_limiter': None
        }
    
        if hasattr(self, 'enterprise_connector'):
            stats['enterprise_connector'] = self.enterprise_connector.connection_stats
    
        if hasattr(self.rate_limiter, 'get_performance_stats'):
            stats['rate_limiter'] = self.rate_limiter.get_performance_stats()
    
        return stats

    async def _make_request_with_detailed_timing(self, method: str, endpoint: str, params: dict = None, data: dict = None) -> dict:
        """
        🔬 PERFORMANCE PROFILING: Детальное отслеживание времени запросов
    
        CRITICAL FIX: Updated to use EnterpriseBybitConnector instead of temporary sessions
        Этот метод добавляет detailed timing для диагностики performance проблем
        """
        timings = {
            'rate_limiting': 0,
            'time_sync': 0, 
            'request_prep': 0,
            'session_acquire': 0,
            'http_request': 0,
            'http_response': 0,
            'total_time': 0
        }

        overall_start = time.time()
        result = None

        try:
            # Rate limiting
            rate_start = time.time()
            await self.rate_limiter.acquire()
            timings['rate_limiting'] = time.time() - rate_start
    
            # Time synchronization
            sync_start = time.time()
            await self.time_sync.ensure_time_sync(self.api_url)  # Асинхронно обеспечиваем синхронизацию
            server_time = self.time_sync.get_server_time()       # Синхронно получаем время
            timings['time_sync'] = time.time() - sync_start
    
            # Request preparation
            prep_start = time.time()
            timestamp = str(server_time)
            recv_window = str(BYBIT_RECV_WINDOW)
    
            url = f"{self.api_url}/v5/{endpoint}"
            query_string = ""
            body = ""
    
            # DIAGNOSTIC: Log market/tickers requests with category
            if endpoint == "market/tickers" and params:
                logger.info(f"{self.name} - Requesting market/tickers with category: {params.get('category', 'MISSING')} for symbol: {params.get('symbol', 'N/A')}")
            # Prevent double v5 prefix
            if endpoint.startswith("v5/"):
                endpoint = endpoint[3:]  # Remove v5/ prefix if already present

            if method == "GET" and params:
                sorted_params = dict(sorted(params.items()))
                query_string = urlencode(sorted_params)
                url += f"?{query_string}"
            elif method == "POST" and data:
                body = json.dumps(data)
    
            # Generate signature
            signature = self._generate_signature(timestamp, recv_window, query_string, body)
    
            headers = {
                'X-BAPI-API-KEY': self.api_key,
                'X-BAPI-SIGN': signature,
                'X-BAPI-SIGN-TYPE': '2',
                'X-BAPI-TIMESTAMP': timestamp,
                'X-BAPI-RECV-WINDOW': recv_window,
                'Content-Type': 'application/json'
            }
    
            timings['request_prep'] = time.time() - prep_start
    
            # CRITICAL FIX: Use enterprise session instead of old session management
            session_start = time.time()
            session = await self.get_or_create_enterprise_session()
            timings['session_acquire'] = time.time() - session_start
    
            # HTTP Request with circuit breaker protection
            http_start = time.time()
        
            # CRITICAL FIX: Execute request with resilience manager
            async def _execute_http_request():
                if method == "GET":
                    async with session.get(url, headers=headers) as response:
                        timings['http_request'] = time.time() - http_start
                
                        response_start = time.time()
                        response_text = await response.text()
                        timings['http_response'] = time.time() - response_start
                
                        # CRITICAL FIX: Update rate limiter with response headers
                        if hasattr(self.rate_limiter, 'update_from_response_headers') and response.headers:
                            try:
                                self.rate_limiter.update_from_response_headers(
                                    dict(response.headers), 
                                    endpoint
                                )
                            except Exception as e:
                                logger.debug(f"{self.name} - Error processing response headers: {e}")
                
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}: {response_text}")
                
                        try:
                            result = json.loads(response_text)
                            return result, response_text
                        except json.JSONDecodeError as e:
                            raise Exception(f"Invalid JSON: {e}")
                
                elif method == "POST":
                    async with session.post(url, headers=headers, data=body) as response:
                        timings['http_request'] = time.time() - http_start
                
                        response_start = time.time()
                        response_text = await response.text()
                        timings['http_response'] = time.time() - response_start
                
                        # CRITICAL FIX: Update rate limiter with response headers
                        if hasattr(self.rate_limiter, 'update_from_response_headers') and response.headers:
                            try:
                                self.rate_limiter.update_from_response_headers(
                                    dict(response.headers), 
                                    endpoint
                                )
                            except Exception as e:
                                logger.debug(f"{self.name} - Error processing response headers: {e}")
                
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}: {response_text}")
                
                        try:
                            result = json.loads(response_text)
                            return result, response_text
                        except json.JSONDecodeError as e:
                            raise Exception(f"Invalid JSON: {e}")
        
            # CRITICAL FIX: Execute with circuit breaker protection
            result, response_text = await self.resilience_manager.call_with_circuit_breaker(_execute_http_request)
    
            # Check API response
            if result.get('retCode') != 0:
                error_msg = result.get('retMsg', 'Unknown API error')
                raise Exception(f"API error: {error_msg}")
    
            timings['total_time'] = time.time() - overall_start
    
            # Логируем detailed timings если запрос медленный
            if timings['total_time'] > 5.0:  # Медленнее 5 секунд
                logger.warning(f"SLOW REQUEST DETECTED: {method} {endpoint}")
                logger.warning(f"Timing breakdown: {timings}")
                logger.warning(f"URL: {url}")
                logger.warning(f"Response size: {len(response_text) if 'response_text' in locals() else 0} bytes")
            
                # CRITICAL FIX: Log enterprise connector stats for slow requests
                if hasattr(self, 'enterprise_connector') and self.enterprise_connector:
                    connector_stats = self.enterprise_connector.connection_stats
                    logger.warning(f"Connection stats: {connector_stats}")
            elif timings['total_time'] > 2.0:  # Предупреждение для запросов > 2s
                logger.info(f"MODERATE DELAY: {method} {endpoint} took {timings['total_time']:.3f}s")
                logger.debug(f"Timing breakdown: {timings}")
    
            # Обновляем статистику
            self.request_stats['total_requests'] += 1
            self.request_stats['successful_requests'] += 1
    
            # Вычисляем среднее время отклика
            total_requests = self.request_stats['total_requests']
            old_avg = self.request_stats.get('avg_response_time', 0)
            self.request_stats['avg_response_time'] = (old_avg * (total_requests - 1) + timings['total_time']) / total_requests
        
            # CRITICAL FIX: Update enterprise connector stats
            if hasattr(self, 'enterprise_connector') and self.enterprise_connector:
                self.enterprise_connector.connection_stats['total_requests'] += 1
                if timings.get('session_acquire', 0) < 0.001:  # Session was reused
                    self.enterprise_connector.connection_stats['reused_connections'] += 1
                else:  # New connection created
                    self.enterprise_connector.connection_stats['new_connections'] += 1
    
            logger.debug(f"✅ {method} {endpoint} completed in {timings['total_time']:.3f}s (Enterprise mode)")
    
            return result.get('result', {})
    
        except Exception as e:
            timings['total_time'] = time.time() - overall_start
    
            self.request_stats['total_requests'] += 1
            self.request_stats['failed_requests'] += 1
            self.request_stats['last_error'] = str(e)
    
            logger.error(f"Request failed: {method} {endpoint}")
            logger.error(f"Error: {e}")
            logger.error(f"Total time before failure: {timings['total_time']:.3f}s")
            logger.error(f"Detailed timings: {timings}")
        
            # CRITICAL FIX: Log circuit breaker state on failure
            if hasattr(self, 'resilience_manager'):
                circuit_state = self.resilience_manager.state
                failure_count = self.resilience_manager.failure_count
                logger.error(f"Circuit breaker state: {circuit_state.value}, failure count: {failure_count}")
    
            raise

    def _update_response_time_stats(self, response_time: float):
        """Обновление статистики времени ответа"""
        if self.request_stats['avg_response_time'] == 0:
            self.request_stats['avg_response_time'] = response_time
        else:
            # Скользящее среднее
            self.request_stats['avg_response_time'] = (
                self.request_stats['avg_response_time'] * 0.9 + response_time * 0.1
            )
    
    # Публичные методы API (БЕЗ ИЗМЕНЕНИЙ - РАБОТАЮТ КОРРЕКТНО)
    async def get_balance(self) -> float:
        """Получить баланс USDT (V5, корректная подпись и окно, детерминированный query)"""
        try:
            params = {
                "accountType": "UNIFIED",
                "coin": "USDT",
            }

            # 1) Гарантируем синхронизацию времени (используем серверный оффсет)
            await self.time_sync.ensure_time_sync(self.api_url)
            timestamp = str(self.time_sync.get_server_timestamp())  # ms

            # 2) Увеличаем окно до 20s (устойчиво к сетевому джиттеру)
            recv_window = str(BYBIT_RECV_WINDOW)

            # 3) Детерминированный queryString (сортировка ключей + URL-encoding)
            #    Важно: именно этот string участвует в подписи V5
            from urllib.parse import quote_plus
            query_items = [f"{k}={quote_plus(str(params[k]))}" for k in sorted(params)]
            query_string = "&".join(query_items)

            url = f"{self.api_url}/v5/account/wallet-balance?{query_string}"

            # 4) Корректная подпись (синхронная функция → без await)
            signature = self._generate_signature(timestamp, recv_window, query_string, "")

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
                "X-BAPI-SIGN": signature,
                "Content-Type": "application/json",
            }

            self.request_stats['total_requests'] += 1
            session = await self.get_or_create_enterprise_session()

            async with session.get(url, headers=headers) as response:
                response_text = await response.text()

                if hasattr(self.rate_limiter, 'update_from_response_headers') and response.headers:
                    try:
                        self.rate_limiter.update_from_response_headers(
                            dict(response.headers), 
                            "account/wallet-balance"
                        )
                    except Exception as e:
                        logger.debug(f"{self.name} - Error processing response headers: {e}")

                if response.status == 200:
                    try:
                        result = json.loads(response_text)
                        if result and result.get('retCode') == 0:
                            accounts = result.get('result', {}).get('list', [])
                            if accounts:
                                for coin in accounts[0].get('coin', []):
                                    if coin.get('coin') == 'USDT':
                                        balance = safe_float(coin.get('walletBalance', 0))
                                        logger.info(f"{self.name} - Balance: {balance:.2f} USDT")
                                        self.request_stats['successful_requests'] += 1
                                        return balance
                            logger.warning(f"{self.name} - No USDT balance found")
                            self.request_stats['successful_requests'] += 1
                            return 0.0

                        logger.error(f"{self.name} - API retCode != 0: {result}")
                        self.request_stats['failed_requests'] += 1
                        return 0.0

                    except json.JSONDecodeError as e:
                        logger.error(f"{self.name} - Invalid JSON response: {e}")
                        self.request_stats['failed_requests'] += 1
                        return 0.0

                else:
                    logger.error(f"{self.name} - HTTP {response.status}: {response_text}")
                    self.request_stats['failed_requests'] += 1
                    return 0.0

        except Exception as e:
            logger.error(f"{self.name} - Balance error: {e}")
            self.request_stats['failed_requests'] += 1
            return 0.0

    
    async def get_positions(self, category: str = "linear", symbol: str = None, settleCoin: str = "USDT") -> List[dict]:
        """Получить открытые позиции с улучшенной обработкой"""
        try:
            params = {
                "category": category,
                "settleCoin": settleCoin,
                "limit": 50
            }
            if symbol:
                params['symbol'] = symbol
            
            result = await self._make_request_with_retry("GET", "position/list", params)
            
            if result and result.get('retCode') == 0:
                positions = result.get('result', {}).get('list', [])
                active_positions = []
                
                for pos in positions:
                    size = safe_float(pos.get('size', 0))
                    if size > 0:
                        active_positions.append(pos)
                
                logger.info(f"{self.name} - Found {len(active_positions)} active positions")
                return active_positions
            
            return []
            
        except Exception as e:
            logger.error(f"{self.name} - Positions error: {e}")
            return []
    
    async def get_recent_trades(self, limit: int = 50) -> List[dict]:
        """Получить последние сделки с улучшенной обработкой"""
        try:
            params = {
                "category": "linear",
                "limit": limit
            }
            
            result = await self._make_request_with_retry("GET", "execution/list", params)
            
            if result and result.get('retCode') == 0:
                trades = result.get('result', {}).get('list', [])
                logger.debug(f"{self.name} - Retrieved {len(trades)} recent trades")
                return trades
            
            return []
            
        except Exception as e:
            logger.error(f"{self.name} - Recent trades error: {e}")
            return []

    async def place_order(self, category: str, symbol: str, side: str, orderType: str, qty: str, reduceOnly: bool = False, **kwargs) -> Optional[dict]:
        """Place an order."""
        try:
            data = {
                "category": category,
                "symbol": symbol,
                "side": side,
                "orderType": orderType,
                "qty": str(qty),
                "reduceOnly": reduceOnly,
            }
            data.update(kwargs)

            logger.info(f"{self.name} - Placing order: {symbol} {side} {qty} {orderType}")
            result = await self._make_request_with_retry("POST", "order/create", data=data)

            if result and result.get('retCode') == 0:
                logger.info(f"{self.name} - Order placed successfully: {result.get('result')}")
                return result.get('result')
            else:
                error_msg = result.get('retMsg', 'Unknown error') if result else 'No response'
                logger.error(f"{self.name} - Failed to place order: {error_msg}")
                return None

        except Exception as e:
            logger.error(f"{self.name} - Order placement error: {e}", exc_info=True)
            return None

    async def set_leverage(self, category: str, symbol: str, leverage: str, on_success_callback: Optional[Callable] = None) -> dict:
        """Sets leverage and calls a callback on success. Returns the raw API response."""
        try:
            data = {
                "category": category,
                "symbol": symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage),
            }
            logger.info(f"{self.name} - Setting leverage for {symbol} to {leverage}x")

            result = await self._make_single_request(
                "POST",
                "position/set-leverage",
                data=data,
                allow_ret_codes=[110043]
            )

            ret_code = (result or {}).get("retCode")

            if ret_code == 110043:
                logger.info(f"Leverage for {symbol} is already {leverage}x (retCode: 110043).")
            else:
                logger.info(f"Leverage for {symbol} set to {leverage}x (retCode: {ret_code}).")

            if on_success_callback:
                try:
                    on_success_callback(symbol, int(leverage))
                except Exception as cb_exc:
                    logger.error(f"Error in set_leverage on_success_callback: {cb_exc}", exc_info=True)

            return result

        except Exception as e:
            logger.error(f"{self.name} - Set leverage error for {symbol}: {e}", exc_info=True)
            return {"retCode": -1, "retMsg": str(e), "result": {}}

    async def add_margin(self, symbol: str, margin: str, position_idx: int = 0) -> dict:
        """Adds margin to an isolated margin position."""
        try:
            data = {
                "category": "linear",
                "symbol": symbol,
                "margin": str(margin),
                "positionIdx": position_idx,
            }
            logger.info(f"{self.name} - Adding margin to {symbol}: {margin} USDT")
            result = await self._make_single_request("POST", "position/add-margin", data=data)

            ret_code = (result or {}).get("retCode")
            ret_msg = (result or {}).get("retMsg", "Unknown error")

            if ret_code == 0:
                logger.info(f"Successfully added margin to {symbol}.")
                return {"success": True, "result": result}
            else:
                logger.error(f"Failed to add margin to {symbol}: {ret_msg} (retCode: {ret_code})")
                return {"success": False, "error": ret_msg, "result": result}

        except Exception as e:
            logger.error(f"{self.name} - Add margin error for {symbol}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    async def close_all_positions_by_market(self) -> Tuple[int, int]:
        """Closes all open linear positions by market order."""
        closed_count = 0
        errors_count = 0
        logger.warning(f"{self.name} - Initiating PANIC CLOSE of all positions.")

        try:
            positions = await self.get_positions()
            if not positions:
                logger.info(f"{self.name} - No open positions to close.")
                return 0, 0

            for pos in positions:
                symbol = pos.get('symbol')
                size = pos.get('size')
                side = pos.get('side')

                if not symbol or not size or not side:
                    logger.warning(f"{self.name} - Skipping invalid position: {pos}")
                    continue

                close_side = "Sell" if side == "Buy" else "Buy"

                try:
                    result = await self.place_order(
                        category='linear',
                        symbol=symbol,
                        side=close_side,
                        orderType='Market',
                        qty=str(size),
                        reduceOnly=True
                    )
                    if result and result.get('orderId'):
                        logger.info(f"{self.name} - Successfully placed closing order for {symbol}. Order ID: {result.get('orderId')}")
                        closed_count += 1
                    else:
                        logger.error(f"{self.name} - Failed to place closing order for {symbol}. Result: {result}")
                        errors_count += 1

                    await asyncio.sleep(0.2)

                except Exception as e:
                    logger.error(f"{self.name} - Error closing position for {symbol}: {e}", exc_info=True)
                    errors_count += 1

        except Exception as e:
            logger.error(f"{self.name} - Critical error during close_all_positions_by_market: {e}", exc_info=True)
            errors_count += len(positions) - closed_count

        logger.warning(f"{self.name} - Panic close summary: Closed={closed_count}, Errors={errors_count}")
        return closed_count, errors_count
    
    def get_stats(self) -> dict:
        """Получить статистику клиента (корректный success_rate 0..100%)"""
        stats = self.request_stats.copy()

        total = int(stats.get('total_requests', 0) or 0)
        ok    = int(stats.get('successful_requests', 0) or 0)

        if total > 0:
            # на всякий случай ограничим ok в пределах [0, total]
            if ok < 0:
                ok = 0
            elif ok > total:
                ok = total
            rate = (ok / total) * 100.0
        else:
            # нет запросов — считаем систему «чистой»
            rate = 100.0

        # жёсткая нормализация 0..100
        stats['success_rate'] = max(0.0, min(100.0, rate))

        # безопасное значение среднего времени ответа
        avg = stats.get('avg_response_time')
        if avg is None or (isinstance(avg, (int, float)) and avg < 0):
            stats['avg_response_time'] = 0.0

        return stats


# ================================
# ✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ СИСТЕМА WEBSOCKET (с интегрированными фиксами)
# ================================

class FinalFixedWebSocketManager:
    """
    ✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ система WebSocket с интегрированными фиксами
    
    ИНТЕГРИРОВАННЫЕ ИСПРАВЛЕНИЯ:
    - ✅ Заменена is_websocket_open() на исправленную версию
    - ✅ Заменена close_websocket_safely() на исправленную версию
    - ✅ Исправлено свойство closed для совместимости с тестами
    - ✅ Добавлена диагностическая функция
    - ✅ Полная совместимость с websockets 15.0.1
    """
    
    def __init__(self, api_key: str, api_secret: str, name: str = "websocket", copy_state=None, final_monitor=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.name = name
        self.copy_state = copy_state
        self.final_monitor = final_monitor
        
        # Состояние WebSocket
        self.ws = None
        self.status = ConnectionStatus.DISCONNECTED
        
        # ✅ ping/pong атрибуты (как в тестере)
        self.last_ping = 0
        self.last_pong = 0
        self.ping_timeout = WEBSOCKET_PONG_TIMEOUT
        
        # Управление задачами
        self.active_tasks = set()
        self.should_stop = False
        self._heartbeat_task = None
        
        # Переподключение
        self.reconnect_attempt = 0
        self.max_reconnect_attempts = MAX_RECONNECT_ATTEMPTS
        self.reconnect_delays = RECONNECT_DELAYS
        
        # Подписки и обработчики
        self.subscriptions = {} # Карта топиков для переподписки topic -> params
        self.message_handlers = {}
        self.message_buffer = deque(maxlen=1000)
        # Новое с исправлением WebSocket
        # КРИТИЧНО: Добавить очередь сообщений (используется в get_stats)
        self.message_queue = asyncio.Queue(maxsize=1000)
        
        # Инициализация _position_states (используется в _handle_position_update)
        self._position_states = {}
        
        # Дополнительные атрибуты из патча (опционально, но рекомендуется)
        self._event_cache = OrderedDict()
        self._event_cache_ttl = 300  # 5 минут TTL
        self._max_cache_size = 10000
        
        # Флаг копирования плеча
        self.copy_leverage = os.getenv('COPY_LEVERAGE', 'true').lower() == 'true'
        
        # Очередь для копирования (если используется система копирования)
        self._copy_queue = asyncio.Queue(maxsize=1000)
        self._copy_processor_task = None
        
        # Последняя ресинхронизация
        self._last_resync = 0
        self._resync_interval = 60
        
        # Статистика
        self.stats = {
            'ws_received_total': 0,
            'ws_processed_private': 0,
            'messages_received': 0,
            'messages_processed': 0,
            'connection_drops': 0,
            'last_message_time': 0,
            'uptime_start': time.time(),
            'ping_pong_success': 0,
            'ping_pong_failures': 0,
            'topic_counts': defaultdict(lambda: {'received': 0, 'processed': 0})
        }
        
        # Система обработки сообщений
        self.processing_active = False
        
        logger.info(f"{self.name} - WebSocket manager initialized (websockets v{get_websockets_version()})")
    
    # ✅ ИСПРАВЛЕННОЕ свойство для совместимости с тестами 
    @property
    def closed(self) -> bool:
        """✅ ИСПРАВЛЕНО: Совместимость с тестами - используем новую функцию"""
        return not is_websocket_open(self.ws)

    async def connect(self):
        """✅ ИСПРАВЛЕННОЕ подключение - с интегрированными фиксами"""
        try:
            if self.status == ConnectionStatus.CONNECTING:
                logger.debug(f"{self.name} - Already connecting, skipping")
                return
                
            self.status = ConnectionStatus.CONNECTING
            logger.info(f"{self.name} - Connecting to WebSocket...")
            
            # ✅ ИСПРАВЛЕНО: Подключение без asyncio.wait_for обертки
            self.ws = await websockets.connect(
                SOURCE_WS_URL,
                ping_interval=None,      # ✅ КРИТИЧНО: отключаем WebSocket автопинг
                ping_timeout=None,       # ✅ КРИТИЧНО: отключаем WebSocket автотimeout  
                close_timeout=10,        # Только close timeout
                max_size=1048576,        # 1MB max message size
                max_queue=16             # Max queued messages
            )
            
            self.status = ConnectionStatus.CONNECTED
            logger.info(f"{self.name} - ✅ WebSocket connected (auto ping/pong DISABLED)")
            
            # ✅ ДИАГНОСТИРУЕМ WEBSOCKET ОБЪЕКТ
            await diagnose_websocket_issue(self.ws, f"{self.name}_Connected")
            
            # Аутентификация
            await self._authenticate_bybit_v5()
            
            # Подписка на события
            await self._subscribe_to_events()
            
            # Запускаем ТОЛЬКО Bybit heartbeat (НЕ websockets ping)
            await self._start_heartbeat()
            
        except Exception as e:
            logger.error(f"{self.name} - Connection error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            self.status = ConnectionStatus.ERROR
            raise
    
    async def _authenticate_bybit_v5(self):
        """Аутентификация для Bybit API v5"""
        try:
            expires = int(time.time() * 1000) + 10000
            
            # Правильная подпись для Bybit API v5
            signature_payload = f"GET/realtime{expires}"
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                signature_payload.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            
            auth_message = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }
            
            logger.debug(f"{self.name} - Sending auth: {auth_message}")
            
            # ✅ ИСПРАВЛЕНО: Проверяем состояние перед отправкой
            if not is_websocket_open(self.ws):
                raise Exception("WebSocket not open for authentication")
                
            await self.ws.send(json.dumps(auth_message))
            
            # Ждем подтверждение аутентификации
            try:
                response = await asyncio.wait_for(self.ws.recv(), timeout=10)
                auth_response = json.loads(response)
                
                logger.debug(f"{self.name} - Auth response: {auth_response}")
                
                # Проверка успешной аутентификации для Bybit v5
                is_authenticated = (
                    auth_response.get('success') is True or
                    auth_response.get('retCode') == 0 or
                    'success' in str(auth_response).lower()
                )
                
                if is_authenticated:
                    self.status = ConnectionStatus.AUTHENTICATED
                    if self.copy_state: self.copy_state.source_ws_ok = True
                    masked_key = f"{self.api_key[:6]}...{self.api_key[-4:]}" if self.api_key else "N/A"
                    logger.info(f"{self.name} - ✅ WebSocket authenticated successfully for key {masked_key} (account_id={DONOR_ACCOUNT_ID})")
                else:
                    if self.copy_state: self.copy_state.source_ws_ok = False
                    raise Exception(f"Authentication failed: {auth_response}")
                    
            except asyncio.TimeoutError:
                if self.copy_state: self.copy_state.source_ws_ok = False
                raise Exception("Authentication timeout after 10 seconds")
                
        except Exception as e:
            if self.copy_state: self.copy_state.source_ws_ok = False
            logger.error(f"{self.name} - Authentication error: {e}")
            await send_telegram_alert(f"WebSocket authentication failed for {self.name}: {e}")
            raise
    
    async def _subscribe_to_events(self):
        """Подписка на критически важные события с Performance Logging"""
    
        start_time = time.time()
        operation_name = "websocket_subscribe_to_events"
        success = False

        topics_to_subscribe = {
            "position": "position",
            "wallet": "wallet",
            "execution": "execution",
            "order": "order"
        }
    
        try:
            args = list(topics_to_subscribe.values())
            subscribe_message = {"op": "subscribe", "args": args}
        
            if not is_websocket_open(self.ws):
                raise Exception("WebSocket not open for subscription")
        
            await self.ws.send(json.dumps(subscribe_message))
            self.subscriptions.update(topics_to_subscribe)
        
            logger.info(f"{self.name} - Subscribed to: {args}")
            success = True
        
        except Exception as e:
            logger.error(f"{self.name} - Subscription error: {e}")
            if 'prod_logger' in globals():
                prod_logger.log_error(e, {'component': 'websocket_subscribe_to_events', 'websocket_name': self.name, 'subscriptions': args}, send_alert=True)
            raise
        
        finally:
            duration = time.time() - start_time
            if 'prod_logger' in globals():
                try:
                    prod_logger.log_performance(operation_name, duration, success, {
                        'websocket_name': self.name,
                        'subscriptions_count': len(topics_to_subscribe),
                        'subscriptions': list(topics_to_subscribe.keys()),
                        'subscribe_time_ms': round(duration * 1000, 2)
                    })
                except Exception as perf_log_error:
                    logger.debug(f"WebSocket subscribe performance logging error: {perf_log_error}")

    async def resubscribe_all(self):
        """Переподписывается на все ранее сохраненные топики."""
        if not self.subscriptions:
            logger.info(f"[{self.name}] No topics to resubscribe to.")
            return 0

        topics_to_resubscribe = list(self.subscriptions.values())
        subscribe_message = {"op": "subscribe", "args": topics_to_resubscribe}

        try:
            if not is_websocket_open(self.ws):
                raise ConnectionError("Cannot resubscribe, WebSocket is not open.")

            await self.ws.send(json.dumps(subscribe_message))

            num_resubscribed = len(topics_to_resubscribe)
            logger.info(f"HOT_RELOAD_WS_RESUBSCRIBED: Successfully sent resubscription request for {num_resubscribed} topics: {topics_to_resubscribe}")
            logger.info(f"WS_RESUBSCRIBED: topics={topics_to_resubscribe} count={num_resubscribed}")
            return num_resubscribed

        except Exception as e:
            logger.error(f"[{self.name}] HOT_RELOAD_WS_RESUBSCRIBE_FAILED: Failed to resubscribe to topics. Error: {e}", exc_info=True)
            raise
    
    async def _start_heartbeat(self):
        """Запуск heartbeat механизма"""
        try:
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self.active_tasks.add(self._heartbeat_task)
            logger.info(f"{self.name} - Bybit custom heartbeat started")
        except Exception as e:
            logger.error(f"{self.name} - Error starting heartbeat: {e}")
    
    async def _heartbeat_loop(self):
        """✅ ИСПРАВЛЕННЫЙ heartbeat цикл с интегрированными фиксами"""
        try:
            while not self.should_stop and is_websocket_open(self.ws):
                await asyncio.sleep(WEBSOCKET_PING_INTERVAL)
                
                if is_websocket_open(self.ws) and not self.should_stop:
                    await self._send_bybit_ping()
                    
                    # Проверяем timeout pong
                    await asyncio.sleep(2)  # Даем время на получение pong
                    if self.last_ping > 0 and self.last_pong < self.last_ping:
                        pong_delay = time.time() - self.last_ping
                        if pong_delay > self.ping_timeout:
                            logger.warning(f"{self.name} - Bybit pong timeout: {pong_delay:.1f}s")
                            self.stats['ping_pong_failures'] += 1
                            # Не разрываем соединение сразу, даем еще один шанс
                            if self.stats['ping_pong_failures'] > 3:
                                logger.error(f"{self.name} - Too many Bybit ping/pong failures")
                                break
                    
        except asyncio.CancelledError:
            logger.debug(f"{self.name} - Heartbeat cancelled")
        except Exception as e:
            logger.error(f"{self.name} - Heartbeat error: {e}")
        finally:
            if self._heartbeat_task in self.active_tasks:
                self.active_tasks.discard(self._heartbeat_task)
    
    async def _send_bybit_ping(self):
        """✅ ИСПРАВЛЕННАЯ отправка Bybit custom ping"""
        try:
            if is_websocket_open(self.ws):
                # Bybit требует именно такой формат
                ping_message = {
                    "op": "ping",
                    "req_id": str(int(time.time() * 1000))
                }
            
                await self.ws.send(json.dumps(ping_message))
                self.last_ping = time.time()
                logger.debug(f"{self.name} - ✅ Bybit custom ping sent: {ping_message}")
            
            else:
                logger.debug(f"{self.name} - Cannot send Bybit ping, WebSocket not open")
            
        except Exception as e:
            logger.error(f"{self.name} - Bybit ping error: {e}")
    
    # ✅ Совместимость с тестами
    async def _send_ping(self):
        """Совместимость с тестами"""
        await self._send_bybit_ping()
    
    async def test_ping_pong(self, timeout=30.0):
        """✅ ИСПРАВЛЕННЫЙ тест Bybit custom ping/pong"""
        try:
            if not is_websocket_open(self.ws):
                return False, "WebSocket not open"
        
            # Запоминаем время до ping
            ping_time = time.time()
        
            # Отправляем Bybit custom ping
            await self._send_bybit_ping()
        
            # Ждем pong с увеличенным timeout
            start_wait = time.time()
            while time.time() - start_wait < timeout:
                if self.last_pong > ping_time:
                    delay = self.last_pong - ping_time  
                    return True, f"✅ Bybit pong received in {delay:.3f}s"
                await asyncio.sleep(0.2)  # Проверяем каждые 200ms
        
            return False, f"❌ Bybit pong timeout after {timeout}s"
        
        except Exception as e:
            return False, f"❌ Bybit ping/pong test error: {e}"

    async def _recv_loop(self):
        """
        Основной цикл получения и обработки сообщений WebSocket.
        """
        logger.info(f"{self.name} - Starting WebSocket recv loop...")
        while not self.should_stop and is_websocket_open(self.ws):
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=WEBSOCKET_TIMEOUT)
                
                # Инкремент счетчика полученных сообщений
                self.stats['ws_received_total'] = self.stats.get('ws_received_total', 0) + 1
                self.stats['messages_received'] = self.stats.get('messages_received', 0) + 1
                self.stats['last_message_time'] = time.time()
                
                # Логирование сырого сообщения
                logger.debug("RAW_WS_MSG %s", message[:800])

                # Обработка сообщения
                await self._process_message(message)
                
            except asyncio.TimeoutError:
                # Тайм-аут - это нормально, просто продолжаем цикл для проверки should_stop
                continue
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"{self.name} - WebSocket connection closed in recv loop: {e}")
                self.stats['connection_drops'] = self.stats.get('connection_drops', 0) + 1
                if not self.should_stop:
                    asyncio.create_task(self._handle_disconnect(f"ConnectionClosed: {e.code}"))
                break
            except Exception as e:
                logger.error(f"{self.name} - Error in recv loop: {e}", exc_info=True)
                # Небольшая пауза после неизвестной ошибки
                await asyncio.sleep(1)
        
        logger.info(f"{self.name} - WebSocket recv loop stopped.")

    async def _process_message_queue(self):
        """Обработчик очереди сообщений"""
        try:
            logger.info(f"{self.name} - Starting message queue processor")
            
            while not self.should_stop:
                try:
                    # Получаем сообщение из очереди с таймаутом
                    message = await asyncio.wait_for(
                        self.message_queue.get(),
                        timeout=1.0
                    )
                    
                    # Обрабатываем сообщение
                    await self._process_message(message)
                    
                    # Обновляем счетчик обработанных
                    self.stats['messages_processed'] = self.stats.get('messages_processed', 0) + 1
                    
                except asyncio.TimeoutError:
                    continue  # Проверяем флаги
                    
                except Exception as e:
                    logger.error(f"{self.name} - Queue processing error: {e}")
                    self.stats['processing_errors'] = self.stats.get('processing_errors', 0) + 1
                    
        except asyncio.CancelledError:
            logger.debug(f"{self.name} - Message processor cancelled")
            
        except Exception as e:
            logger.error(f"{self.name} - Fatal processor error: {e}")
            
        finally:
            # Очищаем оставшиеся сообщения
            while not self.message_queue.empty():
                try:
                    self.message_queue.get_nowait()
                except:
                    break
            
            logger.info(f"{self.name} - Message queue processor stopped")

    async def _process_message(self, message: str):
        """
        ✅ ЕДИНСТВЕННЫЙ правильный метод обработки сообщений
        Объединяет функционал обоих версий + добавляет полную обработку для копирования
        """
        start_time = time.time()
        operation_name = "websocket_handle_message"
        success = False

        try:
            # Логируем ВЕСЬ входящий трафик для полной диагностики
            logger.info(f"[{self.name}] RAW WS MSG: {message}")
            self.stats['raw_message_count'] = self.stats.get('raw_message_count', 0) + 1

            data = json.loads(message)

            # Буферизируем сообщение
            self.message_buffer.append({'timestamp': time.time(), 'data': data})

            # Критично: обработка Bybit pong
            if self._handle_bybit_pong_message(data):
                success = True
                return  # это pong

            # === Главная логика ===
            if 'topic' in data:
                topic = data['topic']
                if topic in ["position", "wallet", "execution", "order"]:
                    self.stats['ws_processed_private'] = self.stats.get('ws_processed_private', 0) + 1

                self.stats['topic_counts'][topic]['received'] += 1
                logger.info(f"[{self.name}] Received message for topic: '{topic}'")

                # Строгий роутер: Игнорируем служебные топики (snapshot, query, periodic)
                if '.' in topic:
                    logger.debug(f"[{self.name}] Ignoring service topic with '.' in name: '{topic}'")
                    return

                handler_called = False
                if topic == "position":
                    logger.info(f"[{self.name}] Routing to position handler.")
                    await self._handle_position_update(data)
                    handler_called = True
                elif topic == "execution":
                    logger.info(f"[{self.name}] Routing to execution handler.")
                    await self._handle_execution_update(data)
                    handler_called = True
                elif topic == "order":
                    logger.info(f"[{self.name}] Routing to order handler.")
                    await self._handle_order_update(data)
                    handler_called = True
                elif topic == "wallet":
                    logger.info(f"[{self.name}] Routing to wallet handler.")
                    await self._handle_wallet_update(data)
                    handler_called = True
                else:
                    logger.debug(f"[{self.name}] Unknown or unhandled topic: '{topic}'")

                # Инкрементируем счетчик только для успешно обработанных основных топиков
                if handler_called:
                    self.stats['messages_processed'] = self.stats.get('messages_processed', 0) + 1
                    self.stats['topic_counts'][topic]['processed'] += 1

            # Обработка системных сообщений
            elif data.get('op') == 'subscribe':
                if data.get('success'):
                    logger.info(f"{self.name} - ✅ Subscription confirmed: {data}")
                else:
                    logger.error(f"{self.name} - ❌ Subscription failed: {data}")

            elif data.get('op') == 'auth':
                if data.get('success'):
                    logger.info(f"{self.name} - ✅ Authentication confirmed")
                else:
                    logger.error(f"{self.name} - ❌ Authentication failed: {data}")

            success = True

        except json.JSONDecodeError as e:
            logger.error(f"{self.name} - JSON decode error: {e}")
            logger.debug(f"Raw message: {message[:200]}...")

            if hasattr(self, 'sys_logger'):
                self.sys_logger.log_error("WebSocket", f"JSON decode error: {str(e)}", {
                    "message_preview": message[:100],
                    "websocket_name": self.name
                })

            if 'prod_logger' in globals():
                prod_logger.log_error(e, {
                    'component': 'websocket_message_processing',
                    'message_preview': message[:100],
                    'websocket_name': self.name
                }, send_alert=False)

        except Exception as e:
            logger.error(f"{self.name} - Message processing error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")

            if hasattr(self, 'sys_logger'):
                self.sys_logger.log_error("WebSocket", str(e), {
                    "message_type": data.get('topic', 'unknown') if 'data' in locals() else 'parse_failed',
                    "websocket_name": self.name
                })

            if 'prod_logger' in globals():
                prod_logger.log_error(e, {
                    'component': 'websocket_message_processing',
                    'websocket_name': self.name,
                    'message_type': data.get('topic', 'unknown') if 'data' in locals() else 'unknown'
                }, send_alert=True)

        finally:
            duration = time.time() - start_time
            if duration > 0.1:
                logger.warning(f"{self.name} - Slow message processing: {duration:.3f}s")

            if 'prod_logger' in globals():
                try:
                    prod_logger.log_performance(
                        operation_name, duration, success,
                        {
                            'websocket_name': self.name,
                            'message_size': len(message) if message else 0,
                            'processing_time_ms': round(duration * 1000, 2)
                        }
                    )
                except Exception as perf_log_error:
                    logger.debug(f"WebSocket performance logging error: {perf_log_error}")


    # ===============================
    # 2. ДОБАВИТЬ НОВЫЙ МЕТОД send_message() 
    # ===============================

    async def send_message(self, message: dict) -> bool:
        """
        ✅ НОВЫЙ МЕТОД: Отправка сообщения через WebSocket с Performance Logging
    
        Args:
            message: Словарь с сообщением для отправки
    
        Returns:
            bool: True если отправлено успешно
        """
    
        # ✅ Performance tracking - начало (уже есть)
        start_time = time.time()
        operation_name = "websocket_send_message"
        success = False
    
        try:
            if not self.ws or not is_websocket_open(self.ws):
                logger.warning(f"{self.name} - Cannot send message: WebSocket not connected")
                return False
        
            # Конвертируем в JSON
            json_message = json.dumps(message)
        
            # Отправляем сообщение
            await self.ws.send(json_message)
        
            # Обновляем статистику
            self.stats['messages_sent'] = self.stats.get('messages_sent', 0) + 1
            logger.debug(f"{self.name} - Message sent: {message}")
        
            success = True
            return True
        
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"{self.name} - Connection closed while sending: {e}")
        
            # ✅ НОВОЕ: Логируем ошибку отправки в БД
            sys_logger.log_error("WebSocket", f"Connection closed during send: {str(e)}", {
                "websocket_name": self.name,
                "message_type": message.get('op', 'unknown')
            })
        
            # Сохраняем существующую логику с prod_logger
            if 'prod_logger' in globals():
                prod_logger.log_error(e, {
                    'component': 'websocket_send',
                    'websocket_name': self.name,
                    'message_type': message.get('op', 'unknown')
                }, send_alert=True)
            return False
        
        except Exception as e:
            logger.error(f"{self.name} - Send message error: {e}")
        
            # ✅ НОВОЕ: Логируем общую ошибку отправки в БД
            sys_logger.log_error("WebSocket", f"Send error: {str(e)}", {
                "websocket_name": self.name,
                "message_size": len(json_message) if 'json_message' in locals() else 0
            })
        
            # Сохраняем существующую логику с prod_logger
            if 'prod_logger' in globals():
                prod_logger.log_error(e, {
                    'component': 'websocket_send',
                    'websocket_name': self.name,
                    'message_size': len(json_message) if 'json_message' in locals() else 0
                }, send_alert=True)
            return False
        
        finally:
            # ✅ Performance logging (уже есть, просто сохраняем)
            duration = time.time() - start_time
        
            if 'prod_logger' in globals():
                try:
                    prod_logger.log_performance(operation_name, duration, success, {
                        'websocket_name': self.name,
                        'message_type': message.get('op', 'unknown'),
                        'message_size': len(json.dumps(message)) if message else 0,
                        'send_time_ms': round(duration * 1000, 2)
                    })
                except Exception as perf_log_error:
                    logger.debug(f"WebSocket send performance logging error: {perf_log_error}")

    
    def _handle_bybit_pong_message(self, data: dict) -> bool:
        """✅ ИСПРАВЛЕННАЯ обработка Bybit pong - точно как в тестере"""
        try:
            # Точно такая же логика как в тестере
            is_bybit_pong = (
                (data.get('op') == 'ping' and data.get('ret_msg') == 'pong') or
                (data.get('op') == 'pong') or 
                (data.get('success') is True and data.get('ret_msg') == 'pong') or
                (data.get('retCode') == 0 and data.get('op') == 'pong')
            )
            
            if is_bybit_pong:
                self.last_pong = time.time()
                self.stats['ping_pong_success'] += 1
                self.stats['ping_pong_failures'] = 0  # Сбрасываем счетчик неудач
                
                if self.last_ping > 0:
                    ping_delay = self.last_pong - self.last_ping
                    logger.debug(f"{self.name} - ✅ Bybit pong received: delay={ping_delay:.3f}s")
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"{self.name} - Error handling Bybit pong: {e}")
            return False

    async def _escalate_shutdown_after_timeout(self, timeout_seconds: int):
        """
        Schedules a graceful shutdown if the connection is not restored within the timeout.
        This is designed to be called from a non-blocking task.
        """
        try:
            logger.warning(f"WS_ESCALATE_SCHEDULED: Escalation to restart scheduled in {timeout_seconds}s.")
            await asyncio.sleep(timeout_seconds)

            # Check status again after waiting
            if not is_websocket_open(self.ws):
                logger.error(f"WS_ESCALATE_TRIGGER: WebSocket still disconnected after {timeout_seconds}s. Triggering monitor restart.")
                self.stats['ws_escalations_total'] = self.stats.get('ws_escalations_total', 0) + 1

                # Safely call monitor restart methods if the monitor reference exists
                if hasattr(self, 'final_monitor') and self.final_monitor:
                    if hasattr(self.final_monitor, "request_graceful_restart"):
                        await self.final_monitor.request_graceful_restart(reason="ws_escalation_timeout")
                    elif hasattr(self.final_monitor, "_request_full_restart"):
                        await self.final_monitor._request_full_restart("ws_escalation_timeout")
                    else:
                        logger.warning("No restart hook available on monitor; escalation logged only.")
                else:
                    logger.warning("final_monitor reference not found; escalation logged only.")
            else:
                logger.info("WS_ESCALATE_ABORTED: WebSocket reconnected before escalation timeout.")

        except asyncio.CancelledError:
            logger.info("WS_ESCALATE_ABORTED: Shutdown escalation task was cancelled.")
        except Exception as e:
            logger.error(f"Error in _escalate_shutdown_after_timeout: {e}", exc_info=True)
    
    # ============================================================
    #  WS: ЛЁГКИЙ ХЭНДЛЕР ПОЗИЦИЙ + ФОНОВЫЙ ВОРКЕР ЗАПИСИ В БД
    #  Очередь/воркеры создаются и запускаются в _run_integrated_monitoring_loop
    #  (см. ранее присланный оркестратор). Здесь — только fast-path.
    # ============================================================

    # self._positions_db_queue: asyncio.Queue[tuple[int, dict]]
    # Инициализируется в оркестраторе; здесь лишь используем.


    async def _handle_position_update(self, data: dict):
        """
        Simple dispatcher for 'position' topic.
        It extracts position items and calls registered handlers.
        """
        try:
            items = []
            payload = data.get("data", data.get("result", []))
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                # Bybit can send a single position object instead of a list
                items = payload.get("list", [payload])

            # The key for handlers should be 'position' to match the topic.
            handlers = self.message_handlers.get('position', [])

            if handlers:
                for position_item in items:
                    for handler in handlers:
                        try:
                            await handler(position_item)
                        except Exception as e:
                            logger.error(f"Position handler error for item {position_item.get('symbol')}: {e}", exc_info=True)
            else:
                logger.debug(f"[{self.name}] No handler for position topic.")

        except Exception as e:
            logger.error("%s - Position update handling error: %s", getattr(self, 'name', 'WS'), e, exc_info=True)


    async def _generate_copy_signal(self, position_data: dict, event_type: str, prev_qty: float = 0):
        """
        Генерация сигнала для копирования на основной аккаунт
        НОВЫЙ метод - нужно добавить если его нет
        """
        try:
            # Ищем систему копирования
            copy_system = None
        
            # Вариант 1: Глобальная переменная
            if 'copy_system' in globals():
                copy_system = globals()['copy_system']
        
            # Вариант 2: Атрибут класса
            if not copy_system and hasattr(self, 'copy_system'):
                copy_system = self.copy_system
        
            # Вариант 3: Через orchestrator
            if not copy_system:
                orchestrator = globals().get('orchestrator')
                if orchestrator:
                    copy_system = getattr(orchestrator, 'copy_system', None)
        
            if not copy_system:
                logger.debug("No copy system available, skipping signal generation")
                return
        
            # Получаем очередь копирования
            copy_queue = getattr(copy_system, 'copy_queue', None)
            if not copy_queue:
                logger.debug("No copy queue available")
                return
        
            # Подготавливаем данные сигнала
            symbol = position_data.get('symbol', '').upper()
            side = position_data.get('side', 'Buy')
            current_qty = float(position_data.get('size', position_data.get('qty', 0)))

            # Создаем сигнал
            signal = {
                'type': event_type,
                'timestamp': time.time(),
                'source': 'donor',
                'symbol': symbol,
                'side': side,
                'current_qty': current_qty,
                'prev_qty': prev_qty,
                'position_idx': int(position_data.get('positionIdx', position_data.get('position_idx', 0))),
                'leverage': int(position_data.get('leverage', 1)),
                'data': position_data
            }
        
            # Определяем приоритет
            priority = 1  # Обычный приоритет
            if event_type in ['CLOSED', 'OPENED']:
                priority = 0  # Высокий приоритет для открытия/закрытия
        
            # Отправляем в очередь
            await copy_queue.put((priority, time.time(), signal))
        
            logger.info(f"📤 Copy signal sent: {event_type} for {symbol} {side} "
                       f"qty={current_qty} (was {prev_qty})")
        
            # Логируем для отладки
            logger.debug(f"COPY_PLAN: {event_type} {symbol} {side} {prev_qty}→{current_qty}")
        
        except Exception as e:
            logger.error(f"Failed to generate copy signal: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")

    async def _positions_db_worker(self, worker_id: int = 1):
        """
        Фоновый воркер записи позиций в БД.
        Читает из self._positions_db_queue и вызывает positions_writer.upsert_position().
        Имя метода совпадает с тем, как его запускает оркестратор (_run_integrated_monitoring_loop).
        """
        try:
            try:
                from app.positions_db_writer import positions_writer
            except ImportError:
                from positions_db_writer import positions_writer

            q: asyncio.Queue = getattr(self, '_positions_db_queue', None)
            if q is None:
                logger.warning("DB worker %s: queue is None — exiting", worker_id)
                return

            while not getattr(self, 'should_stop', False):
                try:
                    account_id, pos = await q.get()
                    try:
                        await positions_writer.upsert_position(pos, account_id)
                    except Exception as e:
                        logger.error("DB writer error: %s", e)
                    finally:
                        q.task_done()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("DB worker %s error: %s", worker_id, e)
                    # не спим долго, чтобы не влиять на задержки WS
                    await asyncio.sleep(0)

        finally:
            logger.info("DB worker %s stopped", worker_id)


    async def _handle_wallet_update(self, data: dict):
        """Обработка обновлений кошелька"""
        try:
            wallets = data.get('data', [])
            for wallet in wallets:
                coin = wallet.get('coin')
                balance = safe_float(wallet.get('walletBalance', 0))

                logger.info(f"{self.name} - Wallet update: {coin} balance={balance}")

                if 'wallet' in self.message_handlers:
                    for handler in self.message_handlers['wallet']:
                        await handler(wallet)

        except Exception as e:
            logger.error(f"{self.name} - Wallet update handling error: {e}")
    
    async def _handle_execution_update(self, data: dict):
        """Обработка исполнения ордеров"""
        try:
            executions = data.get('data', [])
            for execution in executions:
                symbol = execution.get('symbol')
                side = execution.get('side')
                exec_qty = safe_float(execution.get('execQty', 0))
                exec_price = safe_float(execution.get('execPrice', 0))
                
                logger.info(f"{self.name} - Execution: {symbol} {side} {exec_qty}@{exec_price}")
                
                if 'execution' in self.message_handlers:
                    for handler in self.message_handlers['execution']:
                        await handler(execution)
                    
        except Exception as e:
            logger.error(f"{self.name} - Execution update handling error: {e}")
    
    async def _handle_order_update(self, data: dict):
        """Обработка обновлений ордеров"""  
        try:
            orders = data.get('data', [])
            for order in orders:
                symbol = order.get('symbol')
                order_status = order.get('orderStatus')
                order_type = order.get('orderType')
                
                logger.info(f"{self.name} - Order update: {symbol} {order_type} {order_status}")
                
                if 'order' in self.message_handlers:
                    for handler in self.message_handlers['order']:
                        await handler(order)
                    
        except Exception as e:
            logger.error(f"{self.name} - Order update handling error: {e}")
    
    async def _handle_disconnect(self, reason: str):
        """
        Надёжная обработка дисконнекта WS:
        - не запускает параллельных циклов реконнекта;
        - планирует отложенную эскалацию shutdown (отменяется при успешном восстановлении);
        - уведомляет NetworkSupervisor (если есть) о статусе.
        """
    
        logger.warning(f"SOURCE_WS - Handling disconnect ({reason}), starting reconnection loop...")
    
        # ✅ НОВОЕ: Логируем дисконнект в БД
        sys_logger.log_event("WARN", "WebSocket", f"WebSocket disconnected: {reason}", {
            "websocket_name": "SOURCE_WS",
            "reason": reason
        })

        # --- reentrancy guard ---
        if not hasattr(self, "_ws_reconnect_lock"):
            self._ws_reconnect_lock = asyncio.Lock()
        if not hasattr(self, "_ws_reconnecting"):
            self._ws_reconnecting = False

        if self._ws_reconnecting:
            logger.info("SOURCE_WS - Reconnect already in progress; skip duplicate handler call")
            return

        # Non-blocking escalation task
        asyncio.create_task(self._escalate_shutdown_after_timeout(180))

        async with self._ws_reconnect_lock:
            if self._ws_reconnecting:
                logger.info("SOURCE_WS - Reconnect already in progress (lock path); skip")
                return
            self._ws_reconnecting = True
        
            # ✅ НОВОЕ: Логируем начало реконнекта
            sys_logger.log_reconnect("WebSocket", "Bybit SOURCE_WS", 1)

            # --- уведомим супервизор о деградации (если подключён) ---
            try:
                if getattr(self, "network_supervisor", None):
                    await self.network_supervisor.on_connection_failure("websocket", f"disconnect: {reason}")
            except Exception as e:
                logger.debug(f"NetworkSupervisor notify failure skipped: {e}")

            try:
                # --- сам цикл реконнекта с backoff (твоя реализация внутри этого метода) ---
                await self._reconnect_ws_loop()
            
                # ✅ НОВОЕ: Если реконнект успешен, логируем
                if hasattr(self, 'ws') and self.ws and is_websocket_open(self.ws):
                    sys_logger.log_event("INFO", "WebSocket", "WebSocket reconnected successfully", {
                        "websocket_name": "SOURCE_WS"
                    })

            except Exception as e:
                # реконнект не удался — эскалация займётся shutdown; просто залогируем
                logger.error(f"SOURCE_WS - Reconnect loop crashed: {e}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
            
                # ✅ НОВОЕ: Логируем критическую ошибку реконнекта
                sys_logger.log_error("WebSocket", "Reconnect loop crashed", {
                    "websocket_name": "SOURCE_WS",
                    "error": str(e),
                    "critical": True
                })

            else:
                # --- успешный ре-коннект: отменяем отложенную эскалацию и уведомляем супервизор ---
                self._cancel_planned_shutdown()
                logger.info("SOURCE_WS - Successfully reconnected, escalation cancelled")

                try:
                    if getattr(self, "network_supervisor", None):
                        await self.network_supervisor.on_connection_success("websocket")
                except Exception as e:
                    logger.debug(f"NetworkSupervisor notify success skipped: {e}")

            finally:
                self._ws_reconnecting = False

    async def _reconnect_ws_loop(self):
        """
        Цикл реконнекта WebSocket с экспоненциальным backoff и логированием в БД
        Вызывается из _handle_disconnect для восстановления соединения
        """
        # ✅ Импортируем sys_logger
    
        # Используем существующие настройки класса или дефолтные
        max_attempts = getattr(self, 'max_reconnect_attempts', 10)
        reconnect_delays = getattr(self, 'reconnect_delays', [1, 2, 4, 8, 16, 32, 60, 60, 60, 60])
    
        logger.info(f"SOURCE_WS - Starting reconnect loop (max attempts: {max_attempts})")
    
        for attempt in range(1, max_attempts + 1):
            # Проверяем флаг остановки
            if getattr(self, 'should_stop', False):
                logger.info("SOURCE_WS - System stopping, aborting reconnect")
                return False
            
            # Вычисляем задержку для текущей попытки
            delay_index = min(attempt - 1, len(reconnect_delays) - 1)
            delay = reconnect_delays[delay_index]
        
            logger.info(f"SOURCE_WS - Reconnect attempt {attempt}/{max_attempts} in {delay}s")
        
            # ✅ Логируем попытку реконнекта в БД
            sys_logger.log_reconnect("WebSocket", "Bybit SOURCE_WS", attempt)
        
            # Ждем перед попыткой
            await asyncio.sleep(delay)
        
            # Еще раз проверяем флаг остановки после ожидания
            if getattr(self, 'should_stop', False):
                logger.info("SOURCE_WS - System stopping after delay, aborting reconnect")
                return False
            
            try:
                # Пытаемся переподключиться
                logger.info(f"SOURCE_WS - Attempting reconnection (attempt {attempt})")
            
                # Закрываем старое соединение если есть
                if hasattr(self, 'ws') and self.ws:
                    try:
                        await self.ws.close()
                    except:
                        pass
                    self.ws = None
            
                # Подключаемся заново
                await self.connect()
            
                # Проверяем успешность подключения
                if hasattr(self, 'status') and self.status == ConnectionStatus.CONNECTED:
                    logger.info(f"SOURCE_WS - Successfully reconnected on attempt {attempt}")
                
                    # ✅ Логируем успешный реконнект в БД
                    sys_logger.log_event("INFO", "WebSocket", "WebSocket reconnected successfully", {
                        "websocket_name": "SOURCE_WS",
                        "attempt": attempt,
                        "total_attempts": max_attempts,
                        "delay_used": delay
                    })
                
                    # Восстанавливаем подписки
                    try:
                        if hasattr(self, '_resubscribe'):
                            await self._resubscribe()
                        elif hasattr(self, 'subscriptions') and self.subscriptions:
                            # Если нет метода _resubscribe, пытаемся восстановить подписки вручную
                            for subscription in self.subscriptions:
                                await self.subscribe(subscription)
                    
                        logger.info("SOURCE_WS - Subscriptions restored")
                    except Exception as sub_error:
                        logger.error(f"SOURCE_WS - Error restoring subscriptions: {sub_error}")
                        # Не считаем это критической ошибкой - соединение установлено
                
                    # Запускаем задачу прослушивания если нужно
                    if hasattr(self, 'listen'):
                        listen_task = asyncio.create_task(self.listen())
                        if hasattr(self, 'active_tasks'):
                            self.active_tasks.add(listen_task)
                
                    return True
                
                # Если статус не CONNECTED, считаем попытку неудачной
                logger.warning(f"SOURCE_WS - Connection established but status is not CONNECTED")
            
            except asyncio.CancelledError:
                logger.info("SOURCE_WS - Reconnect cancelled")
                raise
            
            except Exception as e:
                logger.error(f"SOURCE_WS - Reconnect attempt {attempt} failed: {e}")
            
                # ✅ Логируем неудачную попытку в БД
                sys_logger.log_error("WebSocket", f"Reconnect attempt failed", {
                    "websocket_name": "SOURCE_WS",
                    "attempt": attempt,
                    "error": str(e),
                    "error_type": type(e).__name__
                })
            
                # Если это была последняя попытка, логируем дополнительно
                if attempt == max_attempts:
                    logger.error(f"SOURCE_WS - Final reconnect attempt failed: {e}")
    
        # Все попытки исчерпаны
        logger.error(f"SOURCE_WS - All {max_attempts} reconnect attempts exhausted")
    
        # ✅ Логируем критическую ошибку в БД - реконнект невозможен
        sys_logger.log_error("WebSocket", "All reconnect attempts exhausted", {
            "websocket_name": "SOURCE_WS",
            "max_attempts": max_attempts,
            "critical": True,
            "final_status": "FAILED"
        })
    
        # Уведомляем об критической ошибке через Telegram если возможно
        try:
            from telegram_utils import send_telegram_alert
            await send_telegram_alert(
                f"🔴 CRITICAL: WebSocket SOURCE_WS failed to reconnect after {max_attempts} attempts. "
                f"Manual intervention required!"
            )
        except:
            pass  # Не блокируем если алерт не отправился
    
        return False

    async def _resubscribe(self):
        """
        Восстанавливает все активные подписки после реконнекта.
        Делегирует на новый метод resubscribe_all.
        """
        logger.info("SOURCE_WS - Restoring subscriptions via resubscribe_all...")
        try:
            await self.resubscribe_all()
            logger.info("SOURCE_WS - Subscriptions restored.")
        except Exception as e:
            logger.error(f"SOURCE_WS - Error restoring subscriptions via resubscribe_all: {e}")
    
    async def _cleanup_tasks(self):
        """Корректное завершение всех задач"""
        try:
            cleanup_tasks = []
            
            for task in list(self.active_tasks):
                if task and not task.done():
                    task.cancel()
                    cleanup_tasks.append(task)
            
            if cleanup_tasks:
                # Ждем завершения задач с timeout
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*cleanup_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"{self.name} - Some tasks didn't finish within timeout")
            
            # Очищаем множество задач
            self.active_tasks.clear()
            
            logger.debug(f"{self.name} - Tasks cleaned up successfully")
        
        except Exception as e:
            logger.debug(f"{self.name} - Error cleaning up tasks: {e}")
    
    async def reconnect(self):
        """Надёжное переподключение с exponential backoff и защитой от параллельных попыток."""
        # --- reentrancy guard (нет параллельных реконнектов) ---
        if not hasattr(self, "_reconnect_lock"):
            self._reconnect_lock = asyncio.Lock()
        if not hasattr(self, "_reconnecting"):
            self._reconnecting = False

        if self._reconnecting:
            logger.info(f"{self.name} - Reconnect already in progress; skipping duplicate call")
            return False

        async with self._reconnect_lock:
            if self._reconnecting:
                logger.info(f"{self.name} - Reconnect already in progress (lock path); skip")
                return False
            self._reconnecting = True

            try:
                # Сбрасываем счётчик, если до этого был успешный коннект
                self.reconnect_attempt = getattr(self, "reconnect_attempt", 0) or 0

                while self.reconnect_attempt < self.max_reconnect_attempts and not self.should_stop:
                    try:
                        # Выбор задержки по таблице бэкоффа
                        idx = min(self.reconnect_attempt, len(self.reconnect_delays) - 1)
                        delay = self.reconnect_delays[idx]
                        logger.info(f"{self.name} - Reconnecting in {delay}s (attempt {self.reconnect_attempt + 1})")

                        await asyncio.sleep(delay)

                        # Корректно закрываем старое соединение
                        if getattr(self, "ws", None):
                            try:
                                await close_websocket_safely(self.ws)
                            except Exception as ce:
                                logger.debug(f"{self.name} - safe close previous ws failed: {ce}")
                            finally:
                                self.ws = None

                        # Пытаемся подключиться заново
                        await self.connect()

                        # Успех
                        logger.info(f"{self.name} - Successfully reconnected")
                        self.reconnect_attempt = 0

                        # --- Интеграция с FinalTradingMonitor (если есть) ---
                        # 1) Отменяем отложенную эскалацию shutdown на стороне монитора
                        fm = getattr(self, "final_monitor", None)
                        if fm and hasattr(fm, "_cancel_planned_shutdown"):
                            try:
                                fm._cancel_planned_shutdown()
                            except Exception as hook_e:
                                logger.debug(f"{self.name} - cancel escalation hook failed: {hook_e}")

                        # 2) Пользовательский колбэк «успешный реконнект» (если задан)
                        cb = getattr(self, "on_reconnect_success", None)
                        if cb:
                            try:
                                res = cb(self)
                                if asyncio.iscoroutine(res):
                                    await res
                            except Exception as cb_e:
                                logger.debug(f"{self.name} - on_reconnect_success callback failed: {cb_e}")

                        return True

                    except Exception as e:
                        self.reconnect_attempt += 1
                        logger.error(f"{self.name} - Reconnection attempt {self.reconnect_attempt} failed: {e}")

                        # Пользовательский колбэк «ошибка попытки реконнекта» (не обязателен)
                        cb_fail = getattr(self, "on_reconnect_attempt_failed", None)
                        if cb_fail:
                            try:
                                res = cb_fail(self, self.reconnect_attempt, e)
                                if asyncio.iscoroutine(res):
                                    await res
                            except Exception as cbfe:
                                logger.debug(f"{self.name} - on_reconnect_attempt_failed callback failed: {cbfe}")

                # --- исчерпали попытки ---
                logger.critical(f"{self.name} - Max reconnection attempts reached")
                try:
                    await send_telegram_alert(
                        f"WebSocket {self.name} failed to reconnect after {self.max_reconnect_attempts} attempts"
                    )
                except Exception:
                    pass

                # Пользовательский колбэк «полный провал реконнекта» (если задан)
                cb_final_fail = getattr(self, "on_reconnect_failed", None)
                if cb_final_fail:
                    try:
                        res = cb_final_fail(self)
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception as cbffe:
                        logger.debug(f"{self.name} - on_reconnect_failed callback failed: {cbffe}")

                return False

            finally:
                self._reconnecting = False

    
    def register_handler(self, event_type: str, handler_func):
        """Регистрация обработчика событий"""
        if event_type not in self.message_handlers:
            self.message_handlers[event_type] = []
    
        self.message_handlers[event_type].append(handler_func)
        logger.info(f"{self.name} - Registered handler for {event_type}")
    
    async def close(self):
        """✅ ИСПРАВЛЕННОЕ корректное закрытие с интегрированными фиксами"""
        try:
            self.should_stop = True
            self.processing_active = False
    
            # Завершаем все задачи
            await self._cleanup_tasks()
        
            # Останавливаем обработчик очереди
            if hasattr(self, '_message_processor_task') and self._message_processor_task:
                self._message_processor_task.cancel()
                try:
                    await asyncio.wait_for(self._message_processor_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        
            # Очищаем очередь сообщений
            if hasattr(self, 'message_queue'):
                while not self.message_queue.empty():
                    try:
                        self.message_queue.get_nowait()
                    except:
                        break
    
            # ✅ ИСПРАВЛЕНО: Используем новую безопасную функцию закрытия
            if self.ws:
                await close_websocket_safely(self.ws)
                self.ws = None
    
            self.status = ConnectionStatus.DISCONNECTED
            logger.info(f"{self.name} - WebSocket manager closed successfully")
    
        except Exception as e:
            logger.error(f"{self.name} - Error closing WebSocket: {e}")
            # Принудительно устанавливаем статус
            self.status = ConnectionStatus.DISCONNECTED
    
    def get_stats(self) -> dict:
        """✅ ИСПРАВЛЕННАЯ статистика с ping/pong информацией"""
        uptime = time.time() - self.stats['uptime_start']
        stats = self.stats.copy()

        # ✅ ИСПРАВЛЕНО: Безопасная проверка ping/pong с новой функцией
        ping_pong_delay = None
        if self.last_ping > 0 and self.last_pong > self.last_ping:
            ping_pong_delay = self.last_pong - self.last_ping

        stats.update({
            'status': self.status.value,
            'uptime_seconds': uptime,
            'subscriptions': list(self.subscriptions.keys()),
            'buffer_size': len(self.message_buffer),
            'queue_size': self._get_queue_size_safe(),
            'active_tasks': len(self.active_tasks),
            'websocket_open': is_websocket_open(self.ws),  # ✅ ИСПРАВЛЕНО: используем новую функцию
            'websockets_version': get_websockets_version(),
            'last_ping': self.last_ping,
            'last_pong': self.last_pong,
            'ping_pong_delay': ping_pong_delay,
            'ping_pong_success_rate': (
                (stats.get('ping_pong_success', 0) / max(1, stats.get('ping_pong_success', 0) + stats.get('ping_pong_failures', 0))) * 100
            ),
            'websocket_auto_ping_disabled': True,  # ✅ показываем что автопинг отключен
            'bybit_custom_ping_enabled': True,     # ✅ показываем что используем Bybit ping
            'websocket_fixes_applied': True        # ✅ НОВОЕ: показываем что фиксы применены
        })
        return stats

    async def get_diagnostic_report(self) -> str:
        """Генерирует текстовый отчет о состоянии WebSocket для Telegram."""
        try:
            stats = self.get_stats()
            is_open = stats.get('websocket_open', False)
            status = stats.get('status', 'UNKNOWN')

            report_lines = [
                f"**WebSocket Diagnostics ({self.name})**",
                f"---------------------------------",
                f"**Status:** `{status}` {'✅' if is_open else '❌'}",
                f"**Connection Open:** `{is_open}`",
                f"**Websockets Lib Version:** `{stats.get('websockets_version', 'N/A')}`",
                f"**Subscriptions:** `{', '.join(self.subscriptions) if self.subscriptions else 'None'}`",
                f"**Uptime:** `{int(stats.get('uptime_seconds', 0))} seconds`",
                f"**Last Message:** `{time.time() - stats.get('last_message_time', 0):.1f}s ago`" if stats.get('last_message_time') else "`Never`",
                f"**Messages Received:** `{stats.get('messages_received', 0)}`",
                f"**Messages Processed:** `{stats.get('messages_processed', 0)}`",
                f"**Queue Size:** `{stats.get('queue_size', 0)}`",
                f"**Connection Drops:** `{stats.get('connection_drops', 0)}`",
                "",
                "**Ping/Pong (Bybit Custom):**",
                f"  **Last Ping:** `{datetime.fromtimestamp(self.last_ping).strftime('%H:%M:%S') if self.last_ping else 'N/A'}`",
                f"  **Last Pong:** `{datetime.fromtimestamp(self.last_pong).strftime('%H:%M:%S') if self.last_pong else 'N/A'}`",
                f"  **Latency:** `{'%.3f' % stats.get('ping_pong_delay') if stats.get('ping_pong_delay') is not None else 'N/A'}s`",
                f"  **Success Rate:** `{stats.get('ping_pong_success_rate', 0):.1f}%`",
            ]
            return "\n".join(report_lines)
        except Exception as e:
            logger.error(f"Failed to generate WS diagnostic report: {e}")
            return f"Error generating report: {e}"

    def _get_queue_size_safe(self) -> int:
        """Безопасное получение размера очереди"""
        try:
            if hasattr(self, 'message_queue') and self.message_queue:
                return self.message_queue.qsize()
            return 0
        except (AttributeError, RuntimeError):
            return 0

# ================================
# ИСПРАВЛЕННАЯ СИСТЕМА ОБРАБОТКИ СИГНАЛОВ (без изменений)
# ================================

class ProductionSignalProcessor:
    """Промышленная система обработки торговых сигналов"""
    
    def __init__(self, account_id: int, monitor: 'FinalTradingMonitor' = None):
        # Состояние позиций
        self.account_id = account_id
        self.monitor = monitor
        self.known_positions = {}
        self.position_history = deque(maxlen=1000)
        self._last_set_leverage: dict[str, int] = {}
        
        # Очереди с ограничением размера для backpressure
        self.signal_queue = asyncio.PriorityQueue(maxsize=PRODUCTION_CONFIG['max_queue_size'])
        self.processed_signals = deque(maxlen=500)
        
        # Система фильтрации подозрительных операций
        self.suspicious_patterns = {
            'rapid_fire_orders': {'threshold': 10, 'timeframe': 60},
            'unusual_size': {'threshold': 5.0},
            'weekend_activity': {},
            'correlation_break': {'threshold': 0.3}
        }
        
        # Расширенная статистика
        self.stats = {
            'signals_received': 0,
            'signals_processed': 0,
            'signals_filtered': 0,
            'signals_dropped': 0,
            'suspicious_detected': 0,
            'processing_errors': 0,
            'queue_full_events': 0
        }
        
        # Правильное управление задачами
        self.processing_active = False
        self._processor_task = None
        self.should_stop = False
        self._active_tasks = 0
        self.workers_idle = asyncio.Event()
        self.workers_idle.set()  # Initially, no workers are active

    async def _ingest_position_to_db(self, position_data: dict):
        """
        Гарантированная запись позиции в БД через positions_writer.
        Нормализует ключевые поля: qty/size, position_idx/positionIdx, symbol.
        """
        try:
            from positions_db_writer import positions_writer
        except ImportError:
            from app.positions_db_writer import positions_writer
        except Exception as e:
            logger.error("WS ingest: cannot import positions_writer: %s", e)
            return

        account_id = self.account_id

        # копия и нормализация полей
        pos = dict(position_data) if isinstance(position_data, dict) else {}
        # qty / size
        qty_raw = pos.get("qty", pos.get("size", 0))
        try:
            pos["qty"] = float(qty_raw)
        except Exception:
            pos["qty"] = 0.0

        # position_idx / positionIdx
        try:
            pos["position_idx"] = int(pos.get("position_idx", pos.get("positionIdx", 0)))
        except Exception:
            pos["position_idx"] = 0

        # SYMBOL UPPER
        if "symbol" in pos and isinstance(pos["symbol"], str):
            pos["symbol"] = pos["symbol"].upper()

        # запись напрямую (writer сам решит: qty>0 => upsert, qty==0 => close)
        try:
            await positions_writer.update_position({**pos, "account_id": account_id})
        except Exception as e:
            logger.error("WS ingest: writer.update_position error: %s", e)

        # необязательно, но можно дублировать в очередь (если она есть)
        q = getattr(self, "_positions_db_queue", None)
        if q is not None:
            try:
                q.put_nowait((account_id, pos))
            except Exception:
                # мягкое вытеснение при переполнении
                try:
                    _ = q.get_nowait(); q.task_done()
                except Exception:
                    pass
                try:
                    q.put_nowait((account_id, pos))
                except Exception:
                    pass
        
    async def start_processing(self):
        """Запуск системы обработки сигналов"""
        if not self.processing_active:
            if self._processor_task and not self._processor_task.done():
                self._processor_task.cancel()
                
            self._processor_task = asyncio.create_task(self._process_signal_queue())
            self.processing_active = True
            logger.info("Signal processing system started")
    
    async def stop_processing(self):
        """Остановка системы обработки сигналов"""
        self.should_stop = True
        self.processing_active = False
        
        if self._processor_task and not self._processor_task.done():
            self._processor_task.cancel()
            try:
                await asyncio.wait_for(self._processor_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        logger.info("Signal processing system stopped")

    async def process_position_update(self, position_data: dict):
        """Обработка обновления позиции от WebSocket (hedge-safe + DB ingest)"""
        try:
            if not isinstance(position_data, dict):
                return

            # ---- НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ ПОЛЕЙ ----
            symbol_raw = position_data.get('symbol')
            if not symbol_raw:
                return
            symbol = str(symbol_raw).upper()

            # поддерживаем оба поля размера
            current_size = safe_float(
                position_data.get('size', position_data.get('qty', 0.0))
            ) or 0.0

            # ВАЖНО: в UTA при нулевой позиции side = "" (пустая строка) — это валидно
            side = (position_data.get('side') or "").strip()

            # ВАЖНО: hedge mode — учитываем positionIdx
            try:
                position_idx = int(position_data.get('position_idx', position_data.get('positionIdx', 0)))
            except Exception:
                position_idx = 0

            # ключ состояния: SYMBOL#IDX (чтобы long/short не мешали друг другу)
            state_key = f"{symbol}#{position_idx}"

            # ---- СРАВНЕНИЕ С ПРЕДЫДУЩИМ СОСТОЯНИЕМ ----
            prev_size = 0.0
            is_known = state_key in self.known_positions
            if is_known:
                prev_position = self.known_positions[state_key]
                prev_size = safe_float(prev_position.get('size', 0.0)) or 0.0

            size_delta = current_size - prev_size

            # ---- ГЕНЕРАЦИЯ СИГНАЛОВ ----
            if abs(size_delta) > 0.001:  # Минимальное значимое изменение

                # Цена входа по V5: entryPrice; запасные варианты — sessionAvgPrice/markPrice
                entry_price = safe_float(
                    position_data.get('entryPrice')
                    or position_data.get('sessionAvgPrice')
                    or position_data.get('markPrice')
                    or 0
                ) or 0.0

                if not is_known and current_size > 0:
                    signal_type = SignalType.POSITION_OPEN
                    eff_size = current_size
                else:
                    if prev_size == 0 and current_size > 0:
                        signal_type = SignalType.POSITION_OPEN
                        eff_size = current_size
                    elif prev_size > 0 and current_size == 0:
                        signal_type = SignalType.POSITION_CLOSE
                        eff_size = prev_size  # закрываем весь предыдущий объём
                    else:
                        signal_type = SignalType.POSITION_MODIFY
                        eff_size = abs(size_delta)

                signal = TradingSignal(
                    signal_type=signal_type,
                    symbol=symbol,
                    side=side,
                    size=eff_size,
                    price=entry_price,
                    timestamp=time.time(),
                    metadata={
                        'prev_size': prev_size,
                        'new_size': current_size,
                        'position_idx': position_idx,
                        'position_data': position_data
                    },
                    priority=3 if signal_type == SignalType.POSITION_CLOSE else 2
                )
                await self.add_signal(signal)

            # ---- ОБНОВЛЯЕМ ЛОКАЛЬНОЕ СОСТОЯНИЕ ----
            self.known_positions[state_key] = {
                'size': current_size,
                'side': side,
                'position_idx': position_idx,
                'last_update': time.time(),
                'data': position_data
            }

            # ---- ИСТОРИЯ ----
            self.position_history.append({
                'symbol': symbol,
                'position_idx': position_idx,
                'size': current_size,
                'timestamp': time.time(),
                'type': 'position_update'
            })

            # ---- СТАТИСТИКА ----
            try:
                self.stats['signals_received'] += 1
            except Exception:
                pass

            # ---- СТРАХОВОЧНАЯ ЗАПИСЬ В БД (гарантированно) ----
            try:
                # _ingest_position_to_db нормализует qty/idx/symbol и вызовет writer
                ingest_data = position_data.copy()
                ingest_data['symbol'] = symbol
                ingest_data['position_idx'] = position_idx
                await self._ingest_position_to_db(ingest_data)
            except Exception as e:
                logger.error("process_position_update -> ingest error: %s", e)

        except Exception as e:
            logger.error(f"Position update processing error: {e}")
            try:
                self.stats['processing_errors'] += 1
            except Exception:
                pass


    
    async def add_signal(self, signal: TradingSignal):
        """Добавление сигнала в очередь с backpressure и учетом паузы."""
        try:
            # Проверка, находится ли система на паузе
            if self.monitor and self.monitor._is_paused:
                if signal.signal_type in [SignalType.POSITION_CLOSE, SignalType.POSITION_MODIFY]:
                    position_idx = signal.metadata.get('position_idx', 0)
                    idempotency_key = f"{signal.symbol}|{position_idx}|{signal.signal_type.value}|{int(signal.timestamp)}"
                    self.monitor._deferred_ops_queue.append((idempotency_key, signal))
                    logger.info(f"Critical op deferred with key {idempotency_key}: {signal.signal_type.value} {signal.symbol}")
                else:
                    self.monitor._deferred_signal_queue.append(signal)
                    logger.info(f"Signal deferred due to system pause: {signal.signal_type.value} {signal.symbol}")
                return

            # Валидация сигнала
            if not await self.validate_signal(signal):
                logger.warning(f"Signal filtered: {signal.signal_type.value} {signal.symbol}")  
                self.stats['signals_filtered'] += 1
                return
            
            # Backpressure система
            try:
                # Приоритетная очередь: чем выше priority, тем раньше обработка
                self.signal_queue.put_nowait((-signal.priority, time.time(), signal))
                logger.info(f"Signal added to main queue: {signal.signal_type.value} {signal.symbol} {signal.side} {signal.size}")
            except asyncio.QueueFull:
                # Очередь переполнена - удаляем сигнал с низким приоритетом
                try:
                    removed_signal = self.signal_queue.get_nowait()
                    self.stats['signals_dropped'] += 1
                    logger.warning(f"Dropped low priority signal due to queue overflow")
                    
                    # Добавляем новый сигнал
                    self.signal_queue.put_nowait((-signal.priority, time.time(), signal))
                    logger.info(f"Signal added after dropping old: {signal.signal_type.value} {signal.symbol}")
                except asyncio.QueueEmpty:
                    # Очередь пуста, но была полная - странная ситуация
                    self.signal_queue.put_nowait((-signal.priority, time.time(), signal))
                
                self.stats['queue_full_events'] += 1
                
        except Exception as e:
            logger.error(f"Signal addition error: {e}")
    
    async def validate_signal(self, signal: TradingSignal) -> bool:
        """Валидация торгового сигнала против подозрительных паттернов"""
        try:
            # Проверка базовых параметров
            if signal.size <= 0 or not signal.symbol:
                return False
            
            # Проверка на подозрительные паттерны
            for pattern_name, pattern_config in self.suspicious_patterns.items():
                if await self.check_suspicious_pattern(signal, pattern_name, pattern_config):
                    logger.warning(f"Suspicious pattern detected: {pattern_name} for {signal.symbol}")
                    self.stats['suspicious_detected'] += 1
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Signal validation error: {e}")
            return False
    
    async def check_suspicious_pattern(self, signal: TradingSignal, pattern_name: str, config: dict) -> bool:
        """Проверка конкретного подозрительного паттерна"""
        try:
            if pattern_name == 'rapid_fire_orders':
                # Проверяем количество операций за последние N секунд
                threshold = config['threshold']
                timeframe = config['timeframe']
                recent_time = time.time() - timeframe
                
                recent_signals = [
                    s for s in self.processed_signals
                    if s.timestamp > recent_time and s.symbol == signal.symbol
                ]
                
                return len(recent_signals) > threshold
                
            elif pattern_name == 'unusual_size':
                # Проверяем размер позиции относительно исторических данных
                threshold = config['threshold']
                
                # Получаем историю размеров для символа
                historical_sizes = [
                    h['size'] for h in self.position_history
                    if h['symbol'] == signal.symbol and h['size'] > 0
                ]
                
                if len(historical_sizes) >= 5:
                    avg_size = sum(historical_sizes) / len(historical_sizes)
                    return signal.size > avg_size * threshold
                
            elif pattern_name == 'weekend_activity':
                # Проверяем активность в выходные (для традиционных рынков)
                current_time = datetime.now()
                if current_time.weekday() >= 5:  # Суббота или воскресенье
                    # Для криптовалют не блокируем, только логируем
                    logger.info(f"Weekend activity detected for {signal.symbol}")
                    return False  # Не блокируем для криптовалют
            
            return False
            
        except Exception as e:
            logger.error(f"Pattern check error for {pattern_name}: {e}")
            return False
    
    def is_trading_hours(self) -> bool:
        """Проверка торговых часов (криптовалюты торгуются 24/7)"""
        return True
    
    async def _process_signal_queue(self):
        """Обработчик очереди сигналов"""
        try:
            while not self.should_stop:
                try:
                    # Получаем сигнал с наивысшим приоритетом
                    priority, timestamp, signal = await asyncio.wait_for(
                        self.signal_queue.get(), timeout=1.0
                    )
                    
                    self._active_tasks += 1
                    self.workers_idle.clear()
                    
                    try:
                        # Обрабатываем сигнал
                        await self._execute_signal_processing(signal)

                        # Добавляем в историю обработанных
                        self.processed_signals.append(signal)
                        self.stats['signals_processed'] += 1

                        self.signal_queue.task_done()
                    finally:
                        self._active_tasks -= 1
                        if self._active_tasks == 0 and self.signal_queue.empty():
                            self.workers_idle.set()
                            logger.info("All in-flight signal workers are idle.")
                    
                except asyncio.TimeoutError:
                    if self._active_tasks == 0 and self.signal_queue.empty():
                        if not self.workers_idle.is_set():
                            self.workers_idle.set()
                            logger.info("Signal queue is empty and workers are idle after timeout.")
                    continue  # Периодически проверяем should_stop
                except Exception as e:
                    logger.error(f"Signal queue processing error: {e}")
                    self.stats['processing_errors'] += 1
        except asyncio.CancelledError:
            logger.debug("Signal processor cancelled")
        except Exception as e:
            logger.error(f"Signal processor error: {e}")
    
    async def _execute_signal_processing(self, signal: TradingSignal):
        """Выполнение обработки конкретного сигнала с логированием в signals_log"""
        try:
            logger.info(f"Processing signal: {signal.signal_type.value} {signal.symbol} {signal.side} {signal.size}")
        
            # Логируем сигнал в БД для дедупликации
            account_id = 1 if signal.metadata.get('source') == 'source_account' else 2
        
            signal_logged = signals_logger.log_signal(
                account_id=account_id,
                symbol=signal.symbol,
                side=signal.side,
                qty=signal.size,
                ext_id=signal.metadata.get('order_id', f"sig_{signal.timestamp}"),
                signal_data={
                    'signal_type': signal.signal_type.value,
                    'price': signal.price,
                    'timestamp': signal.timestamp,
                    'metadata': signal.metadata,
                    'priority': signal.priority
                },
                timestamp=signal.timestamp
            )
        
            if signal_logged:
                logger.debug(f"Signal recorded in signals_log: {signal.symbol}")
            else:
                logger.debug(f"Signal duplicate skipped: {signal.symbol}")
        
            # Существующая логика обработки сигналов
            # В зависимости от типа сигнала выполняем соответствующие действия
            try:
                from positions_db_writer import positions_writer
                position_data = signal.metadata.get('position_data', {})
                position_idx = position_data.get('position_idx', 0) if position_data else 0
                leverage = await self._get_main_leverage_for_log(signal.symbol, position_idx)

                if signal.signal_type in [SignalType.POSITION_OPEN, SignalType.POSITION_MODIFY]:
                    await positions_writer.log_open(position_data, leverage=leverage)
                elif signal.signal_type == SignalType.POSITION_CLOSE:
                    await positions_writer.log_close(position_data, leverage=leverage)
            except Exception as log_exc:
                logger.error(f"Failed to log position to DB with leverage: {log_exc}", exc_info=True)

            if signal.signal_type == SignalType.POSITION_OPEN:
                await self._handle_position_open_signal(signal)
            elif signal.signal_type == SignalType.POSITION_CLOSE:
                await self._handle_position_close_signal(signal)
            elif signal.signal_type == SignalType.POSITION_MODIFY:
                await self._handle_position_modify_signal(signal)
        
        except Exception as e:
            logger.error(f"Signal execution error: {e}")
            self.stats['processing_errors'] += 1

    def invalidate_caches(self):
        """Clears all internal caches to force re-fetching of data."""
        logger.info("SIGNAL_PROCESSOR: Invalidating internal caches...")
        self.known_positions.clear()
        self._last_set_leverage.clear()
        logger.info("SIGNAL_PROCESSOR: Caches (known_positions, _last_set_leverage) cleared.")
    
    async def _handle_signal_with_stage2_check(self, signal: TradingSignal, signal_type_str: str):
        """Generic handler to check Stage-2 readiness before forwarding a signal."""
        try:
            # Check for Stage-2 readiness and attempt recovery if needed
            if await self.monitor._ensure_stage2_ready():
                if self.monitor._copy_system_callback:
                    try:
                        await self.monitor._copy_system_callback(signal)
                        self.monitor.metrics['signals_forwarded_total'] += 1
                        logger.info(f"✅ {signal_type_str} signal forwarded to copy system: {signal.symbol}")
                        # Minimal logging, TG alert can be done by Stage 2
                    except Exception as e:
                        self.monitor.metrics['signals_failed_total'] += 1
                        logger.error(f"Copy system callback error for {signal_type_str}: {e}", exc_info=True)
                        await send_telegram_alert(f"❌ **ОШИБКА КОПИРОВАНИЯ '{signal_type_str}'**: {str(e)}")
                else:
                    logger.warning(f"SIG_BUFFERED: symbol={signal.symbol}, reason='no_callback'")
                    try:
                        self.monitor._copy_signal_buffer.put_nowait(signal)
                        self.monitor.metrics['signals_buffered_total'] += 1
                    except asyncio.QueueFull:
                        try:
                            # Drop-oldest policy
                            dropped_signal = self.monitor._copy_signal_buffer.get_nowait()
                            logger.warning(f"SIG_BUFFER_OVERFLOW_DROP_OLDEST: size={self.monitor._copy_signal_buffer.qsize()}, max_size={self.monitor._copy_signal_buffer.maxsize}. Dropped {dropped_signal.symbol}")
                            self.monitor.metrics['signals_dropped_total'] += 1
                            self.monitor._copy_signal_buffer.put_nowait(signal)
                            self.monitor.metrics['signals_buffered_total'] += 1
                        except asyncio.QueueEmpty:
                            pass # Should not happen
            else:
                logger.warning(f"SIG_BUFFERED: symbol={signal.symbol}, reason='stage2_not_ready'")
                try:
                    self.monitor._copy_signal_buffer.put_nowait(signal)
                    self.monitor.metrics['signals_buffered_total'] += 1
                except asyncio.QueueFull:
                    # Drop-oldest policy
                    dropped_signal = self.monitor._copy_signal_buffer.get_nowait()
                    logger.warning(f"SIG_BUFFER_OVERFLOW_DROP_OLDEST: size={self.monitor._copy_signal_buffer.qsize()}, max_size={self.monitor._copy_signal_buffer.maxsize}. Dropped {dropped_signal.symbol}")
                    self.monitor.metrics['signals_dropped_total'] += 1
                    self.monitor._copy_signal_buffer.put_nowait(signal)
                    self.monitor.metrics['signals_buffered_total'] += 1
        except Exception as e:
            logger.exception(f"Error handling {signal_type_str} signal for {signal.symbol}: {e}")
            await send_telegram_alert(f"❌ **КРИТИЧЕСКАЯ ОШИБКА ОБРАБОТКИ СИГНАЛА '{signal_type_str}'**: {str(e)}")



    async def _handle_position_open_signal(self, signal: TradingSignal):
        """ИСПРАВЛЕННАЯ обработка сигнала открытия позиции"""
        logger.info(f"🟢 POSITION OPEN DETECTED: {signal.symbol} {signal.side} {signal.size} @ {signal.price}")
        await send_telegram_alert(
            f"🟢 **НОВАЯ ПОЗИЦИЯ ОБНАРУЖЕНА**\n"
            f"Symbol: {signal.symbol}\n"
            f"Side: {signal.side}\n"
            f"Size: {signal.size}\n"
            f"Price: ${signal.price:.4f}\n"
            f"Time: {datetime.now().strftime('%H:%M:%S')}"
        )
        await self._handle_signal_with_stage2_check(signal, "OPEN")

    async def _handle_position_close_signal(self, signal: TradingSignal):
        """ИСПРАВЛЕННАЯ обработка сигнала закрытия позиции"""
        logger.info(f"🔴 POSITION CLOSE DETECTED: {signal.symbol} {signal.side} {signal.size} @ {signal.price}")
        await send_telegram_alert(
            f"🔴 **ПОЗИЦИЯ ЗАКРЫТА**\n"
            f"Symbol: {signal.symbol}\n"
            f"Side: {signal.side}\n"
            f"Size: {signal.size}\n"
            f"Price: ${signal.price:.4f}\n"
            f"Time: {datetime.now().strftime('%H:%M:%S')}"
        )
        await self._handle_signal_with_stage2_check(signal, "CLOSE")

    async def _handle_position_modify_signal(self, signal: TradingSignal):
        """ИСПРАВЛЕННАЯ обработка сигнала изменения позиции"""
        logger.info(f"🟡 POSITION MODIFY DETECTED: {signal.symbol} {signal.side} {signal.size} @ {signal.price}")
        await send_telegram_alert(
            f"🟡 **ПОЗИЦИЯ ИЗМЕНЕНА**\n"
            f"Symbol: {signal.symbol}\n"
            f"Side: {signal.side}\n"
            f"New Size: {signal.size}\n"
            f"Price: ${signal.price:.4f}\n"
            f"Time: {datetime.now().strftime('%H:%M:%S')}"
        )
        await self._handle_signal_with_stage2_check(signal, "MODIFY")

    def _remember_leverage(self, symbol: str, lev: int) -> None:
        """Stores the last successfully set leverage for a symbol."""
        self._last_set_leverage[symbol] = int(lev)
        logger.info(f"Leverage for {symbol} remembered: {lev}x")

    async def _get_main_leverage_for_log(self, symbol: str, position_idx: int) -> int | None:
        """
        Gets the leverage for a symbol using a two-level cache.
        1. Checks the Main account's position cache (updated by the reconciliation loop).
        2. Falls back to the last-set leverage cache (updated by set_leverage calls).
        """
        lev = None
        # Level 1: Check the main positions cache from the monitor
        if self.monitor and hasattr(self.monitor, 'main_positions_cache'):
            cache_key = f"{symbol}#{position_idx}"
            cached_pos = self.monitor.main_positions_cache.get(cache_key)
            if cached_pos and 'leverage' in cached_pos:
                try:
                    lev = int(float(cached_pos['leverage']))
                    logger.debug(f"Found live leverage for {symbol} from MAIN position cache: {lev}x")
                except (ValueError, TypeError):
                    pass

        # Level 2: Fallback to the last successfully set leverage
        if lev is None:
            lev = self._last_set_leverage.get(symbol)
            if lev is not None:
                logger.debug(f"Found cached leverage for {symbol} from last set: {lev}x")
            else:
                logger.debug(f"No live or cached leverage found for {symbol}, will use None.")

        return lev
    
    def get_stats(self) -> dict:
        """Получить статистику обработки сигналов"""
        stats = self.stats.copy()
        stats.update({
            'known_positions': len(self.known_positions),
            'queue_size': self.signal_queue.qsize(),
            'queue_max_size': self.signal_queue.maxsize,
            'history_size': len(self.position_history),
            'processed_history_size': len(self.processed_signals),
            'processing_active': self.processing_active,
            'queue_utilization_percent': (self.signal_queue.qsize() / self.signal_queue.maxsize) * 100
        })
        return stats

# ================================
# ✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННЫЙ ГЛАВНЫЙ КЛАСС
# ================================

class FinalTradingMonitor:
    """
    ✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ промышленная система мониторинга торговли
    
    ИНТЕГРИРОВАННЫЕ ИСПРАВЛЕНИЯ:
    - ✅ Использует FinalFixedWebSocketManager (с интегрированными фиксами)
    - ✅ Полная совместимость с websockets 15.0.1
    - ✅ Все WebSocket проблемы решены
    - ✅ Диагностические функции интегрированы
    """
    
    def __init__(self):
        # Инициализация компонентов
        # State object for cross-component status
        self.copy_state = None
        self.stage2_system = None

        self.source_client = EnhancedBybitClient(
            SOURCE_API_KEY, SOURCE_API_SECRET, SOURCE_API_URL, "SOURCE", copy_state=self.copy_state
        )
        
        self.main_client = EnhancedBybitClient(
            MAIN_API_KEY, MAIN_API_SECRET, MAIN_API_URL, "MAIN", copy_state=self.copy_state
        )
        
        # ✅ ИСПРАВЛЕНО: Используем окончательно исправленный WebSocket менеджер
        self.websocket_manager = FinalFixedWebSocketManager(
            SOURCE_API_KEY, SOURCE_API_SECRET, "SOURCE_WS", copy_state=self.copy_state, final_monitor=self
        )
        
        self.signal_processor = ProductionSignalProcessor(account_id=DONOR_ACCOUNT_ID, monitor=self)
        
        # Состояние системы
        self.running = False
        self.start_time = time.time()
        
        # ✅ ПРАВИЛЬНО: Создаем Connection Monitor в __init__
        self.connection_monitor = ConnectionMonitorPro()
        
        # Правильное управление задачами
        self.active_tasks = set()
        self.should_stop = False

        # === NEW: флаги идемпотентности и управления жизненным циклом Stage-1 ===
        self._started = False                 # защищает от повторного запуска start()
        self._monitoring_started = False      # защищает от повторного запуска мониторинга
        self._main_task = None                # хэндл главного цикла Stage-1
        self._monitor_task = None             # хэндл цикла мониторинга подключений
        self._planned_shutdown_task = None    # отложенная "эскалация" на shutdown (отменяем при ре-коннекте)
        self._system_active = False           # главный флаг "жить" для _run_main_loop()
        self.main_positions_cache = {}        # NEW: Cache for MAIN account positions
        self.reconcile_enqueued_last_minute = deque(maxlen=60)

        # --- Hot-Reload and Pause Mechanism ---
        self._reload_lock = asyncio.Lock()
        self._reloading = False
        self._is_paused = False
        self._deferred_signal_queue = deque()
        self._deferred_ops_queue = deque()
        self._processed_op_keys = set()
        # === /NEW ===

        # === Stage-2 Callback & Buffering ---
        self._copy_system_callback = None
        self._copy_callback_lock = asyncio.Lock()

        buffer_size = int(os.getenv("COPY_SIGNAL_BUFFER_SIZE", "128"))
        if buffer_size < 32: buffer_size = 32
        self._copy_signal_buffer = asyncio.Queue(maxsize=buffer_size)
        logger.info(f"Signal buffer size = {buffer_size}")

        self._buffer_drainer_task = None
        self._buffer_drainer_event = asyncio.Event()

        self.metrics = defaultdict(int)
        # === /End ---

    @property
    def _deferred_queue(self):
        """Provides backward compatibility for tests or other components
        that might still reference the old queue name.
        """
        return self._deferred_signal_queue

    async def pause_processing(self, timeout: int = 10):
        """Pauses signal processing and waits for all in-flight tasks to complete."""
        logger.info("Pausing system processing...")
        self._is_paused = True

        try:
            if self.signal_processor and hasattr(self.signal_processor, 'workers_idle'):
                logger.info("Waiting for in-flight signals to complete...")
                await asyncio.wait_for(self.signal_processor.workers_idle.wait(), timeout=timeout)
                logger.info("All in-flight tasks completed. System is fully paused.")
        except asyncio.TimeoutError:
            logger.warning(f"In-flight tasks did not complete within the {timeout}s timeout. System paused, but some tasks may still be running.")
        except Exception as e:
            logger.error(f"Error while waiting for workers to become idle: {e}", exc_info=True)

    async def resume_processing(self):
        """Resumes signal processing and processes any deferred signals."""
        logger.info("Resuming system processing...")
        self._is_paused = False  # Unpause first to allow signals to be added to the main queue

        # Process critical deferred operations first with idempotency
        if self._deferred_ops_queue:
            logger.info(f"Processing {len(self._deferred_ops_queue)} deferred critical operations.")
            while self._deferred_ops_queue:
                idempotency_key, op_signal = self._deferred_ops_queue.popleft()
                if idempotency_key in self._processed_op_keys:
                    logger.warning(f"Skipping duplicate deferred operation: {idempotency_key}")
                    continue

                try:
                    if self.signal_processor:
                        await self.signal_processor.add_signal(op_signal)
                    self._processed_op_keys.add(idempotency_key)
                except Exception as e:
                    logger.error(f"Error processing deferred op {op_signal}: {e}", exc_info=True)

        # Process regular deferred signals
        if self._deferred_signal_queue:
            logger.info(f"Processing {len(self._deferred_signal_queue)} deferred signals.")
            while self._deferred_signal_queue:
                signal = self._deferred_signal_queue.popleft()
                try:
                    # Add signal back to the main processing queue
                    if self.signal_processor:
                        await self.signal_processor.add_signal(signal)
                except Exception as e:
                    logger.error(f"Error processing deferred signal {signal}: {e}", exc_info=True)

        logger.info("System processing resumed.")

    async def reload_credentials_and_reconnect(self):
        """
        Atomically hot-reloads API credentials, ensuring the system remains
        in a consistent state throughout the process.
        """
        async with self._reload_lock:
            if self._reloading:
                logger.warning("Hot-reload already in progress. Skipping.")
                return

            self._reloading = True
            start_time = time.time()
            deferred_signals_before = len(self._deferred_queue)
            logger.info(f"HOT_RELOAD_START: Beginning credentials hot-reload process... (deferred signals: {deferred_signals_before})")

            try:
                # 1. Pause system processing
                await self.pause_processing()

                # 2. Disconnect WebSocket
                if self.websocket_manager:
                    logger.info("HOT_RELOAD_WS_DISCONNECT: Closing WebSocket connection...")
                    if hasattr(self.websocket_manager, 'unsubscribe_all'): # Optional: if implemented
                         await self.websocket_manager.unsubscribe_all()
                    await self.websocket_manager.close()

                # 3. Reload and apply new credentials
                from config import get_api_credentials, TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID

                target_creds = get_api_credentials(TARGET_ACCOUNT_ID)
                if not (target_creds and len(target_creds) == 2):
                    raise ValueError("Failed to load new credentials for TARGET account.")

                self.main_client.api_key, self.main_client.api_secret = target_creds
                logger.info("HOT_RELOAD_CREDS: Main client credentials updated.")

                source_creds = get_api_credentials(DONOR_ACCOUNT_ID)
                if source_creds and len(source_creds) == 2:
                    self.source_client.api_key, self.source_client.api_secret = source_creds
                    if self.websocket_manager:
                        self.websocket_manager.api_key, self.websocket_manager.api_secret = source_creds
                    logger.info("HOT_RELOAD_CREDS: Source client and WebSocket credentials updated.")

                # 4. Invalidate all relevant caches
                logger.info("HOT_RELOAD_CACHE_INVALIDATE: Clearing all cached data...")
                self.main_positions_cache.clear()
                if hasattr(self.main_client, 'invalidate_caches'): await self.main_client.invalidate_caches()
                if hasattr(self.source_client, 'invalidate_caches'): await self.source_client.invalidate_caches()
                if hasattr(self.signal_processor, 'invalidate_caches'): self.signal_processor.invalidate_caches()

                # 5. Reconnect WebSocket with retries
                if self.websocket_manager:
                    reconnect_attempts = 3
                    reconnect_delays = [0.5, 2, 5]
                    for attempt in range(reconnect_attempts):
                        try:
                            logger.info(f"HOT_RELOAD_WS_RECONNECT: Attempt {attempt + 1}/{reconnect_attempts} to reconnect WebSocket...")
                            await self.websocket_manager.connect()
                            if self.websocket_manager.ws and self.websocket_manager.status == 'authenticated':
                                logger.info("HOT_RELOAD_WS_RECONNECTED: WebSocket reconnected and authenticated.")
                                await self.websocket_manager.resubscribe_all()
                                break  # Success, exit loop
                            else:
                                raise ConnectionError("WebSocket connected but not authenticated.")
                        except Exception as e:
                            logger.warning(f"HOT_RELOAD_WS_RECONNECT_ATTEMPT_FAILED: Attempt {attempt + 1} failed: {e}")
                            if attempt < reconnect_attempts - 1:
                                await asyncio.sleep(reconnect_delays[attempt])
                            else:
                                logger.critical("HOT_RELOAD_WS_RECONNECT_FAILED: All WebSocket reconnect attempts failed.")
                                raise ConnectionError("Failed to reconnect and authenticate WebSocket after multiple attempts.")

                # 6. Refresh state from REST API
                logger.info(f"HOT_RELOAD_STATE_REFRESH: Fetching current state from REST API using balance_account_type='{BALANCE_ACCOUNT_TYPE}'.")
                await self.run_reconciliation_cycle(enqueue=False) # Refresh positions without queueing
                new_main_balance = await self._get_balance_safe(self.main_client, "MAIN_RELOAD")

                if new_main_balance is None:
                    raise ValueError("Failed to fetch new balance after reload.")

                # D) Rebind Stage-2 to the fresh client
                if self.stage2_system:
                    logger.info("Rebinding Stage-2 to fresh client after key reload...")
                    self.stage2_system.main_client = self.main_client
                    self.stage2_system.source_client = self.source_client
                    # Re-bind the callback to the fresh instance
                    await self.connect_copy_system(self.stage2_system)
                    logger.info("✅ Stage-2 rebound and callback re-bound after key reload.")


                # 7. Final health check and logging
                duration_ms = (time.time() - start_time) * 1000
                ws_topics_count = len(self.websocket_manager.subscriptions) if self.websocket_manager else 0

                summary_log = (
                    f"HOT_RELOAD_SUMMARY: "
                    f"duration_ms={duration_ms:.0f}, "
                    f"deferred_signals_processed={deferred_signals_before}, "
                    f"ws_topics_re-subscribed={ws_topics_count}"
                )
                logger.info(summary_log)

                await send_telegram_alert(
                    f"✅ Горячая перезагрузка завершена за {duration_ms:.0f}ms.\n"
                    f"Новый баланс: {new_main_balance:.2f} USDT\n"
                    f"Обработано отложенных сигналов: {deferred_signals_before}"
                )

                # 8. Resume processing only on success
                await self.resume_processing()

            except Exception as e:
                logger.critical(f"HOT_RELOAD_FAILED: {e}", exc_info=True)
                await send_telegram_alert(f"❌ Горячая перезагрузка не удалась: {e}\nСистема на паузе. Требуется ручное вмешательство.")
                # Do NOT resume processing on failure. Leave it paused.

            finally:
                self._reloading = False
                logger.info("HOT_RELOAD_FINISH: Reload process finished.")
        
    def set_copy_state_ref(self, state_obj):
        """Sets the reference to the shared copy_state object."""
        self.copy_state = state_obj
        # Propagate the reference to child components
        if hasattr(self, 'main_client'):
            self.main_client.copy_state = state_obj
        if hasattr(self, 'source_client'):
            self.source_client.copy_state = state_obj
        if hasattr(self, 'websocket_manager'):
            self.websocket_manager.copy_state = state_obj
        logger.info("Shared copy_state reference has been set in FinalTradingMonitor and its children.")

        # Регистрируем обработчики WebSocket событий
        self._register_websocket_handlers()
        
        # Регистрируем обработчики сигналов (если поддерживается системой)
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (AttributeError, ValueError):
            # Windows или другие системы могут не поддерживать эти сигналы
            pass

    async def _ensure_stage2_ready(self) -> bool:
        """Ensures Stage-2 is initialized and connected, then re-binds the callback."""
        # Quick check if everything is already fine
        if self.stage2_system and getattr(self.stage2_system, 'copy_connected', False) and self._copy_system_callback:
            return True

        logger.warning("Stage-2 is not connected or callback is missing. Attempting lazy re-initialization and re-binding...")
        try:
            if self.stage2_system is None:
                logger.info("Stage-2 system instance not found, creating a new one.")
                from stage2_copy_system import Stage2CopyTradingSystem
                from config import dry_run

                self.stage2_system = Stage2CopyTradingSystem(base_monitor=self)
                self.set_copy_state_ref(self.stage2_system.copy_state)
                self.stage2_system.demo_mode = dry_run

            # Re-bind clients to ensure they are fresh after any hot-reloads
            self.stage2_system.main_client = self.main_client
            self.stage2_system.source_client = self.source_client

            # Re-initialize the system to ensure all handlers are correctly registered
            await self.stage2_system.initialize()

            # Manually set the flags to connected
            self.stage2_system.copy_connected = True
            self.stage2_system.trade_executor_connected = True

            # CRITICAL: Re-bind the callback using the new reliable method
            await self.connect_copy_system(self.stage2_system)

            if self._copy_system_callback:
                 logger.info("✅ Stage-2 lazy re-initialization and callback binding successful.")
                 return True
            else:
                 logger.error("🔥 Stage-2 re-initialized, but callback binding failed.")
                 return False
        except Exception:
            logger.exception("🔥 Failed to lazy-initialize Stage-2.")
            if self.stage2_system:
                self.stage2_system.copy_connected = False
                self.stage2_system.trade_executor_connected = False
            return False


    async def _register_connections_for_monitoring(self):
        """✅ ПРАВИЛЬНО: Регистрация соединений для мониторинга"""
        try:
            logger.info("Registering connections for monitoring...")
            
            # API клиенты
            await self.connection_monitor.register_connection(
                'source_api',
                self.source_client,
                self._health_check_api_client,
                self._recover_api_client
            )
            
            await self.connection_monitor.register_connection(
                'main_api', 
                self.main_client,
                self._health_check_api_client,
                self._recover_api_client
            )
            
            # WebSocket
            await self.connection_monitor.register_connection(
                'websocket',
                self.websocket_manager,
                self._health_check_websocket,
                self._recover_websocket
            )
            
            logger.info("✅ All connections registered for monitoring")
            
        except Exception as e:
            logger.error(f"Failed to register connections for monitoring: {e}")
            # Не останавливаем систему из-за этой ошибки
    
    async def _health_check_api_client(self, client):
        """Health check для API клиента"""
        try:
            await client.time_sync.sync_server_time(client.api_url)
            return True
        except Exception as e:
            logger.debug(f"API client health check failed: {e}")
            return False

    async def _health_check_websocket(self, ws_manager):
        """Health check для WebSocket"""
        try:
            return ws_manager.status == ConnectionStatus.AUTHENTICATED
        except Exception as e:
            logger.debug(f"WebSocket health check failed: {e}")
            return False

    async def _recover_api_client(self, client):
        """Восстановление API клиента"""
        try:
            logger.info(f"Recovering API client: {client.client_name}")
            await client.time_sync.sync_server_time(client.api_url)
            logger.info(f"✅ API client {client.client_name} recovered")
        except Exception as e:
            logger.error(f"Failed to recover API client {client.client_name}: {e}")

    async def _recover_websocket(self, ws_manager):
        """Восстановление WebSocket"""
        try:
            logger.info("Recovering WebSocket connection...")
            await ws_manager.reconnect()
            logger.info("✅ WebSocket connection recovered")
        except Exception as e:
            logger.error(f"Failed to recover WebSocket: {e}")

    async def _get_balance_safe(self, client, label: str) -> Optional[float]:
        """
        Безопасное чтение баланса: не валит цикл и не инициирует ложные DD при сетевых сбоях.
        """
        try:
            bal = await client.get_balance()  # ваш реальный метод (судя по логу)
            if bal is not None:
                logger.info("%s - Balance: %.2f USDT", label, bal)
                # если есть супервизор, помечаем успех API
                if hasattr(self, "supervisor") and self.supervisor:
                    try:
                        await self.supervisor.on_api_success()
                    except Exception:
                        pass
            return bal
        except Exception as e:
            logger.error("%s - Balance error: %s", label, e)
            # если есть супервизор, сообщаем о деградации API
            if hasattr(self, "supervisor") and self.supervisor:
                try:
                    await self.supervisor.on_api_failure(str(e))
                except Exception:
                    pass
            return None


    @property 
    def ws(self):
        """Доступ к WebSocket для совместимости с тестами"""
        return self.websocket_manager.ws
        
    def _signal_handler(self, signum, frame):
        """Обработчик системных сигналов"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True

    def _register_websocket_handlers(self):
        """Регистрация обработчиков WebSocket событий"""
        self.websocket_manager.register_handler(
            'position',
            self.signal_processor.process_position_update
        )

    def _normalize_rest_position(self, p: dict) -> Optional[dict]:
        """Нормализует данные позиции из REST API согласно требованиям."""
        size = safe_float(p.get('size'))
        if not (size > 0):
            return None

        symbol = p.get('symbol')
        if not symbol:
            return None

        try:
            idx = int(p.get('positionIdx', 0))
        except (ValueError, TypeError):
            idx = 0

        # Нормализация цены по приоритету: entryPrice -> sessionAvgPrice -> markPrice -> 0
        price = safe_float(p.get('entryPrice') or p.get('sessionAvgPrice') or p.get('markPrice') or 0)

        # Нормализация стороны
        side = (p.get('side') or "").strip()

        return {
            'key': f"{symbol}#{idx}",
            'symbol': symbol,
            'size': size,
            'side': side,
            'price': price,
            'leverage': safe_float(p.get('leverage', 1)),
            'position_idx': idx,
        }

    async def run_reconciliation_cycle(self, enqueue: bool = True):
        """
        Выполняет один цикл сверки позиций между ДОНОРОМ и ОСНОВНЫМ аккаунтом.
        Использует корректную нормализацию и генерирует сигналы на синхронизацию.
        """
        logger.info("--- Running REST API Reconciliation Cycle ---")
        try:
            # Ensure time is synchronized before making API calls
            await self.source_client.time_sync.ensure_time_sync(self.source_client.api_url)
            await self.main_client.time_sync.ensure_time_sync(self.main_client.api_url)

            donor_positions_raw = await self.source_client.get_positions()
            main_positions_raw = await self.main_client.get_positions()

            if donor_positions_raw is None or main_positions_raw is None:
                logger.error("RECONCILE: Failed to fetch positions. Aborting cycle.")
                return

            donor_positions = {p['key']: p for p in (self._normalize_rest_position(pos) for pos in donor_positions_raw) if p}
            main_positions = {p['key']: p for p in (self._normalize_rest_position(pos) for pos in main_positions_raw) if p}

            # Update the main positions cache
            self.main_positions_cache = {p['key']: p for p in main_positions.values()}
            logger.info(f"Main positions cache updated with {len(self.main_positions_cache)} positions.")

            enqueued_signals, to_open, to_close, to_modify = 0, 0, 0, 0
            all_keys = set(donor_positions.keys()) | set(main_positions.keys())

            for key in all_keys:
                donor_pos, main_pos = donor_positions.get(key), main_positions.get(key)
                signal_to_add = None
                if donor_pos and not main_pos:
                    to_open += 1
                    signal_to_add = TradingSignal(signal_type=SignalType.POSITION_OPEN, symbol=donor_pos['symbol'], side=donor_pos['side'], size=donor_pos['size'], price=donor_pos['price'], timestamp=time.time(), metadata={'source': 'reconcile', 'position_idx': donor_pos['position_idx'], 'leverage': donor_pos['leverage']}, priority=1)
                elif not donor_pos and main_pos:
                    to_close += 1
                    copy_connected = getattr(self.stage2_system, 'copy_connected', True)

                    if not copy_connected:
                        logger.info(f"RECONCILE: copy_connected=False. Attempting direct REST close for {main_pos['symbol']}.")
                        close_side = "Sell" if main_pos['side'] == "Buy" else "Buy"
                        close_result = await self.main_client.place_order(
                            category='linear',
                            symbol=main_pos['symbol'],
                            side=close_side,
                            orderType='Market',
                            qty=str(main_pos['size']),
                            reduceOnly=True
                        )
                        if close_result and close_result.get('orderId'):
                            exec_price = safe_float(close_result.get('avgPrice', main_pos['price']))
                            synthetic_payload = {
                                "symbol": main_pos['symbol'],
                                "qty": main_pos['size'],
                                "side": main_pos['side'],
                                "close_price": exec_price,
                                "source": "reconcile_forced",
                                "account_id": TARGET_ACCOUNT_ID,
                                "ts": utc_now_iso(),
                            }
                            await positions_writer.log_close(synthetic_payload)
                            logger.info(f"RECONCILE_FORCED_CLOSE_OK symbol={main_pos['symbol']} qty={main_pos['size']} price={exec_price}")
                        else:
                            logger.error(f"RECONCILE_FORCED_CLOSE_FAIL: Failed to close {main_pos['symbol']} via REST.")
                        continue # Skip adding to signal queue

                    signal_to_add = TradingSignal(signal_type=SignalType.POSITION_CLOSE, symbol=main_pos['symbol'], side=main_pos['side'], size=main_pos['size'], price=main_pos['price'], timestamp=time.time(), metadata={'source': 'reconcile', 'position_idx': main_pos['position_idx']}, priority=1)
                elif donor_pos and main_pos and (abs(donor_pos['size'] - main_pos['size']) > 1e-9 or donor_pos['side'] != main_pos['side']):
                    to_modify += 1
                    signal_to_add = TradingSignal(signal_type=SignalType.POSITION_MODIFY, symbol=donor_pos['symbol'], side=donor_pos['side'], size=donor_pos['size'], price=donor_pos['price'], timestamp=time.time(), metadata={'source': 'reconcile', 'position_idx': donor_pos['position_idx'], 'leverage': donor_pos['leverage'], 'prev_size_main': main_pos['size']}, priority=1)

                if signal_to_add and enqueue:
                    logger.info(f"RECONCILE: ENQUEUE {signal_to_add.signal_type.name} {key} size={signal_to_add.size} side={signal_to_add.side}")
                    await self.signal_processor.add_signal(signal_to_add)
                    enqueued_signals += 1
                    self.reconcile_enqueued_last_minute.append(time.time())

            summary_log = f"RECONCILE: fetched donor={len(donor_positions)}, main={len(main_positions)} | to_open={to_open}, to_close={to_close}, to_modify={to_modify}"
            logger.info(summary_log)
            logger.info(f"RECONCILE SUMMARY: enqueued={enqueued_signals}")
            if to_open or to_close or to_modify:
                 await send_telegram_alert(f"✅ Reconciliation Run: {summary_log}. Enqueued {enqueued_signals} signals.")

        except Exception as e:
            logger.error(f"RECONCILE: Critical error during reconciliation cycle: {e}", exc_info=True)
            await send_telegram_alert(f"🔥 RECONCILE FAILED: {e}")

        # Auto-healer logic
        try:
            exchange_positions_raw = await self.main_client.get_positions()
            db_positions_raw = positions_writer.get_open_positions(TARGET_ACCOUNT_ID)

            exchange_keys = {f"{p.get('symbol')}#{int(p.get('positionIdx', 0))}" for p in exchange_positions_raw if safe_float(p.get('size')) > 0}
            db_keys = {f"{p.get('symbol')}#{int(p.get('position_idx', 0))}" for p in db_positions_raw}

            to_heal = db_keys - exchange_keys
            if to_heal:
                logger.info(f"AUTO_HEAL: Found {len(to_heal)} positions to hard-close in DB.")
                healed_count = 0
                for key in to_heal:
                    symbol, pos_idx_str = key.split('#')
                    pos_idx = int(pos_idx_str)

                    # Find the corresponding position in the db_positions_raw list
                    stale_pos = next((p for p in db_positions_raw if p.get('symbol') == symbol and int(p.get('position_idx', 0)) == pos_idx), None)
                    if stale_pos:
                        synthetic_payload = {
                            "symbol": stale_pos['symbol'],
                            "qty": stale_pos['qty'],
                            "side": stale_pos['side'],
                            "close_price": stale_pos.get('mark_price') or stale_pos.get('entry_price'),
                            "source": "auto_heal",
                            "account_id": TARGET_ACCOUNT_ID,
                            "ts": utc_now_iso(),
                            "position_idx": pos_idx,
                        }
                        await positions_writer.log_close(synthetic_payload)
                        healed_count += 1
                if healed_count > 0:
                    logger.info(f"AUTO_HEAL_CLOSED n={healed_count}")
        except Exception as e:
            logger.error(f"AUTO_HEAL: Critical error during auto-healing cycle: {e}", exc_info=True)


    async def _periodic_reconciliation_loop(self, interval_sec: int = 60):
        """Бесконечный цикл, который периодически запускает сверку позиций."""
        logger.info(f"Starting periodic reconciliation loop with interval {interval_sec}s.")
        while self.running and not self.should_stop:
            try:
                await asyncio.sleep(interval_sec)
                logger.info("Triggering periodic reconciliation...")
                await self.run_reconciliation_cycle()
            except asyncio.CancelledError:
                logger.info("Periodic reconciliation loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in periodic reconciliation loop: {e}", exc_info=True)
                # В случае ошибки ждем дольше, чтобы избежать спама
                await asyncio.sleep(interval_sec * 2)

    def _ensure_creds(self):
        from config import get_api_credentials, TARGET_ACCOUNT_ID
        creds = get_api_credentials(TARGET_ACCOUNT_ID)
        if not creds:
            raise RuntimeError("Missing API credentials at runtime (введите ключи через /keys)")
        self.api_key, self.api_secret = creds

    async def start(self):
        """✅ ИСПРАВЛЕННЫЙ И ИДЕМПОТЕНТНЫЙ запуск системы мониторинга (Stage-1)"""
        if getattr(self, "_started", False):
            logger.info("FinalTradingMonitor.start() called again — ignored (idempotent)")
            return

        self._ensure_creds()
        self._started = True
        self._system_active = True

        try:
            logger.info("🚀 Starting Final Trading Monitor System...")
            logger.info("✅ WebSocket fixes applied and integrated!")
            self.running = True

            logger.info("Registering connections for monitoring...")
            await self._register_connections_for_monitoring()
            logger.info("✅ All connections registered for monitoring")

            logger.info("Testing API connections...")
            source_balance = await self._get_balance_safe(self.source_client, "SOURCE")
            main_balance   = await self._get_balance_safe(self.main_client,   "MAIN")

            if source_balance is not None:
                logger.info(f"✅ Source account balance: {source_balance:.2f} USDT")
            if main_balance is not None:
                logger.info(f"✅ Main account balance: {main_balance:.2f} USDT")

            # <<< ADDED: запустим фоновую запись снапшотов TARGET-кошелька
            if not getattr(self, "_snapshot_task", None):
                self._snapshot_task = asyncio.create_task(
                    self._wallet_snapshot_loop(interval_sec=60),
                    name="BalanceSnapshots"
                )
                logger.info("🔄 Balance snapshot task started (interval=60s)")
                # Проверим что снапшоты действительно работают
                asyncio.create_task(self._verify_snapshots_working())


            if hasattr(self, "risk_ctx") and self.risk_ctx:
                try:
                    self.risk_ctx.update_equity(main_balance)
                    if hasattr(self.risk_ctx, "is_data_reliable") and not self.risk_ctx.is_data_reliable():
                        state_name = getattr(getattr(self.risk_ctx, "equity_state", None), "name", "UNKNOWN")
                        logger.warning("Risk data not reliable (%s) — skip DD alerts", state_name)
                except Exception as _e:
                    logger.debug("Risk context update skipped: %s", _e)

            await self.signal_processor.start_processing()

            # Create the single buffer drainer task
            if self._buffer_drainer_task is None:
                self._buffer_drainer_task = asyncio.create_task(self._run_buffer_drainer(), name="SignalBufferDrainer")
                self.active_tasks.add(self._buffer_drainer_task)

            logger.info("Connecting to WebSocket with integrated fixes...")
            await self.websocket_manager.connect()

            # КРИТИЧНО: Запускаем цикл получения сообщений в фоне
            asyncio.create_task(self.websocket_manager._recv_loop(), name="WS_RecvLoop")
            logger.info("WebSocket _recv_loop task started.")

            # Запускаем начальную сверку позиций
            asyncio.create_task(self.run_reconciliation_cycle(), name="InitialReconcile")

            # Запускаем периодическую сверку в фоне
            asyncio.create_task(self._periodic_reconciliation_loop(), name="PeriodicReconcile")

            await send_telegram_alert("✅ Final Trading Monitor System started with WebSocket fixes!")

            if not getattr(self, "_main_task", None):
                if hasattr(self, "_run_main_loop"):
                    self._main_task = asyncio.create_task(self._run_main_loop(), name="Stage1_Monitor")
                else:
                    self._main_task = asyncio.create_task(self._main_monitoring_loop(), name="Stage1_Monitor")

            return

        except Exception as e:
            self._started = False
            self._system_active = False
            logger.error(f"System startup error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            try:
                await send_telegram_alert(f"System startup failed: {e}")
            except Exception:
                pass
            raise

    async def _wallet_snapshot_loop(self, interval_sec: int = 60):
        """
        Фоновая задача: каждые N секунд берём кошелёк TARGET (MAIN),
        нормализуем и сохраняем СНАПШОТ базы (wallet без UPNL) в balance_snapshots.
        КРИТИЧНО: Используем account_id=1 (TARGET), а не 2 (DONOR)!
        """
        # Импортируем writer
        writer = getattr(self, "positions_writer", None)
        if writer is None:
            try:
                from positions_db_writer import positions_writer as writer
                logger.info("Balance snapshots: using positions_writer from positions_db_writer module")
            except ImportError:
                try:
                    from app.positions_db_writer import positions_writer as writer
                    logger.info("Balance snapshots: using positions_writer from app.positions_db_writer")
                except ImportError as e:
                    logger.warning("Balance snapshots disabled: PositionsDBWriter not available (%s)", e)
                    return

        logger.info("✅ Balance snapshot writer initialized successfully")
    
        # КРИТИЧНО: Определяем правильный account_id для TARGET
        # По умолчанию TARGET_ACCOUNT_ID = 1 (из вашего .env)
        import os
        target_account_id = int(os.getenv('TARGET_ACCOUNT_ID', '1'))
    
        # Дополнительная проверка через config если доступен
        try:
            from config import TARGET_ACCOUNT_ID
            if TARGET_ACCOUNT_ID:
                target_account_id = int(TARGET_ACCOUNT_ID)
                logger.info(f"Using TARGET_ACCOUNT_ID={target_account_id} from config")
        except (ImportError, ValueError):
            logger.info(f"Using TARGET_ACCOUNT_ID={target_account_id} from env")
    
        # ФИНАЛЬНАЯ ПРОВЕРКА: убедимся что используем правильный ID
        if target_account_id != 1:
            logger.warning(f"⚠️ TARGET_ACCOUNT_ID={target_account_id} is not 1! Check your config!")
    
        logger.info(f"📊 Balance snapshots will be saved for account_id={target_account_id} (TARGET)")
    
        snapshots_saved = 0
        last_save_time = 0
    
        while getattr(self, "_system_active", False) and getattr(self, "running", False):
            try:
                current_time = time.time()
            
                # Проверяем прошел ли интервал
                if current_time - last_save_time < interval_sec:
                    await asyncio.sleep(1)  # Короткий sleep чтобы не нагружать CPU
                    continue
            
                normalized = await self._fetch_main_wallet_normalized()
                if normalized:
                    # КРИТИЧНО: Явно передаём account_id=1 для TARGET!
                    # НЕ полагаемся на внутреннюю логику writer
                    writer.save_balance_snapshot(
                        account_id=target_account_id,  # ЯВНО указываем TARGET account_id
                        balance_data=normalized
                    )
                
                    snapshots_saved += 1
                    last_save_time = current_time
                
                    wallet_base = float(normalized.get("totalWalletBalance", 0))
                
                    logger.debug(
                        "Snapshot #%d saved: account_id=%d, base=%.2f, free=%.2f, im=%.2f, upnl=%.2f",
                        snapshots_saved,
                        target_account_id,  # Логируем какой account_id используем
                        wallet_base,
                        float(normalized.get("availableBalance", 0)),
                        float(normalized.get("totalInitialMargin", 0)),
                        float(normalized.get("unrealizedPnl", 0)),
                    )
                
                    # Каждые 10 снапшотов логируем прогресс
                    if snapshots_saved % 10 == 0:
                        logger.info(
                            "✅ Balance snapshots: %d saved for account_id=%d (TARGET), wallet_base=%.2f USDT",
                            snapshots_saved,
                            target_account_id,
                            wallet_base
                        )
                
                    # Первый снапшот логируем всегда для диагностики
                    if snapshots_saved == 1:
                        logger.info(
                            "🎯 FIRST snapshot saved: account_id=%d, wallet=%.2f, interval=%ds",
                            target_account_id,
                            wallet_base,
                            interval_sec
                        )
                        # Отправляем уведомление в телеграм
                        try:
                            await send_telegram_alert(
                                f"✅ Снапшоты баланса запущены!\n"
                                f"Account ID: {target_account_id} (TARGET)\n"
                                f"Wallet Base: {wallet_base:.2f} USDT\n"
                                f"Интервал: {interval_sec} сек"
                            )
                        except Exception:
                            pass
                        
            except Exception as e:
                logger.warning("Wallet snapshot loop error: %s", e)
                await asyncio.sleep(5)  # При ошибке ждем 5 секунд перед повтором
                continue

            # Основной sleep между снапшотами
            await asyncio.sleep(1)
    
        logger.info("Balance snapshot loop stopped (snapshots_saved=%d)", snapshots_saved)

    async def _fetch_main_wallet_normalized(self) -> dict | None:
        """
        Возвращает словарь в унифицированном виде, подходящий для save_balance_snapshot():
        {
            "currency": "USDT",
            "totalWalletBalance": <Decimal>,   # БАЗА без UPNL
            "availableBalance":   <Decimal|0>,
            "totalInitialMargin": <Decimal|0>,
            "unrealizedPnl":      <Decimal|0>, # для информации/логики, в БД не учитывается
        }
        """
        # Локальная функция для конвертации в Decimal
        def _D(x):
            try:
                return Decimal(str(x))
            except Exception:
                return None
    
        raw = None

        # 1) Пытаемся вызвать типичные методы клиента без поломки текущей логики
        for meth in ("get_wallet_balance", "get_balance", "wallet_balance"):
            fn = getattr(self.main_client, meth, None)
            if fn:
                try:
                    # допускаем как sync, так и async клиентов
                    raw = await fn() if asyncio.iscoroutinefunction(fn) else fn()
                    logger.debug("Fetched wallet via method: %s", meth)
                    break
                except Exception as e:
                    logger.debug("Main wallet fetch via %s failed: %s", meth, e)

        if not raw:
            logger.warning("Failed to fetch main wallet - all methods failed")
            return None

        # 2) Разбираем несколько популярных форматов ответа Bybit
        try:
            # v5 unified: {"result": {"list": [{"coin": [{"coin":"USDT", ...}], ...}]}}
            result = (raw.get("result") or {})
            lst = result.get("list") or result.get("balances") or []
            if isinstance(lst, list) and lst:
                # либо вложенный coin[], либо сразу список по валютам
                coins = None
                if isinstance(lst[0], dict) and "coin" in lst[0]:
                    coins = lst[0].get("coin") or []
                else:
                    coins = lst

                usdt = None
                for c in coins:
                    name = (c.get("coin") or c.get("currency") or "").upper()
                    if name in ("USDT", "USD", "USDC"):
                        usdt = c
                        break

                if usdt:
                    wallet = _D(usdt.get("walletBalance") or usdt.get("totalWalletBalance") or usdt.get("cashBalance"))
                    avail  = _D(usdt.get("availableBalance") or usdt.get("availableToWithdraw"))
                    im     = _D(usdt.get("totalInitialMargin") or usdt.get("usedMargin") or usdt.get("positionMargin"))
                    upnl   = _D(usdt.get("unrealisedPnl") or usdt.get("unrealizedPnl"))

                    # минимум — должна быть база
                    if wallet is None:
                        # иногда дают только equity — восстановим базу как equity - upnl
                        equity = _D(usdt.get("equity") or usdt.get("totalEquity"))
                        if equity is not None and upnl is not None:
                            wallet = equity - upnl

                    if wallet is None:
                        logger.warning("No wallet balance found in parsed response")
                        return None

                    logger.debug("Normalized wallet: base=%s, available=%s, im=%s, upnl=%s",
                                wallet, avail, im, upnl)

                    return {
                        "currency": "USDT",
                        "totalWalletBalance": wallet,
                        "availableBalance":   avail or Decimal("0"),
                        "totalInitialMargin": im or Decimal("0"),
                        "unrealizedPnl":      upnl or Decimal("0"),
                    }
        except Exception as e:
            logger.debug("Wallet normalization failed: %s", e)

        # 3) Фоллбэк на плоские ответы
        try:
            wallet = _D(raw.get("totalWalletBalance") or raw.get("walletBalance"))
            upnl   = _D(raw.get("unrealisedPnl") or raw.get("unrealizedPnl"))
            if wallet is None:
                equity = _D(raw.get("equity") or raw.get("totalEquity"))
                if equity is not None and upnl is not None:
                    wallet = equity - upnl
            if wallet is None:
                return None
            
            logger.debug("Fallback normalization: wallet=%s, upnl=%s", wallet, upnl)
        
            return {
                "currency": "USDT",
                "totalWalletBalance": wallet,
                "availableBalance":   _D(raw.get("availableBalance"))   or Decimal("0"),
                "totalInitialMargin": _D(raw.get("totalInitialMargin")) or Decimal("0"),
                "unrealizedPnl":      upnl or Decimal("0"),
            }
        except Exception:
            return None
        
    async def _verify_snapshots_working(self):
        """Проверяет что снапшоты баланса действительно сохраняются"""
        await asyncio.sleep(70)  # Ждём чуть больше интервала
    
        try:
            # Проверяем через positions_writer
            from positions_db_writer import positions_writer
        
            # Получаем последний баланс из БД
            latest_balance = positions_writer.get_latest_balance(1)
        
            if latest_balance is not None:
                logger.info("✅ Balance snapshots WORKING! Latest balance in DB: %.2f USDT", latest_balance)
                await send_telegram_alert(f"✅ Balance snapshots активны! База в БД: {latest_balance:.2f} USDT")
            else:
                logger.warning("⚠️ Balance snapshots might not be working - no data in DB after 70s")
                await send_telegram_alert("⚠️ Снапшоты баланса возможно не работают - проверьте логи")
        except Exception as e:
            logger.error("Failed to verify snapshots: %s", e)


    async def _main_monitoring_loop(self):
        """✅ ЖИВОЙ основной цикл Stage-1 (не завершается сам по себе)"""
        try:
            while self._system_active:
                # 1. Проверяем состояние WS (по необходимости можно вставить health-check)
                if self.websocket_manager and not is_websocket_open(self.websocket_manager.ws):
                    logger.warning("WebSocket not connected — awaiting reconnect/recovery...")
            
                # 2. Периодические отчёты/статистика (можно переиспользовать твою логику из _monitoring_loop)
                current_time = time.time()
                if hasattr(self, "_last_stats_report"):
                    if current_time - self._last_stats_report > PRODUCTION_CONFIG['stats_report_interval']:
                        try:
                            await self._report_system_stats()
                        except Exception as e:
                            logger.error(f"Stats report failed: {e}")
                        self._last_stats_report = current_time
                else:
                    self._last_stats_report = current_time

                # 3. Лёгкая задержка цикла
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("Main monitoring loop cancelled")
        except Exception as e:
            logger.error(f"Main monitoring loop error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
        finally:
            # !!! НЕ вызывать здесь self._shutdown(), это делает интегратор !!!
            return

    
    async def _monitoring_loop(self):
        """✅ ИСПРАВЛЕННЫЙ цикл мониторинга системы"""
        last_stats_report = 0
        stats_interval = PRODUCTION_CONFIG['stats_report_interval']
        
        try:
            while self.running and not self.should_stop:
                current_time = time.time()
                
                # Периодическая отчетность
                if current_time - last_stats_report > stats_interval:
                    await self._report_system_stats()
                    last_stats_report = current_time
                
                # Управление памятью каждые 10 минут
                if int(current_time) % 600 == 0:  # Каждые 10 минут
                    await self._memory_management()
                
                await asyncio.sleep(30)  # Основной цикл каждые 30 секунд
                
        except asyncio.CancelledError:
            logger.debug("Monitoring loop cancelled")
        except Exception as e:
            logger.error(f"Monitoring loop error: {e}")
    
    async def connect_copy_system(self, copy_system):
        """
        Connects Stage-2 and reliably binds the copy callback.
        """
        async with self._copy_callback_lock:
            self.stage2_system = copy_system
            
            candidate = None
            candidate_name = None
            # Prefer a known, documented method on Stage2; fallback to common candidates
            for name in ("enqueue_signal", "handle_incoming_signal", "process_signal", "_enqueue_signal", "process_copy_signal"):
                candidate = getattr(self.stage2_system, name, None)
                if callable(candidate):
                    candidate_name = name
                    break
            
            if candidate:
                # If candidate is a bound sync method, wrap to async thread to avoid blocking
                if not asyncio.iscoroutinefunction(candidate):
                    def _sync_wrapper(signal):
                        return asyncio.to_thread(candidate, signal)
                    self._copy_system_callback = _sync_wrapper
                    logger.warning(f"Callback '{candidate_name}' is synchronous. Wrapped in asyncio.to_thread.")
                else:
                    self._copy_system_callback = candidate

                qualname = getattr(candidate, '__qualname__', str(candidate))
                logger.info(f"COPY_CALLBACK_BOUND: method={qualname}")
                await send_telegram_alert(f"🔗 **СИСТЕМА КОПИРОВАНИЯ ПОДКЛЮЧЕНА**\n✅ Callback: {qualname}")

                # If there are pending signals, poke the drainer to start processing
                if not self._copy_signal_buffer.empty():
                    logger.info("Poking buffer drainer to process buffered signals...")
                    self._buffer_drainer_event.set()

                return True
            else:
                self._copy_system_callback = None
                logger.error("❌ Could not find a suitable copy system callback method.")
                await send_telegram_alert("❌ **ОШИБКА ИНТЕГРАЦИИ**: Не найден метод для callback в Stage-2.")
                return False

    async def _report_system_stats(self):
        """✅ ИСПРАВЛЕННЫЙ отчет о состоянии системы с информацией о фиксах"""
        try:
            # 1. Объявляем и вычисляем все переменные до их использования
            source_stats = self.source_client.get_stats()
            main_stats = self.main_client.get_stats()
            ws_stats = self.websocket_manager.get_stats()
            signal_stats = self.signal_processor.get_stats()
            
            memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            uptime = time.time() - self.start_time
            
            copy_connected = getattr(self.stage2_system, 'copy_connected', False)
            trade_executor_connected = getattr(self.stage2_system, 'trade_executor_connected', False)

            reconcile_now = time.time()
            reconcile_enqueued_minute = len([t for t in self.reconcile_enqueued_last_minute if reconcile_now - t <= 60])

            db_open_positions = len(positions_writer.get_open_positions(TARGET_ACCOUNT_ID))
            exchange_open_positions = len([p for p in await self.main_client.get_positions() if safe_float(p.get('size', 0)) > 0])

            # 2. Теперь можно безопасно выводить лог
            logger.info("=" * 80)
            logger.info("FINAL SYSTEM STATUS REPORT (WITH WEBSOCKET FIXES)")
            logger.info("=" * 80)
            logger.info(f"Uptime: {uptime:.0f}s ({uptime/3600:.1f}h)")
            logger.info(f"Memory usage: {memory_usage:.1f} MB")
            logger.info("")

            logger.info("SYSTEM STATE:")

            # Trailing Stop Status
            trailing_enabled_str = "N/A"
            trailing_mode_str = "N/A"
            if hasattr(self, 'stage2_system') and self.stage2_system and hasattr(self.stage2_system, 'trailing_manager'):
                try:
                    # Use the public method to get a safe snapshot of the config
                    trailing_cfg = self.stage2_system.trailing_manager.get_config_snapshot()
                    trailing_enabled_str = str(trailing_cfg.get('enabled', 'N/A'))
                    trailing_mode_str = str(trailing_cfg.get('mode', 'N/A'))
                except Exception as e:
                    logger.exception("Could not retrieve trailing_manager config for status report: %s", e)
            logger.info(f"  Trailing: enabled={trailing_enabled_str}, mode={trailing_mode_str}")

            logger.info(f"  Copy Connected: {copy_connected}")
            logger.info(f"  Trade Executor Connected: {trade_executor_connected}")
            logger.info(f"  DB Open Positions: {db_open_positions}")
            logger.info(f"  Exchange Open Positions: {exchange_open_positions}")
            logger.info(f"  Reconcile Enqueued (last 1m): {reconcile_enqueued_minute}")
            logger.info("")
            
            logger.info("API CLIENTS:")
            logger.info(f"  Source: {source_stats['success_rate']:.1f}% success, {source_stats['avg_response_time']:.3f}s avg")
            logger.info(f"  Main: {main_stats['success_rate']:.1f}% success, {main_stats['avg_response_time']:.3f}s avg")
            logger.info("")

            logger.info("WEBSOCKET (FINAL FIXED VERSION):")
            logger.info(f"  Status: {ws_stats['status']}")
            logger.info(f"  Open: {ws_stats.get('websocket_open', 'Unknown')}")
            logger.info(f"  Version: websockets {ws_stats.get('websockets_version', 'unknown')}")
            logger.info(f"  Fixes Applied: {ws_stats.get('websocket_fixes_applied', False)} ✅")
            logger.info(f"  Messages: {ws_stats.get('ws_received_total', 0)} received, {ws_stats.get('ws_processed_private', 0)} processed private")
            logger.info(f"  Ping/Pong: {ws_stats.get('ping_pong_success_rate', 0):.1f}% success rate")
            logger.info(f"  Auto Ping: DISABLED ✅ (Fixed)")
            logger.info(f"  Bybit Ping: ENABLED ✅ (Fixed)")
            logger.info(f"  Connection drops: {ws_stats['connection_drops']}")
            logger.info("")
            
            logger.info("SIGNAL PROCESSING:")
            logger.info(f"  Received: {signal_stats['signals_received']}")
            logger.info(f"  Processed: {signal_stats['signals_processed']}")
            logger.info(f"  Filtered: {signal_stats['signals_filtered']}")
            logger.info(f"  Dropped: {signal_stats['signals_dropped']}")
            logger.info(f"  Queue utilization: {signal_stats.get('queue_utilization_percent', 0):.1f}%")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.exception(f"Stats reporting error: {e}")
    
    async def _memory_management(self):
        """Управление памятью"""
        try:
            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            
            if memory_mb > PRODUCTION_CONFIG['max_memory_mb'] * 0.8:  # 80% от лимита
                logger.warning(f"High memory usage: {memory_mb:.1f} MB, running cleanup...")
                
                # Принудительная сборка мусора
                gc.collect()
                
                # Очистка старых данных
                cutoff_time = time.time() - PRODUCTION_CONFIG['cleanup_interval']
                
                # Очищаем буферы WebSocket (оставляем последние 100)
                while len(self.websocket_manager.message_buffer) > 100:
                    self.websocket_manager.message_buffer.popleft()
                
                # Очищаем историю позиций (оставляем последние 500)
                while len(self.signal_processor.position_history) > 500:
                    self.signal_processor.position_history.popleft()
                
                # Повторная проверка памяти
                new_memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                saved_mb = memory_mb - new_memory_mb
                
                logger.info(f"Memory cleanup completed: {new_memory_mb:.1f} MB (saved {saved_mb:.1f} MB)")
                
        except Exception as e:
            logger.error(f"Memory management error: {e}")
    
    async def _escalate_shutdown_after_timeout(self, seconds: int):
        """
        Плановая эскалация в контролируемый shutdown, если за отведённое время не восстановились.
        Отменяется при успешном ре-коннекте.
        """
        try:
            await asyncio.sleep(seconds)
            if getattr(self, "network_supervisor", None):
                allow = await self.network_supervisor.should_allow_shutdown()
                if not allow:
                    logger.warning("Escalation skipped by NetworkSupervisor (recovery in progress)")
                    return
            logger.error(f"No reconnection for {seconds}s — escalating to controlled shutdown")
            await self._shutdown(reason=f"escalation_no_recovery_{seconds}s")
        except asyncio.CancelledError:
            # восстановились раньше — эскалация отменена
            return

    def _cancel_planned_shutdown(self):
        """Отменяет запланированную эскалацию shutdown, если такая есть."""
        task = self._planned_shutdown_task
        if task and not task.done():
            task.cancel()

    async def _shutdown(self):
        """✅ Корректное и безопасное завершение работы Stage-1 (c учётом супервизора)"""
        try:
            logger.info("Shutting down Final Trading Monitor System...")

            # 0) больше не активны — главный цикл не должен сам перезапускаться
            self._system_active = False
            self.running = False
            self.should_stop = True

            # 1) остановить мониторинг/таймеры супервизора до закрытия коннекторов
            if getattr(self, "network_supervisor", None):
                try:
                    # мягко гасим его внутренние циклы, чтобы он не вмешивался в закрытие
                    await self.network_supervisor.stop_monitoring()
                except Exception as e:
                    logger.debug(f"NetworkSupervisor stop_monitoring error (ignored): {e}")

            # 2) Останавливаем обработку сигналов
            if hasattr(self, "signal_processor") and self.signal_processor:
                try:
                    await self.signal_processor.stop_processing()
                except Exception as e:
                    logger.debug(f"Signal processor stop failed (ignored): {e}")

            # 3) Закрываем WebSocket соединение (исправленный менеджер)
            if hasattr(self, "websocket_manager") and self.websocket_manager:
                try:
                    await self.websocket_manager.close()
                except Exception as e:
                    logger.debug(f"WebSocket manager close failed (ignored): {e}")

            # 4) Отменяем все активные задачи этого модуля
            if getattr(self, "active_tasks", None):
                alive = [t for t in list(self.active_tasks) if t and not t.done()]
                if alive:
                    logger.info(f"Cancelling {len(alive)} active tasks...")
                    for task in alive:
                        task.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*alive, return_exceptions=True),
                            timeout=10.0
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Some tasks didn't finish within shutdown timeout")
                self.active_tasks.clear()

            # 5) Очистка API-клиентов / коннекторов (aiohttp/httpx и т.п.)
            try:
                for client in (getattr(self, "source_client", None),
                               getattr(self, "main_client", None)):
                    if not client:
                        continue
                    if hasattr(client, "cleanup_connections"):
                        await client.cleanup_connections()
                    elif hasattr(client, "cleanup"):
                        res = client.cleanup()
                        if asyncio.iscoroutine(res):
                            await res
            except Exception as e:
                logger.error(f"Error during client cleanup: {e}")

            # 6) Небольшая пауза — даём сетевым ресурсам корректно закрыться
            await asyncio.sleep(0.5)

            # 7) Уведомление о завершении (best-effort, может не пройти при сетевых проблемах)
            try:
                await send_telegram_alert(
                    "✅ Final Trading Monitor System stopped gracefully (WebSocket fixes confirmed working)"
                )
            except Exception as e:
                logger.debug(f"Error sending shutdown telegram alert: {e}")

            # 8) На всякий случай: отменить возможную отложенную эскалацию
            try:
                if hasattr(self, "_cancel_planned_shutdown"):
                    self._cancel_planned_shutdown()
            except Exception:
                pass

            logger.info("✅ System shutdown completed successfully")

        except Exception as e:
            logger.error(f"Shutdown error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")

    async def _run_buffer_drainer(self):
        """
        A single, long-running task that drains the _copy_signal_buffer when a
        callback becomes available. It is triggered by an asyncio.Event.
        """
        logger.info("Buffer drainer task started.")
        while True:
            try:
                await self._buffer_drainer_event.wait()

                if not self._copy_system_callback:
                    logger.warning("Drainer woken up, but callback is not available. Clearing event.")
                    self._buffer_drainer_event.clear()
                    continue

                logger.info(f"Draining signal buffer ({self._copy_signal_buffer.qsize()} items)...")
                while not self._copy_signal_buffer.empty():
                    signal = None
                    try:
                        signal = self._copy_signal_buffer.get_nowait()

                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                start_time = time.time()
                                await self._copy_system_callback(signal)
                                latency_ms = (time.time() - start_time) * 1000
                                self.metrics['signals_forwarded_total'] += 1
                                logger.info(f"SIG_DRAIN_SUCCESS: symbol={signal.symbol}, latency_ms={latency_ms:.2f}")
                                break # Success
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    delay = 0.5 * (2 ** attempt)
                                    logger.warning(
                                        f"Callback failed for {signal.symbol} (attempt {attempt+1}/{max_retries}). "
                                        f"Retrying in {delay}s. Error: {e}"
                                    )
                                    await asyncio.sleep(delay)
                                else:
                                    raise # Re-raise on final attempt

                        self._copy_signal_buffer.task_done()

                    except Exception as e:
                        if signal:
                            self.metrics['signals_failed_total'] += 1
                            logger.error(f"SIG_DRAIN_FAIL: symbol={signal.symbol} after max retries. Error: {e}", exc_info=True)
                        else:
                            logger.error(f"Buffer drainer error: {e}", exc_info=True)

                logger.info("Signal buffer drain complete.")
                self._buffer_drainer_event.clear()

            except asyncio.CancelledError:
                logger.info("Buffer drainer task cancelled.")
                break
            except Exception as e:
                logger.error(f"Unhandled error in buffer drainer task: {e}", exc_info=True)
                # In case of a major error, wait a bit before retrying to avoid spamming logs
                await asyncio.sleep(5)


# ================================
# ✅ АЛИАСЫ ДЛЯ СОВМЕСТИМОСТИ С ТЕСТАМИ
# ================================

# ✅ Правильные алиасы для совместимости с тестами
EnhancedTradingMonitor = FinalTradingMonitor
ProductionTradingMonitor = FinalTradingMonitor
FixedTradingMonitor = FinalTradingMonitor
BybitWebSocketManager = FinalFixedWebSocketManager  
ProductionWebSocketManager = FinalFixedWebSocketManager
FixedWebSocketManager = FinalFixedWebSocketManager
SignalProcessor = ProductionSignalProcessor

# ================================
# ✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ ТОЧКА ВХОДА
# ================================

async def main():
    """✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ главная функция запуска системы"""
    try:
        print("🚀 Запуск Final Trading Monitor System v5.0")
        print("=" * 80)
        print("✅ ОКОНЧАТЕЛЬНО ИСПРАВЛЕННАЯ ФИНАЛЬНАЯ ВЕРСИЯ")
        print("ИНТЕГРИРОВАННЫЕ ИСПРАВЛЕНИЯ WEBSOCKET:")
        print("✅ ИНТЕГРИРОВАНЫ исправления из websocket_fixed_functions.py")
        print("✅ ЗАМЕНЕНА функция is_websocket_open() на основе результатов тестов")
        print("✅ ЗАМЕНЕНА функция close_websocket_safely() на основе результатов тестов")
        print("✅ ИСПРАВЛЕНО свойство closed в FinalFixedWebSocketManager")
        print("✅ ДОБАВЛЕНА диагностическая функция diagnose_websocket_issue()")
        print("✅ ws.state.name = 'OPEN' - РАБОЧИЙ МЕТОД для websockets 15.0.1")
        print("✅ ws.closed НЕ СУЩЕСТВУЕТ в websockets 15.0.1 - ИСПРАВЛЕНО")
        print("✅ РЕЗУЛЬТАТ: Полная совместимость с websockets 15.0.1!")
        print("=" * 80)
        
        # Создаем и запускаем систему мониторинга
        monitor = FinalTradingMonitor()
        await monitor.start()

    except KeyboardInterrupt:
        logger.info("System stopped by user")
        print("\n🛑 Система остановлена пользователем")
    
        # ✅ НОВОЕ: Логируем graceful shutdown
        if 'prod_logger' in globals():
            try:
                prod_logger.logger.info("System gracefully stopped by user (Ctrl+C)")
            except:
                pass  # Не ломаем shutdown из-за ошибок логирования

    except Exception as e:
        # ✅ НОВОЕ: Production Logger для критических ошибок
        if 'prod_logger' in globals():
            try:
                prod_logger.log_error(e, {
                    'operation': 'system_main_loop',
                    'context': 'critical_system_failure',
                    'component': 'main_application',
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc()
                }, send_alert=True)
            except Exception as log_error:
                # Fallback если Production Logger не работает
                logger.error(f"Production logger failed: {log_error}")
    
        # ✅ СОХРАНЯЕМ: Существующее логирование как fallback
        logger.error(f"Critical system error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
    
        # ✅ СОХРАНЯЕМ: Telegram alert
        try:
            await send_telegram_alert(f"Critical system error: {e}")
        except Exception as alert_error:
            logger.debug(f"Telegram alert failed: {alert_error}")
    
        # ✅ СОХРАНЯЕМ: Пользовательские сообщения
        print(f"\n💥 Критическая ошибка системы: {e}")
        print("Проверьте логи для подробной информации")

if __name__ == "__main__":
    try:
        # Правильная обработка сигналов
        if sys.platform != 'win32':
            # На Unix системах можем использовать сигналы
            asyncio.run(main())
        else:
            # На Windows используем ProactorEventLoop
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа завершена пользователем")
    except Exception as e:
        print(f"\n💥 Ошибка запуска: {e}")
        print("Убедитесь что все зависимости установлены:")
        print("pip install websockets aiohttp psutil")
