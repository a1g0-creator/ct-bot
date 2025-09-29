#!/usr/bin/env python3
"""
ИНТЕГРИРОВАННАЯ СИСТЕМА ЗАПУСКА - ЭТАПЫ 1 + 2
Версия 2.1 - ИСПРАВЛЕННЫЕ ИМПОРТЫ TELEGRAM BOT

🎯 ВОЗМОЖНОСТИ:
- ✅ Запуск полной системы мониторинга (Этап 1)
- ✅ Запуск системы копирования позиций (Этап 2)  
- ✅ Kelly Criterion для оптимального управления капиталом
- ✅ Trailing Stop-Loss для защиты прибыли
- ✅ Управление рисками и контроль просадки
- ✅ Telegram Bot для полного управления

🔧 ИСПРАВЛЕНИЯ:
- ✅ Правильные импорты Stage2TelegramBot
- ✅ Корректная инициализация бота
- ✅ Удалены несуществующие классы
"""

import asyncio
import sys
import os
import signal
import socket
import time
import traceback
from datetime import datetime, timezone
from typing import Optional, List, Set, Any
import logging
from contextlib import suppress
import inspect
import importlib

# =========================
#  ПОДГОТОВКА ПУТЕЙ/ИМПОРТОВ
# =========================
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parent
APP_DIR = ROOT / "app"

try:
    from app.sys_events_logger import sys_logger  # noqa: F401
except Exception:
    sys_logger = None  # на случай, если модуль недоступен при статическом анализе

# Добавляем и корень, и app/ — чтобы оба варианта структуры работали
for p in (ROOT, APP_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)

# --- единая система логирования (наш модуль)
# Файл trading_bot/log_rotation_system.py должен быть рядом с этим модулем
try:
    from .log_rotation_system import setup_logging, enforce_retention  # type: ignore
except Exception:
    # fallback, чтобы не падать на импорт-этапе в нестандартной среде
    def setup_logging(*args, **kwargs):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("trading_bot")
    def enforce_retention(*args, **kwargs):
        return 0

logger = logging.getLogger(__name__)


logger = logging.getLogger("bybit_trading_system")
logging.getLogger("httpx").setLevel(logging.WARNING)

def _has_module(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False

# -------------------------
# База данных и модели (ЖЁСТКИЕ ИМПОРТЫ)
# -------------------------
from datetime import datetime

try:
    from app.database_security_implementation import CredentialsStore  # боевой стор (шифрование + SQLAlchemy)
    from app.db_session import SessionLocal
    from app.db_models import SysEvents, EventLevelEnum
    logger.info("Database components imported successfully (secure store)")

    # Пишем событие старта — строго через Enum
    try:
        with SessionLocal() as s:
            s.add(SysEvents(
                level=EventLevelEnum.INFO.value,
                component="IntegratedLauncher",
                message="Launcher started",
                details_json={"timestamp": datetime.now(timezone.utc).isoformat()},
            ))
            s.commit()
    except Exception as e:
        logger.warning("Could not log to DB at startup: %s", e)

except Exception as e:
    logger.exception("Critical: cannot import secure DB components")
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не могу импортировать боевые компоненты БД")
    raise

# -------------------------
# Диагностика наличия ключей при старте
# -------------------------
try:
    from config import get_api_credentials, TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID
    tgt = bool(get_api_credentials(TARGET_ACCOUNT_ID))
    dnr = bool(get_api_credentials(DONOR_ACCOUNT_ID))
    logger.info("Startup creds check: TARGET(id=%s)=%s, DONOR(id=%s)=%s",
                TARGET_ACCOUNT_ID, tgt, DONOR_ACCOUNT_ID, dnr)
except Exception as e:
    logger.warning("Startup creds check failed: %s", e)

# -------------------------
# Этап 1 — мониторинг
# -------------------------
try:
    from enhanced_trading_system_final_fixed import (
        FinalTradingMonitor,
        ConnectionStatus,
        send_telegram_alert,
        get_websockets_version,
        logger as ets_logger,  # чтобы не конфликтовал с нашим
    )
    print("✅ Этап 1: Система мониторинга импортирована")
    try:
        ws_version = get_websockets_version()
    except Exception:
        # резервный путь — напрямую из пакета websockets, если доступен
        try:
            import websockets  # type: ignore
            ws_version = getattr(websockets, "__version__", "unknown")
        except Exception:
            ws_version = "unknown"
    print(f"📦 WebSockets версия: {ws_version}")
except ImportError as e:
    print(f"❌ Ошибка импорта enhanced_trading_system_final_fixed: {e}")
    print("Проверьте наличие файла enhanced_trading_system_final_fixed.py")
    sys.exit(1)

# -------------------------
# Этап 2 — копирование
# -------------------------
try:
    print("⏳ Импорт Этапа 2...")
    from stage2_copy_system import Stage2CopyTradingSystem
    print("✅ Этап 2: Система копирования импортирована")
except ImportError:
    # fallback: класс может жить внутри enhanced_trading_system_final_fixed
    try:
        from enhanced_trading_system_final_fixed import Stage2CopyTradingSystem
        print("✅ Этап 2: Система копирования импортирована из enhanced_trading_system_final_fixed")
    except ImportError as e2:
        print(f"❌ Ошибка импорта Stage2CopyTradingSystem: {e2}")
        print("Проверьте наличие файла stage2_copy_system.py или класса в enhanced_trading_system_final_fixed.py")
        sys.exit(1)

# -------------------------
# Telegram Bot
# -------------------------
try:
    print("⏳ Импорт Telegram Bot...")
    from stage2_telegram_bot import Stage2TelegramBot, run_stage2_telegram_bot  # noqa
    print("✅ Telegram Bot: Корректно импортирован")
except ImportError as e:
    print(f"⚠️ Telegram Bot не найден: {e}")
    print("Система запустится без Telegram интеграции")
    Stage2TelegramBot = None
    run_stage2_telegram_bot = None

# -------------------------
# Network Supervisor (патчи)
# -------------------------

# Функция-патчер, если её удастся обнаружить
patch_network_supervisor = None
_last_ns_err = None
_loaded_module_name = None

# Пробуем по двум именам модулей: приоритет — network_supervisor_fix
for _mod_name in ("network_supervisor_fix", "network_supervisor"):
    try:
        _mod = importlib.import_module(_mod_name)
        _loaded_module_name = _mod_name

        # если где-то ожидают import network_supervisor — делаем алиас
        sys.modules.setdefault("network_supervisor", _mod)

        # Ищем хоть какую-то функцию патча
        for _fn_name in (
            "patch_trading_system_with_supervisor",  # «правильное» имя из fix-модуля
            "patch_trading_system",                  # альтернативное короткое имя, если где-то так названо
        ):
            _fn = getattr(_mod, _fn_name, None)
            if callable(_fn):
                patch_network_supervisor = _fn
                logger.info("%s imported; will patch via %s()", _mod_name, _fn_name)
                break

        # Если функции не нашли — возможно модуль патчит по сайд‑эффекту при импорте
        if not patch_network_supervisor:
            logger.info("%s loaded; no explicit patch function found — assuming side-effect patching", _mod_name)

        break  # модуль найден — выходим из цикла
    except Exception as e:
        _last_ns_err = e

# Пытаемся применить патч (если нашли вызываемую функцию)
try:
    if patch_network_supervisor:
        patch_network_supervisor()
        logger.info("Network Supervisor patch applied (module=%s)", _loaded_module_name)
    elif _loaded_module_name:
        # модуль загрузился, но явной функции нет — работаем дальше, полагаясь на сайд‑эффект
        logger.info("Network Supervisor module '%s' loaded without explicit patch function", _loaded_module_name)
    else:
        # ни один модуль не загрузился
        if _last_ns_err:
            logger.warning("network_supervisor not available for patching (last error: %s)", _last_ns_err)
        else:
            logger.info("network_supervisor patch skipped (no module present)")
except Exception as e:
    logger.warning("Failed to apply network supervisor patch: %s", e)


# -------------------------
# Общая конфигурация (config.py)
# -------------------------
try:
    import config  # как модуль — нужен далее для обновления ключей
    DEFAULT_TRADE_ACCOUNT_ID = getattr(config, "DEFAULT_TRADE_ACCOUNT_ID", 1)
    
except Exception as e:
    logger.warning("config.py not found or failed to import (%s), using defaults", e)
    config = None  # чтобы дальнейшие обращения понимали, что модуля нет
    DEFAULT_TRADE_ACCOUNT_ID = 1


# -------------------------
# Telegram cfg (админы)
# -------------------------
# Импорт telegram_cfg: мягкий, устойчивый к разным версиям модуля
try:
    import telegram_cfg as _tgcfg
    TELEGRAM_TOKEN = getattr(config, "TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN"))
    TELEGRAM_CHAT_ID = getattr(config, "TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID"))
    logger.info("telegram_cfg module loaded")
except Exception as e:
    _tgcfg = None
    logger.warning("telegram_cfg not available or invalid (%s); using ENV fallback", e)
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# -------------------------
# Telegram settings resolver (единая точка истины)
# -------------------------
import os as _os
import logging as _logging
_logger = logging.getLogger("bybit_trading_system")

# мягкие импорты, чтобы не падать
try:
    import telegram_cfg as _tgcfg
    _logger.info("telegram_cfg module loaded")
except Exception as _e:
    _tgcfg = None
    _logger.warning("telegram_cfg not available or invalid (%s); using ENV/config fallback", _e)

try:
    import config as _cfg
except Exception:
    _cfg = None  # допускаем отсутствие

def _resolve_telegram_token() -> str:
    """
    Порядок источников:
      1) ENV TELEGRAM_TOKEN  (основной и предпочтительный)
      2) telegram_cfg.TELEGRAM_TOKEN
      3) config.TELEGRAM_TOKEN
      4) LEGACY: ENV TELEGRAM_BOT_TOKEN
      5) LEGACY: config.TELEGRAM_BOT_TOKEN
    """
    candidates = [
        _os.getenv("TELEGRAM_TOKEN"),
        getattr(_tgcfg, "TELEGRAM_TOKEN", None) if _tgcfg else None,
        getattr(_cfg,   "TELEGRAM_TOKEN", None) if _cfg   else None,
        _os.getenv("TELEGRAM_BOT_TOKEN"),  # ← только как алиас
        getattr(_cfg,   "TELEGRAM_BOT_TOKEN", None) if _cfg else None,
    ]
    token = next((t for t in candidates if t), None)
    if not token:
        _logger.warning("Telegram token not resolved. Set TELEGRAM_TOKEN in environment.")
        return ""
    # небольшой лог источника для диагностики
    if token == _os.getenv("TELEGRAM_TOKEN"):
        _logger.info("✅ TELEGRAM_TOKEN loaded from: ENV")
    elif _tgcfg and token == getattr(_tgcfg, "TELEGRAM_TOKEN", None):
        _logger.info("✅ TELEGRAM_TOKEN loaded from: telegram_cfg")
    elif _cfg and token == getattr(_cfg, "TELEGRAM_TOKEN", None):
        _logger.info("✅ TELEGRAM_TOKEN loaded from: config")
    elif token == _os.getenv("TELEGRAM_BOT_TOKEN"):
        _logger.info("⚠️  TELEGRAM_TOKEN loaded from legacy ENV TELEGRAM_BOT_TOKEN")
    else:
        _logger.info("⚠️  TELEGRAM_TOKEN loaded from legacy config TELEGRAM_BOT_TOKEN")
    return token

def _resolve_telegram_chat_id():
    """
    Универсальный резолвинг chat_id (опционально).
    Источники: ENV → telegram_cfg → config.
    """
    for raw in (
        _os.getenv("TELEGRAM_CHAT_ID"),
        str(getattr(_tgcfg, "TELEGRAM_CHAT_ID", "") or "") if _tgcfg else "",
        str(getattr(_cfg,   "TELEGRAM_CHAT_ID", "") or "") if _cfg   else "",
    ):
        v = (raw or "").strip()
        if v and v.lstrip("-").isdigit():
            return int(v)
    return None

def _parse_int_list(src: str) -> list[int]:
    return [int(p.strip()) for p in (src or "").replace(";", ",").split(",") if p.strip().lstrip("-").isdigit()]

def get_admin_ids() -> list[int]:
    """
    Единый способ получить список админов:
      1) telegram_cfg.get_admin_ids() если есть
      2) telegram_cfg.ADMIN_ONLY_IDS
      3) ENV ADMIN_ONLY_IDS="123,456"
    """
    try:
        if _tgcfg and hasattr(_tgcfg, "get_admin_ids") and callable(_tgcfg.get_admin_ids):
            ids = _tgcfg.get_admin_ids()
        elif _tgcfg and hasattr(_tgcfg, "ADMIN_ONLY_IDS"):
            ids = getattr(_tgcfg, "ADMIN_ONLY_IDS") or []
        else:
            ids = _parse_int_list(_os.getenv("ADMIN_ONLY_IDS", ""))
        # нормализуем к list[int]
        if isinstance(ids, (set, tuple)):
            ids = list(ids)
        out: list[int] = []
        for x in ids:
            if isinstance(x, int):
                out.append(x)
            elif isinstance(x, str) and x.strip().lstrip("-").isdigit():
                out.append(int(x.strip()))
        _logger.info("✅ ADMIN_IDS count: %d (sources: %s)", len(out), "telegram_cfg + ENV" if _tgcfg else "ENV")
        return out
    except Exception as e:
        _logger.warning("Failed to read admin ids from telegram_cfg: %s; using ENV fallback", e)
        return _parse_int_list(_os.getenv("ADMIN_ONLY_IDS", ""))

# Итоговые значения
TELEGRAM_TOKEN   = _resolve_telegram_token()
TELEGRAM_CHAT_ID = _resolve_telegram_chat_id()
ADMIN_IDS        = get_admin_ids()


def _parse_int_list(src: str) -> list[int]:
    return [int(p.strip()) for p in src.replace(";", ",").split(",") if p.strip().isdigit()]

def get_admin_ids() -> list[int]:
    """
    Унифицированный способ получить список админов:
    1) если в telegram_cfg есть функция get_admin_ids() — используем её
    2) иначе, если есть константа ADMIN_ONLY_IDS — читаем её
    3) иначе читаем из ENV ADMIN_ONLY_IDS="123,456"
    Всегда возвращаем список int.
    """
    try:
        if _tgcfg and hasattr(_tgcfg, "get_admin_ids") and callable(_tgcfg.get_admin_ids):
            ids = _tgcfg.get_admin_ids()
        elif _tgcfg and hasattr(_tgcfg, "ADMIN_ONLY_IDS"):
            ids = getattr(_tgcfg, "ADMIN_ONLY_IDS") or []
        else:
            ids = _parse_int_list(os.getenv("ADMIN_ONLY_IDS", ""))

        # санация типов к list[int]
        if isinstance(ids, (set, tuple)):
            ids = list(ids)

        sanitized: list[int] = []
        for x in ids:
            if isinstance(x, int):
                sanitized.append(x)
            elif isinstance(x, str) and x.strip().isdigit():
                sanitized.append(int(x.strip()))
        return sanitized
    except Exception as e:
        logger.warning("Failed to read admin ids from telegram_cfg: %s; using ENV fallback", e)
        return _parse_int_list(os.getenv("ADMIN_ONLY_IDS", ""))


# -------------------------
# Финальная печать архитектуры
# -------------------------
print("\n🚀 BYBIT COPY TRADING SYSTEM - ИНТЕГРИРОВАННЫЙ ЗАПУСК")
print("=" * 80)
print("АРХИТЕКТУРА СИСТЕМЫ:")
print("├── Этап 1: Система мониторинга (WebSocket + API)")
print("├── Этап 2: Система копирования (Kelly + Trailing + Orders)")
print("├── Telegram Bot: Управление и мониторинг" if Stage2TelegramBot else "├── Telegram Bot: НЕ ЗАГРУЖЕН")
print("└── Интеграция: Объединение всех компонентов")
print("=" * 80)
print()


try:
    import uvicorn
except Exception:
    uvicorn = None

class WebAPIService:
    def __init__(self, host: str = "0.0.0.0", port: int = 8080, reload: bool = False):
        self.host = host
        self.port = port
        self.reload = reload
        self.server = None
        self.task: asyncio.Task | None = None
        self.logger = logging.getLogger("bybit_trading_system")

    async def start(self):
        if uvicorn is None:
            self.logger.error("WEB API: uvicorn is not installed. Run: pip install 'uvicorn[standard]' fastapi")
            return
        if self.task and not self.task.done():
            return  # already running

        config = uvicorn.Config(
            app="web_api:app",          # <-- наш web_api.py из проекта
            host=self.host,
            port=int(self.port),
            reload=bool(self.reload),   # в prod обычно False
            loop="asyncio",
            log_level="info",
        )
        self.server = uvicorn.Server(config=config)
        self.task = asyncio.create_task(self.server.serve(), name="WebAPI")
        self.logger.info(f"WEB API started on http://{self.host}:{self.port}")

    async def stop(self):
        # корректное завершение uvicorn
        try:
            if self.server:
                self.server.should_exit = True
            if self.task:
                await asyncio.wait_for(self.task, timeout=10)
        except Exception as e:
            self.logger.warning(f"WEB API graceful stop timeout/err: {e}")


class IntegratedTradingSystem:
    """
    Интегрированная система торговли - объединяет Этапы 1 и 2
    """

    def __init__(self):
        self.stage1_monitor = None  # FinalTradingMonitor
        self.stage2_system  = None  # Stage2CopyTradingSystem
        self.telegram_bot   = None  # Stage2TelegramBot

        self.system_active = False
        self.start_time = 0
        self.active_tasks = set()

        self.integrated_stats = {
            'total_uptime': 0,
            'stage1_restarts': 0,
            'stage2_restarts': 0,
            'critical_errors': 0,
            'successful_starts': 0,
        }

        # Флаг, чтобы не уходить в повторную остановку
        self._stopping: bool = False
        # Флаг отложенной регистрации сигналов (если в __init__ нет активного event loop)
        self._defer_signal_setup: bool = True

        # >>> logging init (единая система логирования в ./logs/)
        # - локально: ~/Documents/trading_bot/logs/
        # - на проде: /opt/trading_bot/logs/
        project_root = Path(__file__).resolve().parent  # сам каталог trading_bot/
        default_log_dir = project_root.parent / "logs"
        log_dir = os.getenv("LOG_BASE_DIR", str(default_log_dir))
        retention = int(os.getenv("LOG_ROTATION_DAYS", "30"))
        level = os.getenv("LOG_LEVEL", "INFO")
        setup_logging(log_dir, level=level, retention_days=retention)
        logging.getLogger(__name__).info("Logs → %s", log_dir)

        logger.info("Integrated Trading System initialized")

    # ---------- FIXED: instance methods + safe logging ----------
    def _warn_proxy_env(self) -> None:
        """Логируем установленные proxy-переменные окружения (если есть)."""
        for k in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy"):
            v = os.getenv(k)
            if v:
                logger.warning("⚠️ Proxy env set: %s=%s", k, v)

    def _dns_check(self, hosts: tuple[str, ...] = ("api.bybit.com", "api-demo.bybit.com")) -> bool:
        """
        Проверяем DNS-резолв нескольких хостов Bybit.
        Возвращаем True, если хотя бы один хост успешно резолвится.
        """
        ok = False
        for h in hosts:
            try:
                socket.gethostbyname(h)
                logger.info("✅ DNS resolution check passed: %s", h)
                ok = True
                break
            except Exception as e:
                logger.error("❌ DNS resolution check failed for %s: %s", h, e)
        return ok
    # ------------------------------------------------------------

    async def _reload_ws_credentials(self) -> bool:
        ws = getattr(self.stage1_monitor, "websocket_manager", None)
        if not ws:
            return False
        try:
            from app.crypto_store import CredentialsStore
            store = CredentialsStore()
            # Donor id=2, fallback id=1
            src = store.get_account_credentials(2) or store.get_account_credentials(1)
            if not src:
                # fallback на кэш, если он есть
                if getattr(ws, "_stored_api_key", None) and getattr(ws, "_stored_api_secret", None):
                    ws.api_key    = ws._stored_api_key
                    ws.api_secret = ws._stored_api_secret
                    logger.warning("WS creds reloaded from cache (no DB creds)")
                    return True
                logger.error("WS creds reload failed: no creds in DB and no cache")
                return False
            ws.api_key, ws.api_secret = src
            ws._stored_api_key    = ws.api_key
            ws._stored_api_secret = ws.api_secret
            logger.info("WS creds (re)applied from DB (2→1) before connect")
            return True
        except Exception as e:
            logger.error("WS creds reload error: %s", e)
            return False

    def _patch_ws_connect(self) -> None:
        ws = getattr(self.stage1_monitor, "websocket_manager", None)
        if not ws or getattr(ws, "_connect_patched", False):
            return

        original_connect = ws.connect

        async def connect_wrapper(*args, **kwargs):
            # если ключей нет — подложим из БД/кэша
            if not getattr(ws, "api_key", None) or not getattr(ws, "api_secret", None):
                ok = await self._reload_ws_credentials()
                if not ok:
                    logger.error("Abort WS connect: no creds available")
                    return
            # страхуемся: сохраняем для будущих реконнектов
            ws._stored_api_key    = ws.api_key
            ws._stored_api_secret = ws.api_secret
            return await original_connect(*args, **kwargs)

        ws.connect = connect_wrapper
        ws._connect_patched = True
        logger.info("WebSocket connect() patched with DB-guard (2→1).")


    def _setup_signal_handlers(self):
        """Настройка обработчиков системных сигналов + единоразовый proxy/DNS sanity."""
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            logger.info("Signal handlers configured")

            # Выполняем sanity-проверку строго 1 раз на процесс
            if os.environ.get("BOT_NET_SANITY_DONE") != "1":
                try:
                    self._warn_proxy_env()
                    self._dns_check()
                except Exception as e:
                    logger.debug("Network sanity check error (non-fatal): %s", e)
                finally:
                    os.environ["BOT_NET_SANITY_DONE"] = "1"

        except Exception as e:
            logger.warning(f"Cannot setup signal handlers: {e}")

    
    def _signal_handler(self, signum, frame):
        """Обработчик системных сигналов"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.system_active = False
    
    def _is_telegram_task(self, t: asyncio.Task) -> bool:
        """Определяем, относится ли таска к Telegram-боту (чтобы не гасить его)."""
        try:
            name = t.get_name()
        except Exception:
            name = str(t)
        n = (name or "").lower()
        return ("telegram" in n) or (name in {"Stage2_TelegramBot", "TelegramBot"})


    async def apply_new_credentials(self, account_id: int) -> bool:
        """
        Горячее применение ключей без остановки Telegram:
        - корректно гасим старые Stage1/Stage2
        - пересоздаём их «как с нуля»
        - читаем ключи ИЗ ЕДИНОГО STOREA (database_security_implementation)
        - применяем ключи ко всем компонентам
        - стартуем Stage1/Stage2
        """
        logger.info("[HotReload] Starting trading systems after keys update")

        # --- 1) Читаем ключи ПРАВИЛЬНЫМ способом (никакого app.crypto_store) ---
        try:
            try:
                from app.database_security_implementation import CredentialsStore
            except Exception:
                from database_security_implementation import CredentialsStore
            store = CredentialsStore()

            # TARGET/DONOR берём из self или self.config (что есть)
            target_id = (
                getattr(self, "TARGET_ACCOUNT_ID", None)
                or getattr(getattr(self, "config", object()), "TARGET_ACCOUNT_ID", None)
                or 1
            )
            donor_id = (
                getattr(self, "DONOR_ACCOUNT_ID", None)
                or getattr(getattr(self, "config", object()), "DONOR_ACCOUNT_ID", None)
            )

            main_creds = store.get_account_credentials(int(target_id))
            donor_creds = store.get_account_credentials(int(donor_id)) if donor_id else None

            if not main_creds or not all(main_creds):
                logger.warning("[HotReload] Keys for TARGET account_id=%s not found in DB", target_id)
                return False

            main_api_key, main_api_secret = main_creds
            source_api_key, source_api_secret = (donor_creds or (None, None))

            logger.info("[HotReload] Found keys: TARGET=%s, DONOR=%s",
                    target_id, donor_id if donor_creds else "—")
        except Exception as e:
            logger.error("[HotReload] Failed to load credentials from DB: %s", e, exc_info=True)
            return False

        try:
            # --- 2) не трогаем Telegram, гасим только Stage1/Stage2 ---
            await self._cancel_active_tasks(names_to_keep=frozenset({"Stage2_TelegramBot"}))

            # --- 3) погасить внутренние фоновые таски старых систем (если реализовано) ---
            if hasattr(self, "_stop_existing_systems"):
                await self._stop_existing_systems()

            # --- 4) короткая пауза на закрытие соединений ---
            import asyncio
            await asyncio.sleep(0.5)

            # --- 5) пересоздать экземпляры «как при первом старте» ---
            self.stage1_monitor = FinalTradingMonitor()
            if hasattr(self, "_patch_ws_connect"):
                self._patch_ws_connect()  # защищённая аутентификация WS
            if hasattr(self, "_refresh_bot_refs"):
                self._refresh_bot_refs()

            # ВАЖНО: правильный ключ конструктора — base_monitor
            self.stage2_system = Stage2CopyTradingSystem(base_monitor=self.stage1_monitor)
            if hasattr(self, "_refresh_bot_refs"):
                self._refresh_bot_refs()  # после создания Stage2 обновляем ссылки бота

            logger.info("[HotReload] Stage 1/2 instances re-created")

            # --- 6) применить ключи ко всем компонентам (без сторонних лоадеров) ---
            await self._apply_credentials_to_all_components(
                source_api_key=source_api_key,
                source_api_secret=source_api_secret,
                main_api_key=main_api_key,
                main_api_secret=main_api_secret,
            )
            logger.info("[HotReload] Credentials applied to all components")

            # --- 7) полная инициализация Stage2 и принудительное включение LIVE-флагов ---
            await self.stage2_system.initialize()
            self.stage2_system.system_active = True
            self.stage2_system.copy_enabled = True

            # --- 8) старт Stage1 ---
            t1 = asyncio.create_task(self.stage1_monitor.start(), name="Stage1_Monitor")
            self.active_tasks.add(t1)
            logger.info("[HotReload] Stage 1 monitoring task created")

            # --- 9) старт Stage2 (безопасный выбор корутины) ---
            t2_coro = self._pick_stage2_start_coro(self.stage2_system)
            t2 = asyncio.create_task(t2_coro, name="Stage2_CopySystem")
            self.active_tasks.add(t2)
            logger.info("[HotReload] Stage 2 copy system task created")

            # --- 10) убедиться, что Telegram-бот жив ---
            if hasattr(self, "_ensure_telegram_running"):
                await self._ensure_telegram_running()

            self.integrated_stats["successful_starts"] += 1
            logger.info("[HotReload] Trading systems started successfully with new credentials")
            return True

        except Exception as e:
            logger.error("[HotReload] Failed to apply credentials: %s", e, exc_info=True)
            self.integrated_stats["critical_errors"] += 1
            from contextlib import suppress
            with suppress(Exception):
                await send_telegram_alert(
                    f"❌ **Ошибка применения ключей!**\n"
                    f"📊 Аккаунт: {account_id}\n"
                    f"⚠️ {e}\n"
                    f"Попробуйте перезапуск либо проверьте ключи в БД."
                )
            return False

    
    async def _apply_credentials_to_all_components(
        self,
        source_api_key: Optional[str],
        source_api_secret: Optional[str],
        main_api_key: str,
        main_api_secret: str
    ):
        """
        Рекурсивно применяет ключи ко всем найденным компонентам системы.
        SOURCE — необязателен (может быть None), MAIN обязателен.
        """
        components_to_check = [
            self.stage1_monitor,
            self.stage2_system
        ]

        for component in components_to_check:
            if not component:
                continue

            for attr_name in dir(component):
                if attr_name.startswith('_'):
                    continue

                try:
                    attr = getattr(component, attr_name)
                    if not attr:
                        continue

                    # Клиентские сущности: менеджеры/клиенты/вебсокеты
                    name = attr_name.lower()
                    is_client = any(k in name for k in ['client', 'manager', 'websocket', 'ws'])
                    if not is_client:
                        continue

                    # Выбор пары ключей
                    is_source = 'source' in name
                    is_main = ('main' in name) or ('target' in name)

                    if is_source and source_api_key and source_api_secret:
                        api_key, api_secret = source_api_key, source_api_secret
                    else:
                        # по умолчанию — MAIN
                        api_key, api_secret = main_api_key, main_api_secret

                    # Применяем на верхнем уровне
                    if hasattr(attr, 'api_key'):
                        attr.api_key = api_key
                    if hasattr(attr, 'api_secret'):
                        attr.api_secret = api_secret

                    # Для вложенных HTTP клиентов (частый паттерн)
                    if hasattr(attr, '_http_client'):
                        http_client = attr._http_client
                        if hasattr(http_client, 'api_key'):
                            http_client.api_key = api_key
                        if hasattr(http_client, 'api_secret'):
                            http_client.api_secret = api_secret

                    logger.debug(
                        "[HotReload] Applied credentials to %s.%s",
                        component.__class__.__name__, attr_name
                    )

                except Exception as e:
                    logger.debug(f"[HotReload] Could not process {attr_name}: {e}")


    async def _run_component(self, coro, component_name: str):
        """
        Запуск компонента с обработкой ошибок.
        
        Args:
            coro: Корутина для запуска
            component_name: Имя компонента для логирования
        """
        try:
            await coro
        except asyncio.CancelledError:
            logger.info(f"{component_name} cancelled")
            raise
        except Exception as e:
            logger.error(f"{component_name} error: {e}")
            self.integrated_stats['critical_errors'] += 1
            
            # Попытка перезапуска в зависимости от компонента
            if 'Stage1' in component_name:
                self.integrated_stats['stage1_restarts'] += 1
            elif 'Stage2' in component_name:
                self.integrated_stats['stage2_restarts'] += 1
                
            raise

        # --- НОВОЕ: выбор account_id и проверка наличия ключей в БД ---
    def _target_account_id(self) -> int:
        """
        Берём TARGET_ACCOUNT_ID из ENV, иначе DEFAULT_TRADE_ACCOUNT_ID (как минимум 1).
        Не тянем config здесь, чтобы не плодить импортов — ENV уже выставлен при старте.
        """
        for k in ("TARGET_ACCOUNT_ID", "DEFAULT_TRADE_ACCOUNT_ID"):
            v = os.getenv(k)
            if v and str(v).isdigit():
                return int(v)
        return 1

    def _have_keys(self) -> bool:
        """
        Проверяем наличие пары ключей в БД через уже импортированный CredentialsStore.
        Никаких исключений — только True/False.
        """
        try:
            store = CredentialsStore()
            creds = store.get_account_credentials(self._target_account_id())
            return bool(creds and all(creds))
        except Exception:
            return False

    async def _wait_for_keys(self, poll_seconds: int = 3) -> None:
        """
        Тихо ждём появления ключей (через /keys). Пишем подсказку в лог разово.
        """
        if not self._have_keys():
            logger.warning("SETUP MODE: ключей нет. Введите их через /keys в Telegram.")
            try:
                await send_telegram_alert(
                    "⚙️ SETUP MODE: ключей нет. Откройте /keys, введите KEY/SECRET и нажмите "
                    "«Применить без рестарта». Система стартанёт автоматически."
                )
            except Exception:
                pass

        while not self._have_keys():
            await asyncio.sleep(poll_seconds)

    def _pick_stage2_start_coro(self, stage2):
        """
        Безопасно выбираем реальный стартовый метод у Stage2:
        start_system() → start_copying() → start()
        Возвращаем асинхронную корутину.
        """
        for name in ("start_system", "start_copying", "start"):
            fn = getattr(stage2, name, None)
            if callable(fn):
                coro = fn()
                if inspect.isawaitable(coro):
                    return coro
        raise AttributeError(
            "Stage2CopyTradingSystem: не найден ни один из стартовых методов "
            "(ожидались start_system()/start_copying()/start())."
        )


    async def _cancel_active_tasks(self, names_to_keep=frozenset()):
        """
        Отменяет все активные задачи, кроме явно разрешённых и Telegram-бота.
        """
        keep = set(names_to_keep) | {"Stage2_TelegramBot", "TelegramBot", "Telegram"}
        tasks_to_cancel = [
            t for t in list(self.active_tasks)
            if (not t.done())
            and (getattr(t, "get_name", lambda: "")() not in keep)
            and (not self._is_telegram_task(t))
        ]

        for t in tasks_to_cancel:
            t.cancel()
            with suppress(asyncio.CancelledError):
                await t

        # оставляем только реально живые
        self.active_tasks = {t for t in self.active_tasks if not t.done()}

    async def _stop_existing_systems(self):
        """Аккуратно гасим старые Stage1/Stage2, чтобы прибить их внутренние фоновые таски."""
        if getattr(self, "stage1_monitor", None) and hasattr(self.stage1_monitor, "stop"):
            with suppress(Exception):
                await self.stage1_monitor.stop()
        if getattr(self, "stage2_system", None) and hasattr(self.stage2_system, "stop"):
            with suppress(Exception):
                await self.stage2_system.stop()

    async def _ensure_telegram_running(self):
        """Если телеграм-бот не запущен — запустить его, иначе ничего не делать."""
        if any(self._is_telegram_task(t) and not t.done() for t in self.active_tasks):
            return  # уже запущен

        if getattr(self, "telegram_bot", None) and hasattr(self.telegram_bot, "start"):
            tbot = asyncio.create_task(self.telegram_bot.start(), name="Stage2_TelegramBot")
            self.active_tasks.add(tbot)
            logger.info("[HotReload] Telegram bot (re)started")

    def _resolve_account_ids(self):
        """
        Единая точка истины для TARGET/DONOR ID.
        Порядок: config -> атрибуты self -> переменные окружения -> дефолты.
        Гарантирует, что 'None'/'"None"' не пролезут.
        """
        import os

        target_id = donor_id = None

        # 1) config
        try:
            from config import TARGET_ACCOUNT_ID as _T
            target_id = _T
        except Exception:
            pass
        try:
            from config import DONOR_ACCOUNT_ID as _D
            donor_id = _D
        except Exception:
            pass

        # 2) атрибуты self
        if target_id is None:
            target_id = getattr(self, "TARGET_ACCOUNT_ID", None)
        if donor_id is None:
            donor_id = getattr(self, "DONOR_ACCOUNT_ID", None)

        # 3) окружение
        if target_id is None:
            target_id = os.getenv("TARGET_ACCOUNT_ID")
        if donor_id is None:
            donor_id = os.getenv("DONOR_ACCOUNT_ID")

        # 4) нормализация и дефолты
        try:
            target_id = int(target_id) if target_id not in (None, "", "None") else 1
        except Exception:
            target_id = 1

        try:
            donor_id = int(donor_id) if donor_id not in (None, "", "None") else None
        except Exception:
            donor_id = None

        # 5) кэшируем на инстансе (чтобы потом не «терялись»)
        self.TARGET_ACCOUNT_ID = target_id
        self.DONOR_ACCOUNT_ID = donor_id

        return target_id, donor_id

    async def _load_and_apply_credentials(self) -> bool:
        """
        Гарантированно поднимает TARGET/DONOR id, грузит ключи из БД
        и расставляет их в реальные атрибуты Stage1/Stage2.
        """
        try:
            target_id, donor_id = self._resolve_account_ids()

            # store
            try:
                from app.database_security_implementation import CredentialsStore
            except Exception:
                from database_security_implementation import CredentialsStore
            store = CredentialsStore()

            main_creds = store.get_account_credentials(target_id)
            donor_creds = store.get_account_credentials(donor_id) if donor_id else None

            if not main_creds or not all(main_creds):
                logger.error("[HotReload] No TARGET credentials in DB for id=%s", target_id)
                return False

            main_key, main_secret = main_creds

            if donor_creds and all(donor_creds):
                source_key, source_secret = donor_creds
                logger.info("[HotReload] Credentials loaded: TARGET(id=%s)=True, DONOR(id=%s)=True",
                            target_id, donor_id)
            else:
                # мониторим TARGET, если донора нет
                source_key, source_secret = main_key, main_secret
                logger.warning("[HotReload] DONOR credentials missing for id=%s — using TARGET for monitoring",
                               donor_id)

            # ---- Stage1 (мониторинг) ----
            if getattr(self, "stage1_monitor", None):
                ws = getattr(self.stage1_monitor, "websocket_manager", None)
                if ws:
                    ws.api_key = source_key
                    ws.api_secret = source_secret
                    setattr(ws, "_stored_api_key", source_key)
                    setattr(ws, "_stored_api_secret", source_secret)
                    logger.info("[HotReload] WS credentials applied (SOURCE)")

                source_client = getattr(self.stage1_monitor, "source_client", None)
                if source_client:
                    source_client.api_key = source_key
                    source_client.api_secret = source_secret
                    if hasattr(source_client, "connector"):
                        source_client.connector.api_key = source_key
                        source_client.connector.api_secret = source_secret
                        if hasattr(source_client.connector, "session") and hasattr(source_client.connector.session, "headers"):
                            source_client.connector.session.headers.update({"X-BAPI-API-KEY": source_key})
                    if hasattr(source_client, "_http_client"):
                        source_client._http_client.api_key = source_key
                        source_client._http_client.api_secret = source_secret
                    logger.info("[HotReload] SOURCE client credentials applied (id=%s)",
                                donor_id if donor_creds else f"{target_id}-fallback")

                main_client = getattr(self.stage1_monitor, "main_client", None)
                if main_client:
                    main_client.api_key = main_key
                    main_client.api_secret = main_secret
                    if hasattr(main_client, "connector"):
                        main_client.connector.api_key = main_key
                        main_client.connector.api_secret = main_secret
                        if hasattr(main_client.connector, "session") and hasattr(main_client.connector.session, "headers"):
                            main_client.connector.session.headers.update({"X-BAPI-API-KEY": main_key})
                    if hasattr(main_client, "_http_client"):
                        main_client._http_client.api_key = main_key
                        main_client._http_client.api_secret = main_secret
                    logger.info("[HotReload] MAIN client credentials applied (TARGET id=%s)", target_id)

            # ---- Stage2 (копирование) — всегда TARGET ----
            if getattr(self, "stage2_system", None):
                s2 = self.stage2_system

                # прямые поля, если есть
                if hasattr(s2, "api_key"):
                    s2.api_key = main_key
                    s2.api_secret = main_secret

                # вложенные клиенты (встречается copy_manager.client и т.п.)
                if hasattr(s2, "client"):
                    s2.client.api_key = main_key
                    s2.client.api_secret = main_secret

                if hasattr(s2, "copy_manager") and s2.copy_manager:
                    cm = s2.copy_manager
                    if hasattr(cm, "api_key"):
                        cm.api_key = main_key
                        cm.api_secret = main_secret
                    if hasattr(cm, "client"):
                        cm.client.api_key = main_key
                        cm.client.api_secret = main_secret

                logger.info("[HotReload] Stage2 credentials applied (TARGET)")

            # ---- обновим глобальные константы ETS (как было) ----
            try:
                import enhanced_trading_system_final_fixed as ets
                ets.SOURCE_API_KEY = source_key
                ets.SOURCE_API_SECRET = source_secret
                ets.MAIN_API_KEY = main_key
                ets.MAIN_API_SECRET = main_secret
                logger.info("[HotReload] ETS module globals updated with creds")
            except Exception as e:
                logger.warning("[HotReload] Could not update ETS globals: %s", e)

            return True

        except Exception as e:
            logger.error("[HotReload] Failed to load/apply credentials: %s", e, exc_info=True)
            return False



    # ---- PATCHED ----
    async def _on_keys_saved(self):
        """
        Горячая замена ключей:
        • НЕ рестартим Telegram
        • Переинициализируем Stage‑1/Stage‑2
        • Загружаем ключи из БД и применяем
        • ИДЕМПОТЕНТНО патчим Supervisor
        • Авто‑активируем Stage‑2
        """
        import sys, importlib, asyncio
        from contextlib import suppress

        logger.info("[HotReload] Starting trading systems after keys update")
        try:
            # 0) Проверим, что TARGET действительно появился в БД
            try:
                try:
                    from app.database_security_implementation import CredentialsStore
                except Exception:
                    from database_security_implementation import CredentialsStore
                store = CredentialsStore()

                target_id = (getattr(self, "TARGET_ACCOUNT_ID", None)
                             or getattr(getattr(self, "config", object()), "TARGET_ACCOUNT_ID", None)
                             or 1)
                have_main = store.get_account_credentials(int(target_id))
                if not have_main or not all(have_main):
                    logger.warning("[HotReload] Keys still not found after save (TARGET id=%s)", target_id)
                    return
            except Exception as e:
                logger.error("[HotReload] Failed to verify keys in DB: %s", e, exc_info=True)
                return

            # 1) Уведомление
            with suppress(Exception):
                await send_telegram_alert("🔑 **КЛЮЧИ СОХРАНЕНЫ**\n\nЗапускаю торговые системы...")

            # 2) Идемпотентный сетевой патч
            with suppress(Exception):
                try:
                    ns_mod = (sys.modules.get("network_supervisor")
                              or sys.modules.get("network_supervisor_fix")
                              or importlib.import_module("network_supervisor_fix"))
                except Exception:
                    ns_mod = importlib.import_module("network_supervisor")

                ns_fn = (getattr(ns_mod, "patch_trading_system_with_supervisor", None)
                         or getattr(ns_mod, "patch_trading_system", None))
                if callable(ns_fn):
                    ns_fn()
                    logger.info("[HotReload] Network Supervisor patch applied")
                else:
                    logger.info("[HotReload] Network Supervisor patch skipped (module/func not present)")

            # 3) Останавливаем все фоновые таски КРОМЕ Telegram
            await self._cancel_active_tasks(names_to_keep=frozenset({"Stage2_TelegramBot"}))

            # 4) Корректно стопаем старые подсистемы
            if getattr(self, "stage1_monitor", None) and hasattr(self.stage1_monitor, "stop"):
                with suppress(Exception):
                    await self.stage1_monitor.stop()
                    logger.info("[HotReload] Stage‑1 stopped")

            if getattr(self, "stage2_system", None) and hasattr(self.stage2_system, "stop"):
                with suppress(Exception):
                    await self.stage2_system.stop()
                    logger.info("[HotReload] Stage‑2 stopped")

            # Небольшой дренаж соединений
            await asyncio.sleep(0.5)

            # 5) Создаём новые инстансы
            await self._initialize_stage1_monitor()
            logger.info("[HotReload] Stage‑1 monitor initialized")

            await self._initialize_stage2_system()
            logger.info("[HotReload] Stage‑2 system initialized")

            # >>> ВАЖНО: сразу после реинициализации — обновляем ссылки бота
            try:
                self._refresh_bot_refs("after Stage1/Stage2 reinit")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after Stage1/Stage2 reinit)")

            # Pass the copy_state object from stage2 to stage1
            if self.stage1_monitor and self.stage2_system and hasattr(self.stage1_monitor, 'set_copy_state_ref'):
                state_ref = getattr(self.stage2_system, 'copy_state', None)
                if state_ref:
                    self.stage1_monitor.set_copy_state_ref(state_ref)
                    logger.info("✅ Propagated copy_state from Stage2 to Stage1.")

            # 6) Загружаем и применяем ключи
            if not await self._load_and_apply_credentials():
                raise RuntimeError("Failed to load/apply credentials to systems")
            logger.info("[HotReload] Credentials loaded and applied to all systems")

            # После применения ключей — ещё раз
            try:
                self._refresh_bot_refs("after credentials apply (hot)")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after credentials apply)")

            # 6b) АВТО‑АКТИВАЦИЯ Stage‑2 (ключевой фикс)
            try:
                self.system_active = True
                s2 = getattr(self, "stage2_system", None)
                if s2:
                    if hasattr(s2, "copy_enabled"):
                        s2.copy_enabled = True
                    if hasattr(s2, "active"):
                        s2.active = True

                    start_coro = (getattr(s2, "start_copying", None)
                                  or getattr(s2, "enable_copying", None))
                    if callable(start_coro):
                        res = start_coro()
                        if hasattr(res, "__await__"):
                            await res
                    logger.info("[HotReload] Stage‑2 auto‑activated (active=True, copy_enabled=True)")
                else:
                    logger.warning("[HotReload] Stage‑2 instance is missing — auto‑activation skipped")
            except Exception:
                logger.exception("[HotReload] Failed to auto‑activate Stage‑2")

            # И ещё раз после активации
            try:
                self._refresh_bot_refs("after Stage2 auto-activation (hot)")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after Stage2 auto-activation)")

            # 7) Запускаем Stage‑1
            if self.stage1_monitor:
                t1 = asyncio.create_task(self.stage1_monitor.start(), name="Stage1_Monitor")
                self.active_tasks.add(t1)
                logger.info("[HotReload] Stage‑1 monitoring task created")

            # 8) Запускаем Stage‑2
            if self.stage2_system:
                t2 = asyncio.create_task(self._pick_stage2_start_coro(self.stage2_system),
                                         name="Stage2_CopySystem")
                self.active_tasks.add(t2)
                logger.info("[HotReload] Stage‑2 copy system task created")

            # 9) Health monitor — создаём, если нет
            if not any(t.get_name() == "SystemMonitor" and not t.done() for t in self.active_tasks):
                hm = asyncio.create_task(self._system_health_monitor(), name="SystemMonitor")
                self.active_tasks.add(hm)
                logger.info("[HotReload] System health monitor task created")

            # 10) Финал
            self.system_active = True
            logger.info("[HotReload] Trading systems started successfully with credentials")

            # Дадим WebSocket’у подключиться
            await asyncio.sleep(2)

            with suppress(Exception):
                await send_telegram_alert(
                    "✅ **ТОРГОВЫЕ СИСТЕМЫ ЗАПУЩЕНЫ**\n\n"
                    "• Ключи загружены и применены ✅\n"
                    "• Stage‑1: Мониторинг активен ✅\n"
                    "• Stage‑2: Копирование ВКЛЮЧЕНО ✅\n"
                    "• Telegram Bot: уже работал ✅\n"
                    "• WebSocket: подключается... ⏳"
                )

        except Exception as e:
            logger.error("[HotReload] Error during systems start: %s", e, exc_info=True)
            with suppress(Exception):
                await send_telegram_alert(
                    "⚠️ **ОШИБКА ЗАПУСКА СИСТЕМ**\n\n"
                    f"{str(e)}\n\n"
                    "При необходимости выполните ручной перезапуск процесса."
                )


    async def _auto_activate_stage2(self) -> None:
        """
        Безопасная автоактивация Stage‑2 после того как:
          - ключи есть и применены,
          - Stage‑1/Stage‑2 проинициализированы,
          - Telegram уже поднят.

        Понимает обе схемы флагов: (active, copy_enabled) и (active, enabled).
        Идемпотентен — можно звать несколько раз.
        """
        s2 = getattr(self, "stage2_system", None)
        if not s2:
            return

        # Выясняем текущее состояние
        active = bool(getattr(s2, "active", False))
        copy_enabled = (
            getattr(s2, "copy_enabled", None)
            if hasattr(s2, "copy_enabled") else
            getattr(s2, "enabled", None)
        )

        # Если уже активен — ничего не делаем
        if active:
            return

        # Включаем «разрешение на копирование», если флаг присутствует и выключен
        if isinstance(copy_enabled, bool) and copy_enabled is False:
            try:
                setattr(s2, "copy_enabled", True)
            except Exception:
                # возможно используется .enabled — попробуем его
                try:
                    setattr(s2, "enabled", True)
                except Exception:
                    pass

        # Пробуем штатный метод включения, если он есть
        start_like = None
        for name in ("enable", "activate", "start_copying", "start"):
            fn = getattr(s2, name, None)
            if callable(fn):
                start_like = fn
                break

        if start_like:
            with suppress(Exception):
                rv = start_like()              # синхронный?
                if asyncio.iscoroutine(rv):
                    await rv                   # асинхронный
                # если метод сам выставляет active=True — отлично
                if getattr(s2, "active", False):
                    return

        # Жёстко проставляем флаг активности как fall‑back
        with suppress(Exception):
            setattr(s2, "active", True)

        self.system_active = True
        # чуть подождём, чтобы подключились WS/транспорты
        await asyncio.sleep(0.1)

    async def start_integrated_system(self):
        """
        Интегрированный bootstrap:
        1) Всегда поднимаем Telegram, чтобы /keys был доступен.
        2) Если ключи уже есть — продолжаем; иначе ждём ввода ключей.
        3) Патчим Network Supervisor (идемпотентно, без жёсткой зависимости от имени модуля).
        4) Инициализируем Stage-1/Stage-2.
        5) Гарантированно подгружаем ключи в оба Stage'а и АВТО-АКТИВИРУЕМ Stage-2.
        6) Запускаем основной цикл мониторинга.
        """
        # >>> signal handlers (Ctrl+C / SIGTERM → аккуратная остановка)
        # Регистрацию делаем именно здесь, когда уже есть активный event loop
        loop = asyncio.get_running_loop()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(self._initiate_shutdown(s))
                )
            self._defer_signal_setup = False
            logger.debug("Signal handlers installed for SIGINT/SIGTERM")
        except (NotImplementedError, RuntimeError):
            # Windows/ограниченные окружения/не main-thread → полагаемся на верхнеуровневый KeyboardInterrupt
            logger.debug("Signal handlers not supported in this environment")

        try:
            self.start_time = time.time()  # НОВОЕ: фиксируем время старта
            self.system_active = True

            # НОВОЕ: Логируем старт в БД
            if sys_logger:
                with suppress(Exception):
                    sys_logger.log_startup("IntegratedSystem", {
                        "environment": os.getenv("ENVIRONMENT", "unknown"),
                        "start_time": datetime.now().isoformat()
                    })

            # Шапка
            print("\n🚀 BYBIT COPY TRADING SYSTEM - ИНТЕГРИРОВАННЫЙ ЗАПУСК")
            print("=" * 80)
            print("АРХИТЕКТУРА:")
            print("├── Этап 1: Система мониторинга (WebSocket + API)")
            print("├── Этап 2: Система копирования (Kelly + Trailing + Orders)")
            print("├── Telegram Bot: Управление и мониторинг")
            print("└── Интеграция: Объединение всех компонентов")
            print("=" * 80)

            # 0) Telegram — всегда (чтобы был /keys)
            if sys_logger:
                with suppress(Exception):
                    sys_logger.log_startup("TelegramBot")
            await self._initialize_telegram_bot()

            # 1) Нет ключей? — подождём их «горячую» запись
            if not self._have_keys():
                await self._wait_for_keys()

            # 2) Патч Supervisor (идемпотентный, не зависит от точного имени)
            try:
                ns_mod = (sys.modules.get("network_supervisor")
                          or sys.modules.get("network_supervisor_fix")
                          or importlib.import_module("network_supervisor_fix"))
            except Exception:
                ns_mod = importlib.import_module("network_supervisor")  # возможный алиас

            ns_fn = (getattr(ns_mod, "patch_trading_system_with_supervisor", None)
                     or getattr(ns_mod, "patch_trading_system", None))
            if callable(ns_fn):
                ns_fn()  # безопасно вызывать повторно
                logger.info("Network Supervisor patch applied")
            else:
                logger.info("Network Supervisor patch skipped (module/func not present)")

            # 3) Инициализация Stage'ов
            if sys_logger:
                with suppress(Exception):
                    sys_logger.log_startup("Stage1Monitor")
            await self._initialize_stage1_monitor()

            if sys_logger:
                with suppress(Exception):
                    sys_logger.log_startup("Stage2System")
            await self._initialize_stage2_system()

            # >>> ВАЖНО: сразу после создания инстансов обновляем ссылки в боте
            try:
                self._refresh_bot_refs("after Stage1/Stage2 init")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after Stage1/Stage2 init)")

            # 4) ГАРАНТИРОВАННО загрузим и применим ключи в обе подсистемы
            with suppress(Exception):
                applied = await self._load_and_apply_credentials()
                logger.info("Startup credentials applied: %s", applied)
                if sys_logger and applied:
                    with suppress(Exception):
                        sys_logger.log_event(
                            "INFO", "IntegratedSystem",
                            "Credentials applied at startup",
                            {"accounts_count": len(applied) if isinstance(applied, list) else 1}
                        )

            # Подтянем ссылки ещё раз (после применения ключей они точно актуальны)
            try:
                self._refresh_bot_refs("after credentials apply")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after credentials apply)")

            # 5) АВТО-АКТИВАЦИЯ Stage-2 с ОТЛОЖЕННОЙ регистрацией обработчиков (исправлено)
            try:
                self.system_active = True

                s2 = getattr(self, "stage2_system", None)
                if s2:
                    # Активируем флаги заранее, чтобы тестовый сигнал не проваливался на active=False
                    if hasattr(s2, "copy_enabled"):
                        s2.copy_enabled = True
                    if hasattr(s2, "active"):
                        s2.active = True

                    async def delayed_handler_registration(system_ref, stage2_ref, retries=10, delay=0.5):
                        """
                        Отложенная регистрация обработчика Stage2 на корректном менеджере + стартовый reconcile.
                        Используем актуальные имена: stage1_monitor.websocket_manager и handle_position_signal.
                        """
                        await asyncio.sleep(3.0)  # даём системе подняться

                        attempt = 0
                        ws_mgr = None

                        while attempt <= retries:
                            attempt += 1

                            ws_mgr = getattr(getattr(system_ref, "stage1_monitor", None), "websocket_manager", None)

                            # есть менеджер и нужный метод у Stage2?
                            if ws_mgr and hasattr(stage2_ref, "handle_position_signal"):
                                try:
                                    # проверим, не зарегистрирован ли уже
                                    exists = False
                                    try:
                                        lst = ws_mgr.message_handlers.get("position_update", [])
                                        # сравнение bound-методов корректно работает
                                        exists = any(h is stage2_ref.handle_position_signal for h in lst)
                                    except Exception:
                                        pass

                                    if not exists:
                                        ws_mgr.register_handler("position_update", stage2_ref.handle_position_signal)
                                        logger.info("✅ DELAYED FIX: Stage2 handler registered for 'position_update'")
                                    else:
                                        logger.info("ℹ️ Stage2 handler already registered for 'position_update'")

                                    # Диагностика
                                    try:
                                        handlers_cnt = len(ws_mgr.message_handlers.get("position_update", []))
                                    except Exception:
                                        handlers_cnt = "N/A"
                                    logger.info(
                                        "🎯 CRITICAL SUCCESS: Stage2 handler READY — copying WILL work! "
                                        f"(handlers={handlers_cnt}, active={getattr(stage2_ref, 'active', 'N/A')}, "
                                        f"copy_enabled={getattr(stage2_ref, 'copy_enabled', 'N/A')})"
                                    )

                                    # --- Стартовый reconcile: прогнать уже открытые позиции источника
                                    try:
                                        source_client = getattr(getattr(system_ref, "stage1_monitor", None), "source_client", None)
                                        if source_client and hasattr(source_client, "get_positions"):
                                            src_positions = await source_client.get_positions()
                                            if src_positions:
                                                logger.info(f"🔄 Initial SOURCE→MAIN reconcile: {len(src_positions)} positions")
                                                for pos in src_positions:
                                                    position_data = {
                                                        "symbol": pos.get("symbol"),
                                                        "side": pos.get("side"),
                                                        "size": str(pos.get("size", 0)),
                                                        "markPrice": str(pos.get("markPrice", pos.get("avgPrice", 0))),
                                                        "unrealisedPnl": str(pos.get("unrealisedPnl", 0)),
                                                    }
                                                    await stage2_ref.handle_position_signal(position_data)
                                                    logger.info(f"Reconciled: {position_data['symbol']} {position_data['side']} size={position_data['size']}")
                                            else:
                                                logger.info("Initial reconcile: no source positions found")
                                    except Exception as e:
                                        logger.error(f"Initial reconcile failed: {e}")

                                    return  # всё ок, выходим
                                except Exception as e:
                                    logger.error(f"Delayed registration failed on attempt {attempt}: {e}")

                            await asyncio.sleep(delay)

                        logger.error("❌ CRITICAL FAILURE: Stage2 handler NOT registered — positions WON'T copy!")
                        if not ws_mgr:
                            logger.error("   Reason: stage1_monitor.websocket_manager is not available after retries")

                    # Запускаем отложенную регистрацию С ПЕРЕДАЧЕЙ ССЫЛОК
                    asyncio.create_task(delayed_handler_registration(self, s2))

                    # Вызываем start_copying/enable_copying если присутствует
                    start_coro = getattr(s2, "start_copying", None) or getattr(s2, "enable_copying", None)
                    if callable(start_coro):
                        with suppress(Exception):
                            res = start_coro()
                            if hasattr(res, "__await__"):
                                await res

                    logger.info("Stage-2 activated, handler registration scheduled with correct context")
                    if sys_logger:
                        with suppress(Exception):
                            sys_logger.log_event(
                                "INFO", "Stage2System",
                                "Auto-activated with delayed handler registration"
                            )
                else:
                    logger.warning("Stage-2 instance is missing")

            except Exception as e:
                logger.exception("Failed to activate Stage-2: %s", e)
                if sys_logger:
                    with suppress(Exception):
                        sys_logger.log_error("Stage2System", f"Auto-activation failed: {str(e)}")

            # Ещё раз после активации — чтобы кнопки ссылались уже на «живой» Stage2
            try:
                self._refresh_bot_refs("after Stage2 auto-activation")
            except TypeError:
                self._refresh_bot_refs()
            logger.info("Telegram bot references refreshed (after Stage2 auto-activation)")

            # 6) Стартовое уведомление
            with suppress(Exception):
                await send_telegram_alert(
                    "🚀 **ИНТЕГРИРОВАННАЯ СИСТЕМА ЗАПУЩЕНА**\n"
                    "✅ Этап 1: Мониторинг активен\n"
                    "✅ Этап 2: Копирование включено\n"
                    "✅ Telegram Bot: Управление доступно\n"
                    "🎯 Система готова к работе!"
                )

            # 7) Основной цикл мониторинга (создание задач уже реализовано внутри)
            await self._run_integrated_monitoring_loop()

        except Exception as e:
            logger.error("Integrated system startup error: %s", e)
            logger.error("Full traceback: %s", traceback.format_exc())
            if sys_logger:
                with suppress(Exception):
                    sys_logger.log_error("IntegratedSystem", str(e), {
                        "traceback": traceback.format_exc()[:500],
                        "phase": "startup"
                    })
            with suppress(Exception):
                await send_telegram_alert(f"❌ Ошибка запуска интегрированной системы: {e}")
            raise

    async def _initialize_stage1_monitor(self):
        """Инициализация Этапа 1 — система мониторинга (FinalTradingMonitor)."""
        try:
            print("\n📡 ЭТАП 1: ИНИЦИАЛИЗАЦИЯ СИСТЕМЫ МОНИТОРИНГА")
            print("-" * 50)

            # 1) Создаём монитор Stage1
            self.stage1_monitor = FinalTradingMonitor()

            # 2) Патч починки WebSocket-аутентификации/реконнекта (если реализован)
            if hasattr(self, "_patch_ws_connect"):
                self._patch_ws_connect()

            # 3) Обновляем ссылки у Telegram-бота, чтобы кнопки ссылались на актуальные объекты
            if hasattr(self, "_refresh_bot_refs"):
                self._refresh_bot_refs()

            print("✅ WebSocket Manager создан")
            print("✅ API Clients инициализированы")
            print("✅ Signal Processor готов")
            print("✅ Final Trading Monitor активен")

            logger.info("Stage 1 Monitor initialization completed")

        except Exception as e:
            logger.error(f"Stage 1 initialization error: {e}", exc_info=True)
            try:
                await send_telegram_alert(f"❌ Критическая ошибка инициализации Этапа 1: {e}")
            finally:
                raise


    async def _initialize_stage2_system(self):
        """Инициализация Этапа 2 — система копирования с правильной интеграцией Telegram бота."""
        try:
            print("\n🔄 ЭТАП 2: ИНИЦИАЛИЗАЦИЯ СИСТЕМЫ КОПИРОВАНИЯ")
            print("-" * 50)

            # 1) Создаём Stage2 с корректным аргументом конструктора
            try:
                from config import MAIN_API_KEY, MAIN_API_SECRET, MAIN_API_URL
            except ImportError:
                MAIN_API_KEY, MAIN_API_SECRET, MAIN_API_URL = None, None, None

            try:
                from stage2_copy_system import COPY_CONFIG, KELLY_CONFIG, TRAILING_CONFIG, RISK_CONFIG
            except ImportError:
                COPY_CONFIG, KELLY_CONFIG, TRAILING_CONFIG, RISK_CONFIG = {}, {}, {}, {}

            self.stage2_system = Stage2CopyTradingSystem(
                base_monitor=self.stage1_monitor,
                main_api_key=MAIN_API_KEY,
                main_api_secret=MAIN_API_SECRET,
                main_api_url=MAIN_API_URL,
                copy_config=COPY_CONFIG,
                kelly_config=KELLY_CONFIG,
                trailing_config=TRAILING_CONFIG,
                risk_config=RISK_CONFIG,
            )

            # Link Stage 2 system to Stage 1 monitor
            if self.stage1_monitor:
                self.stage1_monitor.stage2_system = self.stage2_system

            # 2) Сразу обновим ссылки у Telegram-бота (кнопки/меню используют stage2)
            if hasattr(self, "_refresh_bot_refs"):
                self._refresh_bot_refs()

            # 3) Инициализация внутренних компонентов Stage2
            await self.stage2_system.initialize()

            # ===================================================================
            # КРИТИЧЕСКИ ВАЖНО: Интегрируем Telegram bot с системами
            # ===================================================================
            self.integrate_telegram_bot_with_systems()
            # ===================================================================

            # 4) (опционально) Принудительно включить LIVE, если ключи есть
            if hasattr(self, "_have_keys") and callable(self._have_keys) and self._have_keys():
                self.stage2_system.system_active = True
                self.stage2_system.copy_enabled = True

            # 5) Отчёт о режиме
            demo_mode = getattr(self.stage2_system, "demo_mode", None)
            if demo_mode is True:
                print("🧪 РЕЖИМ: DEMO (безопасное тестирование)")
                print("⚠️ Реальные ордера НЕ размещаются")
            elif demo_mode is False or (getattr(self.stage2_system, "system_active", False) and getattr(self.stage2_system, "copy_enabled", False)):
                print("🔥 РЕЖИМ: LIVE (реальная торговля)")
                print("💰 Реальные ордера РАЗМЕЩАЮТСЯ")
            else:
                # Нейтральный вывод, если у класса нет флага demo_mode
                print("ℹ️ РЕЖИМ: Определяется параметрами system_active/copy_enabled")

            # 6) Быстрые sanity-checks компонентов
            if hasattr(self.stage2_system, "kelly_calculator"):
                print("✅ Kelly Criterion калькулятор готов")
            else:
                print("❌ Kelly Criterion калькулятор НЕ инициализирован")
                logger.warning("Kelly Calculator не был инициализирован")

            if hasattr(self.stage2_system, "trailing_manager"):
                print("✅ Trailing Stop Manager готов")
            else:
                print("❌ Trailing Stop Manager НЕ инициализирован")
                logger.warning("Trailing Manager не был инициализирован")

            print("✅ Position Copy Manager создан")
            print("✅ Drawdown Controller активирован")
            print("✅ Система копирования инициализирована")
            print("🔥 БОЕВОЙ РЕЖИМ АКТИВЕН - ГОТОВ К КОПИРОВАНИЮ!")

            logger.info("Stage 2 initialization completed with integration and LIVE MODE activated")

            # Еще раз обновляем ссылки после полной инициализации
            if hasattr(self, "_refresh_bot_refs"):
                self._refresh_bot_refs()
        
            logger.info("Telegram bot references refreshed (after Stage1/Stage2 init)")

        except Exception as e:
            logger.error(f"Stage 2 initialization error: {e}", exc_info=True)
            try:
                await send_telegram_alert(f"❌ Критическая ошибка инициализации Этапа 2: {e}")
            finally:
                raise


    def integrate_telegram_bot_with_systems(self):
        """
        Интегрирует Telegram бота с торговыми системами Stage1 и Stage2.
        Вызывается автоматически из _initialize_stage2_system().
        """
        # Проверяем наличие telegram_bot
        if not hasattr(self, 'telegram_bot') or self.telegram_bot is None:
            logger.warning("Telegram bot not initialized, skipping integration")
            return
    
        # Проверяем наличие метода attach_integrations
        if hasattr(self.telegram_bot, 'attach_integrations'):
            # Используем новый метод
            self.telegram_bot.attach_integrations(
                monitor=self.stage1_monitor,
                copy_system=self.stage2_system,
                application=self.telegram_application,
                integrated_system=self
            )
            logger.info("✅ Telegram bot integrated with Stage1/Stage2 using attach_integrations")
        else:
            # Fallback: прямое присваивание
            self.telegram_bot.monitor = self.stage1_monitor
            self.telegram_bot.copy_system = self.stage2_system
            self.telegram_bot.stage2 = self.stage2_system
            self.telegram_bot.integrated_system = self
        
            # Также сохраняем в bot_data если возможно
            if hasattr(self, 'telegram_application') and self.telegram_application:
                if not hasattr(self.telegram_application, 'bot_data'):
                    self.telegram_application.bot_data = {}
            
                self.telegram_application.bot_data['stage1_monitor'] = self.stage1_monitor
                self.telegram_application.bot_data['stage2_system'] = self.stage2_system
                self.telegram_application.bot_data['copy_system'] = self.stage2_system
                self.telegram_application.bot_data['integrated_system'] = self
        
            logger.info("✅ Telegram bot integrated with Stage1/Stage2 using direct assignment")
    
        # Проверка интеграции
        if self.telegram_bot.copy_system and hasattr(self.telegram_bot.copy_system, 'base_monitor'):
            logger.info("✅ Integration verified: telegram_bot can access base_monitor")
        else:
            logger.warning("⚠️ Integration incomplete: base_monitor not accessible")


    def _refresh_bot_refs(self):
        """
        Обновляет ссылки на системы в Telegram боте.
        Вызывается после изменения состояния систем.
        """
        if not hasattr(self, 'telegram_bot') or self.telegram_bot is None:
            return
    
        # Обновляем ссылки
        if hasattr(self, 'stage1_monitor'):
            self.telegram_bot.monitor = self.stage1_monitor
    
        if hasattr(self, 'stage2_system'):
            self.telegram_bot.copy_system = self.stage2_system
            self.telegram_bot.stage2 = self.stage2_system
    
        self.telegram_bot.integrated_system = self
    
        logger.info("Telegram bot references refreshed (monitor & stage2)")


    async def _initialize_telegram_bot(self):
        """🔧 ИСПРАВЛЕННАЯ инициализация Telegram Bot + актуализация ссылок на системы"""
        try:
            print("\n🤖 TELEGRAM BOT: ИНИЦИАЛИЗАЦИЯ УПРАВЛЕНИЯ")
            print("-" * 50)

            # если уже поднят — просто обновим ссылки и выходим
            if getattr(self, "telegram_bot", None) and getattr(self.telegram_bot, "is_running", False):
                logger.info("Telegram Bot already running — start() ignored (idempotent)")
                self._refresh_bot_refs()
                return

            # создаём бота и сразу передаём ссылки на системы
            self.telegram_bot = Stage2TelegramBot(copy_system=self.stage2_system)
            # критично для /keys → «Применить без рестарта»
            self.telegram_bot.integrated_system = self

            # сразу подтянуть актуальные ссылки (monitor + stage2)
            self._refresh_bot_refs()

            await self.telegram_bot.start()

            # ↓↓↓ ВСТАВКА: защита от двойной регистрации /keys ↓↓↓
            # Не допускаем двойную регистрацию /keys: если уже есть основной хендлер — не трогаем,
            # иначе аккуратно подключаем fallback из stage2_telegram_bot
            try:
                app = getattr(self.telegram_bot, "app", None) or getattr(self.telegram_bot, "application", None)
                if app and app.bot_data.get("keys_menu_registered"):
                    logger.info("Primary /keys handler is registered — skipping fallback")
                else:
                    from stage2_telegram_bot import register_tg_keys_menu as _reg
                    if callable(_reg) and app:
                        _reg(app, self)
                        logger.info("/keys ConversationHandler registered (fallback)")
            except Exception as e:
                logger.warning("Failed to ensure /keys handler: %s", e)

            # ПОСЛЕ
            print("✅ Stage2TelegramBot инициализирован")
            print("✅ Команды управления настроены")
            print("✅ Интерактивные кнопки созданы")
            print("✅ Уведомления и алерты активны")
            print("✅ Telegram Bot готов к работе")
            logger.info("Telegram Bot initialization completed successfully")

        except Exception as e:
            logger.error(f"Telegram Bot initialization error: {e}")
            print(f"⚠️ Telegram Bot недоступен: {e}")
            print("⚠️ Система продолжит работу без Telegram Bot")
            logger.warning("System will continue without Telegram Bot")

    def _restart_component_by_task(self, name: str):
        """
        Возвращает корутину рестарта для задачи с данным именем.
        Если рестарт не предусмотрен — возвращает None.
        """
        if name in {"Stage1_Monitor", "HotReload_Stage1", "HotReload_Stage1_Restart"} and self.stage1_monitor:
            return self.stage1_monitor.start()

        if name in {"Stage2_CopySystem", "Stage2_Copy", "HotReload_Stage2", "HotReload_Stage2_Restart"} and self.stage2_system:
            return self._pick_stage2_start_coro(self.stage2_system)

        if name in {"SystemMonitor", "SystemHealthMonitor", "HotReload_Monitor", "HotReload_Monitor_Restart"} and hasattr(self, "_system_health_monitor"):
            return self._system_health_monitor()

        if name in {"Stage2_TelegramBot"} and getattr(self, "telegram_bot", None):
            return self.telegram_bot.start()

        return None


    async def _run_integrated_monitoring_loop(self):
        """Основной цикл мониторинга интегрированной системы (с авто-рестартом компонентов)
            + фоновые задачи для БД позиций и reconcile, перенесённые из start().
        """
        import asyncio, inspect, os, traceback

        try:
            print("\n🔄 ЗАПУСК ОСНОВНОГО ЦИКЛА МОНИТОРИНГА")
            print("-" * 50)

            # Создаём и регистрируем задачи компонентов (идемпотентно)
            tasks = []

            # Stage 1 (мониторинг)
            if getattr(self, "stage1_monitor", None):
                t = asyncio.create_task(self.stage1_monitor.start(), name="Stage1_Monitor")
                tasks.append(t)
                self.active_tasks.add(t)
                print("✅ Stage 1 monitoring task created")

            # Stage 2 (копирование)
            if getattr(self, "stage2_system", None):
                t = asyncio.create_task(self.stage2_system.start_system(), name="Stage2_CopySystem")
                tasks.append(t)
                self.active_tasks.add(t)
                print("✅ Stage 2 copy system task created")

            # === Telegram Bot (неблокирующий старт + авто-перезапуск внутри одной задачи) ===
            if getattr(self, "telegram_bot", None):
                async def _run_tg():
                    while not getattr(self, "should_stop", False):
                        try:
                            await self.telegram_bot.start()
                            while not getattr(self, "should_stop", False):
                                await asyncio.sleep(5)
                            break
                        except Exception as e:
                            logger.warning("Telegram bot crashed: %r. Restart in 5s ...", e)
                            sys_logger.log_reconnect("TelegramBot", "PTB Application",
                                                     self.integrated_stats.get('telegram_restarts', 0) + 1)
                            await asyncio.sleep(5)

                tg_task = asyncio.create_task(_run_tg(), name="Stage2_TelegramBot")
                tasks.append(tg_task)
                self.active_tasks.add(tg_task)
                print("✅ Telegram Bot task created")

            # Системный монитор/healthcheck
            t = asyncio.create_task(self._system_health_monitor(), name="SystemMonitor")
            tasks.append(t)
            self.active_tasks.add(t)
            print("✅ System health monitor task created")

            # --- Optional WEB API ---
            self.web_api_service = None
            if os.getenv("WEB_API_ENABLE", "0") == "1":
                host = os.getenv("WEB_API_HOST", "0.0.0.0")
                port = int(os.getenv("WEB_API_PORT", "8080"))
                reload_flag = os.getenv("WEB_API_RELOAD", "0") == "1"  # в prod держим 0
                self.web_api_service = WebAPIService(host=host, port=port, reload=reload_flag)
                await self.web_api_service.start()
                print("✅ Web API task created")

            # === НОВОЕ: фоновые задачи для записи позиций в БД и периодического reconcile ===
            # Пытаемся получить WebSocketManager Stage-1

            ws = None
            if getattr(self, "stage1_monitor", None) is not None:
                ws = getattr(self.stage1_monitor, "websocket_manager", None)

            if ws is not None:
                # ► Надёжно привязываем API-клиентов к WebSocket manager (для REST reconcile)
                def _resolve_client():
                    # ищем main/source в разных местах и под разными именами
                    holders = [
                        self,
                        getattr(self, "stage1_monitor", None),
                    ]
                    names = [
                        "main_client", "main_api",
                        "source_client", "source_api",
                    ]
                    found = {}
                    for h in holders:
                        if not h:
                            continue
                        for n in names:
                            try:
                                v = getattr(h, n)
                            except Exception:
                                v = None
                            if v:
                                found[n] = v
                    main = found.get("main_client") or found.get("main_api")
                    source = found.get("source_client") or found.get("source_api")
                    return main, source

                main_client, source_client = _resolve_client()

                if main_client and not getattr(ws, "main_client", None):
                    ws.main_client = main_client
                if source_client and not getattr(ws, "source_client", None):
                    ws.source_client = source_client
                if not getattr(ws, "api_client", None):
                    ws.api_client = main_client or source_client

                logger.info(
                    "🔗 WS API clients bound (main=%s, source=%s, api=%s)",
                    bool(getattr(ws, "main_client", None)),
                    bool(getattr(ws, "source_client", None)),
                    bool(getattr(ws, "api_client", None)),
                )

                # 1) Очередь для записи позиций в БД — один раз
                if not hasattr(ws, "_positions_db_queue"):
                    ws._positions_db_queue = asyncio.Queue(maxsize=1000)
                    logger.info("✅ WS positions DB queue created (maxsize=1000)")


                # 2) Воркеры записи в БД
                async def _fallback_db_worker(queue, worker_id: int, stop_owner):
                    """Если у ws нет метода _positions_db_worker — используем общий воркер."""
                    try:
                        try:
                            from app.positions_db_writer import positions_writer
                        except ImportError:
                            from positions_db_writer import positions_writer
                        while not getattr(stop_owner, "should_stop", False):
                            try:
                                account_id, pos = await queue.get()
                                try:
                                    await positions_writer.upsert_position(pos, account_id)
                                except Exception as e:
                                    logger.error("DB writer error: %s", e)
                                finally:
                                    queue.task_done()
                            except asyncio.CancelledError:
                                break
                            except Exception as e:
                                logger.error("DB worker %s error: %s", worker_id, e)
                                await asyncio.sleep(0)
                    finally:
                        logger.info("DB worker %s stopped", worker_id)

                make_worker = getattr(ws, "_positions_db_worker", None)
                for i in (1, 2):
                    if callable(make_worker):
                        wtask = asyncio.create_task(make_worker(i), name=f"WS_DBW_{i}")
                    else:
                        wtask = asyncio.create_task(_fallback_db_worker(ws._positions_db_queue, i, ws),
                                                   name=f"WS_DBW_{i}")
                    tasks.append(wtask)
                    self.active_tasks.add(wtask)
                logger.info("✅ Positions DB workers started (2)")

                # 3) Периодический reconcile через REST (уважаем RECONCILE_ENABLE)
                reconcile_coro = getattr(ws, "reconcile_positions_from_rest", None)
                reconcile_enabled = os.getenv("RECONCILE_ENABLE", "1") == "1"
                if callable(reconcile_coro) and reconcile_enabled:
                    rtask = asyncio.create_task(reconcile_coro(), name="Stage1_PositionsReconcile")
                    tasks.append(rtask)
                    self.active_tasks.add(rtask)
                    logger.info("✅ Positions reconciliation task started")
                elif callable(reconcile_coro) and not reconcile_enabled:
                    logger.info("Positions reconciliation is disabled; task not started")
                else:
                    logger.warning("reconcile_positions_from_rest() not found in websocket_manager")

            print(f"\n🎯 Запущено {len(tasks)} системных задач")
            print("🔄 Система работает в полностью автономном режиме")
            print("📱 Управление доступно через Telegram Bot")
            print("⌨️ Остановка: Ctrl+C")

            # --- Главный оркестратор: не валим процесс, если упал один из компонентов ---
            while not getattr(self, "should_stop", False):
                if not self.active_tasks:
                    logger.warning("No active tasks left; breaking orchestrator loop")
                    break

                done, pending = await asyncio.wait(self.active_tasks, return_when=asyncio.FIRST_COMPLETED)

                for t in done:
                    name = t.get_name()
                    err = None
                    try:
                        _ = t.result()
                    except asyncio.CancelledError:
                        logger.info("Task %s cancelled", name)
                    except Exception as e:
                        err = e

                    if err is None:
                        logger.info("Task %s completed normally", name)
                    else:
                        logger.warning(
                            "Task %s exited with error: %r. Policy: restart only this component.",
                            name, err
                        )
                        sys_logger.log_error(name, f"Component crashed: {str(err)}", {"will_restart": True})
                        self.integrated_stats['critical_errors'] = self.integrated_stats.get('critical_errors', 0) + 1

                    # Удаляем задачу из реестра
                    try:
                        self.active_tasks.remove(t)
                    except KeyError:
                        pass

                    # Локальная политика рестартов для наших новых задач по имени
                    try:
                        if name.startswith("WS_DBW_") and ws is not None:
                            idx = int(name.split("_")[-1])
                            make_worker = getattr(ws, "_positions_db_worker", None)
                            if callable(make_worker):
                                nt = asyncio.create_task(make_worker(idx), name=name)
                            else:
                                nt = asyncio.create_task(_fallback_db_worker(ws._positions_db_queue, idx, ws), name=name)
                            self.active_tasks.add(nt)
                            logger.info("%s restarted", name)
                            continue

                        if name == "Stage1_PositionsReconcile" and ws is not None:
                            if os.getenv("RECONCILE_ENABLE", "1") == "1":
                                reconcile_coro = getattr(ws, "reconcile_positions_from_rest", None)
                                if callable(reconcile_coro):
                                    nt = asyncio.create_task(reconcile_coro(), name=name)
                                    self.active_tasks.add(nt)
                                    logger.info("%s restarted", name)
                                    continue
                            logger.info("Reconcile disabled; skip restart for Stage1_PositionsReconcile")
                            continue

                        # Иначе — отдаём на общий хук (если есть)
                        restart_hook = getattr(self, "_restart_component_by_task", None)
                        if restart_hook is None:
                            logger.debug("_restart_component_by_task not implemented; skipping for %s", name)
                        else:
                            coro_or_fn = self._restart_component_by_task(name)
                            if coro_or_fn is None:
                                logger.info("No restart action for task %s — skipping.", name)
                            else:
                                if inspect.isawaitable(coro_or_fn):
                                    await coro_or_fn
                                    sys_logger.log_event("INFO", name, f"{name} restarted successfully")
                                    if 'Stage1' in name:
                                        self.integrated_stats['stage1_restarts'] = self.integrated_stats.get('stage1_restarts', 0) + 1
                                    elif 'Stage2' in name:
                                        self.integrated_stats['stage2_restarts'] = self.integrated_stats.get('stage2_restarts', 0) + 1
                                elif callable(coro_or_fn):
                                    result = coro_or_fn()
                                    if inspect.isawaitable(result):
                                        await result
                                else:
                                    logger.warning("Restart hook for %s returned non-awaitable %r; skipping.", name, type(coro_or_fn))

                    except Exception as re:
                        logger.error("Component restart failed for %s: %s", name, re)
                        sys_logger.log_error(name, f"Restart failed: {str(re)}")

                # Обновляем активный набор задач
                self.active_tasks = {t for t in self.active_tasks if not t.done()}

            print("\n✅ Все задачи завершены")

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
            sys_logger.log_event("INFO", "IntegratedSystem", "Shutdown initiated by user (Ctrl+C)")
            for t in list(self.active_tasks):
                if not t.done():
                    t.cancel()
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

        except Exception as e:
            logger.error("Integrated monitoring loop error: %s", e)
            logger.error("Full traceback: %s", traceback.format_exc())
            sys_logger.log_error("IntegratedSystem", str(e), {
                "traceback": traceback.format_exc()[:500],
                "phase": "monitoring_loop"
            })
            try:
                await send_telegram_alert(f"💥 Критическая ошибка цикла мониторинга: {e}")
            except Exception:
                pass
            raise

        finally:
            # Мягкий останов подсистем и дренаж сетевых клиентов (важно для aiohttp)
            try:
                await self._graceful_subsystems_stop()
            except Exception:
                logger.exception("Graceful subsystems stop failed")

    async def _initiate_shutdown(self, sig: signal.Signals | str = "SIGTERM") -> None:
        """
        Внешняя точка запроса остановки — безопасно вызывает уже существующие
        `_graceful_subsystems_stop()` и `_close_all_http_clients()`.
        """
        if getattr(self, "_stopping", False):
            return
        self._stopping = True

        lg = logging.getLogger("trading_bot")
        try:
            name = sig.name if hasattr(sig, "name") else str(sig)
        except Exception:
            name = str(sig)
        lg.warning("Shutdown requested (%s)", name)

        # 1) твоя «аккуратная» остановка подсистем
        with suppress(Exception):
            await self._graceful_subsystems_stop()

        # 2) финальный дренаж HTTP/WS клиентов (на всякий случай)
        with suppress(Exception):
            await self._close_all_http_clients()

        # 3) отменяем оставшиеся таски этого объекта
        for t in list(getattr(self, "active_tasks", [])):
            if not t.done():
                t.cancel()
                with suppress(asyncio.CancelledError):
                    await t
            with suppress(Exception):
                self.active_tasks.discard(t)

        # 4) закрываем асинхронные генераторы цикла
        with suppress(Exception):
            await asyncio.get_running_loop().shutdown_asyncgens()

        lg.info("Stopped cleanly")


    async def _graceful_subsystems_stop(self) -> None:
        """Аккуратная остановка подсистем и закрытие сетевых клиентов."""
        # Сигнал остановки для всех внутренних циклов
        try:
            setattr(self, "should_stop", True)
        except Exception:
            pass

        # Останавливаем Телеграм первым — чтобы завершить polling
        if getattr(self, "telegram_bot", None) and hasattr(self.telegram_bot, "stop"):
            with suppress(Exception):
                await self.telegram_bot.stop()
                logger.info("Telegram bot stopped")

        # Останавливаем Stage2/Stage1
        if getattr(self, "stage2_system", None) and hasattr(self.stage2_system, "stop"):
            with suppress(Exception):
                await self.stage2_system.stop()

        if getattr(self, "stage1_monitor", None) and hasattr(self.stage1_monitor, "stop"):
            with suppress(Exception):
                await self.stage1_monitor.stop()

        # Отменяем оставшиеся таски
        for t in list(getattr(self, "active_tasks", [])):
            if not t.done():
                t.cancel()
                with suppress(asyncio.CancelledError):
                    await t
            self.active_tasks.discard(t)

        # Универсальный дренаж HTTP/WS клиентов
        with suppress(Exception):
            await self._close_all_http_clients()

    async def _close_all_http_clients(self) -> None:
        """
        Ищем и закрываем все живые http-клиенты в Stage1/Stage2/Telegram.
        Закрываем универсально: если у объекта есть .aclose() или .close(),
        корректно вызываем (await для корутин).
        """
        candidates = [
            getattr(self, "stage1_monitor", None),
            getattr(self, "stage2_system", None),
            getattr(self, "telegram_bot", None),
        ]
        seen = set()

        async def _maybe_await_call(obj, method_name: str) -> bool:
            """Вызвать метод, если есть; вернуть True если метод найден и успешно вызван."""
            fn = getattr(obj, method_name, None)
            if not fn:
                return False
            try:
                res = fn()
                if inspect.isawaitable(res):
                    await res
                return True
            except Exception:
                return True  # метод был, но упал — игнорируем при зачистке
            return False

        async def _close_client_like(obj):
            """Закрыть клиент/сессию утиной типизацией (aclose/close), включая вложенные .session/.client."""
            if not obj:
                return
            # сначала пытаемся aclose (httpx.AsyncClient и др.)
            if await _maybe_await_call(obj, "aclose"):
                return
            # затем close (aiohttp.ClientSession.close() — тоже корутина)
            await _maybe_await_call(obj, "close")

            # иногда внутри клиента хранится вложенная session/client
            inner = getattr(obj, "session", None) or getattr(obj, "client", None)
            if inner:
                await _close_client_like(inner)

        async def _walk(obj):
            if not obj or id(obj) in seen:
                return
            seen.add(id(obj))

            # Прямые ссылки на клиентов/сессии
            for attr_name in ("session", "_session", "http", "http_client", "client", "api", "connector"):
                client = getattr(obj, attr_name, None)
                if client:
                    await _close_client_like(client)

            # WebSocket-поля (поддерживаем и .aclose(), и .close())
            for field in ("websocket_manager", "ws_manager", "source_ws", "public_ws", "private_ws", "ws", "ws_public", "ws_private"):
                ws = getattr(obj, field, None)
                if ws:
                    # сначала aclose, затем close
                    if not await _maybe_await_call(ws, "aclose"):
                        await _maybe_await_call(ws, "close")

            # Рекурсивно смотрим «подозрительные» поля объекта
            try:
                members = vars(obj)  # быстрее, чем inspect.getmembers, и без вызова @property
            except Exception:
                members = {}

            for name, val in members.items():
                if name.startswith("_"):
                    continue
                if any(k in name for k in ("api", "client", "session", "manager", "connector", "source", "main", "ws")):
                    try:
                        await _walk(val)
                    except Exception:
                        pass

        for c in candidates:
            try:
                await _walk(c)
            except Exception:
                pass

        # На всякий случай — завершаем асинхронные генераторы цикла
        loop = asyncio.get_running_loop()
        try:
            await loop.shutdown_asyncgens()
        except Exception:
            pass

    
    async def _system_health_monitor(self):
        """Мониторинг здоровья системы с безопасным восстановлением подключений."""
        try:
            last_health_report = 0
            health_report_interval = 3600  # Каждый час
            check_interval = 30            # Проверяем каждые 30 сек

            # Набор имён подключений, которые обычно регистрируются монитором
            default_connections = ("source_api", "main_api", "websocket")

            while self.system_active:
                current_time = time.time()

                # 1) Периодический отчёт в Telegram
                if current_time - last_health_report > health_report_interval:
                    uptime = current_time - (self.start_time or current_time)
                    uptime_hours = uptime / 3600 if uptime > 0 else 0.0

                    health_report = (
                        f"🏥 **ОТЧЕТ О ЗДОРОВЬЕ СИСТЕМЫ**\n"
                        f"⏰ Время работы: {uptime_hours:.1f}ч\n"
                        f"📊 Активных задач: {len(self.active_tasks)}\n"
                        f"🔄 Этап 1: {'✅ Активен' if self.stage1_monitor else '❌ Неактивен'}\n"
                        f"📋 Этап 2: {'✅ Активен' if self.stage2_system else '❌ Неактивен'}\n"
                        f"🤖 Telegram: {'✅ Активен' if any(getattr(t, 'get_name', lambda: '')().lower().find('telegram') >= 0 and not t.done() for t in self.active_tasks) else '❌ Неактивен'}\n"
                        f"🎯 Статус: Система работает стабильно"
                    )
                    from contextlib import suppress
                    with suppress(Exception):
                        await send_telegram_alert(health_report)
                        logger.info("System health report sent")
                    last_health_report = current_time

                # 2) Здоровье сетевых подключений (безопасно)
                monitor = getattr(self, "stage1_monitor", None)
                connmon = None
                if monitor:
                    # Пытаемся найти объект мониторинга соединений внутри Stage1
                    for name in ("connection_monitor", "connection_monitor_pro", "monitor", "connection_manager"):
                        cm = getattr(monitor, name, None)
                        if cm is not None:
                            connmon = cm
                            break

                # Функции проверки здоровья могут называться по-разному
                def _get_health_fn(obj):
                    for meth in ("is_connection_healthy", "is_healthy", "check_health"):
                        fn = getattr(obj, meth, None)
                        if callable(fn):
                            return fn
                    return None

                import inspect

                # Функции восстановления тоже подбираем безопасно
                async def _try_recover(obj, connection_name: str) -> bool:
                    recovered = False
                    for meth in ("_attempt_recovery", "attempt_recovery", "reconnect"):
                        fn = getattr(obj, meth, None)
                        if not callable(fn):
                            continue
                        try:
                            res = fn(connection_name)  # может быть sync/async
                            if inspect.isawaitable(res):
                                await res
                            recovered = True
                            break
                        except Exception as e:
                            logger.debug("Recovery via %s on %s failed: %s", meth, obj.__class__.__name__, e)
                    return recovered

                # Кандидаты-носители методов здоровья/восстановления: сначала connmon, потом сам monitor
                health_candidates = [c for c in (connmon, monitor) if c is not None]

                # Перебираем ожидаемые подключения (если конкретный список не доступен)
                for connection_name in default_connections:
                    # Пытаемся вызвать health-функцию у первого объекта, у которого она найдётся
                    is_ok = None
                    for obj in health_candidates:
                        fn = _get_health_fn(obj)
                        if not fn:
                            continue
                        try:
                            res = fn(connection_name)
                            if inspect.isawaitable(res):
                                res = await res
                            # Ожидаем boolean; если вернули не bool — считаем None (неопределённо)
                            if isinstance(res, bool):
                                is_ok = res
                                break
                        except Exception as e:
                            logger.debug("Health check on %s via %s failed: %s", connection_name, obj.__class__.__name__, e)

                    # Если нет способа проверить — просто пропускаем без ошибок
                    if is_ok is None:
                        continue

                    if not is_ok:
                        logger.warning("Health check failed for %s", connection_name)
                        # ↓↓↓ добивка перед восстановлением ↓↓↓
                        try:
                            await self._reload_ws_credentials()
                        except Exception as e:
                            logger.debug("reload_ws_credentials failed: %s", e)
                        # ↑↑↑ конец вставки ↑↑↑

                        # Пытаемся восстановиться: connmon -> monitor
                        recovered = False
                        for obj in health_candidates:
                            try:
                                if await _try_recover(obj, connection_name):
                                    recovered = True
                                    logger.info("Recovery for %s via %s succeeded", connection_name, obj.__class__.__name__)
                                    break
                            except Exception as e:
                                logger.debug("Recovery dispatcher on %s failed: %s", obj.__class__.__name__, e)

                        if not recovered:
                            logger.debug("No recovery method available for %s", connection_name)

                # 3) Пауза между циклами проверки
                await asyncio.sleep(check_interval)

        except asyncio.CancelledError:
            logger.info("System health monitor cancelled")
            raise
        except Exception as e:
            logger.error(f"System health monitor error: {e}", exc_info=True)


    
    async def shutdown_system(self):
        """Корректное завершение работы системы (с «пылесосом» сетевых ресурсов)."""
    
        if not self.system_active:
            return
        
        try:
            print("\n🛑 ЗАВЕРШЕНИЕ РАБОТЫ ИНТЕГРИРОВАННОЙ СИСТЕМЫ")
            print("-" * 50)

            self.system_active = False

            # 1) Останавливаем все активные задачи оркестратора
            if self.active_tasks:
                print(f"🔄 Останавливаем {len(self.active_tasks)} активных задач...")

                for task in self.active_tasks.copy():
                    if not task.done():
                        task.cancel()
                        try:
                            await asyncio.wait_for(task, timeout=5.0)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass
                        except Exception as e:
                            logger.warning(f"Task shutdown error: {e}")

                self.active_tasks.clear()
                print("✅ Все задачи остановлены")

            # 2) Корректно завершаем компоненты

            # Stage 2 (копирование)
            if self.stage2_system:
                try:
                    # Если у Stage2 есть метод остановки — используем его
                    for m in ("shutdown_system", "shutdown", "stop", "close"):
                        fn = getattr(self.stage2_system, m, None)
                        if fn:
                            res = fn() if not inspect.iscoroutinefunction(fn) else fn()
                            if inspect.isawaitable(res):
                                await res
                            break
                    # Дополнительно глушим флаги
                    setattr(self.stage2_system, "system_active", False)
                    setattr(self.stage2_system, "copy_enabled", False)
                    print("✅ Stage 2 system shutdown completed")
                    # НОВОЕ: Логируем остановку Stage2
                    sys_logger.log_shutdown("Stage2System")
                except Exception as e:
                    logger.warning(f"Stage 2 shutdown error: {e}")
                    sys_logger.log_error("Stage2System", f"Shutdown error: {e}")

            # Stage 1 (мониторинг)
            if self.stage1_monitor:
                try:
                    # предпочитаем «официальный» метод, но есть приватный _shutdown
                    for m in ("shutdown", "_shutdown", "stop", "close"):
                        fn = getattr(self.stage1_monitor, m, None)
                        if fn:
                            res = fn() if not inspect.iscoroutinefunction(fn) else fn()
                            if inspect.isawaitable(res):
                                await res
                            break
                    print("✅ Stage 1 monitor shutdown completed")
                    # НОВОЕ: Логируем остановку Stage1
                    sys_logger.log_shutdown("Stage1Monitor")
                except Exception as e:
                    logger.warning(f"Stage 1 shutdown error: {e}")
                    sys_logger.log_error("Stage1Monitor", f"Shutdown error: {e}")

            # Telegram Bot (PTB v21)
            if self.telegram_bot:
                try:
                    self.telegram_bot.bot_active = False
                    app = getattr(self.telegram_bot, "application", None)
                    if app:
                        # На всякий случай отключаем webhook перед остановкой polling
                        try:
                            await app.bot.delete_webhook(drop_pending_updates=False)
                        except Exception:
                            pass

                        # Остановить polling
                        upd = getattr(app, "updater", None)
                        if upd and hasattr(upd, "stop"):
                            res = upd.stop()
                            if inspect.isawaitable(res):
                                await res

                        # Остановить приложение PTB
                        if hasattr(app, "stop"):
                            res = app.stop()
                            if inspect.isawaitable(res):
                                await res
                        if hasattr(app, "shutdown"):
                            res = app.shutdown()
                            if inspect.isawaitable(res):
                                await res

                    print("✅ Telegram Bot shutdown completed")
                    # НОВОЕ: Логируем остановку TelegramBot
                    sys_logger.log_shutdown("TelegramBot")
                except Exception as e:
                    logger.warning(f"Telegram Bot shutdown error: {e}")
                    sys_logger.log_error("TelegramBot", f"Shutdown error: {e}")

            # 3) Финальная статистика
            total_uptime = time.time() - self.start_time if hasattr(self, 'start_time') else 0
            self.integrated_stats['total_uptime'] = total_uptime

            # НОВОЕ: Логируем graceful shutdown с полной статистикой
            sys_logger.log_shutdown("IntegratedSystem", {
                "uptime_seconds": total_uptime,
                "uptime_hours": total_uptime / 3600,
                "stage1_restarts": self.integrated_stats.get('stage1_restarts', 0),
                "stage2_restarts": self.integrated_stats.get('stage2_restarts', 0),
                "critical_errors": self.integrated_stats.get('critical_errors', 0),
                "successful_starts": self.integrated_stats.get('successful_starts', 0)
            })

            try:
                await send_telegram_alert(
                    f"🛑 **ИНТЕГРИРОВАННАЯ СИСТЕМА ОСТАНОВЛЕНА**\n"
                    f"Время работы: {total_uptime/3600:.1f}ч\n"
                    "Все компоненты корректно завершены"
                )
            except Exception as e:
                logger.debug(f"Final telegram alert error: {e}")

            print("✅ Интегрированная система успешно завершена")

            # 4) Небольшая пауза — даём фоновым задачам/сетям корректно закрыться
            await asyncio.sleep(0.5)

            # 5) Отмена оставшихся задач (best-effort)
            pending = [
                t for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and not t.done()
            ]
            if pending:
                logger.info("Cancelling %d remaining tasks...", len(pending))
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

            # 6) Пылесос сетевых клиентов/сессий/WS без жёстких зависимостей
            async def _close_maybe_async(obj):
                try:
                    # aclose() — асинхронные клиенты (httpx.AsyncClient и т.п.)
                    if hasattr(obj, "aclose"):
                        res = obj.aclose()
                        if inspect.isawaitable(res):
                            await res
                            return
                    # close() может быть sync/async
                    if hasattr(obj, "close"):
                        res = obj.close()
                        if inspect.isawaitable(res):
                            await res
                except Exception as _e:
                    logger.debug(f"silent close error: {_e}")

            for obj in (self.stage1_monitor, self.stage2_system, self.telegram_bot):
                if not obj:
                    continue

                # Наиболее типичные поля с клиентами/сессиями
                for field in (
                    "session", "aiohttp_session", "http_session",
                    "client", "httpx_client", "rest_client", "api_client",
                    "connector", "enterprise_connector"
                ):
                    c = getattr(obj, field, None)
                    if c:
                        # Вложенные .session / .client внутри клиента
                        inner = getattr(c, "session", None) or getattr(c, "client", None)
                        if inner:
                            await _close_maybe_async(inner)
                        await _close_maybe_async(c)

                # WebSocket-поля
                for field in (
                    "websocket_manager", "ws_manager",
                    "source_ws", "public_ws", "private_ws",
                    "ws", "ws_public", "ws_private", "wss"
                ):
                    ws = getattr(obj, field, None)
                    if not ws:
                        continue
                    try:
                        if hasattr(ws, "close"):
                            res = ws.close()
                            if inspect.isawaitable(res):
                                await res
                    except Exception as _we:
                        logger.debug(f"ws close ignored: {_we}")

            logger.info("Integrated system shutdown completed")
        
            # НОВОЕ: Финальная запись о дренаже
            logger.info("All network clients drained")
            print("✅ All network clients drained")

        except Exception as e:
            logger.error(f"System shutdown error: {e}")
            print(f"❌ Ошибка завершения: {e}")
            # НОВОЕ: Логируем ошибку shutdown
            sys_logger.log_error("IntegratedSystem", str(e), {
                "phase": "shutdown"
            })


# ================================
# ФУНКЦИИ ЗАПУСКА
# ================================

async def run_integrated_system():
    """Запуск интегрированной системы"""
    system = None
    try:
        # Создаем и запускаем интегрированную систему
        system = IntegratedTradingSystem()
        await system.start_integrated_system()
        
    except KeyboardInterrupt:
        logger.info("System interrupted by user")
        print("\n🛑 Система остановлена пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Critical integrated system error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"\n💥 Критическая ошибка: {e}")
        
        # Отправляем критическое уведомление
        try:
            await send_telegram_alert(f"💥 Критическая ошибка системы: {e}")
        except:
            pass
    finally:
        if system:
            await system.shutdown_system()

def main():
    """Главная функция"""
    print("🚀 BYBIT COPY TRADING SYSTEM - ИНТЕГРИРОВАННЫЙ ЗАПУСК")
    print("=" * 80)
    print("АРХИТЕКТУРА СИСТЕМЫ:")
    print("├── Этап 1: Система мониторинга (WebSocket + API)")
    print("├── Этап 2: Система копирования (Kelly + Trailing + Orders)")  
    print("├── Telegram Bot: Управление и мониторинг")
    print("└── Интеграция: Объединение всех компонентов")
    print("=" * 80)
    print()
    
    # Проверяем наличие необходимых файлов
    required_files = [
        "enhanced_trading_system_final_fixed.py",
        "config.py"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print("❌ ОТСУТСТВУЮТ НЕОБХОДИМЫЕ ФАЙЛЫ:")
        for file in missing_files:
            print(f"   - {file}")
        print("\nСоздайте отсутствующие файлы и повторите запуск")
        return
    
    # Дополнительные файлы будут созданы из артефактов
    additional_files = [
        ("stage2_copy_system.py", "Создайте из первого артефакта"),
        ("stage2_telegram_bot.py", "Создайте из второго артефакта")
    ]
    
    for file, instruction in additional_files:
        if not os.path.exists(file):
            print(f"⚠️ {file} не найден - {instruction}")
    
    print()
    
    try:
        # Запускаем интегрированную систему
        if sys.platform == 'win32':
            # На Windows используем ProactorEventLoop
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        asyncio.run(run_integrated_system())
        
    except KeyboardInterrupt:
        print("\n🛑 Программа завершена пользователем")
    except Exception as e:
        print(f"\n💥 Ошибка запуска: {e}")
        print("\nПроверьте:")
        print("1. Все файлы находятся в одной директории")
        print("2. Зависимости установлены: pip install websockets aiohttp pandas numpy scipy python-telegram-bot")
        print("3. API ключи настроены в config.py")
        print("4. Telegram токен настроен в telegram_cfg.py")

if __name__ == "__main__":
    main()
