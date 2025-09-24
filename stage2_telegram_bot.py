#!/usr/bin/env python3
"""
РАСШИРЕННЫЙ TELEGRAM BOT ДЛЯ ЭТАПА 2
Версия 2.2 - ПРОФЕССИОНАЛЬНОЕ УПРАВЛЕНИЕ С ДОПОЛНИТЕЛЬНЫМИ ФУНКЦИЯМИ

🎯 ВОЗМОЖНОСТИ:
- ✅ Управление системой копирования
- ✅ Мониторинг Kelly Criterion расчетов
- ✅ Контроль Trailing Stop-Loss
- ✅ Управление рисками и просадкой
- ✅ Детальная аналитика производительности
- ✅ Экстренное управление позициями
- 🆕 Дополнительные команды настроек (set_kelly, performance, risks, daily_report, health_check)
- 🆕 НОВЫЕ ФУНКЦИИ ИЗ ПЛАНА ИНТЕГРАЦИИ:
  - Автоматические уведомления каждые 2 часа
  - Тест всех подключений
  - Просмотр логов системы
  - Управление настройками просадки
  - Управление настройками trailing stops
  - Экспорт отчетов
  - Backup и restore настроек
"""

import asyncio
import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import traceback
from contextlib import suppress
import sys
import importlib
from telegram.error import BadRequest
from app.db_session import SessionLocal
from app.db_models import SysEvents
import hashlib

from app.sys_events_logger import sys_logger

logger = logging.getLogger('bybit_trading_system')

# Гарантируем, что корень и папка app в sys.path
ROOT = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.join(ROOT, "app")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
if os.path.isdir(APP_DIR) and APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)




# ============================================
# ПОИСК И ИМПОРТ TG_KEYS_MENU
# ============================================
tg_keys_available = False
register_tg_keys_menu = None
_last_tgkm_err = None

# Список путей для поиска tg_keys_menu
tgkm_search_paths = ["tg_keys_menu", "app.tg_keys_menu", "telegram.tg_keys_menu"]

for _mod in tgkm_search_paths:
    try:
        exec(f"from {_mod} import register_tg_keys_menu")
        tg_keys_available = True
        break
    except ImportError as e:
        _last_tgkm_err = f"ImportError from {_mod}: {e}"
        continue
    except SyntaxError as e:
        _last_tgkm_err = f"SyntaxError in {_mod} → {getattr(e, 'filename', '?')}:{getattr(e, 'lineno', '?')} — {e.msg}"
        break
    except Exception as e:
        _last_tgkm_err = e

if not tg_keys_available:
    register_tg_keys_menu = None
    logging.warning("tg_keys_menu not found in any location (last error: %s)", _last_tgkm_err)

# ============================================
# ОСНОВНЫЕ ИМПОРТЫ TELEGRAM
# ============================================
try:
    from telegram import (
        Update,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        ReplyKeyboardMarkup,
        KeyboardButton,
    )
    from telegram.ext import (
        Application,
        CommandHandler,
        CallbackQueryHandler,
        MessageHandler,
        filters,
        ContextTypes,
        ConversationHandler,
    )
    from telegram.constants import ParseMode
    from telegram_helpers import patch_telegram_methods

except ImportError as e:
    print(f"❌ Ошибка импорта Telegram: {e}")
    print("Установите зависимости: pip install python-telegram-bot")
    raise

async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Глобальный обработчик ошибок Telegram Application.
    Не даёт ошибкам теряться в логах и показывает traceback.
    """
    # NB: context.error содержит исходное исключение
    # update может быть None (например, ошибка в JobQueue) — учитываем это
    try:
        logger.exception("Telegram error", exc_info=context.error)
    except Exception:
        # на всякий случай логируем в 'root', чтобы точно не потерять
        import traceback as _tb, logging as _lg
        _lg.getLogger(__name__).error("Telegram error: %s\n%s", context.error, _tb.format_exc())

async def _safe_edit_message(query, text, reply_markup=None, **kwargs):
    """
    Безопасный редактор сообщений для inline-кнопок.
    Избегает BadRequest при попытке отправить идентичное сообщение.
    """
    if not query or not query.message:
        logger.warning("_safe_edit_message: query or query.message is None")
        return
    
    msg = query.message
    
    # Получаем текущий текст и разметку
    prev_text = getattr(msg, "text_html", None) or msg.text or ""
    same_text = (prev_text == text)
    
    def _mkd(m):
        try:
            return m.to_dict() if m else None
        except Exception:
            return None
    
    same_markup = (_mkd(msg.reply_markup) == _mkd(reply_markup))
    
    # Если контент идентичен - просто отвечаем
    if same_text and same_markup:
        with suppress(Exception):
            await query.answer("Без изменений")
        return
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            with suppress(Exception):
                await query.answer("Без изменений")
            return
        # Для других ошибок - пробуем отправить новое сообщение
        try:
            await query.message.reply_text(text, reply_markup=reply_markup, **kwargs)
        except Exception as fallback_error:
            logger.error(f"Failed to send fallback message: {fallback_error}")
            raise e
    except Exception as e:
        # Критическая ошибка - логируем
        logger.error(f"Failed to edit message: {e}")
        raise


# ============================================
# ИСПРАВЛЕННЫЕ ИМПОРТЫ КОНФИГУРАЦИИ
# ============================================

# 🔧 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Правильный порядок импорта конфигурации
try:
    # 1) Сначала пробуем telegram_cfg (приоритет)
    from telegram_cfg import TELEGRAM_TOKEN, ADMIN_ONLY_IDS as CFG_ADMIN_IDS
    TELEGRAM_TOKEN_SOURCE = "telegram_cfg"
except ImportError:
    try:
        # 2) Fallback на enhanced_trading_system_final_fixed
        from enhanced_trading_system_final_fixed import TELEGRAM_TOKEN
        TELEGRAM_TOKEN_SOURCE = "enhanced_trading_system_final_fixed"
        CFG_ADMIN_IDS = set()
    except ImportError:
        # 3) Последний fallback - переменные окружения
        TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
        TELEGRAM_TOKEN_SOURCE = "environment"
        CFG_ADMIN_IDS = set()

# Дополнительный импорт из enhanced_trading_system_final_fixed
try:
    from enhanced_trading_system_final_fixed import (
        send_telegram_alert,
        logger,
        TELEGRAM_CHAT_ID,
        safe_float,
        TradingSignal,
        SignalType,
    )
except ImportError as e:
    print(f"⚠️ Частичная ошибка импорта из enhanced_trading_system_final_fixed: {e}")
    # Создаем fallback функции
    logger = logging.getLogger(__name__)
    TELEGRAM_CHAT_ID = None
    
    async def send_telegram_alert(message: str):
        logger.warning(f"Telegram alert (no bot): {message}")
    
    def safe_float(value, default=0.0):
        try:
            return float(value)
        except:
            return default

# 🔧 ИСПРАВЛЕНИЕ: Объединение админов из всех источников
ENV_ADMIN_IDS = {
    int(x.strip()) 
    for x in os.getenv("ADMIN_ONLY_IDS", "").replace(" ", "").split(",") 
    if x.strip().isdigit()
}

# Объединяем админов из telegram_cfg и ENV
ADMIN_IDS = set(CFG_ADMIN_IDS) | ENV_ADMIN_IDS

# Добавляем TELEGRAM_CHAT_ID как админа (совместимость)
if TELEGRAM_CHAT_ID:
    try:
        ADMIN_IDS.add(int(TELEGRAM_CHAT_ID))
    except (ValueError, TypeError):
        pass

# Конвертируем в список для совместимости
ADMIN_IDS = list(ADMIN_IDS)

# Логируем источники конфигурации
logger.info(f"✅ TELEGRAM_TOKEN loaded from: {TELEGRAM_TOKEN_SOURCE}")
logger.info(f"✅ ADMIN_IDS count: {len(ADMIN_IDS)} (sources: telegram_cfg + ENV)")

# Проверяем что токен существует
if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN not found in any source!")

# ============================================
# FALLBACK HANDLER ДЛЯ /keys ЕСЛИ ОСНОВНОЙ МОДУЛЬ НЕДОСТУПЕН
# ============================================

if not tg_keys_available:
    # Создаем fallback implementation только если основной модуль недоступен
    try:
        from app.database_security_implementation import CredentialsStore
        
        # Константы для FSM состояний
        WAIT_API_KEY, WAIT_API_SECRET = range(2)
        
        async def fallback_keys_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Fallback команда /keys"""
            await update.message.reply_text(
                "🔑 **УПРАВЛЕНИЕ API КЛЮЧАМИ** (Fallback режим)\n\n"
                "⚠️ Основной модуль tg_keys_menu недоступен.\n"
                "Используется упрощенная версия.\n\n"
                "Отправьте API Key для настройки:"
            )
            return WAIT_API_KEY

        async def fallback_receive_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Получение API ключа"""
            context.user_data['api_key'] = update.message.text
            await update.message.reply_text("✅ API Key получен. Теперь отправьте API Secret:")
            return WAIT_API_SECRET

        async def fallback_receive_secret(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Получение API секрета и сохранение"""
            api_secret = update.message.text
            api_key = context.user_data.get('api_key')
            
            if not api_key:
                await update.message.reply_text("❌ Ошибка: API Key потерян. Начните заново с /keys")
                return ConversationHandler.END
            
            # Сохраняем в БД
            try:
                store = CredentialsStore()
                store.set_account_credentials(1, api_key, api_secret)  # Account ID = 1 по умолчанию
                await update.message.reply_text("✅ Ключи успешно сохранены!")
                
                # Применяем новые ключи если есть интегрированная система
                integrated_system = context.application.bot_data.get("integrated_system")
                if integrated_system and hasattr(integrated_system, "_on_keys_saved"):
                    await integrated_system._on_keys_saved()
                    
            except Exception as e:
                logger.error(f"Failed to save keys: {e}")
                await update.message.reply_text(f"❌ Ошибка сохранения: {e}")
            
            return ConversationHandler.END

        async def fallback_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Отмена операции"""
            await update.message.reply_text("❌ Операция отменена")
            return ConversationHandler.END

        def register_tg_keys_menu(app, integrated_system):
            """Fallback регистратор /keys команды"""
            app.bot_data["integrated_system"] = integrated_system

            # Создаём ConversationHandler для ввода ключей
            conv_handler = ConversationHandler(
                entry_points=[CommandHandler("keys", fallback_keys_command)],
                states={
                    WAIT_API_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_receive_key)],
                    WAIT_API_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_receive_secret)],
                },
                fallbacks=[
                    CommandHandler("cancel", fallback_cancel),
                    CommandHandler("keys", fallback_keys_command),  # Позволяет начать заново
                ],
                name="keys_fallback_fsm",
                persistent=False,
            )

            app.add_handler(conv_handler)
            logger.info("✅ Fallback /keys handler registered successfully")

            # Устанавливаем флаг регистрации
            app.bot_data["keys_menu_registered"] = True
            app.bot_data["keys_menu_fallback"] = True
    except ImportError:
        # Если даже CredentialsStore недоступен, создаём заглушку
        def register_tg_keys_menu(app, integrated_system):
            """Заглушка когда невозможно работать с ключами"""
            logger.error("Cannot register /keys handler - CredentialsStore not available")
            app.bot_data["keys_menu_registered"] = False
            app.bot_data["keys_menu_error"] = "CredentialsStore not available"

# Финальная проверка
if register_tg_keys_menu:
    if tg_keys_available:
        logger.info("✅ Using original tg_keys_menu module")
    else:
        logger.info("✅ Using fallback /keys implementation")
else:
    logger.error("❌ No /keys handler available - system will work without key management")


class Stage2TelegramBot:
    """
    Расширенный Telegram Bot для управления системой копирования Этап 2
    """
    
    def __init__(self, copy_system=None):
        """
        🔧 ИСПРАВЛЕННЫЙ КОНСТРУКТОР с правильной обработкой конфигурации
        """
        # --- Основные поля (сохранены) ---
        self.copy_system = copy_system
        self.monitor = None  
        self.copy_system = copy_system  
        self.integrated_system = None  # НЕ copy_system!
        self.stage2 = copy_system  # Алиас
        self.bot_active = False

        # 🔧 ИСПРАВЛЕНИЕ: Правильная обработка админов
        # Используем уже подготовленный ADMIN_IDS список
        self.authorized_users = set(ADMIN_IDS)
        
        # Дополнительно добавляем TELEGRAM_CHAT_ID если он есть
        if TELEGRAM_CHAT_ID:
            try:
                self.authorized_users.add(int(TELEGRAM_CHAT_ID))
            except (ValueError, TypeError):
                logger.warning(f"Invalid TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")

        # Совместимость: admin_ids как список
        self.admin_ids = list(self.authorized_users)

        # 🔧 ИСПРАВЛЕНИЕ: Проверка токена с детальным логированием
        self.token = TELEGRAM_TOKEN
        if not self.token:
            error_msg = (
                f"❌ TELEGRAM_TOKEN не найден!\n"
                f"Источник конфигурации: {TELEGRAM_TOKEN_SOURCE}\n"
                f"Проверьте файл telegram_cfg.py или переменную окружения TELEGRAM_TOKEN"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Патч методов Telegram (если доступен)
        try:
            patch_telegram_methods()
        except NameError:
            logger.warning("patch_telegram_methods() not available")

        # Анти-спам настройки
        self.command_cooldown = {}
        self.cooldown_time = 2

        # 🆕 Настройки автоматических уведомлений
        self.notification_settings = {
            'enabled': True,
            'interval_hours': 2,
            'last_notification': 0,
            'risk_alerts': True,
            'performance_reports': True
        }

        # 🆕 Настройки системы
        self.system_settings = {
            'kelly': {
                'confidence_threshold': 0.65,
                'max_kelly_fraction': 0.25,
                'conservative_factor': 0.5,
                'lookback_period': 30
            },
            'drawdown': {
                'daily_limit': 0.05,
                'total_limit': 0.15,
                'emergency_threshold': 0.08
            },
            'trailing': {
                'initial_distance': 0.02,
                'min_step': 0.005,
                'max_distance': 0.05,
                'aggressive_mode': False
            }
        }

        # Планировщик уведомлений
        self._setup_notification_scheduler()

        # 🚀 НОВЫЕ ПОЛЯ: Контроль состояния Application
        self._running = False
        self._start_lock = asyncio.Lock()
        self.application = None

        # 🔧 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Флаг регистрации команд
        self._commands_registered = False

        # Логирование успешной инициализации
        logger.info("✅ Stage2TelegramBot initialized successfully")
        logger.info(f"   Token source: {TELEGRAM_TOKEN_SOURCE}")
        logger.info(f"   Admin IDs: {sorted(self.admin_ids)}")
        logger.info(f"   Commands pre-registration: enabled")

    def system_ready(self) -> bool:
        """
        Система готова, когда:
        - есть monitor и stage2
        - stage2 активирован (active=True) и copy_enabled=True (если есть такие флаги)
        Метод не падает, если атрибутов нет.
        """
        try:
            if getattr(self, "monitor", None) is None:
                return False
            s2 = getattr(self, "stage2", None)
            if s2 is None:
                return False
            active = bool(getattr(s2, "active", False))
            enabled = bool(getattr(s2, "copy_enabled", getattr(s2, "enabled", False)))
            return active and enabled
        except Exception:
            return False

    def _setup_notification_scheduler(self):
        """Настройка планировщика для периодических уведомлений"""
        try:
            # Создаем задачу для автоматических уведомлений
            asyncio.create_task(self._notification_scheduler())
            logger.info("Notification scheduler started successfully")
        except Exception as e:
            logger.error(f"Failed to setup notification scheduler: {e}")

    def ensure_commands_registered(self, application):
        """
        🚀 ИСПРАВЛЕННАЯ ВЕРСИЯ: регистрируем только существующие методы.
        Вызывается ПРИНУДИТЕЛЬНО при каждом создании Application.
        """
        if getattr(self, "_commands_registered", False):
            logger.debug("Commands already registered — skipping duplicate registration")
            return

        try:
            logger.info("🔧 PRODUCTION FIX: Registering EXISTING Telegram Bot commands...")

            # --- БАЗОВОЕ, ТОЧНО ЕСТЬ ---
            application.add_handler(CommandHandler("start", self.start_command))
            application.add_handler(CommandHandler("emergency", self.emergency_command))
            application.add_handler(CommandHandler("copy", self.copy_command))

            application.add_handler(CommandHandler("set_kelly", self.set_kelly_command))
            application.add_handler(CommandHandler("set_trailing", self.set_trailing_command))
            application.add_handler(CommandHandler("set_drawdown", self.set_drawdown_command))

            application.add_handler(CommandHandler("notifications", self.notifications_command))
            application.add_handler(CommandHandler("logs", self.logs_command))
            application.add_handler(CommandHandler("export", self.export_command))
            application.add_handler(CommandHandler("backup", self.backup_command))
            application.add_handler(CommandHandler("restore", self.restore_command))

            application.add_handler(CommandHandler("force_copy", self.force_copy_command))
            application.add_handler(CallbackQueryHandler(self.force_copy_callback, pattern=r"^force_copy_"))
            application.add_handler(CommandHandler("positions", self.show_positions_detailed))

            # --- ОБРАБОТЧИК ТЕКСТОВЫХ КНОПОК (клавиатура ReplyKeyboard) ---
            application.add_handler(
                MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_menu_buttons),
                group=3,
            )

            # --- ГЛОБАЛЬНЫЙ ERROR‑HANDLER (PTB) ---
            application.add_error_handler(_on_error)

            # --- НОВЫЕ КОМАНДЫ ДИАГНОСТИКИ ---
            application.add_handler(CommandHandler("ws_diag", self.ws_diag_command))
            application.add_handler(CommandHandler("reconcile_now", self.reconcile_now_command))

            # --- КОМАНДЫ УПРАВЛЕНИЯ КОПИРОВАНИЕМ ---
            application.add_handler(CommandHandler("status", self.status_command))
            application.add_handler(CommandHandler("copy_on", self.copy_on_command))
            application.add_handler(CommandHandler("copy_off", self.copy_off_command))
            application.add_handler(CommandHandler("scale", self.scale_command))
            application.add_handler(CommandHandler("panic_close", self.panic_close_command))
            application.add_handler(CallbackQueryHandler(self.panic_close_callback, pattern="^panic_close_confirm"))

            # Защита от повторной регистрации
            self._commands_registered = True

            try:
                total = sum(len(v) for v in getattr(application, "handlers", {}).values())
            except Exception:
                total = 0
            logger.info("✅ EXISTING Telegram Bot commands registered successfully")
            logger.info("⚠️  NOTE: Some commands (help, status, balance, etc.) not available - methods don't exist")
            logger.info("📊 Total handlers registered: %s", total)

        except Exception as e:
            logger.error("❌ Failed to register Telegram Bot commands: %s", e, exc_info=True)
            raise


    # ---- REPLACE WHOLE METHOD ----
    async def start(self):
        """
        Исправленный запуск Telegram Bot:
        - ранний CBQ-трассировщик (group=-1, block=False);
        - ранний логгер любых сообщений;
        - polling с allowed_updates=Update.ALL_TYPES и drop_pending_updates=True.
        """
        if self._running:
            logger.info("Telegram Bot already running — start() ignored (idempotent)")
            return

        async with self._start_lock:
            if self._running:
                return

            if not self.token:
                raise RuntimeError("Telegram token is not configured")

            try:
                # 1) Собираем Application один раз
                if self.application is None:
                    from telegram.ext import Application, CallbackQueryHandler, MessageHandler, filters, ContextTypes
                    from telegram import Update

                    self.application = (
                        Application
                        .builder()
                        .token(self.token)
                        .build()
                    )
                    # Команды
                    self.ensure_commands_registered(self.application)

                    # [NEW] Глобальный логгер сообщений (раньше всех)
                    if not self.application.bot_data.get("msg_trace_registered"):
                        async def _msg_trace(update, context: ContextTypes.DEFAULT_TYPE):
                            try:
                                uid = update.effective_user.id if update.effective_user else None
                                txt = getattr(getattr(update, "message", None), "text", None)
                                logger.info("MSG TRACE: user=%s text=%r", uid, txt)
                            except Exception:
                                pass
                        self.application.add_handler(
                            MessageHandler(filters.ALL, _msg_trace),
                            group=-1
                        )
                        self.application.bot_data["msg_trace_registered"] = True

                    # [NEW] Ранний CBQ-трассировщик (снимает спиннер)
                    if not self.application.bot_data.get("cbq_trace_registered"):
                        async def _cbq_trace(update, context: ContextTypes.DEFAULT_TYPE):
                            q = getattr(update, "callback_query", None)
                            if not q:
                                return
                            try:
                                await q.answer()   # критично для исчезновения спиннера
                            except Exception:
                                pass
                            try:
                                user_id = getattr(getattr(q, "from_user", None), "id", None)
                                chat_id = getattr(getattr(q, "message", None), "chat_id", None)
                                logger.info("CBQ TRACE: data=%r chat=%s user=%s", getattr(q, "data", None), chat_id, user_id)
                            except Exception:
                                pass

                        self.application.add_handler(
                            CallbackQueryHandler(_cbq_trace, pattern=".*", block=False),
                            group=-1
                        )
                        self.application.bot_data["cbq_trace_registered"] = True

                # 2) /keys FSM — один раз
                try:
                    if register_tg_keys_menu is None:
                        logger.warning("register_tg_keys_menu not available — /keys menu is skipped")
                    else:
                        if not self.application.bot_data.get("keys_menu_registered"):
                            target_system = getattr(self, "integrated_system", None)
                            register_tg_keys_menu(self.application, target_system)
                            self.application.bot_data["keys_menu_registered"] = True
                            logger.info("/keys ConversationHandler registered")
                            try:
                                mod = getattr(register_tg_keys_menu, "__module__", None)
                                fn = getattr(getattr(register_tg_keys_menu, "__code__", None), "co_filename", None)
                                logger.info("tg_keys_menu.register_tg_keys_menu from module=%s file=%s", mod, fn)
                            except Exception:
                                pass
                except Exception as e:
                    logger.exception("Failed to attach /keys menu: %s", e)

                if not self.application.bot_data.get("keys_menu_registered"):
                    raise RuntimeError("tg_keys_menu FSM (/keys) not registered in Application")

                # 3) initialize + start
                await self.application.initialize()
                logger.info("Telegram Application initialized")

                await self.application.start()
                logger.info("Telegram Application started")

                # 4) Чистим webhook (если был)
                try:
                    await self.application.bot.delete_webhook()
                    logger.info("Webhook deleted (if existed)")
                except Exception:
                    pass

                # 5) Polling с полным набором апдейтов и сбросом очереди
                from telegram import Update
                await self.application.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True
                )
                logger.info("Telegram Updater polling started")

                self._running = True
                self.bot_active = True
                logger.info("✅ Telegram Bot is up and running")

            except Exception as e:
                logger.error(f"Failed to start Telegram Bot: {e}")
                raise


    async def stop(self) -> None:
        """Грейсфул-стоп: остановить polling -> stop() -> shutdown()."""
        if not self.application:
            return
        try:
            upd = getattr(self.application, "updater", None)
            if upd:
                await upd.stop()
                logger.info("Telegram Updater polling stopped")
            await self.application.stop()
            await self.application.shutdown()
            logger.info("Telegram Application stopped & shutdown")
        except Exception:
            logger.exception("Error during Telegram bot stop()")
        finally:
            self._running = False
            self.bot_active = False
            self.application = None
            logger.info("Telegram Bot resources cleaned up")



    async def _idle_until_stop(self) -> None:
        """Простая 'блокировка' до stop() — чтобы таска жила, пока не попросим остановиться."""
        await self._tg_stop_event.wait()
    
    async def _notification_scheduler(self):
        """Асинхронный планировщик для отправки периодических уведомлений"""
        while self.bot_active:
            try:
                # Проверяем, включены ли уведомления
                if self.notification_settings['enabled']:
                    current_time = time.time()
                    last_notification = self.notification_settings['last_notification']
                    interval_seconds = self.notification_settings['interval_hours'] * 3600
                    
                    # Проверяем, пора ли отправлять уведомление
                    if current_time - last_notification >= interval_seconds:
                        await self._send_scheduled_notification()
                        self.notification_settings['last_notification'] = current_time
                
                # Ждем 5 минут перед следующей проверкой
                await asyncio.sleep(300)  # 5 минут
                
            except Exception as e:
                logger.error(f"Error in notification scheduler: {e}")
                await asyncio.sleep(600)  # При ошибке ждем 10 минут
    
    async def _send_scheduled_notification(self):
        """Отправка запланированного уведомления"""
        try:
            if not self.copy_system:
                return
            
            # Собираем базовую информацию о состоянии системы
            system_active = getattr(self.copy_system, 'system_active', False)
            copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
            
            # Получаем статистику
            stats = getattr(self.copy_system, 'system_stats', {})
            
            # Получаем информацию о балансе
            source_balance = 0.0
            main_balance = 0.0
            try:
                if hasattr(self.copy_system, 'base_monitor'):
                    source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                    main_balance = await self.copy_system.base_monitor.main_client.get_balance()
            except Exception as e:
                logger.warning(f"Failed to get balance for notification: {e}")
            
            # Получаем информацию о позициях
            positions_count = 0
            total_pnl = 0.0
            try:
                if hasattr(self.copy_system, 'base_monitor'):
                    positions = await self.copy_system.base_monitor.main_client.get_positions()
                    positions_count = len([p for p in positions if self._safe_float(p.get('size', 0)) > 0])
                    total_pnl = sum([self._safe_float(p.get('unrealisedPnl', 0)) for p in positions])
            except Exception as e:
                logger.warning(f"Failed to get positions for notification: {e}")
            
            # Формируем сообщение
            message = (
                "🔔 **АВТОМАТИЧЕСКОЕ УВЕДОМЛЕНИЕ**\n"
                f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                f"📊 **СТАТУС СИСТЕМЫ:**\n"
                f"   Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                f"   Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
                
                f"💰 **ФИНАНСЫ:**\n"
                f"   Источник: ${source_balance:.2f}\n"
                f"   Основной: ${main_balance:.2f}\n"
                f"   Всего: ${source_balance + main_balance:.2f}\n\n"
                
                f"📈 **ПОЗИЦИИ:**\n"
                f"   Активных: {positions_count}\n"
                f"   Общий P&L: ${total_pnl:+.2f}\n\n"
                
                f"📋 **СТАТИСТИКА:**\n"
                f"   Обработано сигналов: {stats.get('total_signals_processed', 0)}\n"
                f"   Успешных копий: {stats.get('successful_copies', 0)}\n"
            )
            
            # Отправляем уведомление
            await send_telegram_alert(message)
            logger.info(f"Scheduled notification sent at {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"Failed to send scheduled notification: {e}")
    
    def check_authorization(self, user_id: int, chat_id: Optional[int] = None) -> bool:
        """
        ИСПРАВЛЕННАЯ ВЕРСИЯ: Проверка авторизации пользователя
        Совместимость с существующей логикой authorized_users + admin_ids
        """
        # Проверяем по user_id в authorized_users
        if hasattr(self, 'authorized_users') and user_id in self.authorized_users:
            return True
        
        # Проверяем по chat_id в authorized_users (для групп)
        if chat_id is not None and hasattr(self, 'authorized_users') and chat_id in self.authorized_users:
            return True
        
        # Fallback на admin_ids
        if hasattr(self, 'admin_ids') and user_id in self.admin_ids:
            return True
        
        # Последний fallback на ENV переменные
        try:
            import os
            admin_ids_str = os.getenv("ADMIN_ONLY_IDS", "")
            admin_ids = {int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()}
            return user_id in admin_ids
        except Exception:
            return False
    
    def check_cooldown(self, user_id: int, command: str) -> bool:
        """Проверка cooldown для команды"""
        key = f"{user_id}_{command}"
        current_time = time.time()
        
        if key in self.command_cooldown:
            if current_time - self.command_cooldown[key] < self.cooldown_time:
                return False
        
        self.command_cooldown[key] = current_time
        return True
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start — главное меню (с ReplyKeyboardMarkup сразу)"""
        sys_logger.log_telegram_command("/start", update.effective_user.id)

        user_id = getattr(update.effective_user, "id", None)
        chat_id = getattr(update.effective_chat, "id", None)
        if not self.check_authorization(user_id, chat_id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        # Сборка постоянной клавиатуры
        keyboard = [
            [KeyboardButton("📊 Статус"),  KeyboardButton("💰 Баланс")],
            [KeyboardButton("📈 Позиции"), KeyboardButton("🚨 Риски")],
            [KeyboardButton("⚙️ Режим"),   KeyboardButton("🆘 Помощь")],
        ]
        reply_markup = ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False,
        )

        # Определяем статусы системы (если copy_system есть)
        system_active = getattr(getattr(self, "copy_system", None), "system_active", False)
        copy_enabled  = getattr(getattr(self, "copy_system", None), "copy_enabled",  False)

        text = (
            f"🚀 <b>BYBIT COPY TRADING BOT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Статус: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
            f"Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
            f"Используйте меню снизу для управления системой.\n\n"
            f"📌 <b>Основные команды:</b>\n"
            f"/keys — Управление API ключами\n"
            f"/emergency — Экстренное управление\n"
            f"/force_copy — Принудительное копирование"
        )

        # Отправляем приветствие с клавиатурой
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )


    async def handle_menu_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННЫЙ обработчик нажатий на кнопки меню"""
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        text = update.message.text

        if not self.check_cooldown(update.effective_user.id, text):
            await update.message.reply_text("⏳ Подождите немного между командами")
            return

        try:
            # Специальная обработка кнопки "Назад"
            if text == '⬅️ Назад':
                # Возвращаемся к основному состоянию
                main_message = (
                    "🚀 <b>BYBIT COPY TRADING BOT</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Используйте меню снизу для управления системой."
                )
            
                # Основная клавиатура
                keyboard = [
                    [KeyboardButton("📊 Статус"), KeyboardButton("💰 Баланс")],
                    [KeyboardButton("📈 Позиции"), KeyboardButton("🚨 Риски")],
                    [KeyboardButton("⚙️ Режим"), KeyboardButton("🆘 Помощь")]
                ]
            
                reply_markup = ReplyKeyboardMarkup(
                    keyboard,
                    resize_keyboard=True,
                )
            
                await update.message.reply_text(
                    main_message,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Основное меню
            elif text == '📊 Статус':
                await self.show_system_status_text(update)
            elif text == '💰 Баланс':
                await self.show_balances_text(update)
            elif text == '📈 Позиции':
                await self.show_positions_text(update)
            elif text == '🚨 Риски':
                await self.show_risk_management_text(update)
            elif text == '⚙️ Режим':
                await self.show_copying_controls_text(update)
            elif text == '🆘 Помощь':
                await self.show_help_menu(update)
    
            # Подменю помощи - СУЩЕСТВУЮЩИЕ
            elif text == '📋 Отчет':
                await self.show_full_report_text(update)
            elif text == '🎯 Kelly':
                await self.show_kelly_stats_text(update)
            elif text == '🛡️ Trailing':
                await self.show_trailing_stops_text(update)
            elif text == '⚙️ Настройки':
                await self.show_settings_text(update)
            elif text == '🚨 Экстренное':
                await self.show_emergency_controls_text(update)
        
            # 🆕 НОВЫЕ ФУНКЦИИ В ПОДМЕНЮ ПОМОЩИ
            elif text == '🎯 Настройки Kelly':
                await self.show_kelly_settings_text(update)
            elif text == '📊 Производительность':
                await self.show_performance_text(update)
            elif text == '⚠️ Анализ рисков':
                await self.show_risks_analysis_text(update)
            elif text == '📅 Дневной отчет':
                await self.show_daily_report_text(update)
            elif text == '🔧 Диагностика':
                await self.show_health_check_text(update)
            elif text == '🔄 Синхронизация':
                await self.show_sync_status_text(update)
        
            # 🆕 ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ
            elif text == '🔔 Уведомления':
                await self.show_notifications_settings_text(update)
            elif text == '🧪 Тест подключений':
                await self.test_all_connections_text(update)
            elif text == '📜 Логи системы':
                await self.show_system_logs_text(update)
            elif text == '⚙️ Настройки просадки':
                await self.show_drawdown_settings_text(update)
            elif text == '🛡️ Настройки trailing':
                await self.show_trailing_settings_text(update)
            elif text == '📤 Экспорт отчетов':
                await self.export_reports_text(update)
            elif text == '💾 Backup настроек':
                await self.backup_settings_text(update)
            elif text == '🔄 Restore настроек':
                await self.restore_settings_text(update)
            
        except Exception as e:
            logger.error(f"Menu button error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка: {e}")
    
    async def show_help_menu(self, update):
        """🆕 ОБНОВЛЕННОЕ меню помощи с дополнительными функциями"""
        keyboard = [
            [
                KeyboardButton("📋 Отчет"),
                KeyboardButton("🎯 Kelly")
            ],
            [
                KeyboardButton("🛡️ Trailing"),
                KeyboardButton("⚙️ Настройки")
            ],
            [
                KeyboardButton("🚨 Экстренное"),
                KeyboardButton("🔧 Диагностика")
            ],
            # СУЩЕСТВУЮЩИЕ НОВЫЕ КНОПКИ
            [
                KeyboardButton("🎯 Настройки Kelly"),
                KeyboardButton("📊 Производительность")
            ],
            [
                KeyboardButton("⚠️ Анализ рисков"),
                KeyboardButton("📅 Дневной отчет")
            ],
            [
                KeyboardButton("🔄 Синхронизация"),
                KeyboardButton("🔔 Уведомления")
            ],
            # 🆕 ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ИЗ ПЛАНА ИНТЕГРАЦИИ
            [
                KeyboardButton("🧪 Тест подключений"),
                KeyboardButton("📜 Логи системы")
            ],
            [
                KeyboardButton("⚙️ Настройки просадки"),
                KeyboardButton("🛡️ Настройки trailing")
            ],
            [
                KeyboardButton("📤 Экспорт отчетов"),
                KeyboardButton("💾 Backup настроек")
            ],
            [
                KeyboardButton("🔄 Restore настроек"),
                KeyboardButton("⬅️ Назад")
            ]
        ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        help_message = (
            "🆘 *РАСШИРЕННЫЕ ФУНКЦИИ v2.2*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🆕 **НОВЫЕ ВОЗМОЖНОСТИ:**\n"
            "   • Настройки Kelly Criterion\n"
            "   • Детальная аналитика\n"
            "   • Анализ рисков с рекомендациями\n"
            "   • Дневные отчеты\n"
            "   • Диагностика системы\n"
            "   • Статус синхронизации\n\n"
            "🔧 **ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ:**\n"
            "   • Автоматические уведомления\n"
            "   • Тестирование всех подключений\n"
            "   • Просмотр логов системы\n"
            "   • Управление настройками просадки\n"
            "   • Управление trailing stops\n"
            "   • Экспорт отчетов\n"
            "   • Backup/Restore настроек\n\n"
            "Выберите нужную функцию:"
        )
        
        await update.message.reply_text(
            help_message,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ================================
    # 🆕 ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ИЗ ПЛАНА ИНТЕГРАЦИИ
    # ================================
    
    async def show_notifications_settings_text(self, update):
        """🆕 Настройки автоматических уведомлений"""
        try:
            current_settings = self.notification_settings
            
            message = (
                "🔔 *НАСТРОЙКИ УВЕДОМЛЕНИЙ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Автоуведомления:** {'✅ Включены' if current_settings['enabled'] else '❌ Выключены'}\n"
                f"⏰ **Интервал:** {current_settings['interval_hours']} часов\n"
                f"⚠️ **Алерты рисков:** {'✅ Включены' if current_settings['risk_alerts'] else '❌ Выключены'}\n"
                f"📊 **Отчеты производительности:** {'✅ Включены' if current_settings['performance_reports'] else '❌ Выключены'}\n\n"
                
                "📋 **ТИПЫ УВЕДОМЛЕНИЙ:**\n"
                "   📊 Информационные (статус системы)\n"
                "   ⚠️ Предупреждения (превышение лимитов)\n"
                "   🚨 Критические алерты (emergency stop)\n"
                "   💰 Торговые уведомления (крупные позиции)\n"
                "   📈 Отчеты производительности\n\n"
                
                "🔧 **КОМАНДЫ УПРАВЛЕНИЯ:**\n"
                "`/notifications on` - включить уведомления\n"
                "`/notifications off` - выключить уведомления\n"
                "`/notifications interval 4` - установить интервал 4 часа\n"
                "`/notifications test` - отправить тестовое уведомление\n\n"
                
                f"⏰ Последнее уведомление: {datetime.fromtimestamp(current_settings['last_notification']).strftime('%H:%M:%S') if current_settings['last_notification'] > 0 else 'Никогда'}"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Notifications settings error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка настроек уведомлений: {e}")
    
    async def test_all_connections_text(self, update):
        """🆕 Тестирование всех подключений системы"""
        try:
            test_message = await update.message.reply_text("🧪 Тестирование подключений...")
            
            results = []
            
            # Тест 1: Проверка системы копирования
            if self.copy_system:
                results.append("✅ Copy System: Инициализирован")
                
                # Тест 2: API подключения
                try:
                    if hasattr(self.copy_system, 'base_monitor'):
                        # Тест источника
                        try:
                            source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                            results.append(f"✅ Source API: Работает (${source_balance:.2f})")
                        except Exception as e:
                            results.append(f"❌ Source API: Ошибка ({str(e)[:30]}...)")
                        
                        # Тест основного аккаунта
                        try:
                            main_balance = await self.copy_system.base_monitor.main_client.get_balance()
                            results.append(f"✅ Main API: Работает (${main_balance:.2f})")
                        except Exception as e:
                            results.append(f"❌ Main API: Ошибка ({str(e)[:30]}...)")
                    else:
                        results.append("❌ Base Monitor: Не найден")
                        
                except Exception as e:
                    results.append(f"❌ API Testing: Критическая ошибка {str(e)[:30]}...")
                
                # Тест 3: WebSocket подключения
                try:
                    if (hasattr(self.copy_system, 'base_monitor') and 
                        hasattr(self.copy_system.base_monitor, 'websocket_manager')):
                        ws_manager = self.copy_system.base_monitor.websocket_manager
                        
                        if hasattr(ws_manager, 'ws') and ws_manager.ws:
                            results.append("✅ WebSocket: Подключен")
                        else:
                            results.append("❌ WebSocket: Не подключен")
                    else:
                        results.append("❌ WebSocket Manager: Не найден")
                        
                except Exception as e:
                    results.append(f"⚠️ WebSocket Test: {str(e)[:40]}...")
                
                # Тест 4: Компоненты системы
                components = [
                    ('Kelly Calculator', 'kelly_calculator'),
                    ('Copy Manager', 'copy_manager'),
                    ('Drawdown Controller', 'drawdown_controller'),
                    ('Trailing Manager', 'trailing_manager')
                ]
                
                for name, attr in components:
                    if hasattr(self.copy_system, attr):
                        results.append(f"✅ {name}: Активен")
                    else:
                        results.append(f"❌ {name}: Не найден")
            else:
                results.append("❌ Copy System: Не инициализирован")
            
            # Тест 5: Telegram Bot
            results.append("✅ Telegram Bot: Активен (этот тест)")
            
            # Тест 6: Файловая система
            try:
                test_file = "test_write.tmp"
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                results.append("✅ File System: Доступен")
            except Exception:
                results.append("❌ File System: Ошибка записи")
            
            # Подсчет результатов
            success_count = len([r for r in results if r.startswith("✅")])
            error_count = len([r for r in results if r.startswith("❌")])
            warning_count = len([r for r in results if r.startswith("⚠️")])
            
            # Определение общего статуса
            if error_count == 0 and warning_count == 0:
                overall_status = "🟢 ОТЛИЧНО"
            elif error_count == 0:
                overall_status = "🟡 ХОРОШО"
            elif error_count < 3:
                overall_status = "🟠 ЕСТЬ ПРОБЛЕМЫ"
            else:
                overall_status = "🔴 КРИТИЧЕСКИЕ ОШИБКИ"
            
            final_message = (
                "🧪 *РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **ОБЩИЙ СТАТУС:** {overall_status}\n\n"
                
                "📋 **ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ:**\n" +
                "\n".join([f"   {result}" for result in results]) + "\n\n"
                
                f"📊 **СТАТИСТИКА:**\n"
                f"   Успешных: {success_count}\n"
                f"   Ошибок: {error_count}\n"
                f"   Предупреждений: {warning_count}\n\n"
                
                f"⏰ Тестирование завершено: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await test_message.edit_text(final_message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Connection test error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка тестирования: {e}")
    
    async def show_system_logs_text(self, update):
        """🆕 Просмотр логов системы"""
        sys_logger.log_telegram_command("/logs", update.effective_user.id)

        try:
            # Проверяем доступные лог файлы
            log_files = []
            log_directories = ['logs', '.', 'log']
            
            for log_dir in log_directories:
                if os.path.exists(log_dir):
                    for file in os.listdir(log_dir):
                        if file.endswith('.log'):
                            log_files.append(os.path.join(log_dir, file))
            
            if not log_files:
                await update.message.reply_text(
                    "📜 *ЛОГИ СИСТЕМЫ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Лог файлы не найдены\n"
                    "Проверьте директории: logs, log или текущую папку",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Получаем последние записи из основного лога
            main_log_content = ""
            try:
                # Пытаемся найти главный лог файл
                main_log_files = [f for f in log_files if 'trading' in f.lower() or 'main' in f.lower()]
                if not main_log_files:
                    main_log_files = log_files[:1]  # Берем первый доступный
                
                main_log_file = main_log_files[0]
                
                # Читаем последние 20 строк
                with open(main_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    last_lines = lines[-20:] if len(lines) > 20 else lines
                    main_log_content = ''.join(last_lines)
                    
            except Exception as e:
                main_log_content = f"Ошибка чтения лога: {e}"
            
            # Обрезаем содержимое для Telegram (лимит сообщения)
            if len(main_log_content) > 2000:
                main_log_content = "...\n" + main_log_content[-1900:]
            
            message = (
                "📜 *ЛОГИ СИСТЕМЫ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📁 **Найдено файлов:** {len(log_files)}\n"
                f"📄 **Основной лог:** {os.path.basename(main_log_files[0]) if main_log_files else 'Не найден'}\n\n"
                
                "📋 **ПОСЛЕДНИЕ ЗАПИСИ:**\n"
                f"```\n{main_log_content.strip()}\n```\n\n"
                
                "🔧 **КОМАНДЫ:**\n"
                "`/logs 50` - показать последние 50 строк\n"
                "`/logs errors` - показать только ошибки\n"
                "`/logs clear` - очистить логи (требует подтверждения)\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"System logs error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка чтения логов: {e}")
    
    async def show_drawdown_settings_text(self, update):
        """🆕 Настройки управления просадкой"""
        try:
            current_settings = self.system_settings['drawdown']
            
            # Получаем текущие значения из системы, если доступны
            if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                try:
                    controller = self.copy_system.drawdown_controller
                    current_settings['daily_limit'] = getattr(controller, 'daily_drawdown_limit', 0.05)
                    current_settings['total_limit'] = getattr(controller, 'max_drawdown_threshold', 0.15)
                    current_settings['emergency_threshold'] = getattr(controller, 'emergency_stop_threshold', 0.08)
                except Exception as e:
                    logger.warning(f"Failed to get drawdown settings: {e}")
            
            message = (
                "📉 *НАСТРОЙКИ УПРАВЛЕНИЯ ПРОСАДКОЙ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 **ТЕКУЩИЕ ЛИМИТЫ:**\n"
                f"   Дневной лимит: {current_settings['daily_limit']:.1%}\n"
                f"   Общий лимит: {current_settings['total_limit']:.1%}\n"
                f"   Emergency порог: {current_settings['emergency_threshold']:.1%}\n\n"
                
                "🛡️ **ПРИНЦИП РАБОТЫ:**\n"
                "   📅 Дневной лимит - максимальная просадка за день\n"
                "   📊 Общий лимит - максимальная просадка от пика\n"
                "   🚨 Emergency порог - критический уровень остановки\n\n"
                
                "🔧 **КОМАНДЫ НАСТРОЙКИ:**\n"
                "`/set_drawdown daily 3` - дневной лимит 3%\n"
                "`/set_drawdown total 12` - общий лимит 12%\n"
                "`/set_drawdown emergency 8` - emergency порог 8%\n"
                "`/set_drawdown reset` - сброс счетчиков просадки\n\n"
                
                "💡 **РЕКОМЕНДАЦИИ:**\n"
                "   • Дневной лимит: 3-5% (защита от волатильности)\n"
                "   • Общий лимит: 10-15% (защита капитала)\n"
                "   • Emergency: 6-8% (критическая остановка)\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Drawdown settings error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка настроек просадки: {e}")
    
async def show_trailing_settings_text(self, update):
    """🆕 Настройки trailing stops (совместимо с новым DynamicTrailingStopManager)"""
    from datetime import datetime
    from telegram.constants import ParseMode

    def _pct(v, fmt="{:.1%}"):
        try:
            return fmt.format(float(v))
        except Exception:
            return "—"

    try:
        active_stops = 0
        snap = {
            "enabled": None,
            "mode": None,
            "step_pct": None,
            "max_pct": None,
            "activation_pct": None,
            "atr_period": None,
            "atr_multiplier": None,
        }

        tm = None
        if getattr(self, "copy_system", None) and hasattr(self.copy_system, "copy_manager"):
            tm = getattr(self.copy_system.copy_manager, "trailing_manager", None)

        if tm:
            try:
                if hasattr(tm, "get_config_snapshot"):
                    snap.update(tm.get_config_snapshot() or {})
                else:
                    snap["activation_pct"] = getattr(tm, "default_distance_percent", None)
                    snap["step_pct"]       = getattr(tm, "min_trail_step", None)
                    snap["max_pct"]        = getattr(tm, "max_distance_percent", None)
                    snap["mode"]           = "aggressive" if getattr(tm, "aggressive_mode", False) else "conservative"
                    snap["enabled"]        = True
            except Exception as e:
                logger.warning(f"Failed to get trailing snapshot: {e}")

            try:
                if hasattr(tm, "get_all_stops"):
                    active_stops = len(tm.get_all_stops() or [])
            except Exception as e:
                logger.warning(f"Failed to get active trailing stops count: {e}")

        message = (
            "🛡️ *НАСТРОЙКИ TRAILING STOPS*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 *ТЕКУЩИЕ ПАРАМЕТРЫ:*\n"
            f"   Начальная дистанция: {_pct(snap.get('activation_pct'))}\n"
            f"   Минимальный шаг:     {_pct(snap.get('step_pct'), '{:.2%}')}\n"
            f"   Максимальная дистанция: {_pct(snap.get('max_pct'))}\n"
            f"   ATR: период={snap.get('atr_period') or '—'}, множитель={snap.get('atr_multiplier') or '—'}\n"
            f"   Режим: {'Агрессивный' if (snap.get('mode') == 'aggressive') else 'Консервативный'}\n"
            f"   Статус: {'✅ Включен' if snap.get('enabled') else '❌ Выключен'}\n\n"
            "📊 *СТАТИСТИКА:*\n"
            f"   Активных trailing stops: {active_stops}\n\n"
            "🔧 *КОМАНДЫ НАСТРОЙКИ:*\n"
            "`/set_trailing distance 2.5` — начальная дистанция 2.5%\n"
            "`/set_trailing step 0.3` — минимальный шаг 0.3%\n"
            "`/set_trailing max 4.0` — максимальная дистанция 4.0%\n"
            "`/set_trailing atr 14 1.5` — ATR период=14, множитель=1.5\n"
            "`/set_trailing aggressive on|off` — режим\n"
            "`/set_trailing enabled on|off` — вкл/выкл\n"
            "`/set_trailing clear_all` — закрыть все trailing stops\n"
            "`/set_trailing status` — показать текущие значения\n\n"
            "💡 *РЕКОМЕНДАЦИИ:*\n"
            "   • Дистанция: 1.5–3.0% (по волатильности)\n"
            "   • Шаг: 0.2–0.5% (точность следования)\n"
            "   • Максимум: 3.0–5.0% (защита от больших откатов)\n\n"
            f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Trailing settings error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка настроек trailing: {e}")


    async def export_reports_text(self, update):
        """🆕 Экспорт отчетов в различных форматах"""
        try:
            # Собираем данные для экспорта
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'system_status': {},
                'balances': {},
                'positions': [],
                'performance': {},
                'settings': self.system_settings
            }
            
            # Получаем данные системы
            if self.copy_system:
                try:
                    # Статус системы
                    export_data['system_status'] = {
                        'active': getattr(self.copy_system, 'system_active', False),
                        'copy_enabled': getattr(self.copy_system, 'copy_enabled', False),
                        'uptime': time.time() - getattr(self.copy_system, 'start_time', time.time())
                    }
                    
                    # Балансы
                    if hasattr(self.copy_system, 'base_monitor'):
                        try:
                            source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                            main_balance = await self.copy_system.base_monitor.main_client.get_balance()
                            export_data['balances'] = {
                                'source': source_balance,
                                'main': main_balance,
                                'total': source_balance + main_balance
                            }
                        except Exception as e:
                            export_data['balances'] = {'error': f'Unable to fetch balances: {e}'}
                    
                    # Позиции (детально)
                    try:
                        if hasattr(self.copy_system, 'base_monitor'):
                            # Получаем позиции из источника
                            source_positions = await self.copy_system.base_monitor.source_client.get_positions()
                            # Получаем позиции из основного аккаунта
                            main_positions = await self.copy_system.base_monitor.main_client.get_positions()
                            
                            # Добавляем в отчет
                            export_data['positions'] = {
                                'source': [p for p in source_positions if self._safe_float(p.get('size', 0)) > 0],
                                'main': [p for p in main_positions if self._safe_float(p.get('size', 0)) > 0]
                            }
                    except Exception as e:
                        export_data['positions'] = {'error': f'Unable to fetch positions: {e}'}
                    
                    # Статистика производительности
                    if hasattr(self.copy_system, 'system_stats'):
                        stats = self.copy_system.system_stats
                        export_data['performance'] = {
                            'total_signals': stats.get('total_signals_processed', 0),
                            'successful_copies': stats.get('successful_copies', 0),
                            'failed_copies': stats.get('failed_copies', 0),
                            'success_rate': stats.get('success_rate', 0.0)
                        }
                        
                except Exception as e:
                    export_data['error'] = f"Data collection error: {e}"
            
            # Создаем JSON отчет
            report_filename = f"trading_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            try:
                with open(report_filename, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                
                file_size = os.path.getsize(report_filename)
                
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка создания файла отчета: {e}")
                return
            
            # Создаем краткий текстовый отчет для Telegram
            text_report = (
                "📤 *ЭКСПОРТ ОТЧЕТОВ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 **Файл создан:** `{report_filename}`\n"
                f"📊 **Размер:** {file_size} байт\n"
                f"⏰ **Дата:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                
                "📋 **СОДЕРЖИМОЕ ОТЧЕТА:**\n"
                "   • Статус системы и компонентов\n"
                "   • Балансы аккаунтов\n"
                "   • Информация о позициях\n"
                "   • Статистика производительности\n"
                "   • Все настройки системы\n"
                "   • Конфигурация Kelly, Drawdown, Trailing\n\n"
                
                "📤 **ДОСТУПНЫЕ ФОРМАТЫ:**\n"
                "`/export json` - детальный JSON отчет\n"
                "`/export csv` - CSV для Excel\n"
                "`/export summary` - краткий текстовый отчет\n"
                "`/export settings` - только настройки\n\n"
                
                f"✅ JSON отчет успешно создан в файле `{report_filename}`\n"
                "Файл доступен на сервере для скачивания"
            )
            
            await update.message.reply_text(text_report, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Export reports error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка экспорта отчетов: {e}")
    
    async def backup_settings_text(self, update):
        """🆕 Резервное копирование настроек"""
        try:
            # Собираем все настройки системы
            backup_data = {
                'backup_timestamp': datetime.now().isoformat(),
                'version': '2.2',
                'telegram_bot_settings': {
                    'notification_settings': self.notification_settings,
                    'system_settings': self.system_settings,
                    'cooldown_time': self.cooldown_time
                }
            }
            
            # Добавляем настройки из реальной системы
            if self.copy_system:
                try:
                    # Kelly Calculator настройки
                    if hasattr(self.copy_system, 'kelly_calculator'):
                        kelly_calc = self.copy_system.kelly_calculator
                        backup_data['kelly_settings'] = {
                            'confidence_threshold': getattr(kelly_calc, 'confidence_threshold', 0.65),
                            'max_kelly_fraction': getattr(kelly_calc, 'max_kelly_fraction', 0.25),
                            'conservative_factor': getattr(kelly_calc, 'conservative_factor', 0.5),
                            'lookback_period': getattr(kelly_calc, 'lookback_period', 30)
                        }
                    
                    # Drawdown Controller настройки
                    if hasattr(self.copy_system, 'drawdown_controller'):
                        dd_controller = self.copy_system.drawdown_controller
                        backup_data['drawdown_settings'] = {
                            'daily_limit': getattr(dd_controller, 'daily_drawdown_limit', 0.05),
                            'total_limit': getattr(dd_controller, 'max_drawdown_threshold', 0.15),
                            'emergency_threshold': getattr(dd_controller, 'emergency_stop_threshold', 0.08)
                        }
                    
                    # Copy Manager настройки
                    if hasattr(self.copy_system, 'copy_manager'):
                        copy_manager = self.copy_system.copy_manager
                        backup_data['copy_settings'] = {
                            'copy_mode': str(getattr(copy_manager, 'copy_mode', 'DEFAULT')),
                            'max_positions': getattr(copy_manager, 'max_positions', 10),
                            'position_scaling': getattr(copy_manager, 'position_scaling', 1.0)
                        }
                        
                except Exception as e:
                    backup_data['collection_error'] = str(e)
            
            # Создаем файл резервной копии
            backup_filename = f"settings_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            try:
                with open(backup_filename, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False)
                
                file_size = os.path.getsize(backup_filename)
                
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка создания backup файла: {e}")
                return
            
            # Также создаем архивную копию с именем по дате
            archive_filename = f"backup_{datetime.now().strftime('%Y%m%d')}.json"
            try:
                with open(archive_filename, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Failed to create archive backup: {e}")
            
            message = (
                "💾 *РЕЗЕРВНОЕ КОПИРОВАНИЕ НАСТРОЕК*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 **Backup файл:** `{backup_filename}`\n"
                f"📊 **Размер:** {file_size} байт\n"
                f"⏰ **Дата создания:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                
                "📋 **СОХРАНЕННЫЕ НАСТРОЙКИ:**\n"
                "   ✅ Настройки Kelly Criterion\n"
                "   ✅ Лимиты просадки\n"
                "   ✅ Параметры Trailing Stops\n"
                "   ✅ Настройки уведомлений\n"
                "   ✅ Конфигурация Copy Manager\n"
                "   ✅ Системные параметры\n\n"
                
                "🔧 **КОМАНДЫ УПРАВЛЕНИЯ:**\n"
                "`/backup create` - создать новый backup\n"
                "`/backup list` - список всех backup'ов\n"
                "`/backup restore latest` - восстановить последний\n"
                "`/backup clean old` - удалить старые backup'ы\n\n"
                
                "💡 **РЕКОМЕНДАЦИИ:**\n"
                "   • Создавайте backup перед изменениями\n"
                "   • Сохраняйте backup'ы еженедельно\n"
                "   • Тестируйте восстановление на тестовом окружении\n\n"
                
                f"✅ Backup успешно создан: `{backup_filename}`"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Backup settings error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка резервного копирования: {e}")
    
    async def restore_settings_text(self, update):
        """🆕 Восстановление настроек из backup"""
        try:
            # Ищем доступные backup файлы
            backup_files = []
            for file in os.listdir('.'):
                if file.startswith('settings_backup_') and file.endswith('.json'):
                    backup_files.append(file)
            
            backup_files.sort(reverse=True)  # Новые сначала
            
            if not backup_files:
                await update.message.reply_text(
                    "🔄 *ВОССТАНОВЛЕНИЕ НАСТРОЕК*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Backup файлы не найдены\n"
                    "Создайте backup командой: 💾 Backup настроек",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Показываем доступные backup'ы
            backup_list = ""
            for i, backup_file in enumerate(backup_files[:5]):  # Показываем последние 5
                try:
                    file_time = os.path.getmtime(backup_file)
                    file_size = os.path.getsize(backup_file)
                    backup_list += f"   {i+1}. `{backup_file}`\n      📅 {datetime.fromtimestamp(file_time).strftime('%d.%m.%Y %H:%M')} ({file_size} байт)\n"
                except Exception as e:
                    backup_list += f"   {i+1}. `{backup_file}` (ошибка чтения метаданных: {e})\n"
            
            # Пытаемся загрузить последний backup для предпросмотра
            preview_info = "Данные недоступны"
            if backup_files:
                try:
                    with open(backup_files[0], 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                        
                    preview_info = (
                        f"📄 Версия: {backup_data.get('version', 'Неизвестно')}\n"
                        f"⏰ Создан: {backup_data.get('backup_timestamp', 'Неизвестно')[:19]}\n"
                        f"📊 Разделов: {len(backup_data)} секций"
                    )
                    
                except Exception as e:
                    preview_info = f"Ошибка чтения: {e}"
            
            message = (
                "🔄 *ВОССТАНОВЛЕНИЕ НАСТРОЕК*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 **ДОСТУПНЫЕ BACKUP'Ы:** {len(backup_files)}\n\n"
                
                "📂 **ПОСЛЕДНИЕ BACKUP'Ы:**\n"
                f"{backup_list}\n"
                
                "🔍 **ПРЕВЬЮ ПОСЛЕДНЕГО BACKUP'А:**\n"
                f"{preview_info}\n\n"
                
                "🔧 **КОМАНДЫ ВОССТАНОВЛЕНИЯ:**\n"
                "`/restore latest` - восстановить последний backup\n"
                "`/restore filename.json` - восстановить конкретный файл\n"
                "`/restore preview latest` - предпросмотр настроек\n"
                "`/restore kelly only` - восстановить только Kelly настройки\n\n"
                
                "⚠️ **ВАЖНО:**\n"
                "   • Восстановление перезапишет текущие настройки\n"
                "   • Рекомендуется создать backup перед восстановлением\n"
                "   • Система будет перезагружена после восстановления\n\n"
                
                f"📈 Последний backup: `{backup_files[0] if backup_files else 'Нет'}`"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Restore settings error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка восстановления настроек: {e}")
    
    # ================================
    # 🆕 ДОПОЛНИТЕЛЬНЫЕ КОМАНДЫ
    # ================================
    
    async def notifications_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /notifications - управление уведомлениями"""
        sys_logger.log_telegram_command("/notifications", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
        
        try:
            args = context.args
            if not args:
                await self.show_notifications_settings_text(update)
                return
            
            command = args[0].lower()
            
            if command == 'on':
                self.notification_settings['enabled'] = True
                await update.message.reply_text("✅ Автоматические уведомления включены")
                
            elif command == 'off':
                self.notification_settings['enabled'] = False
                await update.message.reply_text("❌ Автоматические уведомления выключены")
                
            elif command == 'interval' and len(args) > 1:
                try:
                    hours = int(args[1])
                    if 1 <= hours <= 24:
                        self.notification_settings['interval_hours'] = hours
                        await update.message.reply_text(f"⏰ Интервал уведомлений установлен: {hours} часов")
                    else:
                        await update.message.reply_text("❌ Интервал должен быть от 1 до 24 часов")
                except ValueError:
                    await update.message.reply_text("❌ Неверный формат. Используйте: /notifications interval 4")
                    
            elif command == 'test':
                await self._send_test_notification()
                await update.message.reply_text("📤 Тестовое уведомление отправлено")
                
            else:
                await update.message.reply_text(
                    "❓ Неизвестная команда\n"
                    "Доступные: on, off, interval <часы>, test"
                )
                
        except Exception as e:
            logger.error(f"Notifications command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка команды уведомлений: {e}")
    
    async def _send_test_notification(self):
        """Отправка тестового уведомления"""
        try:
            test_message = (
                "🧪 **ТЕСТОВОЕ УВЕДОМЛЕНИЕ**\n"
                f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                "✅ Система уведомлений работает корректно"
            )
            await send_telegram_alert(test_message)
        except Exception as e:
            logger.error(f"Test notification error: {e}")
            logger.error(traceback.format_exc())
    
async def set_trailing_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 /set_trailing — работает через DynamicTrailingStopManager.reload_config(patch)"""
    from datetime import datetime
    from telegram.constants import ParseMode
    sys_logger.log_telegram_command("/set_trailing", update.effective_user.id)

    if not self.check_authorization(update.effective_user.id):
        await update.message.reply_text("❌ Доступ запрещен")
        return

    def _parse_bool(val: str) -> bool:
        v = (val or "").strip().lower()
        if v in {"1", "on", "true", "yes", "y", "enable", "enabled"}:
            return True
        if v in {"0", "off", "false", "no", "n", "disable", "disabled"}:
            return False
        raise ValueError("Ожидалось on/off")

    def _parse_float_pct(val: str, name: str, min_pct: float, max_pct: float) -> float:
        try:
            x = float(val.replace(",", "."))
        except Exception:
            raise ValueError(f"{name}: неверное число '{val}'")
        if not (min_pct <= x <= max_pct):
            raise ValueError(f"{name}: должно быть в диапазоне {min_pct}–{max_pct}%")
        return x / 100.0

    def _reply_snapshot(mgr) -> str:
        try:
            snap = mgr.get_config_snapshot()
        except Exception:
            snap = {
                "activation_pct": getattr(mgr, "default_distance_percent", None),
                "step_pct": getattr(mgr, "min_trail_step", None),
                "max_pct": getattr(mgr, "max_distance_percent", None),
                "mode": "aggressive" if getattr(mgr, "aggressive_mode", False) else "conservative",
                "enabled": True,
                "atr_period": getattr(mgr, "atr_period", None),
                "atr_multiplier": getattr(mgr, "atr_multiplier", None),
            }
        def _pct(v, fmt="{:.2%}"):
            try:
                return fmt.format(float(v))
            except Exception:
                return "—"
        return (
            "🛡️ Текущие настройки:\n"
            f"• enabled: {snap.get('enabled')}\n"
            f"• mode: {snap.get('mode')}\n"
            f"• activation: {_pct(snap.get('activation_pct'))}\n"
            f"• step: {_pct(snap.get('step_pct'))}\n"
            f"• max: {_pct(snap.get('max_pct'))}\n"
            f"• atr_period: {snap.get('atr_period') or '—'}\n"
            f"• atr_multiplier: {snap.get('atr_multiplier') or '—'}"
        )

    tm = None
    if getattr(self, "copy_system", None) and hasattr(self.copy_system, "copy_manager"):
        tm = getattr(self.copy_system.copy_manager, "trailing_manager", None)

    if not tm:
        await update.message.reply_text("❌ Trailing Manager недоступен")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "🛡️ *НАСТРОЙКА TRAILING STOP*\n\n"
            "`/set_trailing distance 2.5` — начальная дистанция 2.5%\n"
            "`/set_trailing step 0.3` — минимальный шаг 0.3%\n"
            "`/set_trailing max 4.0` — максимальная дистанция 4.0%\n"
            "`/set_trailing atr 14 1.5` — ATR период=14, множитель=1.5\n"
            "`/set_trailing aggressive on|off` — режим\n"
            "`/set_trailing enabled on|off` — вкл/выкл\n"
            "`/set_trailing clear_all` — закрыть все trailing stops\n"
            "`/set_trailing status` — показать текущие значения",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    sub = args[0].lower()

    if sub in {"status", "show"}:
        await update.message.reply_text(_reply_snapshot(tm))
        return

    if sub == "clear_all":
        try:
            if hasattr(tm, "clear_all_stops"):
                fn = tm.clear_all_stops
                if callable(getattr(fn, "__call__", None)):
                    res = fn()
                    if hasattr(res, "__await__"):
                        await res
            await update.message.reply_text("✅ Все trailing stops очищены")
            try:
                from app.db_session import SessionLocal
                from app.db_models import SysEvents
                with SessionLocal() as session:
                    event = SysEvents(
                        level="INFO",
                        component="TelegramBot",
                        message="All trailing stops cleared",
                        details_json={"action": "clear_all", "user_id": update.effective_user.id},
                    )
                    session.add(event)
                    session.commit()
            except Exception as e:
                logger.error(f"Failed to log clear_all to sys_events: {e}")
            try:
                await send_telegram_alert("🛡️ Все trailing stops были очищены через бота")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Failed to clear trailing stops: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка очистки trailing stops: {e}")
        return

    patch = {}
    try:
        if sub in {"aggressive", "enabled", "mode"}:
            if sub == "aggressive":
                if len(args) < 2:
                    raise ValueError("Usage: /set_trailing aggressive on|off")
                patch["mode"] = "aggressive" if _parse_bool(args[1]) else "conservative"
            elif sub == "enabled":
                if len(args) < 2:
                    raise ValueError("Usage: /set_trailing enabled on|off")
                patch["enabled"] = _parse_bool(args[1])
            else:
                if len(args) < 2:
                    raise ValueError("Usage: /set_trailing mode aggressive|conservative")
                mode = args[1].strip().lower()
                if mode not in {"aggressive", "conservative"}:
                    raise ValueError("mode must be aggressive|conservative")
                patch["mode"] = mode

        elif sub in {"distance", "initial_distance", "activation"}:
            if len(args) < 2:
                raise ValueError("Usage: /set_trailing distance <percent>")
            patch["activation_pct"] = _parse_float_pct(args[1], "Дистанция", 0.1, 10.0)

        elif sub in {"step", "min_step"}:
            if len(args) < 2:
                raise ValueError("Usage: /set_trailing step <percent>")
            patch["step_pct"] = _parse_float_pct(args[1], "Шаг", 0.05, 5.0)

        elif sub in {"max", "max_distance"}:
            if len(args) < 2:
                raise ValueError("Usage: /set_trailing max <percent>")
            patch["max_pct"] = _parse_float_pct(args[1], "Максимальная дистанция", 1.0, 15.0)

        elif sub == "atr":
            if len(args) < 3:
                raise ValueError("Usage: /set_trailing atr <period:int> <multiplier:float>")
            period = int(args[1]);  mult = float(args[2].replace(",", "."))
            if period < 1:  raise ValueError("atr_period must be ≥ 1")
            if mult <= 0:   raise ValueError("atr_multiplier must be > 0")
            patch["atr_period"] = period
            patch["atr_multiplier"] = mult

        elif sub in {"atr_period", "atrperiod"}:
            if len(args) < 2:
                raise ValueError("Usage: /set_trailing atr_period <int>")
            period = int(args[1])
            if period < 1:
                raise ValueError("atr_period must be ≥ 1")
            patch["atr_period"] = period

        elif sub in {"atr_mult", "atr_multiplier", "atrmult"}:
            if len(args) < 2:
                raise ValueError("Usage: /set_trailing atr_multiplier <float>")
            mult = float(args[1].replace(",", "."))
            if mult <= 0:
                raise ValueError("atr_multiplier must be > 0")
            patch["atr_multiplier"] = mult

        else:
            raise ValueError("Неизвестный параметр. Используйте: distance, step, max, atr, aggressive, enabled, status")

    except ValueError as ve:
        await update.message.reply_text(f"❌ {ve}")
        return

    try:
        tm.reload_config(patch)
        try:
            from app.db_session import SessionLocal
            from app.db_models import SysEvents
            with SessionLocal() as session:
                event = SysEvents(
                    level="INFO",
                    component="TelegramBot",
                    message="Trailing config updated",
                    details_json={
                        "patch": patch,
                        "user_id": update.effective_user.id,
                        "timestamp": datetime.now().isoformat(),
                    },
                )
                session.add(event)
                session.commit()
        except Exception as e:
            logger.error(f"Failed to log trailing patch to sys_events: {e}")

        reply = "✅ Настройка применена.\n" + _reply_snapshot(tm)
        await update.message.reply_text(reply)

    except Exception as e:
        logger.error("Set trailing command error: %r", e, exc_info=True)
        await update.message.reply_text(f"❌ Ошибка применения настроек: {e}")

    async def set_kelly_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /set_kelly - настройка Kelly Criterion"""
        sys_logger.log_telegram_command("/set_kelly", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        if not self.copy_system or not hasattr(self.copy_system, 'kelly_calculator'):
            await update.message.reply_text("❌ Kelly Calculator не инициализирован")
            return

        try:
            args = context.args
            if len(args) < 2:
                kelly_calc = self.copy_system.kelly_calculator
                confidence = getattr(kelly_calc, 'confidence_threshold', 0.65) * 100
                max_fraction = getattr(kelly_calc, 'max_kelly_fraction', 0.25) * 100
                conservative = getattr(kelly_calc, 'conservative_factor', 0.5) * 100
        
                message = (
                    "🎯 **НАСТРОЙКИ KELLY CRITERION**\n\n"
                    f"📊 **ТЕКУЩИЕ ПАРАМЕТРЫ:**\n"
                    f"   Confidence Threshold: {confidence:.0f}%\n"
                    f"   Max Kelly Fraction: {max_fraction:.0f}%\n"
                    f"   Conservative Factor: {conservative:.0f}%\n\n"
                    "📝 **ИСПОЛЬЗОВАНИЕ:**\n"
                    "`/set_kelly confidence 70` - установить confidence 70%\n"
                    "`/set_kelly max_fraction 25` - максимальная доля 25%\n"
                    "`/set_kelly conservative 50` - консервативный фактор 50%"
                )
        
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return
    
            param = args[0].lower()
            value = float(args[1])
        
            # НОВОЕ: Импортируем глобальный конфиг и модели БД
            from stage2_copy_system import KELLY_CONFIG
            from app.db_session import SessionLocal
            from app.db_models import SysEvents
    
            kelly_calc = self.copy_system.kelly_calculator
        
            # Обновляем параметры в калькуляторе И в глобальном конфиге
            if param in ['confidence', 'confidence_threshold']:
                KELLY_CONFIG['confidence_threshold'] = value / 100  # НОВОЕ
                kelly_calc.confidence_threshold = value / 100
                param_name = "Confidence Threshold"
                new_value = f"{value:.0f}%"
            elif param in ['max_fraction', 'max_kelly_fraction']:
                KELLY_CONFIG['max_kelly_fraction'] = value / 100  # НОВОЕ
                kelly_calc.max_kelly_fraction = value / 100
                param_name = "Max Kelly Fraction"
                new_value = f"{value:.0f}%"
            elif param in ['conservative', 'conservative_factor']:
                KELLY_CONFIG['conservative_factor'] = value / 100  # НОВОЕ
                kelly_calc.conservative_factor = value / 100
                param_name = "Conservative Factor"
                new_value = f"{value:.0f}%"
            elif param in ['lookback', 'lookback_period']:
                KELLY_CONFIG['lookback_window'] = int(value)  # НОВОЕ: используем правильный ключ
                kelly_calc.lookback_period = int(value)
                param_name = "Lookback Period"
                new_value = f"{int(value)} дней"
            else:
                await update.message.reply_text("❌ Неизвестный параметр. Используйте: confidence, max_fraction, conservative, lookback")
                return
        
            # НОВОЕ: Применяем конфиг к реальному калькулятору через apply_config
            if self.copy_system and hasattr(self.copy_system, 'kelly_calculator'):
                if hasattr(kelly_calc, 'apply_config'):
                    kelly_calc.apply_config(KELLY_CONFIG)
                else:
                    logger.warning("Kelly calculator doesn't have apply_config method")
        
            # НОВОЕ: Логируем изменения в sys_events
            try:
                with SessionLocal() as session:
                    event = SysEvents(
                        level="INFO",
                        component="TelegramBot",
                        message=f"Kelly config updated: {param_name}={new_value}",
                        details_json={
                            "param": param,
                            "value": value,
                            "user_id": update.effective_user.id,
                            "timestamp": datetime.now().isoformat()
                        }
                    )
                    session.add(event)
                    session.commit()
                    logger.info(f"Kelly config change logged to sys_events: {param_name}={new_value}")
            except Exception as e:
                logger.error(f"Failed to log to sys_events: {e}")
                # Не прерываем выполнение, если логирование не удалось
    
            message = (
                f"✅ **НАСТРОЙКА KELLY ОБНОВЛЕНА**\n\n"
                f"🔧 Параметр: {param_name}\n"
                f"📊 Новое значение: {new_value}\n"
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}\n\n"
                "🔄 Новые расчеты будут использовать обновленные настройки"
            )
    
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            await send_telegram_alert(f"Kelly Criterion настройка изменена: {param_name} = {new_value}")
    
        except ValueError:
            await update.message.reply_text("❌ Неверный формат числа")
        except Exception as e:
            logger.error(f"Set Kelly command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка настройки Kelly: {str(e)[:100]}")

    async def set_drawdown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /set_drawdown - настройка контроллера просадки"""
        sys_logger.log_telegram_command("/set_drawdown", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        try:
            args = context.args
        
            # НОВОЕ: Импортируем глобальный конфиг и модели БД
            from stage2_copy_system import RISK_CONFIG
            from app.db_session import SessionLocal
            from app.db_models import SysEvents
        
            if len(args) < 1:
                # Показываем текущие настройки
                daily_limit = RISK_CONFIG.get('max_daily_loss', 0.05) * 100
                total_limit = RISK_CONFIG.get('max_total_drawdown', 0.15) * 100
                emergency = RISK_CONFIG.get('emergency_stop_threshold', 0.1) * 100
            
                await update.message.reply_text(
                    "📉 *НАСТРОЙКА КОНТРОЛЯ ПРОСАДКИ*\n\n"
                    f"**ТЕКУЩИЕ ЛИМИТЫ:**\n"
                    f"   Дневная просадка: {daily_limit:.0f}%\n"
                    f"   Общая просадка: {total_limit:.0f}%\n"
                    f"   Emergency Stop: {emergency:.0f}%\n\n"
                    "📝 **ИСПОЛЬЗОВАНИЕ:**\n"
                    "`/set_drawdown daily 3` - дневной лимит 3%\n"
                    "`/set_drawdown total 12` - общий лимит 12%\n"
                    "`/set_drawdown emergency 8` - emergency порог 8%\n"
                    "`/set_drawdown reset` - сброс счетчиков просадки",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
    
            param = args[0].lower()
    
            # Сброс счетчиков просадки
            if param == 'reset':
                if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                    try:
                        await self.copy_system.drawdown_controller.reset_drawdown_counters()
                    
                        # НОВОЕ: Логируем сброс в sys_events
                        try:
                            with SessionLocal() as session:
                                event = SysEvents(
                                    level="INFO",
                                    component="TelegramBot",
                                    message="Drawdown counters reset",
                                    details_json={
                                        "action": "reset_drawdown",
                                        "user_id": update.effective_user.id,
                                        "timestamp": datetime.now().isoformat()
                                    }
                                )
                                session.add(event)
                                session.commit()
                        except Exception as e:
                            logger.error(f"Failed to log reset to sys_events: {e}")
                    
                        await update.message.reply_text("✅ Счетчики просадки сброшены")
                        await send_telegram_alert("📉 Счетчики просадки были сброшены через бота")
                    except Exception as e:
                        logger.error(f"Failed to reset drawdown counters: {e}")
                        await update.message.reply_text(f"❌ Ошибка сброса счетчиков: {e}")
                else:
                    await update.message.reply_text("❌ Drawdown Controller недоступен")
                return
    
            # Остальные параметры требуют числового значения
            if len(args) < 2:
                await update.message.reply_text("❌ Укажите значение параметра")
                return
        
            try:
                value = float(args[1])
            except ValueError:
                await update.message.reply_text("❌ Неверный формат числа")
                return
        
            # Устанавливаем параметры
            if param in ('daily', 'daily_limit'):
                if value < 1 or value > 10:
                    await update.message.reply_text("❌ Дневной лимит должен быть от 1% до 10%")
                    return
        
                # НОВОЕ: Обновляем глобальный конфиг
                RISK_CONFIG['max_daily_loss'] = value / 100
                self.system_settings['drawdown']['daily_limit'] = value / 100
                param_name = "Дневной лимит просадки"
        
                # Применяем к реальной системе если возможно
                if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                    try:
                        self.copy_system.drawdown_controller.daily_drawdown_limit = value / 100
                    except Exception as e:
                        logger.warning(f"Failed to apply daily drawdown limit: {e}")
            
                # НОВОЕ: Также обновляем risk_monitor если есть
                if self.copy_system and hasattr(self.copy_system, 'risk_monitor'):
                    self.copy_system.risk_monitor.config = RISK_CONFIG
        
            elif param in ('total', 'total_limit'):
                if value < 5 or value > 30:
                    await update.message.reply_text("❌ Общий лимит должен быть от 5% до 30%")
                    return
        
                # НОВОЕ: Обновляем глобальный конфиг
                RISK_CONFIG['max_total_drawdown'] = value / 100
                self.system_settings['drawdown']['total_limit'] = value / 100
                param_name = "Общий лимит просадки"
        
                # Применяем к реальной системе если возможно
                if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                    try:
                        self.copy_system.drawdown_controller.max_drawdown_threshold = value / 100
                    except Exception as e:
                        logger.warning(f"Failed to apply total drawdown limit: {e}")
            
                # НОВОЕ: Также обновляем risk_monitor если есть
                if self.copy_system and hasattr(self.copy_system, 'risk_monitor'):
                    self.copy_system.risk_monitor.config = RISK_CONFIG
        
            elif param in ('emergency', 'emergency_threshold'):
                if value < 3 or value > 15:
                    await update.message.reply_text("❌ Emergency порог должен быть от 3% до 15%")
                    return
        
                # НОВОЕ: Обновляем глобальный конфиг
                RISK_CONFIG['emergency_stop_threshold'] = value / 100
                self.system_settings['drawdown']['emergency_threshold'] = value / 100
                param_name = "Emergency порог"
        
                # Применяем к реальной системе если возможно
                if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                    try:
                        self.copy_system.drawdown_controller.emergency_stop_threshold = value / 100
                    except Exception as e:
                        logger.warning(f"Failed to apply emergency threshold: {e}")
            
                # НОВОЕ: Также обновляем risk_monitor если есть
                if self.copy_system and hasattr(self.copy_system, 'risk_monitor'):
                    self.copy_system.risk_monitor.config = RISK_CONFIG
        
            else:
                await update.message.reply_text("❌ Неизвестный параметр. Используйте: daily, total, emergency, reset")
                return
        
            # НОВОЕ: Логируем изменения в sys_events
            try:
                with SessionLocal() as session:
                    event = SysEvents(
                        level="INFO",
                        component="TelegramBot",
                        message=f"Risk config updated: {param_name}={value:.1f}%",
                        details_json={
                            "param": param,
                            "value": value,
                            "user_id": update.effective_user.id,
                            "timestamp": datetime.now().isoformat(),
                            "risk_config": {
                                "daily_limit": RISK_CONFIG.get('max_daily_loss', 0.05) * 100,
                                "total_limit": RISK_CONFIG.get('max_total_drawdown', 0.15) * 100,
                                "emergency": RISK_CONFIG.get('emergency_stop_threshold', 0.1) * 100
                            }
                        }
                    )
                    session.add(event)
                    session.commit()
                    logger.info(f"Risk config change logged to sys_events: {param_name}={value:.1f}%")
            except Exception as e:
                logger.error(f"Failed to log to sys_events: {e}")
    
            message = (
                f"✅ **НАСТРОЙКА КОНТРОЛЯ ПРОСАДКИ ОБНОВЛЕНА**\n\n"
                f"🔧 Параметр: {param_name}\n"
                f"📊 Новое значение: {value:.1f}%\n"
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}\n\n"
                "🔄 Новые лимиты применены немедленно"
            )
    
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
            # Дополнительное уведомление для важных изменений
            if param == 'emergency' or (param == 'daily' and value <= 3):
                await send_telegram_alert(f"⚠️ Важное изменение риск-параметра: {param_name} = {value:.1f}%")
    
        except Exception as e:
            logger.error(f"Set drawdown command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка настройки контроля просадки: {e}")

    async def logs_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /logs - управление логами системы и просмотр событий БД"""

        # Логируем выполнение команды
        sys_logger.log_telegram_command("/logs", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        try:
            args = context.args
        
            # Если нет аргументов - показываем события из БД (по умолчанию 10)
            if not args:
                # НОВОЕ: Показываем последние события из sys_events
                limit = 10
                with SessionLocal() as session:
                    events = session.query(SysEvents)\
                        .order_by(SysEvents.id.desc())\
                        .limit(limit)\
                        .all()
                
                    if not events:
                        await update.message.reply_text("📋 Нет событий в системе")
                        return
                
                    # Форматируем вывод
                    message = f"📋 **ПОСЛЕДНИЕ {len(events)} СИСТЕМНЫХ СОБЫТИЙ:**\n\n"
                
                    for event in reversed(events):  # Показываем от старых к новым
                        level_emoji = {
                            "INFO": "ℹ️",
                            "WARN": "⚠️", 
                            "ERROR": "❌"
                        }.get(event.level, "📝")
                    
                        time_str = event.created_at.strftime("%H:%M:%S")
                        msg = f"{level_emoji} `{time_str}` **{event.component}**\n"
                        msg += f"   {event.message[:100]}\n"  # Ограничиваем длину
                    
                        message += msg
                
                    message += f"\n💡 Используйте `/logs 20` для большего количества\n"
                    message += f"📄 Или `/logs file` для просмотра файлов"
                
                    # Отправляем частями если слишком длинное
                    if len(message) > 4000:
                        parts = [message[i:i+4000] for i in range(0, len(message), 4000)]
                        for part in parts:
                            await update.message.reply_text(part, parse_mode=ParseMode.MARKDOWN)
                    else:
                        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return

            command = args[0].lower()
        
            # НОВОЕ: Если первый аргумент - число, показываем события из БД
            if command.isdigit():
                limit = min(int(command), 50)  # Максимум 50
            
                # Логируем с параметром
                sys_logger.log_telegram_command(f"/logs {limit}", update.effective_user.id)
            
                with SessionLocal() as session:
                    events = session.query(SysEvents)\
                        .order_by(SysEvents.id.desc())\
                        .limit(limit)\
                        .all()
                
                    if not events:
                        await update.message.reply_text("📋 Нет событий в логе")
                        return
                
                    # Форматируем вывод
                    message = f"📋 **ПОСЛЕДНИЕ {len(events)} СОБЫТИЙ:**\n\n"
                
                    for event in reversed(events):
                        level_emoji = {
                            "INFO": "ℹ️",
                            "WARN": "⚠️", 
                            "ERROR": "❌"
                        }.get(event.level, "📝")
                    
                        time_str = event.created_at.strftime("%H:%M:%S")
                        msg = f"{level_emoji} `{time_str}` **{event.component}**\n"
                        msg += f"   {event.message[:100]}\n"
                    
                        message += msg
                
                    # Отправляем частями если слишком длинное
                    if len(message) > 4000:
                        parts = [message[i:i+4000] for i in range(0, len(message), 4000)]
                        for part in parts:
                            await update.message.reply_text(part, parse_mode=ParseMode.MARKDOWN)
                    else:
                        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return
        
            # НОВОЕ: Показать только ошибки из БД
            elif command == 'errors':
                with SessionLocal() as session:
                    events = session.query(SysEvents)\
                        .filter(SysEvents.level.in_(["ERROR", "WARN"]))\
                        .order_by(SysEvents.id.desc())\
                        .limit(30)\
                        .all()
                
                    if not events:
                        message = (
                            "📜 *ОШИБКИ В СИСТЕМЕ*\n\n"
                            "✅ Ошибки не найдены"
                        )
                    else:
                        message = "📜 *ПОСЛЕДНИЕ ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ:*\n\n"
                    
                        for event in reversed(events):
                            level_emoji = "❌" if event.level == "ERROR" else "⚠️"
                            time_str = event.created_at.strftime("%H:%M:%S")
                            msg = f"{level_emoji} `{time_str}` **{event.component}**\n"
                            msg += f"   {event.message[:100]}\n"
                        
                            message += msg
                    
                        message += f"\n⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return
        
            # Работа с файлами логов (старая функциональность)
            elif command == 'file':
                # Показать последние строки из файла
                if len(args) > 1 and args[1].isdigit():
                    lines_count = int(args[1])
                    if lines_count < 1 or lines_count > 200:
                        await update.message.reply_text("❌ Количество строк должно быть от 1 до 200")
                        return
                
                    # Определяем, какие лог файлы доступны
                    log_files = []
                    log_directories = ['logs', '.', 'log']
                
                    for log_dir in log_directories:
                        if os.path.exists(log_dir):
                            for file in os.listdir(log_dir):
                                if file.endswith('.log'):
                                    log_files.append(os.path.join(log_dir, file))
                
                    if not log_files:
                        await update.message.reply_text("❌ Лог файлы не найдены")
                        return
                
                    # Сортируем по времени изменения (новые сначала)
                    log_files.sort(key=os.path.getmtime, reverse=True)
                    main_log_file = log_files[0]
                
                    try:
                        with open(main_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                            lines = f.readlines()
                            last_lines = lines[-lines_count:] if len(lines) >= lines_count else lines
                            log_content = ''.join(last_lines)
                    
                        # Обрезаем для Telegram
                        if len(log_content) > 3000:
                            log_content = "...\n" + log_content[-2900:]
                    
                        message = (
                            f"📜 *ПОСЛЕДНИЕ {lines_count} СТРОК ФАЙЛА*\n"
                            f"📄 Файл: `{os.path.basename(main_log_file)}`\n\n"
                            f"```\n{log_content.strip()}\n```\n\n"
                            f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                        )
                    
                        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                    
                    except Exception as e:
                        logger.error(f"Failed to read log file: {e}")
                        await update.message.reply_text(f"❌ Ошибка чтения файла: {e}")
                else:
                    await update.message.reply_text(
                        "📄 **РАБОТА С ФАЙЛАМИ ЛОГОВ**\n\n"
                        "Используйте:\n"
                        "`/logs file 50` - последние 50 строк из файла\n"
                        "`/logs file errors` - ошибки из файла",
                        parse_mode=ParseMode.MARKDOWN
                    )
                return
        
            # Очистка логов
            elif command == 'clear':
                # НОВОЕ: Очистка событий из БД
                if len(args) > 1 and args[1].lower() == 'db':
                    if len(args) > 2 and args[2].lower() == 'confirm':
                        try:
                            with SessionLocal() as session:
                                # Удаляем старые события (старше 7 дней)
                                cutoff_date = datetime.now() - timedelta(days=7)
                                deleted = session.query(SysEvents)\
                                    .filter(SysEvents.created_at < cutoff_date)\
                                    .delete()
                                session.commit()
                            
                                await update.message.reply_text(
                                    f"✅ Удалено {deleted} старых событий из БД\n"
                                    "(старше 7 дней)"
                                )
                            
                                # Логируем очистку
                                sys_logger.log_event("INFO", "TelegramBot", 
                                                   f"Database events cleared (deleted: {deleted})",
                                                   {"user_id": update.effective_user.id})
                        except Exception as e:
                            logger.error(f"Failed to clear DB events: {e}")
                            await update.message.reply_text(f"❌ Ошибка очистки БД: {e}")
                    else:
                        await update.message.reply_text(
                            "⚠️ **ПОДТВЕРЖДЕНИЕ ОЧИСТКИ БД**\n\n"
                            "Будут удалены события старше 7 дней.\n\n"
                            "Для подтверждения выполните:\n"
                            "`/logs clear db confirm`",
                            parse_mode=ParseMode.MARKDOWN
                        )
            
                # Очистка файлов (полный код)
                elif len(args) > 1 and args[1].lower() == 'files':
                    if len(args) > 2 and args[2].lower() == 'confirm':
                        # Реальный код очистки файлов
                        cleared_count = 0
                        errors_count = 0
                    
                        # Определяем, какие лог файлы доступны
                        log_files = []
                        log_directories = ['logs', '.', 'log']
                    
                        for log_dir in log_directories:
                            if os.path.exists(log_dir):
                                for file in os.listdir(log_dir):
                                    if file.endswith('.log'):
                                        log_files.append(os.path.join(log_dir, file))
                    
                        if not log_files:
                            await update.message.reply_text("❌ Лог файлы не найдены")
                            return
                    
                        # Очищаем каждый файл
                        for log_file in log_files:
                            try:
                                # Не удаляем файл, а только очищаем содержимое
                                with open(log_file, 'w') as f:
                                    f.write(f"Log cleared at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                                cleared_count += 1
                            except Exception as e:
                                logger.warning(f"Failed to clear log file {log_file}: {e}")
                                errors_count += 1
                    
                        # Логируем очистку файлов в БД
                        sys_logger.log_event("INFO", "TelegramBot", 
                                           f"Log files cleared (success: {cleared_count}, failed: {errors_count})",
                                           {"user_id": update.effective_user.id, 
                                            "cleared_files": cleared_count,
                                            "failed_files": errors_count})
                    
                        # Формируем ответ
                        if errors_count > 0:
                            await update.message.reply_text(
                                f"✅ Очищено файлов: {cleared_count}\n"
                                f"⚠️ Не удалось очистить: {errors_count}"
                            )
                        else:
                            await update.message.reply_text(
                                f"✅ Успешно очищено файлов логов: {cleared_count}"
                            )
                    else:
                        # Запрос подтверждения
                        # Сначала подсчитаем количество файлов
                        log_files_count = 0
                        log_directories = ['logs', '.', 'log']
                    
                        for log_dir in log_directories:
                            if os.path.exists(log_dir):
                                for file in os.listdir(log_dir):
                                    if file.endswith('.log'):
                                        log_files_count += 1
                    
                        await update.message.reply_text(
                            f"⚠️ **ПОДТВЕРЖДЕНИЕ ОЧИСТКИ ФАЙЛОВ**\n\n"
                            f"Будет очищено файлов: {log_files_count}\n"
                            f"Это действие необратимо!\n\n"
                            f"Для подтверждения выполните:\n"
                            f"`/logs clear files confirm`",
                            parse_mode=ParseMode.MARKDOWN
                        )
                else:
                    await update.message.reply_text(
                        "🗑️ **ОЧИСТКА ЛОГОВ**\n\n"
                        "`/logs clear db` - очистить старые события БД\n"
                        "`/logs clear files` - очистить файлы логов",
                        parse_mode=ParseMode.MARKDOWN
                    )
                return
        
            # НОВОЕ: Статистика событий
            elif command == 'stats':
                with SessionLocal() as session:
                    total = session.query(SysEvents).count()
                    errors = session.query(SysEvents).filter_by(level="ERROR").count()
                    warnings = session.query(SysEvents).filter_by(level="WARN").count()
                
                    # События за последний час
                    hour_ago = datetime.now() - timedelta(hours=1)
                    recent = session.query(SysEvents)\
                        .filter(SysEvents.created_at > hour_ago).count()
                
                    # Топ компонентов по событиям
                    from sqlalchemy import func
                    top_components = session.query(
                        SysEvents.component,
                        func.count(SysEvents.id).label('count')
                    ).group_by(SysEvents.component)\
                    .order_by(func.count(SysEvents.id).desc())\
                    .limit(5).all()
                
                    message = (
                        "📊 **СТАТИСТИКА СИСТЕМНЫХ СОБЫТИЙ**\n\n"
                        f"📈 **ОБЩЕЕ:**\n"
                        f"   Всего событий: {total}\n"
                        f"   Ошибок: {errors}\n"
                        f"   Предупреждений: {warnings}\n"
                        f"   За последний час: {recent}\n\n"
                        f"🏆 **ТОП КОМПОНЕНТОВ:**\n"
                    )
                
                    for comp, count in top_components:
                        message += f"   {comp}: {count}\n"
                
                    message += f"\n⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return
        
            else:
                # Справка по командам
                await update.message.reply_text(
                    "📋 **КОМАНДЫ ЛОГИРОВАНИЯ**\n\n"
                    "**События из БД:**\n"
                    "`/logs` - последние 10 событий\n"
                    "`/logs 30` - последние 30 событий\n"
                    "`/logs errors` - только ошибки\n"
                    "`/logs stats` - статистика событий\n\n"
                    "**Файлы логов:**\n"
                    "`/logs file 50` - последние 50 строк\n\n"
                    "**Очистка:**\n"
                    "`/logs clear db` - очистить старые события БД\n"
                    "`/logs clear files` - очистить файлы",
                    parse_mode=ParseMode.MARKDOWN
                )
            
        except Exception as e:
            logger.error(f"Logs command error: {e}")
            logger.error(traceback.format_exc())
        
            # НОВОЕ: Логируем ошибку команды
            sys_logger.log_error("TelegramBot", f"Logs command failed: {str(e)}", 
                               {"user_id": update.effective_user.id})
        
            await update.message.reply_text(f"❌ Ошибка команды логов: {e}")

    async def export_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /export - экспорт отчетов и данных"""
        sys_logger.log_telegram_command("/export", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
    
        try:
            args = context.args
            export_type = args[0].lower() if args else "json"
        
            # Собираем данные для экспорта
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'system_status': {},
                'balances': {},
                'positions': {},
                'performance': {},
                'settings': self.system_settings
            }
        
            # Получаем данные системы
            if self.copy_system:
                try:
                    # Статус системы
                    export_data['system_status'] = {
                        'active': getattr(self.copy_system, 'system_active', False),
                        'copy_enabled': getattr(self.copy_system, 'copy_enabled', False),
                        'uptime': time.time() - getattr(self.copy_system, 'start_time', time.time())
                    }
                
                    # Балансы
                    if hasattr(self.copy_system, 'base_monitor'):
                        try:
                            source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                            main_balance = await self.copy_system.base_monitor.main_client.get_balance()
                            export_data['balances'] = {
                                'source': source_balance,
                                'main': main_balance,
                                'total': source_balance + main_balance
                            }
                        except Exception as e:
                            export_data['balances'] = {'error': f'Unable to fetch balances: {e}'}
                
                    # Позиции
                    try:
                        if hasattr(self.copy_system, 'base_monitor'):
                            # Получаем позиции из источника
                            source_positions = await self.copy_system.base_monitor.source_client.get_positions()
                            # Получаем позиции из основного аккаунта
                            main_positions = await self.copy_system.base_monitor.main_client.get_positions()
                        
                            # Отфильтровываем только активные позиции
                            active_source = [p for p in source_positions if self._safe_float(p.get('size', 0)) > 0]
                            active_main = [p for p in main_positions if self._safe_float(p.get('size', 0)) > 0]
                        
                            # Добавляем в отчет
                            export_data['positions'] = {
                                'source': active_source,
                                'main': active_main
                            }
                    except Exception as e:
                        export_data['positions'] = {'error': f'Unable to fetch positions: {e}'}
                
                    # Статистика производительности
                    if hasattr(self.copy_system, 'system_stats'):
                        stats = self.copy_system.system_stats
                        export_data['performance'] = {
                            'total_signals': stats.get('total_signals_processed', 0),
                            'successful_copies': stats.get('successful_copies', 0),
                            'failed_copies': stats.get('failed_copies', 0),
                            'success_rate': stats.get('success_rate', 0.0)
                        }
                    
                except Exception as e:
                    export_data['error'] = f"Data collection error: {e}"
        
            # Генерируем отчет в зависимости от типа
            if export_type == "json":
                report_filename = f"trading_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
                try:
                    with open(report_filename, 'w', encoding='utf-8') as f:
                        json.dump(export_data, f, indent=2, ensure_ascii=False)
                
                    file_size = os.path.getsize(report_filename)
                
                    message = (
                        "📤 *ЭКСПОРТ JSON ОТЧЕТА*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📄 **Файл создан:** `{report_filename}`\n"
                        f"📊 **Размер:** {file_size} байт\n"
                        f"⏰ **Дата:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                        "✅ JSON отчет успешно создан"
                    )
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                
                except Exception as e:
                    logger.error(f"Failed to create JSON report: {e}")
                    await update.message.reply_text(f"❌ Ошибка создания JSON отчета: {e}")
        
            elif export_type == "csv":
                report_filename = f"trading_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
                try:
                    with open(report_filename, 'w', encoding='utf-8') as f:
                        # Заголовок CSV
                        f.write("Дата,Тип,Метрика,Значение\n")
                    
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                        # Статус
                        for key, value in export_data['system_status'].items():
                            f.write(f"{timestamp},System,{key},{value}\n")
                    
                        # Балансы
                        for key, value in export_data['balances'].items():
                            f.write(f"{timestamp},Balance,{key},{value}\n")
                    
                        # Производительность
                        for key, value in export_data['performance'].items():
                            f.write(f"{timestamp},Performance,{key},{value}\n")
                    
                        # Позиции (упрощенно для CSV)
                        f.write(f"{timestamp},Positions,source_count,{len(export_data['positions'].get('source', []))}\n")
                        f.write(f"{timestamp},Positions,main_count,{len(export_data['positions'].get('main', []))}\n")
                    
                        # Общий P&L
                        source_pnl = sum([self._safe_float(p.get('unrealisedPnl', 0)) for p in export_data['positions'].get('source', [])])
                        main_pnl = sum([self._safe_float(p.get('unrealisedPnl', 0)) for p in export_data['positions'].get('main', [])])
                        f.write(f"{timestamp},Positions,source_pnl,{source_pnl}\n")
                        f.write(f"{timestamp},Positions,main_pnl,{main_pnl}\n")
                
                    file_size = os.path.getsize(report_filename)
                
                    message = (
                        "📤 *ЭКСПОРТ CSV ОТЧЕТА*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📄 **Файл создан:** `{report_filename}`\n"
                        f"📊 **Размер:** {file_size} байт\n"
                        f"⏰ **Дата:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                        "✅ CSV отчет успешно создан"
                    )
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                
                except Exception as e:
                    logger.error(f"Failed to create CSV report: {e}")
                    await update.message.reply_text(f"❌ Ошибка создания CSV отчета: {e}")
        
            elif export_type == "summary":
                # Создаем текстовый отчет для Telegram
                try:
                    # Статус системы
                    system_active = export_data['system_status'].get('active', False)
                    copy_enabled = export_data['system_status'].get('copy_enabled', False)
                    uptime_seconds = export_data['system_status'].get('uptime', 0)
                    uptime_hours = int(uptime_seconds / 3600)
                    uptime_minutes = int((uptime_seconds % 3600) / 60)
                
                    # Балансы
                    source_balance = export_data['balances'].get('source', 0)
                    main_balance = export_data['balances'].get('main', 0)
                    total_balance = export_data['balances'].get('total', 0)
                
                    # Позиции
                    source_positions_count = len(export_data['positions'].get('source', []))
                    main_positions_count = len(export_data['positions'].get('main', []))
                
                    # P&L
                    source_pnl = sum([self._safe_float(p.get('unrealisedPnl', 0)) for p in export_data['positions'].get('source', [])])
                    main_pnl = sum([self._safe_float(p.get('unrealisedPnl', 0)) for p in export_data['positions'].get('main', [])])
                
                    # Статистика
                    signals = export_data['performance'].get('total_signals', 0)
                    successful = export_data['performance'].get('successful_copies', 0)
                    success_rate = export_data['performance'].get('success_rate', 0)
                
                    message = (
                        "📋 *КРАТКИЙ ОТЧЕТ СИСТЕМЫ*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⏰ **ВРЕМЯ РАБОТЫ:** {uptime_hours}ч {uptime_minutes}м\n"
                        f"📅 **ДАТА:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                    
                        f"🔄 **СТАТУС СИСТЕМЫ:**\n"
                        f"   Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                        f"   Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
                    
                        f"💰 **БАЛАНСЫ:**\n"
                        f"   Источник: ${source_balance:.2f}\n"
                        f"   Основной: ${main_balance:.2f}\n"
                        f"   Всего: ${total_balance:.2f}\n\n"
                    
                        f"📈 **ПОЗИЦИИ:**\n"
                        f"   Источник: {source_positions_count} (P&L: ${source_pnl:+.2f})\n"
                        f"   Основной: {main_positions_count} (P&L: ${main_pnl:+.2f})\n\n"
                    
                        f"📊 **СТАТИСТИКА:**\n"
                        f"   Обработано сигналов: {signals}\n"
                        f"   Успешных копий: {successful}\n"
                        f"   Успешность: {success_rate:.1%}\n"
                    )
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                
                except Exception as e:
                    logger.error(f"Failed to create summary report: {e}")
                    await update.message.reply_text(f"❌ Ошибка создания краткого отчета: {e}")
        
            elif export_type == "settings":
                settings_filename = f"settings_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
                try:
                    with open(settings_filename, 'w', encoding='utf-8') as f:
                        json.dump(self.system_settings, f, indent=2, ensure_ascii=False)
                
                    file_size = os.path.getsize(settings_filename)
                
                    message = (
                        "📤 *ЭКСПОРТ НАСТРОЕК*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📄 **Файл создан:** `{settings_filename}`\n"
                        f"📊 **Размер:** {file_size} байт\n"
                        f"⏰ **Дата:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                        "✅ Настройки успешно экспортированы"
                    )
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                
                except Exception as e:
                    logger.error(f"Failed to export settings: {e}")
                    await update.message.reply_text(f"❌ Ошибка экспорта настроек: {e}")
        
            else:
                await update.message.reply_text(
                    "❓ Неизвестный формат экспорта\n"
                    "Доступные: json, csv, summary, settings"
                )
            
        except Exception as e:
            logger.error(f"Export command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка экспорта: {e}")

    async def backup_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /backup - управление резервными копиями"""
        sys_logger.log_telegram_command("/backup", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
    
        try:
            args = context.args
            command = args[0].lower() if args else "create"
        
            if command == "create":
                await self.backup_settings_text(update)
            
            elif command == "list":
                # Ищем доступные backup файлы
                backup_files = []
                for file in os.listdir('.'):
                    if file.startswith('settings_backup_') and file.endswith('.json'):
                        backup_files.append(file)
            
                if not backup_files:
                    await update.message.reply_text("❌ Backup файлы не найдены")
                    return
            
                backup_files.sort(reverse=True)  # Новые сначала
            
                # Формируем список с информацией о файлах
                backup_list = ""
                for i, backup_file in enumerate(backup_files):
                    try:
                        file_time = os.path.getmtime(backup_file)
                        file_size = os.path.getsize(backup_file)
                        backup_list += f"{i+1}. `{backup_file}`\n   📅 {datetime.fromtimestamp(file_time).strftime('%d.%m.%Y %H:%M')} ({file_size} байт)\n\n"
                    except Exception as e:
                        backup_list += f"{i+1}. `{backup_file}` (ошибка чтения: {e})\n\n"
            
                message = (
                    "📋 *СПИСОК BACKUP ФАЙЛОВ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📁 **Найдено файлов:** {len(backup_files)}\n\n"
                    f"{backup_list}"
                    "🔧 **КОМАНДЫ:**\n"
                    "`/backup restore <имя_файла>` - восстановить конкретный файл\n"
                    "`/backup clean old` - удалить старые backup'ы"
                )
            
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
            elif command == "clean" and len(args) > 1 and args[1].lower() == "old":
                # Удаление старых резервных копий (оставляем последние 5)
                backup_files = []
                for file in os.listdir('.'):
                    if file.startswith('settings_backup_') and file.endswith('.json'):
                        backup_files.append(file)
            
                if len(backup_files) <= 5:
                    await update.message.reply_text("ℹ️ Нет старых backup файлов для удаления (сохраняем минимум 5)")
                    return
            
                # Сортируем по времени изменения (старые в начале)
                backup_files.sort(key=os.path.getmtime)
            
                # Удаляем все кроме 5 самых новых
                files_to_delete = backup_files[:-5]
                deleted_count = 0
            
                for file in files_to_delete:
                    try:
                        os.remove(file)
                        deleted_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to delete old backup {file}: {e}")
            
                await update.message.reply_text(f"✅ Удалено старых backup файлов: {deleted_count}")
            
            else:
                await update.message.reply_text(
                    "❓ Неизвестная команда\n"
                    "Доступные: create, list, clean old"
                )
            
        except Exception as e:
            logger.error(f"Backup command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка команды backup: {e}")

    async def restore_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """🆕 Команда /restore - восстановление из резервной копии"""
        sys_logger.log_telegram_command("/restore", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
    
        try:
            args = context.args
            if not args:
                await self.restore_settings_text(update)
                return
        
            restore_target = args[0].lower()
        
            # Получаем список файлов backup
            backup_files = []
            for file in os.listdir('.'):
                if file.startswith('settings_backup_') and file.endswith('.json'):
                    backup_files.append(file)
        
            backup_files.sort(reverse=True)  # Новые сначала
        
            if not backup_files:
                await update.message.reply_text("❌ Backup файлы не найдены")
                return
        
            # Определяем файл для восстановления
            restore_file = None
        
            if restore_target == "latest":
                restore_file = backup_files[0]
            elif restore_target == "preview" and len(args) > 1 and args[1].lower() == "latest":
                # Предпросмотр содержимого файла
                restore_file = backup_files[0]
            
                try:
                    with open(restore_file, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                
                    # Форматируем для показа
                    preview = json.dumps(backup_data, indent=2, ensure_ascii=False)
                    if len(preview) > 3000:
                        preview = preview[:3000] + "...\n[Обрезано из-за ограничений Telegram]"
                
                    message = (
                        f"🔍 *ПРЕДПРОСМОТР BACKUP*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📄 **Файл:** `{restore_file}`\n"
                        f"⏰ **Создан:** {backup_data.get('backup_timestamp', 'Неизвестно')[:19]}\n\n"
                        f"📋 **СОДЕРЖИМОЕ:**\n"
                        f"```\n{preview}\n```"
                    )
                
                    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                    return
                
                except Exception as e:
                    logger.error(f"Failed to preview backup: {e}")
                    await update.message.reply_text(f"❌ Ошибка предпросмотра backup: {e}")
                    return
            
            elif restore_target.endswith('.json'):
                # Проверяем, существует ли файл
                if restore_target in backup_files or os.path.exists(restore_target):
                    restore_file = restore_target
                else:
                    await update.message.reply_text(f"❌ Файл не найден: {restore_target}")
                    return
        
            # Восстановление определенной части настроек
            elif restore_target in ("kelly", "drawdown", "trailing", "notifications") and len(args) > 1 and args[1].lower() == "only":
                restore_file = backup_files[0]
                section_map = {
                    "kelly": "kelly_settings",
                    "drawdown": "drawdown_settings",
                    "trailing": "telegram_bot_settings.system_settings.trailing",
                    "notifications": "telegram_bot_settings.notification_settings"
                }
            
                section = section_map.get(restore_target)
                if not section:
                    await update.message.reply_text(f"❌ Неизвестная секция: {restore_target}")
                    return
            
                try:
                    with open(restore_file, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                
                    # Получаем нужную секцию настроек
                    if "." in section:
                        # Для вложенных секций
                        parts = section.split(".")
                        section_data = backup_data
                        for part in parts:
                            if part in section_data:
                                section_data = section_data[part]
                            else:
                                await update.message.reply_text(f"❌ Секция {section} не найдена в backup")
                                return
                    else:
                        # Для обычных секций
                        if section not in backup_data:
                            await update.message.reply_text(f"❌ Секция {section} не найдена в backup")
                            return
                        section_data = backup_data[section]
                
                    # Применяем только нужную секцию
                    if restore_target == "kelly":
                        self.system_settings['kelly'] = section_data
                        if self.copy_system and hasattr(self.copy_system, 'kelly_calculator'):
                            for key, value in section_data.items():
                                setattr(self.copy_system.kelly_calculator, key, value)
                
                    elif restore_target == "drawdown":
                        self.system_settings['drawdown'] = section_data
                        if self.copy_system and hasattr(self.copy_system, 'drawdown_controller'):
                            for key, value in section_data.items():
                                setattr(self.copy_system.drawdown_controller, key, value)
                
                    elif restore_target == "trailing":
                        self.system_settings['trailing'] = section_data
                        if self.copy_system and hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'trailing_manager'):
                            tm = self.copy_system.copy_manager.trailing_manager
                            tm.default_distance_percent = section_data.get('initial_distance', 0.02)
                            tm.min_trail_step = section_data.get('min_step', 0.005)
                            tm.max_distance_percent = section_data.get('max_distance', 0.05)
                            tm.aggressive_mode = section_data.get('aggressive_mode', False)
                
                    elif restore_target == "notifications":
                        self.notification_settings = section_data
                
                    await update.message.reply_text(
                        f"✅ Настройки {restore_target} успешно восстановлены из {restore_file}"
                    )
                    return
                
                except Exception as e:
                    logger.error(f"Failed to restore section {restore_target}: {e}")
                    await update.message.reply_text(f"❌ Ошибка восстановления секции {restore_target}: {e}")
                    return
        
            else:
                await update.message.reply_text(
                    "❓ Неизвестная команда\n"
                    "Доступные: latest, preview latest, <имя_файла.json>, kelly only, drawdown only, trailing only, notifications only"
                )
                return
        
            # Восстановление всех настроек из файла
            if restore_file:
                try:
                    with open(restore_file, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                
                    # Проверяем структуру файла
                    if 'version' not in backup_data or 'telegram_bot_settings' not in backup_data:
                        await update.message.reply_text("❌ Некорректный формат backup файла")
                        return
                
                    # Восстанавливаем настройки Telegram бота
                    if 'notification_settings' in backup_data['telegram_bot_settings']:
                        self.notification_settings = backup_data['telegram_bot_settings']['notification_settings']
                
                    if 'system_settings' in backup_data['telegram_bot_settings']:
                        self.system_settings = backup_data['telegram_bot_settings']['system_settings']
                
                    if 'cooldown_time' in backup_data['telegram_bot_settings']:
                        self.cooldown_time = backup_data['telegram_bot_settings']['cooldown_time']
                
                    # Восстанавливаем настройки реальной системы
                    if self.copy_system:
                        # Kelly Calculator настройки
                        if 'kelly_settings' in backup_data and hasattr(self.copy_system, 'kelly_calculator'):
                            kelly_calc = self.copy_system.kelly_calculator
                            for key, value in backup_data['kelly_settings'].items():
                                setattr(kelly_calc, key, value)
                    
                        # Drawdown Controller настройки
                        if 'drawdown_settings' in backup_data and hasattr(self.copy_system, 'drawdown_controller'):
                            dd_controller = self.copy_system.drawdown_controller
                            for key, value in backup_data['drawdown_settings'].items():
                                setattr(dd_controller, key, value)
                    
                        # Copy Manager настройки
                        if 'copy_settings' in backup_data and hasattr(self.copy_system, 'copy_manager'):
                            copy_manager = self.copy_system.copy_manager
                            for key, value in backup_data['copy_settings'].items():
                                if key != 'copy_mode':  # copy_mode обычно строка, но должен быть enum
                                    setattr(copy_manager, key, value)
                
                    await update.message.reply_text(
                        f"✅ Настройки успешно восстановлены из файла {restore_file}\n"
                        "🔄 Все настройки применены к системе"
                    )
                
                    # Отправляем уведомление о восстановлении
                    await send_telegram_alert(f"⚠️ Настройки системы были восстановлены из backup: {restore_file}")
                
                except Exception as e:
                    logger.error(f"Failed to restore settings: {e}")
                    logger.error(traceback.format_exc())
                    await update.message.reply_text(f"❌ Ошибка восстановления настроек: {e}")
        
        except Exception as e:
            logger.error(f"Restore command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка команды restore: {e}")

    async def emergency_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /emergency - экстренная остановка"""
        sys_logger.log_telegram_command("/emergency", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
    
        try:
            args = context.args
            command = args[0].lower() if args else "stop"
        
            if not self.copy_system:
                await update.message.reply_text("❌ Система не инициализирована")
                return
        
            if command == "stop":
                # Экстренная остановка копирования
                self.copy_system.copy_enabled = False
                if hasattr(self.copy_system, 'copy_manager'):
                    await self.copy_system.copy_manager.stop_copying()
            
                await update.message.reply_text(
                    "🚨 **ЭКСТРЕННАЯ ОСТАНОВКА ВЫПОЛНЕНА**\n"
                    "Копирование остановлено",
                    parse_mode=ParseMode.MARKDOWN
                )
            
                # Отправляем уведомление
                await send_telegram_alert("🚨 Экстренная остановка выполнена пользователем через бота")
        
            elif command == "close_all":
                # Экстренное закрытие всех позиций
                if hasattr(self.copy_system, 'base_monitor') and hasattr(self.copy_system.base_monitor, 'main_client'):
                    # Получаем все активные позиции
                    positions = await self.copy_system.base_monitor.main_client.get_positions()
                    active_positions = [p for p in positions if self._safe_float(p.get('size', 0)) > 0]
                
                    if not active_positions:
                        await update.message.reply_text("ℹ️ Нет активных позиций для закрытия")
                        return
                
                    # Закрываем все позиции
                    closed_count = 0
                    for position in active_positions:
                        try:
                            symbol = position.get('symbol')
                            side = position.get('side')
                            close_side = "Sell" if side == "Buy" else "Buy"
                            size = self._safe_float(position.get('size', 0))
                        
                            if size > 0:
                                await self.copy_system.base_monitor.main_client.close_position(
                                    symbol=symbol,
                                    side=close_side,
                                    size=size
                                )
                                closed_count += 1
                        except Exception as e:
                            logger.error(f"Failed to close position {position.get('symbol')}: {e}")
                
                    await update.message.reply_text(
                        f"🚨 **ЭКСТРЕННОЕ ЗАКРЫТИЕ ПОЗИЦИЙ**\n"
                        f"Закрыто позиций: {closed_count}/{len(active_positions)}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                    # Отправляем уведомление
                    await send_telegram_alert(f"🚨 Экстренное закрытие всех позиций ({closed_count}) выполнено через бота")
                
                else:
                    await update.message.reply_text("❌ Невозможно получить доступ к клиенту API")
        
            elif command == "restart":
                # Перезапуск системы
                if hasattr(self.copy_system, 'restart'):
                    await self.copy_system.restart()
                    await update.message.reply_text(
                        "🔄 **СИСТЕМА ПЕРЕЗАПУЩЕНА**\n"
                        "Все компоненты перезагружены",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                    # Отправляем уведомление
                    await send_telegram_alert("🔄 Система была перезапущена через бота")
                else:
                    # Если метод restart не доступен, делаем простое включение/выключение
                    self.copy_system.system_active = False
                    await asyncio.sleep(1)
                    self.copy_system.system_active = True
                    self.copy_system.copy_enabled = True
                
                    await update.message.reply_text(
                        "🔄 **СИСТЕМА ПЕРЕЗАПУЩЕНА**\n"
                        "Выполнен программный перезапуск",
                        parse_mode=ParseMode.MARKDOWN
                    )
        
            else:
                await update.message.reply_text(
                    "❓ Неизвестная команда\n"
                    "Доступные: stop, close_all, restart"
                )
            
        except Exception as e:
            logger.error(f"Emergency command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка выполнения экстренной команды: {e}")

    async def ws_diag_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /ws_diag - диагностика WebSocket."""
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        logger.info("Received /ws_diag command from Telegram.")

        if not self.copy_system or not hasattr(self.copy_system, 'base_monitor') or not hasattr(self.copy_system.base_monitor, 'websocket_manager'):
            await update.message.reply_text("❌ WebSocket manager не найден.")
            return

        try:
            await update.message.reply_text("🔍 Generating WebSocket diagnostics...")

            ws_manager = self.copy_system.base_monitor.websocket_manager
            stats = ws_manager.get_stats()

            report = await ws_manager.get_diagnostic_report() # Basic report

            # Add per-topic stats
            topic_counts = stats.get('topic_counts')
            if topic_counts:
                report += "\n\n**Per-Topic Message Counts:**\n"
                for topic, counts in sorted(topic_counts.items()):
                    received = counts.get('received', 0)
                    processed = counts.get('processed', 0)
                    report += f"  - `{topic}`: Rcvd {received}, Proc {processed}\n"

            await update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"WS Diag command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка диагностики: {e}")

    async def reconcile_now_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /reconcile_now - ручная сверка позиций."""
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        logger.info("Received /reconcile_now command from Telegram.")

        if not self.copy_system or not hasattr(self.copy_system, 'base_monitor'):
            await update.message.reply_text("❌ Trading monitor не найден.")
            return

        try:
            await update.message.reply_text("⏳ Запускаю ручную сверку позиций...")
            # Запускаем в отдельной задаче, чтобы не блокировать бота
            asyncio.create_task(self.copy_system.base_monitor.reconcile_positions_on_startup())
            # Сообщение об успешном запуске будет отправлено из самого метода reconcile
        except Exception as e:
            logger.error(f"Reconcile Now command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка запуска сверки: {e}")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced /status command for a comprehensive system overview."""
        sys_logger.log_telegram_command("/status", update.effective_user.id)
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        if not self.copy_system or not hasattr(self.copy_system, 'base_monitor'):
            await update.message.reply_text("❌ Система не инициализирована.")
            return

        try:
            msg = await update.message.reply_text("🔄 Собираю полную сводку по системе...")

            # 1. System Status & Uptime
            start_time = self.copy_system.system_stats.get('start_time', time.time())
            uptime_seconds = time.time() - start_time
            uptime_str = str(timedelta(seconds=int(uptime_seconds)))

            # 2. Balances
            source_summary = await self.get_account_summary(self.copy_system.base_monitor.source_client)
            main_summary = await self.get_account_summary(self.copy_system.base_monitor.main_client)

            # 3. WS Diagnostics
            ws_manager = self.copy_system.base_monitor.websocket_manager
            ws_status = ws_manager.get_stats().get('status', 'UNKNOWN')
            ws_diag_info = (
                f"  Status: {ws_status}\n"
                f"  Authenticated: {'✅' if ws_status == 'authenticated' else '❌'}\n"
                f"  Messages (R/P): {ws_manager.stats.get('messages_received', 0)}/{ws_manager.stats.get('messages_processed', 0)}"
            )

            # 4. Copy State ("Single Source of Truth")
            copy_state = self.copy_system.copy_state
            state_details = (
                f"  Copy Ready: {'✅' if copy_state.ready else '❌'}\n"
                f"     - Main REST OK: {'✅' if copy_state.main_rest_ok else '❌'}\n"
                f"     - Source WS OK: {'✅' if copy_state.source_ws_ok else '❌'}\n"
                f"     - Keys Loaded: {'✅' if copy_state.keys_loaded else '❌'}"
            )
            if copy_state.last_error:
                state_details += f"\n     - Last Error: `{copy_state.last_error}`"


            report = (
                f"📊 *ПОЛНЫЙ СТАТУС СИСТЕМЫ*\n"
                f"_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"*Состояние Копирования:*\n"
                f"{state_details}\n"
                f"  Uptime: {uptime_str}\n\n"
                f"*Финансы (Донор / Основной):*\n"
                f"  Баланс: `${source_summary['balance']:.2f}` / `${main_summary['balance']:.2f}`\n"
                f"  P&L: `${source_summary['total_unrealized_pnl']:+.2f}` / `${main_summary['total_unrealized_pnl']:+.2f}`\n\n"

                f"*Позиции (Донор / Основной):*\n"
                f"  Кол-во: {source_summary['positions_count']} / {main_summary['positions_count']}\n\n"

                f"*Подключения:*\n"
                f"{ws_diag_info}\n"
            )

            await msg.edit_text(report, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Enhanced Status command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка получения статуса: {e}")

    async def copy_on_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to enable copy trading."""
        sys_logger.log_telegram_command("/copy_on", update.effective_user.id)
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
        if not self.copy_system:
            await update.message.reply_text("❌ Система не инициализирована")
            return

        self.copy_system.copy_enabled = True
        if hasattr(self.copy_system, 'copy_manager'):
            await self.copy_system.copy_manager.start_copying()

        await update.message.reply_text("✅ Копирование включено.")
        await send_telegram_alert("✅ Копирование было ВКЛЮЧЕНО через /copy_on")

    async def copy_off_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to disable copy trading."""
        sys_logger.log_telegram_command("/copy_off", update.effective_user.id)
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
        if not self.copy_system:
            await update.message.reply_text("❌ Система не инициализирована")
            return

        self.copy_system.copy_enabled = False
        if hasattr(self.copy_system, 'copy_manager'):
            await self.copy_system.copy_manager.stop_copying()

        await update.message.reply_text("❌ Копирование выключено.")
        await send_telegram_alert("🛑 Копирование было ВЫКЛЮЧЕНО через /copy_off")

    async def scale_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to set the position scaling factor."""
        sys_logger.log_telegram_command("/scale", update.effective_user.id)
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
        if not self.copy_system or not hasattr(self.copy_system, 'copy_manager'):
            await update.message.reply_text("❌ Copy Manager не инициализирован.")
            return

        args = context.args
        if not args:
            current_scale = getattr(self.copy_system.copy_manager, 'position_scaling', 1.0)
            await update.message.reply_text(
                f"Текущий множитель размера позиции: `{current_scale}`\n"
                f"Используйте: `/scale <множитель>` (например, `/scale 1.5`)",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            scale_factor = float(args[0])
            if not (0.1 <= scale_factor <= 10.0):
                await update.message.reply_text("❌ Множитель должен быть в диапазоне от 0.1 до 10.0")
                return

            self.copy_system.copy_manager.position_scaling = scale_factor
            await update.message.reply_text(f"✅ Новый множитель размера позиции установлен: `{scale_factor}`")
            await send_telegram_alert(f"⚠️ Множитель размера позиции изменен на `{scale_factor}` через /scale")

        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Укажите число (например, 1.5).")
        except Exception as e:
            logger.error(f"Scale command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка установки множителя: {e}")

    async def panic_close_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command for emergency closing of all positions on the main account."""
        sys_logger.log_telegram_command("/panic_close", update.effective_user.id)
        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return

        if not self.copy_system or not hasattr(self.copy_system, 'base_monitor') or not hasattr(self.copy_system.base_monitor, 'main_client'):
            await update.message.reply_text("❌ Основной API клиент не найден.")
            return

        try:
            keyboard = [[InlineKeyboardButton("🚨 ДА, ЗАКРЫТЬ ВСЕ ПОЗИЦИИ", callback_data="panic_close_confirm")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "⚠️ *ВНИМАНИЕ!* Вы уверены, что хотите экстренно закрыть *ВСЕ* позиции на основном аккаунте по рынку?\n\n"
                "Это действие необратимо.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Panic Close command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Критическая ошибка при паническом закрытии: {e}")

    async def panic_close_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Callback handler for /panic_close confirmation."""
        query = update.callback_query
        await query.answer()

        if query.data != "panic_close_confirm":
            return

        user_id = query.from_user.id
        if not self.check_authorization(user_id):
            await query.edit_message_text("❌ Доступ запрещен")
            return

        await query.edit_message_text("🚨 Закрываю все позиции...")

        if not self.copy_system or not hasattr(self.copy_system, 'base_monitor') or not hasattr(self.copy_system.base_monitor, 'main_client'):
            await query.edit_message_text("❌ Основной API клиент не найден.")
            return

        try:
            main_client = self.copy_system.base_monitor.main_client
            closed_count, errors_count = await main_client.close_all_positions_by_market()

            report = (
                f"🚨 **ЭКСТРЕННОЕ ЗАКРЫТИЕ ЗАВЕРШЕНО**\n"
                f"✅ Успешно закрыто: {closed_count}\n"
                f"❌ Ошибок: {errors_count}"
            )
            await query.edit_message_text(report, parse_mode=ParseMode.MARKDOWN)
            await send_telegram_alert(report)

        except Exception as e:
            logger.error(f"Panic Close callback error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ Критическая ошибка при закрытии позиций: {e}")

        # ================================
        # ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
        # ================================

    def _safe_float(self, value, default=0.0):
        """Безопасное преобразование в float"""
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    def _format_position_info_detailed(self, pos: dict) -> str:
        """
        РАСШИРЕННОЕ форматирование информации о позиции
        """
        try:
            symbol = pos.get('symbol', 'Unknown')
            side = pos.get('side', 'Unknown')
            size = self._safe_float(pos.get('size', 0))
            entry_price = self._safe_float(pos.get('avgPrice', 0))
            mark_price = self._safe_float(pos.get('markPrice', 0))
            unrealized_pnl = self._safe_float(pos.get('unrealisedPnl', 0))
            position_value = self._safe_float(pos.get('positionValue', 0))
    
            # НОВЫЕ технические детали
            leverage = self._safe_float(pos.get('leverage', 1))
            position_idx = pos.get('positionIdx', 0)
            margin_mode = pos.get('tradeMode', 'Unknown')  # Cross/Isolated
            auto_add_margin = pos.get('autoAddMargin', 0)
            position_status = pos.get('positionStatus', 'Normal')
    
            # Маржинальная информация
            position_margin = self._safe_float(pos.get('positionIM', 0))  # Initial Margin
            position_mm = self._safe_float(pos.get('positionMM', 0))      # Maintenance Margin
    
            # Ликвидация и стоп-лоссы
            liq_price = self._safe_float(pos.get('liqPrice', 0))
            stop_loss = self._safe_float(pos.get('stopLoss', 0))
            take_profit = self._safe_float(pos.get('takeProfit', 0))
            trailing_stop = self._safe_float(pos.get('trailingStop', 0))
    
            # Времення информация
            created_time = pos.get('createdTime', '')
            updated_time = pos.get('updatedTime', '')
    
            # Расчеты
            roe = (unrealized_pnl / position_margin * 100) if position_margin > 0 else 0
            margin_ratio = (position_mm / position_value * 100) if position_value > 0 else 0
    
            side_emoji = "📈" if side == "Buy" else "📉"
            pnl_emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
    
            # Форматирование времени
            created_str = ""
            if created_time:
                try:
                    created_dt = datetime.fromtimestamp(int(created_time) / 1000)
                    created_str = created_dt.strftime('%H:%M:%S')
                except Exception:
                    created_str = "N/A"
    
            info = (
                f"{side_emoji} {symbol} - {side}\n"
                f"   💰 Размер: {size:.6f} (${position_value:.2f})\n"
                f"   🎯 Вход: {entry_price:.6f} | Текущая: {mark_price:.6f}\n"
                f"   {pnl_emoji} P&L: {unrealized_pnl:+.2f} USDT ({roe:+.2f}%)\n"
                f"   ⚡ Плечо: {leverage}x | Режим: {margin_mode}\n"
                f"   💎 Маржа: {position_margin:.2f} USDT | MM: {position_mm:.2f} USDT\n"
            )
    
            # Добавляем информацию о стоп-лоссе/тейк-профите если есть
            if stop_loss > 0 or take_profit > 0:
                info += f"   🛡️ SL: {stop_loss:.6f} | TP: {take_profit:.6f}\n"
    
            # Добавляем цену ликвидации
            if liq_price > 0:
                info += f"   ⚠️ Ликвидация: {liq_price:.6f}\n"
    
            # Время создания
            if created_str:
                info += f"   ⏰ Открыта: {created_str}\n"
    
            return info.rstrip()
    
        except Exception as e:
            logger.error(f"Position formatting error: {e}")
            logger.error(traceback.format_exc())
            return f"Ошибка форматирования: {e}"

    async def get_account_summary(self, client) -> dict:
        """
        Получить сводку по аккаунту
        """
        try:
            balance = await client.get_balance() if hasattr(client, 'get_balance') else 0.0
            positions = await client.get_positions() if hasattr(client, 'get_positions') else []
        
            # Рассчитываем общий P&L
            total_unrealized_pnl = 0.0
            total_position_value = 0.0
        
            # Фильтруем только активные позиции (size > 0)
            active_positions = []
            for pos in positions:
                size = self._safe_float(pos.get('size', 0))
                if size > 0:
                    active_positions.append(pos)
                    unrealized_pnl = self._safe_float(pos.get('unrealisedPnl', 0))
                    position_value = self._safe_float(pos.get('positionValue', 0))
                    total_unrealized_pnl += unrealized_pnl
                    total_position_value += position_value
        
            return {
                'balance': balance,
                'positions_count': len(active_positions),
                'total_unrealized_pnl': total_unrealized_pnl,
                'total_position_value': total_position_value,
                'positions': active_positions
            }
        except Exception as e:
            logger.error(f"Error getting account summary: {e}")
            logger.error(traceback.format_exc())
            return {
                'balance': 0.0,
                'positions_count': 0,
                'total_unrealized_pnl': 0.0,
                'total_position_value': 0.0,
                'positions': []
            }
        
    async def force_copy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /force_copy с ИСПРАВЛЕННЫМ вызовом Kelly Calculator
        """
        sys_logger.log_telegram_command("/force_copy", update.effective_user.id)

        user_id = update.effective_user.id

        try:
            # Проверка демо режима
            demo_mode = getattr(self.copy_system, 'demo_mode', True)

            await update.message.reply_text(
                f"🔄 Сканирую позиции на источнике..."
                f"\n{'🟡 DEMO MODE' if demo_mode else '🔴 LIVE MODE'}",
                parse_mode=ParseMode.MARKDOWN
            )

            # Получаем позиции источника
            source_positions = await self.copy_system.base_monitor.source_client.get_positions()

            if not source_positions or source_positions[0].get('size', 0) == 0:
                await update.message.reply_text("❌ Нет активных позиций на источнике")
                return

            # Баланс основного аккаунта
            main_balance = await self.copy_system.base_monitor.main_client.get_balance()

            # Анализируем позиции
            full_report = "📊 **АНАЛИЗ ПОЗИЦИЙ ДЛЯ КОПИРОВАНИЯ**\n"
            full_report += "=" * 40 + "\n\n"

            positions_to_copy = []
            total_positions = 0

            for position_data in source_positions:
                size = safe_float(position_data.get('size', 0))
                if size <= 0:
                    continue

                total_positions += 1
                symbol = position_data['symbol']
                side = position_data['side']
                avg_price = safe_float(position_data.get('avgPrice', 0))
                price = safe_float(position_data.get('markPrice', avg_price))
                unrealized_pnl = safe_float(position_data.get('unrealisedPnl', 0))

                # ===== ИСПРАВЛЕННЫЙ ВЫЗОВ KELLY CALCULATOR =====
                kelly_calculation = self.copy_system.kelly_calculator.calculate_kelly_fraction(
                    symbol=symbol,
                    current_balance=main_balance
                )

                # Преобразуем результат в нужный формат
                if kelly_calculation:
                    kelly_result = {
                        'recommended_size': kelly_calculation.recommended_size / price if price > 0 else size,
                        'confidence': kelly_calculation.confidence_score,
                        'win_rate': kelly_calculation.win_rate,
                        'profit_factor': kelly_calculation.profit_factor,
                        'kelly_fraction': kelly_calculation.kelly_fraction
                    }
                else:
                    # Fallback значения если Kelly расчет не удался
                    kelly_result = {
                        'recommended_size': size,  # Копируем как есть
                        'confidence': 0.5,
                        'win_rate': 0.5,
                        'profit_factor': 1.0,
                        'kelly_fraction': 0.02  # 2% по умолчанию
                    }

                # Рыночные условия
                market_conditions = await self.copy_system.copy_manager.order_manager.get_market_analysis(symbol)

                # Trailing Stop
                trailing_stop = self.copy_system.copy_manager.trailing_manager.create_trailing_stop(
                    symbol=symbol,
                    side=side,
                    current_price=price,
                    position_size=kelly_result['recommended_size'] * price,
                    market_conditions=market_conditions
                )

                # РАСШИРЕННЫЙ отчет по позиции
                full_report += f"**{total_positions}. {symbol} - {side}**\n"
                full_report += f"📈 **Источник:**\n"
                full_report += f"  • Размер: {size:.6f} ({size * price:.2f} USDT)\n"
                full_report += f"  • Цена входа: ${avg_price:.2f}\n"
                full_report += f"  • Текущая цена: ${price:.2f}\n"
                full_report += f"  • P&L: ${unrealized_pnl:.2f} ({unrealized_pnl/avg_price/size*100:.1f}%)\n"
                full_report += f"\n💡 **Kelly Criterion:**\n"
                full_report += f"  • Рекомендуемый размер: {kelly_result['recommended_size']:.6f}\n"
                full_report += f"  • Уверенность: {kelly_result['confidence']:.1%}\n"
                full_report += f"  • Win Rate: {kelly_result.get('win_rate', 0.5):.1%}\n"
                full_report += f"  • Profit Factor: {kelly_result.get('profit_factor', 1.0):.2f}\n"
                full_report += f"\n🛡️ **Risk Management:**\n"
                full_report += f"  • Trailing Stop: ${trailing_stop.stop_price:.2f}\n"
                full_report += f"  • Дистанция: {trailing_stop.distance_percent:.2%}\n"
                full_report += f"  • Стиль: {trailing_stop.trail_style.value}\n"
                full_report += f"  • ATR: ${trailing_stop.atr_value:.2f}\n"
                full_report += f"\n📊 **Рыночные условия:**\n"
                full_report += f"  • Волатильность: {market_conditions.volatility:.2%}\n"
                full_report += f"  • Спред: {market_conditions.spread_percent:.3%}\n"
                full_report += f"  • Ликвидность: {market_conditions.liquidity_score:.1f}/10\n"
                full_report += f"  • Тренд: {'↑' if market_conditions.trend_strength > 0 else '↓'} "
                full_report += f"{abs(market_conditions.trend_strength):.1%}\n"
                full_report += "\n" + "-" * 30 + "\n\n"

                # Сохраняем для копирования
                positions_to_copy.append({
                    'symbol': symbol,
                    'side': side,
                    'original_size': size,
                    'kelly_size': kelly_result['recommended_size'],
                    'price': price,
                    'trailing_stop': trailing_stop,
                    'market_conditions': market_conditions,
                    'kelly_data': kelly_result
                })

            # Проверка рисков
            risk_check = await self.copy_system.drawdown_controller.check_risk_limits()

            full_report += "⚠️ **RISK CHECK:**\n"
            full_report += f"• Статус: {'✅ OK' if risk_check['can_open_position'] else '❌ BLOCKED'}\n"
            full_report += f"• Причина: {risk_check['reason']}\n"
            full_report += f"• Текущий Drawdown: {risk_check.get('current_drawdown', 0):.1%}\n"
            full_report += f"• Max Drawdown: {risk_check.get('max_allowed', 20):.1%}\n\n"

            # Итоговая статистика
            total_value = sum(p['kelly_size'] * p['price'] for p in positions_to_copy)
            full_report += "💰 **ИТОГО:**\n"
            full_report += f"• Позиций найдено: {total_positions}\n"
            full_report += f"• К копированию: {len(positions_to_copy)}\n"
            full_report += f"• Общая стоимость: ${total_value:.2f}\n"
            full_report += f"• % от баланса: {(total_value/main_balance*100):.1f}%\n"
            full_report += f"• Доступный баланс: ${main_balance:.2f}\n"

            # Отправляем отчет
            await update.message.reply_text(full_report, parse_mode=ParseMode.MARKDOWN)

            # Кнопки подтверждения
            keyboard = [
                [
                    InlineKeyboardButton("✅ Копировать ВСЕ", callback_data=f"force_copy_all_{user_id}"),
                    InlineKeyboardButton("❌ Отмена", callback_data=f"force_copy_cancel_{user_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Сохраняем позиции
            if not hasattr(self, 'pending_positions'):
                self.pending_positions = {}
            self.pending_positions[user_id] = positions_to_copy

            confirmation_text = (
                f"**Подтвердите копирование {len(positions_to_copy)} позиций**\n"
                f"Общая стоимость: ${total_value:.2f}\n"
                f"Kelly Criterion и Trailing Stops будут применены автоматически"
            )

            await update.message.reply_text(
                confirmation_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Force copy error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")


    # ==========================================
    # 3. ОБНОВЛЕННЫЙ force_copy_callback
    # ==========================================

    async def force_copy_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик подтверждения с детальным отчетом результатов
        """
        query = update.callback_query
        await query.answer()

        data_parts = query.data.split('_')
        if len(data_parts) < 4:
            await query.edit_message_text("❌ Неверный формат команды")
            return

        action = '_'.join(data_parts[:3])
        user_id = int(data_parts[-1])

        if query.from_user.id != user_id:
            await query.answer("❌ Это не ваш запрос", show_alert=True)
            return

        if action == "force_copy_cancel":
            await query.edit_message_text("❌ Копирование отменено")
            self.pending_positions.pop(user_id, None)
            return

        if action == "force_copy_all":
            positions = self.pending_positions.get(user_id)

            if not positions:
                await query.edit_message_text("❌ Нет позиций для копирования")
                return

            await query.edit_message_text("🔄 Начинаю копирование позиций...")

            results = []
            successful_copies = 0
            failed_copies = 0

            for pos in positions:
                try:
                    from enhanced_trading_system_final_fixed import TradingSignal, SignalType

                    signal = TradingSignal(
                        signal_type=SignalType.POSITION_OPEN,
                        symbol=pos['symbol'],
                        side=pos['side'],
                        size=pos['kelly_size'],
                        price=pos['price'],
                        timestamp=time.time(),
                        metadata={
                            'force_copy': True,
                            'trailing_stop_price': pos['trailing_stop'].stop_price,
                            'trailing_stop_distance': pos['trailing_stop'].distance_percent,
                            'kelly_confidence': pos['kelly_data']['confidence'],
                            'market_volatility': pos['market_conditions'].volatility
                        },
                        priority=2
                    )

                    # Обрабатываем сигнал
                    result = None
                    if hasattr(self.copy_system, 'process_copy_signal'):
                        result = await self.copy_system.process_copy_signal(signal)
                    elif hasattr(self.copy_system, '_handle_position_open_for_copy'):
                        result = await self.copy_system._handle_position_open_for_copy(signal)
                    else:
                        logger.error(f"No suitable method found to process signal for {pos['symbol']}")
                        raise Exception("Copy system doesn't have required methods")

                    # Нормализуем формат результата
                    if not isinstance(result, dict):
                        result = {"success": True}

                    # == ДОБАВЛЕНО: сохраняем в БД позицию, открываемую force_copy ==
                    if result.get("success", True):
                        await self._save_force_copy_position(signal, result)

                    results.append({
                        'symbol': pos['symbol'],
                        'status': 'success' if result.get("success", True) else 'failed',
                        'size': pos['kelly_size'],
                        'trailing_stop': pos['trailing_stop'].stop_price
                    })
                    if result.get("success", True):
                        successful_copies += 1
                    else:
                        failed_copies += 1

                except Exception as e:
                    logger.error(f"Error copying {pos['symbol']}: {e}")
                    results.append({
                        'symbol': pos['symbol'],
                        'status': 'failed',
                        'error': str(e)
                    })
                    failed_copies += 1

            # Детальный отчет о результатах
            final_report = "📋 **РЕЗУЛЬТАТЫ КОПИРОВАНИЯ:**\n"
            final_report += "=" * 40 + "\n\n"

            for i, result in enumerate(results, 1):
                if result['status'] == 'success':
                    final_report += f"✅ **{i}. {result['symbol']}**\n"
                    final_report += f"  • Размер: {result['size']:.6f}\n"
                    final_report += f"  • Trailing Stop: ${result['trailing_stop']:.2f}\n"
                else:
                    final_report += f"❌ **{i}. {result['symbol']}**\n"
                    final_report += f"  • Ошибка: {result.get('error', 'unknown')}\n"
                final_report += "\n"

            final_report += "-" * 30 + "\n"
            final_report += f"**ИТОГО:**\n"
            final_report += f"• Успешно: {successful_copies}\n"
            final_report += f"• Ошибок: {failed_copies}\n"
            final_report += f"• Режим: {'DEMO' if getattr(self.copy_system, 'demo_mode', True) else 'LIVE'}\n"

            await query.message.reply_text(final_report, parse_mode=ParseMode.MARKDOWN)

            # Очищаем pending позиции
            self.pending_positions.pop(user_id, None)

    # ==========================================
    # Вспомогательный метод (из патча): сохраняет позицию force_copy в БД
    # ==========================================
    async def _save_force_copy_position(self, signal, result: dict):
        """Сохранение позиции от force_copy в БД"""
        try:
            # Импорт writer
            try:
                from app.positions_db_writer import positions_writer
            except ImportError:
                from positions_db_writer import positions_writer

            # Определяем account_id
            try:
                from config import TARGET_ACCOUNT_ID as _TARGET_ACCOUNT_ID
            except Exception:
                _TARGET_ACCOUNT_ID = getattr(self.copy_system, 'target_account_id', 1)

            # Подготавливаем данные позиции (timestamp в мс, так ожидает normalizer)
            position_data = {
                "symbol": signal.symbol,
                "side": signal.side,
                "size": signal.size,
                "entryPrice": signal.price,
                "markPrice": signal.price,
                "leverage": 10,              # можно заменить на значение из конфига
                "unrealisedPnl": 0,
                "positionIdx": 0,            # One-way mode
                "marginMode": "cross",
                "timestamp": int(time.time() * 1000),
                "positionId": result.get("exchange_position_id") if isinstance(result, dict) else None
            }

            # Сохраняем в БД
            await positions_writer.upsert_position(position_data, _TARGET_ACCOUNT_ID)

            logger.info(f"Force copy position saved to DB: {signal.symbol}")

        except Exception as e:
            logger.error(f"Failed to save force copy position: {e}")

    # ================================
    # ФУНКЦИИ ОТОБРАЖЕНИЯ ДАННЫХ
    # ================================
    
    async def show_system_status_text(self, update):
        """ИСПРАВЛЕННАЯ версия показа статуса для текстовых сообщений"""
        try:
            if not self.copy_system:
                message = (
                    "📊 *СТАТУС СИСТЕМЫ ЭТАПА 2*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система копирования не инициализирована\n"
                    "⚠️ Требуется запуск системы"
                )
            else:
                try:
                    # Безопасное получение статуса
                    system_active = getattr(self.copy_system, 'system_active', False)
                    copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
                
                    # Безопасное получение uptime
                    start_time = getattr(self.copy_system, 'start_time', time.time())
                    if hasattr(self.copy_system, 'system_stats'):
                        start_time = self.copy_system.system_stats.get('start_time', start_time)
                    uptime_hours = (time.time() - start_time) / 3600
                    
                    # Получаем статистику компонентов
                    components_status = []
                    
                    # Базовый монитор
                    if hasattr(self.copy_system, 'base_monitor'):
                        components_status.append("✅ Base Monitor")
                    else:
                        components_status.append("❌ Base Monitor")
                    
                    # Kelly Calculator
                    if hasattr(self.copy_system, 'kelly_calculator'):
                        components_status.append("✅ Kelly Calculator")
                    else:
                        components_status.append("❌ Kelly Calculator")
                    
                    # Copy Manager
                    if hasattr(self.copy_system, 'copy_manager'):
                        components_status.append("✅ Copy Manager")
                    else:
                        components_status.append("❌ Copy Manager")
                    
                    # Drawdown Controller
                    if hasattr(self.copy_system, 'drawdown_controller'):
                        emergency_active = False
                        try:
                            emergency_active = getattr(self.copy_system.drawdown_controller, 'emergency_stop_active', False)
                        except:
                            pass
                        components_status.append(f"✅ Drawdown Controller ({'🔴 E-Stop' if emergency_active else '🟢 OK'})")
                    else:
                        components_status.append("❌ Drawdown Controller")
                
                    message = (
                        "📊 *СТАТУС СИСТЕМЫ ЭТАПА 2*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔄 Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                        f"📋 Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n"
                        f"⏰ Время работы: {uptime_hours:.1f}ч\n\n"
                        f"⚙️ *Компоненты системы:*\n"
                        f"   {components_status[0]}\n"
                        f"   {components_status[1]}\n"
                        f"   {components_status[2]}\n"
                        f"   {components_status[3]}\n\n"
                        f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                    )
                
                except Exception as e:
                    logger.error(f"Status data error: {e}")
                    logger.error(traceback.format_exc())
                    message = (
                        "📊 *СТАТУС СИСТЕМЫ ЭТАПА 2*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "⚠️ Система инициализирована, но есть ошибки сбора данных\n"
                        f"Ошибка: {str(e)[:100]}"
                    )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"System status text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка получения статуса: {e}")

    async def show_balances_text(self, update):
        """Показать балансы для текстовых сообщений"""
        try:
            if not self.copy_system or not hasattr(self.copy_system, 'base_monitor'):
                await update.message.reply_text(
                    "💰 *БАЛАНСЫ АККАУНТОВ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        
            try:
                source_balance = 0.0
                main_balance = 0.0
            
                try:
                    source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                except Exception as e:
                    logger.warning(f"Cannot get source balance: {e}")
                
                try:
                    main_balance = await self.copy_system.base_monitor.main_client.get_balance()
                except Exception as e:
                    logger.warning(f"Cannot get main balance: {e}")
                
                balance_ratio = (main_balance / source_balance * 100) if source_balance > 0 else 0
                total_balance = source_balance + main_balance
            
                message = (
                    "💰 *БАЛАНСЫ АККАУНТОВ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🎯 **Источник (копируем):** ${source_balance:.2f}\n"
                    f"🏠 **Основной (копии):** ${main_balance:.2f}\n"
                    f"📊 **Соотношение:** {balance_ratio:.1f}%\n"
                    f"💹 **Общий капитал:** ${total_balance:.2f}\n\n"
                    f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                )
            
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
            except Exception as e:
                logger.error(f"Balance display error: {e}")
                logger.error(traceback.format_exc())
                await update.message.reply_text(f"❌ Ошибка получения балансов: {str(e)[:100]}")
            
        except Exception as e:
            logger.error(f"Balances text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Критическая ошибка балансов: {e}")

    async def show_positions_text(self, update):
        """
        ПРОФЕССИОНАЛЬНАЯ ПЕРЕДЕЛКА функции show_positions_text
        Использует ТОЧНО ТУ ЖЕ логику что и в успешном тестере
        """
        try:
            # Проверяем инициализацию системы
            if not self.copy_system or not hasattr(self.copy_system, 'base_monitor'):
                await update.message.reply_text(
                    "📈 *АКТИВНЫЕ ПОЗИЦИИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        
            # Получение данных с обоих аккаунтов
            source_summary = await self.get_account_summary(self.copy_system.base_monitor.source_client)
            main_summary = await self.get_account_summary(self.copy_system.base_monitor.main_client)
        
            # Время работы
            uptime_seconds = 0
            if hasattr(self.copy_system, 'system_stats') and 'start_time' in self.copy_system.system_stats:
                uptime_seconds = time.time() - self.copy_system.system_stats['start_time']
            elif hasattr(self.copy_system, 'start_time'):
                uptime_seconds = time.time() - self.copy_system.start_time
        
            uptime_hours = int(uptime_seconds / 3600)
            uptime_minutes = int((uptime_seconds % 3600) / 60)
        
            # Формируем ПОЛНЫЙ ОТЧЕТ
            report = "*📊 ПОЛНЫЙ ОТЧЕТ СИСТЕМЫ*\n"
            report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
            # ИСТОЧНИК (тестируемый аккаунт)
            report += "*🎯 АККАУНТ-ИСТОЧНИК:*\n"
            report += f"💰 Баланс: {source_summary.get('balance', 0):.2f} USDT\n"
            report += f"📈 Позиций: {source_summary.get('positions_count', 0)}\n"
            report += f"💎 P&L: {source_summary.get('total_unrealized_pnl', 0):+.2f} USDT\n"
        
            if source_summary.get('positions'):
                report += "\n*📊 АКТИВНЫЕ ПОЗИЦИИ ИСТОЧНИКА:*\n"
                for pos in source_summary['positions']:
                    pos_info = self._format_position_info_detailed(pos)
                    # Экранируем для Markdown
                    pos_info_escaped = pos_info.replace("_", "\\_").replace("*", "\\*")
                    report += pos_info_escaped + "\n\n"
        
            # ОСНОВНОЙ АККАУНТ
            report += "*🏠 ОСНОВНОЙ АККАУНТ:*\n"
            report += f"💰 Баланс: {main_summary.get('balance', 0):.2f} USDT\n"
            report += f"📈 Позиций: {main_summary.get('positions_count', 0)}\n"
            report += f"💎 P&L: {main_summary.get('total_unrealized_pnl', 0):+.2f} USDT\n"
        
            if main_summary.get('positions'):
                report += "\n*📊 АКТИВНЫЕ ПОЗИЦИИ ОСНОВНОГО:*\n"
                for pos in main_summary['positions']:
                    pos_info = self._format_position_info_detailed(pos)
                    # Экранируем для Markdown
                    pos_info_escaped = pos_info.replace("_", "\\_").replace("*", "\\*")
                    report += pos_info_escaped + "\n\n"
        
            # СТАТИСТИКА КОПИРОВАНИЯ
            if hasattr(self.copy_system, 'system_stats'):
                stats = self.copy_system.system_stats
                report += "*🔄 СТАТИСТИКА КОПИРОВАНИЯ:*\n"
                report += f"✅ Скопировано сделок: {stats.get('successful_copies', 0)}\n"
                report += f"❌ Ошибок копирования: {stats.get('failed_copies', 0)}\n"
                report += f"💰 Общий объем: {stats.get('total_volume', 0):.2f} USDT\n"
            
                if hasattr(self.copy_system, 'copy_ratio'):
                    report += f"📊 Коэффициент: {self.copy_system.copy_ratio}x\n"
            
                if hasattr(self.copy_system, 'copy_enabled'):
                    status = 'Включено' if self.copy_system.copy_enabled else 'Выключено'
                    report += f"🔄 Копирование: {status}\n"
        
            # ОБЩАЯ ИНФОРМАЦИЯ
            report += f"\n⏰ Время работы: {uptime_hours}ч {uptime_minutes}м\n"
            report += f"🕐 Время отчета: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
            # Отправляем отчет
            await update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Positions text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Критическая ошибка позиций: {e}")

    # ==========================================
    # 4. НОВЫЙ МЕТОД ДЛЯ ОТОБРАЖЕНИЯ ПОЗИЦИЙ
    # ==========================================

    async def show_positions_detailed(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показать детальную информацию о позициях основного аккаунта
        """
        try:
            # Получаем позиции
            positions = await self.copy_system.base_monitor.main_client.get_positions()
        
            if not positions or all(p.get('size', 0) == 0 for p in positions):
                await update.message.reply_text("📭 Нет активных позиций на основном аккаунте")
                return
        
            report = "📊 **ПОЗИЦИИ ОСНОВНОГО АККАУНТА**\n"
            report += "=" * 40 + "\n\n"
        
            total_value = 0
            total_pnl = 0
        
            for i, pos in enumerate(positions, 1):
                size = safe_float(pos.get('size', 0))
                if size <= 0:
                    continue
            
                symbol = pos['symbol']
                side = pos['side']
                avg_price = safe_float(pos.get('avgPrice', 0))
                mark_price = safe_float(pos.get('markPrice', avg_price))
                unrealized_pnl = safe_float(pos.get('unrealisedPnl', 0))
                position_value = size * mark_price
            
                report += f"**{i}. {symbol} - {side}**\n"
                report += f"📈 **Позиция:**\n"
                report += f"  • Размер: {size:.6f}\n"
                report += f"  • Стоимость: ${position_value:.2f}\n"
                report += f"  • Цена входа: ${avg_price:.2f}\n"
                report += f"  • Текущая цена: ${mark_price:.2f}\n"
                report += f"  • P&L: ${unrealized_pnl:.2f} ({unrealized_pnl/position_value*100:.1f}%)\n"
            
                # Информация о trailing stop если есть
                if hasattr(self.copy_system.copy_manager.trailing_manager, 'active_stops'):
                    stop = self.copy_system.copy_manager.trailing_manager.active_stops.get(symbol)
                    if stop:
                        report += f"\n🛡️ **Trailing Stop:**\n"
                        report += f"  • Цена стопа: ${stop.stop_price:.2f}\n"
                        report += f"  • Дистанция: {stop.distance_percent:.2%}\n"
                        report += f"  • Стиль: {stop.trail_style.value}\n"
                        report += f"  • До стопа: ${abs(mark_price - stop.stop_price):.2f} "
                        report += f"({abs(mark_price - stop.stop_price)/mark_price:.2%})\n"

            
                # Kelly информация если доступна
                if hasattr(self.copy_system.copy_manager, 'position_kelly_data'):
                    kelly_data = self.copy_system.copy_manager.position_kelly_data.get(symbol)
                    if kelly_data:
                        report += f"\n💡 **Kelly Data:**\n"
                        report += f"  • Kelly размер: {kelly_data.get('recommended_size', 0):.6f}\n"
                        report += f"  • Уверенность: {kelly_data.get('confidence', 0):.1%}\n"
                        report += f"  • Win Rate: {kelly_data.get('win_rate', 0):.1%}\n"
            
                report += "\n" + "-" * 30 + "\n\n"
            
                total_value += position_value
                total_pnl += unrealized_pnl
        
            # Общая статистика
            balance = await self.copy_system.base_monitor.main_client.get_balance()
        
            report += "💰 **ИТОГО:**\n"
            report += f"• Позиций: {len([p for p in positions if safe_float(p.get('size', 0)) > 0])}\n"
            report += f"• Общая стоимость: ${total_value:.2f}\n"
            report += f"• Общий P&L: ${total_pnl:.2f} ({total_pnl/total_value*100:.1f}%)\n"
            report += f"• Баланс: ${balance:.2f}\n"
            report += f"• Использовано: {total_value/balance*100:.1f}%\n"
        
            await update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Show positions error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")

    async def show_risk_management_text(self, update):
        """Показать управление рисками для текстовых сообщений"""
        try:
            # 1) Система инициализирована?
            if not getattr(self, "copy_system", None):
                await update.message.reply_text(
                    "⚠️ *УПРАВЛЕНИЕ РИСКАМИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return

            # 2) Контроллер просадки
            drawdown_controller_available = hasattr(self.copy_system, "drawdown_controller")
            drawdown_info = "Недоступно"
            emergency_active = False

            if drawdown_controller_available:
                try:
                    controller = self.copy_system.drawdown_controller

                    # настройки
                    daily_limit = getattr(controller, "daily_drawdown_limit", 0.05)
                    total_limit = getattr(controller, "max_drawdown_threshold", 0.15)
                    emergency_threshold = getattr(controller, "emergency_stop_threshold", 0.08)

                    # состояние
                    current_drawdown = 0
                    peak_balance = 0
                    if hasattr(controller, "get_risk_stats"):
                        risk_stats = controller.get_risk_stats()
                        current_drawdown = risk_stats.get("current_drawdown", 0) * 100  # %
                        peak_balance = risk_stats.get("peak_balance", 0)
                        emergency_active = risk_stats.get("emergency_stop_active", False)

                    drawdown_info = (
                        f"Текущая просадка: {current_drawdown:.2f}%\n"
                        f"Пиковый баланс: ${peak_balance:.2f}\n"
                        f"Дневной лимит: {daily_limit*100:.1f}%\n"
                        f"Общий лимит: {total_limit*100:.1f}%\n"
                        f"Emergency порог: {emergency_threshold*100:.1f}%"
                    )
                except Exception as e:
                    logger.error(f"Drawdown controller info error: {e}")
                    drawdown_info = f"Ошибка получения данных: {str(e)[:50]}..."

            # 3) Kelly calculator
            kelly_available = hasattr(self.copy_system, "kelly_calculator")
            kelly_info = "Недоступно"

            if kelly_available:
                try:
                    kelly_calc = self.copy_system.kelly_calculator
                    confidence = getattr(kelly_calc, "confidence_threshold", 0.65) * 100
                    max_fraction = getattr(kelly_calc, "max_kelly_fraction", 0.25) * 100
                    conservative = getattr(kelly_calc, "conservative_factor", 0.5) * 100

                    kelly_info = (
                        f"Confidence: {confidence:.0f}%\n"
                        f"Max Fraction: {max_fraction:.0f}%\n"
                        f"Conservative: {conservative:.0f}%"
                    )
                except Exception as e:
                    logger.error(f"Kelly calculator info error: {e}")
                    kelly_info = f"Ошибка получения данных: {str(e)[:50]}..."

            # 4) Trailing manager
            trailing_available = False
            trailing_count = 0
            if (
                hasattr(self.copy_system, "copy_manager")
                and hasattr(self.copy_system.copy_manager, "trailing_manager")
            ):
                trailing_available = True
                try:
                    trailing_count = len(
                        self.copy_system.copy_manager.trailing_manager.get_all_stops()
                    )
                except Exception:
                    pass

            # 5) Безопасная подготовка строк (НЕ кладём .replace() внутрь f-строки)
            drawdown_info_safe = drawdown_info.replace("\n", "\n   ")
            kelly_info_safe = kelly_info.replace("\n", "\n   ")

            # 6) Сообщение
            message = (
                "⚠️ *УПРАВЛЕНИЕ РИСКАМИ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📉 **Контроль просадки:** {'✅ Активен' if drawdown_controller_available else '❌ Недоступен'}\n"
                f"🚨 **Emergency Stop:** {'🔴 АКТИВЕН' if emergency_active else '🟢 Неактивен'}\n\n"
                "📉 **ПРОСАДКА:**\n"
                f"   {drawdown_info_safe}\n\n"
                "🎯 **KELLY CRITERION:**\n"
                f"   {kelly_info_safe}\n\n"
                "🛡️ **TRAILING STOPS:**\n"
                f"   {'✅ Активны' if trailing_available else '❌ Недоступны'}\n"
                f"   Активных trailing: {trailing_count}\n\n"
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Risk management text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка управления рисками: {e}")


    async def show_copying_controls_text(self, update):
        """Показать управление копированием для текстовых сообщений"""
        try:
            if not self.copy_system:
                await update.message.reply_text(
                    "🔄 *УПРАВЛЕНИЕ КОПИРОВАНИЕМ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        
            system_active = getattr(self.copy_system, 'system_active', False)
            copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
        
            # Получаем статистику копирования
            stats = getattr(self.copy_system, 'system_stats', {})
            successful_copies = stats.get('successful_copies', 0) or 0  # Защита от None
            failed_copies = stats.get('failed_copies', 0) or 0  # Защита от None
            total_signals = stats.get('total_signals_processed', 0) or 0  # Защита от None
        
            # Защита от деления на ноль
            success_rate = 0.0
            if total_signals > 0:
                success_rate = (successful_copies / total_signals * 100)
        
            # Получаем активные позиции
            positions_count = 0
            if hasattr(self.copy_system, 'base_monitor'):
                try:
                    positions = await self.copy_system.base_monitor.main_client.get_positions()
                    positions_count = len([p for p in positions if self._safe_float(p.get('size', 0)) > 0])
                except Exception as e:
                    logger.warning(f"Failed to get positions count: {e}")
        
            # Получаем режим копирования
            copy_mode = "DEFAULT"
            max_positions = 10
        
            if hasattr(self.copy_system, 'copy_manager'):
                copy_mode = getattr(self.copy_system.copy_manager, 'copy_mode', "DEFAULT")
                max_positions = getattr(self.copy_system.copy_manager, 'max_positions', 10)
    
            message = (
                "🔄 *УПРАВЛЕНИЕ КОПИРОВАНИЕМ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Система:** {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                f"📋 **Копирование:** {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
            
                f"⚙️ **НАСТРОЙКИ:**\n"
                f"   Режим копирования: {copy_mode}\n"
                f"   Макс. позиций: {max_positions}\n\n"
            
                f"📊 **СТАТИСТИКА:**\n"
                f"   Позиций скопировано: {successful_copies}\n"
                f"   Позиций закрыто: {stats.get('closed_positions', 0) or 0}\n"
                f"   Активных позиций: {positions_count}\n"
                f"   Успешность: {success_rate:.1f}%\n\n"
            
                f"⚙️ **СИСТЕМА:**\n"
                f"   Процессор: {'🟢 Активен' if system_active else '🔴 Остановлен'}\n"
                f"   Очередь: {'Работает' if copy_enabled else 'Остановлена'}\n\n"
            
                "🔧 **КОМАНДЫ:**\n"
                "`/copy start` - включить копирование\n"
                "`/copy stop` - остановить копирование\n"
                "`/copy mode DEFAULT` - сменить режим\n"
                "`/emergency` - экстренная остановка"
            )
    
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    
        except Exception as e:
            logger.error(f"Copying controls text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка управления копированием: {e}")

    async def show_full_report_text(self, update):
        """Показать полный отчет для текстовых сообщений"""
        try:
            if not self.copy_system:
                await update.message.reply_text(
                    "📋 *ПОЛНЫЙ ОТЧЕТ СИСТЕМЫ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Время работы
            start_time = getattr(self.copy_system, 'start_time', time.time())
            if hasattr(self.copy_system, 'system_stats'):
                start_time = self.copy_system.system_stats.get('start_time', start_time)
            
            uptime_hours = (time.time() - start_time) / 3600
            
            # Получаем балансы
            source_balance = 0.0
            main_balance = 0.0
            
            if hasattr(self.copy_system, 'base_monitor'):
                try:
                    source_balance = await self.copy_system.base_monitor.source_client.get_balance()
                except Exception as e:
                    logger.warning(f"Cannot get source balance: {e}")
                
                try:
                    main_balance = await self.copy_system.base_monitor.main_client.get_balance()
                except Exception as e:
                    logger.warning(f"Cannot get main balance: {e}")
            
            # Получаем статистику копирования
            stats = getattr(self.copy_system, 'system_stats', {})
            successful_copies = stats.get('successful_copies', 0)
            failed_copies = stats.get('failed_copies', 0)
            total_signals = stats.get('total_signals_processed', 0)
            
            success_rate = (successful_copies / total_signals * 100) if total_signals > 0 else 0.0
            
            # Проверяем интеграцию компонентов
            system_active = getattr(self.copy_system, 'system_active', False)
            copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
            
            websocket_connected = False
            if hasattr(self.copy_system, 'base_monitor') and hasattr(self.copy_system.base_monitor, 'websocket_manager'):
                ws_manager = self.copy_system.base_monitor.websocket_manager
                websocket_connected = hasattr(ws_manager, 'ws') and ws_manager.ws
            
            # Получаем состояние рисков
            drawdown_controller_available = hasattr(self.copy_system, 'drawdown_controller')
            emergency_active = False
            current_drawdown = 0.0
            
            if drawdown_controller_available:
                try:
                    controller = self.copy_system.drawdown_controller
                    if hasattr(controller, 'get_risk_stats'):
                        risk_stats = controller.get_risk_stats()
                        current_drawdown = risk_stats.get('current_drawdown', 0) * 100  # переводим в проценты
                        emergency_active = risk_stats.get('emergency_stop_active', False)
                except Exception as e:
                    logger.warning(f"Failed to get risk stats: {e}")
            
            message = (
                "📋 *ПОЛНЫЙ ОТЧЕТ СИСТЕМЫ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⏰ Время работы: {uptime_hours:.1f}ч\n"
                f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                
                "💰 **БАЛАНСЫ:**\n"
                f"   🎯 Источник: ${source_balance:.2f}\n"
                f"   🏠 Основной: ${main_balance:.2f}\n"
                f"   💹 Всего: ${source_balance + main_balance:.2f}\n\n"
                
                "📊 **СТАТИСТИКА КОПИРОВАНИЯ:**\n"
                f"   Обработано сигналов: {total_signals}\n"
                f"   Успешных копий: {successful_copies}\n"
                f"   Ошибок копирования: {failed_copies}\n"
                f"   Успешность: {success_rate:.1f}%\n\n"
                
                "🔄 **ИНТЕГРАЦИЯ:**\n"
                f"   Этап 1 → Этап 2: {'✅ Активна' if hasattr(self.copy_system, 'process_copy_signal') else '❌ Ошибка'}\n"
                f"   WebSocket: {'🟢 Подключен' if websocket_connected else '🔴 Отключен'}\n"
                "   Telegram Bot: 🟢 Активен\n\n"
                
                "🚨 **РИСКИ:**\n"
                f"   Текущая просадка: {current_drawdown:.2f}%\n"
                f"   Emergency Stop: {'🔴 АКТИВЕН' if emergency_active else '🟢 Неактивен'}\n\n"
                
                "⚙️ **СТАТУС СИСТЕМЫ:**\n"
                f"   Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                f"   Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Full report text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка генерации отчета: {e}")

    async def show_kelly_stats_text(self, update):
        """Показать Kelly статистику для текстовых сообщений"""
        try:
            if not self.copy_system or not hasattr(self.copy_system, 'kelly_calculator'):
                await update.message.reply_text(
                    "🎯 *KELLY CRITERION СТАТИСТИКА*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Kelly Calculator не инициализирован",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            kelly_calc = self.copy_system.kelly_calculator
            
            # Получаем текущие настройки
            confidence_threshold = getattr(kelly_calc, 'confidence_threshold', 0.65) * 100
            max_kelly_fraction = getattr(kelly_calc, 'max_kelly_fraction', 0.25) * 100
            conservative_factor = getattr(kelly_calc, 'conservative_factor', 0.5) * 100
            lookback_period = getattr(kelly_calc, 'lookback_period', 30)
            
            # Получаем статистику торговли
            win_probability = 0.0
            profit_loss_ratio = 0.0
            optimal_fraction = 0.0
            
            try:
                if hasattr(kelly_calc, 'get_kelly_stats'):
                    kelly_stats = kelly_calc.get_kelly_stats()
                    win_probability = kelly_stats.get('win_probability', 0.0) * 100
                    profit_loss_ratio = kelly_stats.get('profit_loss_ratio', 0.0)
                    optimal_fraction = kelly_stats.get('optimal_fraction', 0.0) * 100
            except Exception as e:
                logger.warning(f"Failed to get Kelly stats: {e}")
            
            message = (
                "🎯 *KELLY CRITERION СТАТИСТИКА*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 **Калькулятор:** ✅ Активен\n"
                f"🔄 **Оптимизация размеров:** {'Включена' if getattr(kelly_calc, 'enabled', True) else 'Отключена'}\n\n"
                
                "🎯 **ТЕКУЩИЕ ПАРАМЕТРЫ:**\n"
                f"   Порог уверенности: {confidence_threshold:.0f}%\n"
                f"   Макс. доля Kelly: {max_kelly_fraction:.0f}%\n"
                f"   Консервативный фактор: {conservative_factor:.0f}%\n"
                f"   Период анализа: {lookback_period} дней\n\n"
                
                "📈 **СТАТИСТИКА ТОРГОВЛИ:**\n"
                f"   Вероятность выигрыша: {win_probability:.1f}%\n"
                f"   Соотношение выигрыш/проигрыш: {profit_loss_ratio:.2f}\n"
                f"   Оптимальный размер: {optimal_fraction:.2f}%\n\n"
                
                "🔧 **КОМАНДЫ:**\n"
                "`/set_kelly confidence 70` - порог уверенности\n"
                "`/set_kelly max_fraction 20` - максимальная доля\n"
                "`/set_kelly conservative 50` - консервативный фактор\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Kelly stats text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка Kelly статистики: {e}")

    async def show_trailing_stops_text(self, update):
        """Показать trailing stops для текстовых сообщений"""
        try:
            trailing_manager_available = False
            trailing_stops = []
            
            if self.copy_system and hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'trailing_manager'):
                trailing_manager_available = True
                
                try:
                    trailing_manager = self.copy_system.copy_manager.trailing_manager
                    trailing_stops = trailing_manager.get_all_stops()
                    
                    # Получаем настройки
                    initial_distance = getattr(trailing_manager, 'default_distance_percent', 0.02) * 100
                    min_step = getattr(trailing_manager, 'min_trail_step', 0.005) * 100
                    max_distance = getattr(trailing_manager, 'max_distance_percent', 0.05) * 100
                    aggressive_mode = getattr(trailing_manager, 'aggressive_mode', False)
                    
                except Exception as e:
                    logger.warning(f"Failed to get trailing stops: {e}")
            
            if not trailing_manager_available:
                await update.message.reply_text(
                    "🛡️ *TRAILING STOP-LOSS*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Trailing Manager недоступен",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Форматируем информацию о trailing stops
            stops_info = ""
            if trailing_stops:
                for i, stop in enumerate(trailing_stops[:5]):  # Показываем только первые 5
                    symbol = stop.get('symbol', 'Unknown')
                    side = stop.get('side', 'Unknown')
                    entry_price = self._safe_float(stop.get('entry_price', 0))
                    current_price = self._safe_float(stop.get('current_price', 0))
                    trailing_price = self._safe_float(stop.get('trailing_price', 0))
                    distance_percent = self._safe_float(stop.get('distance_percent', 0)) * 100
                    
                    stops_info += (
                        f"{i+1}. {symbol} ({side})\n"
                        f"   Вход: {entry_price:.6f} | Текущая: {current_price:.6f}\n"
                        f"   Trailing Price: {trailing_price:.6f} | Дистанция: {distance_percent:.2f}%\n\n"
                    )
                
                if len(trailing_stops) > 5:
                    stops_info += f"...и еще {len(trailing_stops) - 5} trailing stops\n\n"
            else:
                stops_info = "   Нет активных trailing stops\n\n"
            
            message = (
                "🛡️ *TRAILING STOP-LOSS*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Система:** ✅ Активна\n"
                f"📈 **Активных trailing:** {len(trailing_stops)}\n\n"
                
                "⚙️ **НАСТРОЙКИ:**\n"
                f"   Начальный trailing: {initial_distance:.2f}%\n"
                f"   Минимальный шаг: {min_step:.2f}%\n"
                f"   Максимальный размер: {max_distance:.2f}%\n"
                f"   Режим: {'Агрессивный' if aggressive_mode else 'Консервативный'}\n\n"
                
                "📋 **АКТИВНЫЕ TRAILING STOPS:**\n"
                f"{stops_info}"
                
                "🔧 **КОМАНДЫ:**\n"
                "`/set_trailing distance 2.5` - начальная дистанция\n"
                "`/set_trailing step 0.3` - минимальный шаг\n"
                "`/set_trailing aggressive on` - агрессивный режим\n"
                "`/set_trailing clear_all` - очистить все trailing stops\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Trailing stops text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка trailing stops: {e}")

    async def show_settings_text(self, update):
        """Показать настройки для текстовых сообщений"""
        try:
            if not self.copy_system:
                await update.message.reply_text(
                    "⚙️ *НАСТРОЙКИ СИСТЕМЫ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return

            # Режим копирования
            copy_mode = "DEFAULT"
            if hasattr(self.copy_system, "copy_manager"):
                copy_mode = getattr(self.copy_system.copy_manager, "copy_mode", "DEFAULT")

            # KELLY
            kelly_enabled = hasattr(self.copy_system, "kelly_calculator")
            if kelly_enabled:
                try:
                    kelly_calc = self.copy_system.kelly_calculator
                    confidence = getattr(kelly_calc, "confidence_threshold", 0.65) * 100
                    max_fraction = getattr(kelly_calc, "max_kelly_fraction", 0.25) * 100
                    conservative = getattr(kelly_calc, "conservative_factor", 0.5) * 100
                    kelly_settings = (
                        f"Порог уверенности: {confidence:.0f}%\n"
                        f"Макс. доля Kelly: {max_fraction:.0f}%\n"
                        f"Консервативный фактор: {conservative:.0f}%"
                    )
                except Exception as e:
                    logger.error(f"Kelly settings read error: {e}")
                    kelly_settings = f"Ошибка получения настроек Kelly: {str(e)[:50]}..."
            else:
                kelly_settings = "Kelly Calculator не инициализирован"

            # TRAILING
            trailing_enabled = False
            if hasattr(self.copy_system, "copy_manager") and hasattr(self.copy_system.copy_manager, "trailing_manager"):
                trailing_enabled = True
                try:
                    tm = self.copy_system.copy_manager.trailing_manager
                    initial_distance = getattr(tm, "default_distance_percent", 0.02) * 100
                    min_step = getattr(tm, "min_trail_step", 0.005) * 100
                    max_distance = getattr(tm, "max_distance_percent", 0.05) * 100
                    aggressive_mode = getattr(tm, "aggressive_mode", False)
                    trailing_settings = (
                        f"Начальная дистанция: {initial_distance:.2f}%\n"
                        f"Минимальный шаг: {min_step:.2f}%\n"
                        f"Максимальная дистанция: {max_distance:.2f}%\n"
                        f"Режим: {'Агрессивный' if aggressive_mode else 'Консервативный'}"
                    )
                except Exception as e:
                    logger.error(f"Trailing settings read error: {e}")
                    trailing_settings = f"Ошибка получения настроек Trailing: {str(e)[:50]}..."
            else:
                trailing_settings = "Trailing Manager не инициализирован"

            # DRAWDOWN
            drawdown_enabled = hasattr(self.copy_system, "drawdown_controller")
            if drawdown_enabled:
                try:
                    controller = self.copy_system.drawdown_controller
                    daily_limit = getattr(controller, "daily_drawdown_limit", 0.05) * 100
                    total_limit = getattr(controller, "max_drawdown_threshold", 0.15) * 100
                    emergency_threshold = getattr(controller, "emergency_stop_threshold", 0.08) * 100
                    drawdown_settings = (
                        f"Дневной лимит: {daily_limit:.1f}%\n"
                        f"Общий лимит: {total_limit:.1f}%\n"
                        f"Emergency порог: {emergency_threshold:.1f}%"
                    )
                except Exception as e:
                    logger.error(f"Drawdown settings read error: {e}")
                    drawdown_settings = f"Ошибка получения настроек просадки: {str(e)[:50]}..."
            else:
                drawdown_settings = "Drawdown Controller не инициализирован"

            # Форматируем блоки заранее (никаких бэкслэшей в f-строках)
            kelly_block     = kelly_settings.replace("\n", "\n   ")
            trailing_block  = trailing_settings.replace("\n", "\n   ")
            drawdown_block  = drawdown_settings.replace("\n", "\n   ")

            message = (
                "⚙️ *НАСТРОЙКИ СИСТЕМЫ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Режим копирования:** {copy_mode}\n"
                f"🎯 **Kelly Criterion:** {'✅ Включен' if kelly_enabled else '❌ Выключен'}\n"
                f"🛡️ **Trailing Stops:** {'✅ Включен' if trailing_enabled else '❌ Выключен'}\n"
                f"📉 **Контроль просадки:** {'✅ Включен' if drawdown_enabled else '❌ Выключен'}\n\n"
                "🎯 **KELLY SETTINGS:**\n"
                f"   {kelly_block}\n\n"
                "🛡️ **TRAILING SETTINGS:**\n"
                f"   {trailing_block}\n\n"
                "📉 **DRAWDOWN SETTINGS:**\n"
                f"   {drawdown_block}\n\n"
                "🔧 **КОМАНДЫ НАСТРОЙКИ:**\n"
                "`/set_kelly` - настройка Kelly\n"
                "`/set_trailing` - настройка Trailing\n"
                "`/set_drawdown` - настройка просадки\n\n"
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Settings text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка получения настроек: {e}")


    async def show_emergency_controls_text(self, update):
        """Показать экстренное управление для текстовых сообщений"""
        try:
            if not self.copy_system:
                await update.message.reply_text(
                    "🚨 *ЭКСТРЕННОЕ УПРАВЛЕНИЕ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            system_active = getattr(self.copy_system, 'system_active', False)
            copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
            
            # Проверяем Emergency Stop
            emergency_active = False
            if hasattr(self.copy_system, 'drawdown_controller'):
                try:
                    emergency_active = getattr(self.copy_system.drawdown_controller, 'emergency_stop_active', False)
                except Exception as e:
                    logger.warning(f"Failed to get emergency status: {e}")
            
            # Получаем количество активных позиций
            positions_count = 0
            if hasattr(self.copy_system, 'base_monitor'):
                try:
                    positions = await self.copy_system.base_monitor.main_client.get_positions()
                    positions_count = len([p for p in positions if self._safe_float(p.get('size', 0)) > 0])
                except Exception as e:
                    logger.warning(f"Failed to get positions count: {e}")
        
            message = (
                "🚨 *ЭКСТРЕННОЕ УПРАВЛЕНИЕ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ **ВНИМАНИЕ:** Эти действия могут повлиять на торговлю!\n\n"
                
                f"📊 **ТЕКУЩИЙ СТАТУС:**\n"
                f"   Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                f"   Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n"
                f"   Emergency Stop: {'🔴 АКТИВЕН' if emergency_active else '🟢 Неактивен'}\n"
                f"   Активных позиций: {positions_count}\n\n"
                
                "🛑 **ДОСТУПНЫЕ ДЕЙСТВИЯ:**\n"
                "   • `/emergency stop` - остановить копирование\n"
                "   • `/emergency close_all` - закрыть все позиции\n"
                "   • `/emergency restart` - перезапуск системы\n\n"
                
                "⚠️ **ВАЖНО:**\n"
                "   • Остановка копирования не закрывает позиции\n"
                "   • Закрытие позиций происходит по рыночной цене\n"
                "   • Перезапуск может занять некоторое время\n\n"
                
                f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Emergency controls text error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка экстренного управления: {e}")

    async def show_performance_text(self, update):
        """🆕 Показать детальный отчет производительности"""
        try:
            if not self.copy_system:
                message = (
                    "📊 *ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована"
                )
            else:
                # Получаем статистику
                stats = getattr(self.copy_system, 'system_stats', {})
                start_time = stats.get('start_time', time.time())
                uptime_hours = (time.time() - start_time) / 3600
                
                total_signals = stats.get('total_signals_processed', 0)
                successful = stats.get('successful_copies', 0)
                failed = stats.get('failed_copies', 0)
                success_rate = (successful / total_signals * 100) if total_signals > 0 else 0
                
                # Получаем текущий баланс
                current_balance = 0.0
                try:
                    if hasattr(self.copy_system, 'base_monitor'):
                        current_balance = await self.copy_system.base_monitor.main_client.get_balance()
                except Exception as e:
                    logger.warning(f"Failed to get balance: {e}")
                
                # Получаем данные о производительности
                avg_latency = stats.get('average_latency_ms', 0) / 1000  # переводим в секунды
                max_latency = stats.get('max_latency_ms', 0) / 1000
                
                message = (
                    "📊 *ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ **ВРЕМЯ РАБОТЫ:**\n"
                    f"   Общее время: {uptime_hours:.1f} часов\n"
                    f"   Запущена: {datetime.fromtimestamp(start_time).strftime('%d.%m %H:%M')}\n\n"
                    
                    f"📈 **СТАТИСТИКА КОПИРОВАНИЯ:**\n"
                    f"   Обработано сигналов: {total_signals}\n"
                    f"   Успешных копий: {successful}\n"
                    f"   Неудачных копий: {failed}\n"
                    f"   Успешность: {success_rate:.1f}%\n\n"
                    
                    f"⚡ **ЗАДЕРЖКИ:**\n"
                    f"   Средняя задержка: {avg_latency:.3f}с\n"
                    f"   Максимальная задержка: {max_latency:.3f}с\n"
                    f"   Пропущено сигналов: {stats.get('missed_signals', 0)}\n\n"
                    
                    f"💰 **ФИНАНСОВЫЕ ПОКАЗАТЕЛИ:**\n"
                    f"   Текущий баланс: ${current_balance:.2f}\n"
                    f"   Общий объем: ${stats.get('total_volume', 0):.2f}\n"
                    f"   Emergency остановок: {stats.get('emergency_stops', 0)}\n\n"
                    
                    f"🎯 **КАЧЕСТВО РАБОТЫ:**\n"
                    f"   Отклик системы: {'✅ Отличный' if success_rate > 95 else '✅ Хороший' if success_rate > 80 else '⚠️ Требует внимания'}\n"
                    f"   Стабильность: {'✅ Высокая' if failed < 5 else '⚠️ Средняя'}\n\n"
                    
                    f"⏰ Создан: {datetime.now().strftime('%H:%M:%S')}"
                )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Performance report error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка отчета производительности: {e}")

    async def show_risks_analysis_text(self, update):
        """🆕 Показать анализ рисков с рекомендациями"""
        try:
            if not self.copy_system:
                message = (
                    "⚠️ *АНАЛИЗ РИСКОВ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована"
                )
            else:
                # Получаем текущий баланс
                current_balance = 0.0
                try:
                    if hasattr(self.copy_system, 'base_monitor'):
                        current_balance = await self.copy_system.base_monitor.main_client.get_balance()
                except Exception as e:
                    logger.warning(f"Failed to get balance: {e}")
            
                # Анализ рисков
                current_drawdown = 0.0
                peak_balance = current_balance
                emergency_active = False
            
                if hasattr(self.copy_system, 'drawdown_controller'):
                    try:
                        if hasattr(self.copy_system.drawdown_controller, 'get_risk_stats'):
                            risk_stats = self.copy_system.drawdown_controller.get_risk_stats()
                            peak_balance = risk_stats.get('peak_balance', current_balance)
                            current_drawdown = ((peak_balance - current_balance) / peak_balance * 100) if peak_balance > 0 else 0
                            emergency_active = risk_stats.get('emergency_stop_active', False)
                    except Exception as e:
                        logger.warning(f"Failed to get risk stats: {e}")
            
                # Определение уровня риска
                if current_drawdown < 3:
                    risk_level = "🟢 НИЗКИЙ"
                    recommendation = "Система работает в нормальном режиме"
                elif current_drawdown < 5:
                    risk_level = "🟡 СРЕДНИЙ"
                    recommendation = "Рекомендуется повышенное внимание"
                elif current_drawdown < 8:
                    risk_level = "🟠 ВЫСОКИЙ"
                    recommendation = "Рассмотрите снижение размеров позиций"
                else:
                    risk_level = "🔴 КРИТИЧЕСКИЙ"
                    recommendation = "Немедленно проверьте систему"
            
                # Получаем настройки управления рисками
                daily_limit = 5.0
                total_limit = 15.0
                emergency_threshold = 10.0
            
                if hasattr(self.copy_system, 'drawdown_controller'):
                    controller = self.copy_system.drawdown_controller
                    daily_limit = getattr(controller, 'daily_drawdown_limit', 0.05) * 100
                    total_limit = getattr(controller, 'max_drawdown_threshold', 0.15) * 100
                    emergency_threshold = getattr(controller, 'emergency_stop_threshold', 0.1) * 100
            
                # Получаем данные о позициях для анализа рисков
                positions_count = 0
                max_position_size = 0.0
                total_exposure = 0.0
            
                try:
                    if hasattr(self.copy_system, 'base_monitor'):
                        positions = await self.copy_system.base_monitor.main_client.get_positions()
                        active_positions = [p for p in positions if self._safe_float(p.get('size', 0)) > 0]
                        positions_count = len(active_positions)
                    
                        for pos in active_positions:
                            position_value = self._safe_float(pos.get('positionValue', 0))
                            max_position_size = max(max_position_size, position_value)
                            total_exposure += position_value
                except Exception as e:
                    logger.warning(f"Failed to get positions: {e}")
            
                # Рассчитываем риск-метрики
                position_risk = (max_position_size / current_balance * 100) if current_balance > 0 else 0
                exposure_risk = (total_exposure / current_balance * 100) if current_balance > 0 else 0
            
                message = (
                    "⚠️ *АНАЛИЗ РИСКОВ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 **ТЕКУЩИЕ РИСКИ:**\n"
                    f"   Просадка: {current_drawdown:.2f}%\n"
                    f"   Пиковый баланс: ${peak_balance:.2f}\n"
                    f"   Текущий баланс: ${current_balance:.2f}\n"
                    f"   Emergency Stop: {'🔴 Активен' if emergency_active else '🟢 Неактивен'}\n\n"
                
                    f"🎯 **УРОВЕНЬ РИСКА:** {risk_level}\n\n"
                
                    f"💡 **РЕКОМЕНДАЦИЯ:**\n"
                    f"   {recommendation}\n\n"
                
                    f"📈 **МЕТРИКИ ПОЗИЦИЙ:**\n"
                    f"   Количество позиций: {positions_count}\n"
                    f"   Макс. размер позиции: ${max_position_size:.2f} ({position_risk:.1f}%)\n"
                    f"   Общая экспозиция: ${total_exposure:.2f} ({exposure_risk:.1f}%)\n\n"
                
                    f"📋 **ЛИМИТЫ:**\n"
                    f"   Дневной лимит: {daily_limit:.1f}%\n"
                    f"   Общий лимит: {total_limit:.1f}%\n"
                    f"   Emergency порог: {emergency_threshold:.1f}%\n\n"
                
                    f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                )
        
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        
        except Exception as e:
            logger.error(f"Risk analysis error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка анализа рисков: {e}")

    async def show_daily_report_text(self, update):
        """🆕 Показать дневной отчет"""
        try:
            today = datetime.now().strftime('%d.%m.%Y')
        
            if not self.copy_system:
                message = (
                    f"📅 *ДНЕВНОЙ ОТЧЕТ ({today})*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована"
                )
            else:
                # Получаем статистику дня
                stats = getattr(self.copy_system, 'system_stats', {})
            
                # Получаем данные о торговле за день
                daily_signals = stats.get('daily_signals', 0)
                daily_success = stats.get('daily_successful_copies', 0)
                daily_failed = stats.get('daily_failed_copies', 0)
            
                # Если данные по дням недоступны, используем общие данные
                if daily_signals == 0:
                    daily_signals = stats.get('total_signals_processed', 0)
                    daily_success = stats.get('successful_copies', 0)
                    daily_failed = stats.get('failed_copies', 0)
            
                # Получаем прибыль/убыток за день
                daily_pnl = 0.0
                daily_pnl_percent = 0.0
                current_balance = 0.0
            
                try:
                    if hasattr(self.copy_system, 'base_monitor'):
                        current_balance = await self.copy_system.base_monitor.main_client.get_balance()
                    
                        # Пытаемся получить дневную прибыль
                        if hasattr(self.copy_system, 'drawdown_controller'):
                            controller = self.copy_system.drawdown_controller
                            if hasattr(controller, 'get_daily_pnl'):
                                daily_pnl = controller.get_daily_pnl()
                                # Защита от деления на ноль
                                base_capital = current_balance - daily_pnl
                                if base_capital > 0:
                                    daily_pnl_percent = (daily_pnl / base_capital * 100)
                                else:
                                    daily_pnl_percent = 0
                except Exception as e:
                    logger.warning(f"Failed to get daily P&L: {e}")
            
                # Получаем топ сделок дня
                top_trades = []
                try:
                    if hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'get_top_trades'):
                        top_trades = self.copy_system.copy_manager.get_top_trades(limit=3)
                
                    # Если функция недоступна, создаем пример данных
                    if not top_trades:
                        # Получаем все активные позиции и сортируем по P&L
                        if hasattr(self.copy_system, 'base_monitor'):
                            positions = await self.copy_system.base_monitor.main_client.get_positions()
                            active_positions = [p for p in positions if self._safe_float(p.get('size', 0)) > 0]
                        
                            # Сортируем по P&L (от большего к меньшему)
                            active_positions.sort(key=lambda p: self._safe_float(p.get('unrealisedPnl', 0)), reverse=True)
                        
                            # Берем топ-3 позиции
                            for pos in active_positions[:3]:
                                symbol = pos.get('symbol', 'Unknown')
                                pnl = self._safe_float(pos.get('unrealisedPnl', 0))
                            
                                # Пытаемся получить время открытия
                                hold_time = "?"
                                created_time = pos.get('createdTime', 0)
                                if created_time:
                                    try:
                                        created_dt = datetime.fromtimestamp(int(created_time) / 1000)
                                        hold_seconds = (datetime.now() - created_dt).total_seconds()
                                    
                                        if hold_seconds < 3600:
                                            hold_time = f"{int(hold_seconds / 60)}m"
                                        else:
                                            hold_time = f"{hold_seconds / 3600:.1f}h"
                                    except:
                                        hold_time = "?"
                            
                                top_trades.append((symbol, pnl, hold_time))
                except Exception as e:
                    logger.warning(f"Failed to get top trades: {e}")
            
                # Форматируем топ сделки
                top_trades_text = ""
                if top_trades:
                    for i, (symbol, pnl, hold_time) in enumerate(top_trades):
                        top_trades_text += f"   {i+1}. {symbol}: ${pnl:+.2f} ({hold_time})\n"
                else:
                    top_trades_text = "   Нет данных о сделках\n"
            
                # Получаем данные о задержках
                avg_latency = stats.get('average_latency_ms', 0) / 1000  # переводим в секунды
                max_latency = stats.get('max_latency_ms', 0) / 1000
            
                # Получаем данные о рисках
                max_drawdown = 0.0
                emergency_activations = 0
                trailing_activations = 0
            
                if hasattr(self.copy_system, 'drawdown_controller'):
                    controller = self.copy_system.drawdown_controller
                    if hasattr(controller, 'get_daily_max_drawdown'):
                        max_drawdown = controller.get_daily_max_drawdown() * 100  # переводим в проценты
                    emergency_activations = stats.get('emergency_stops', 0)
            
                if hasattr(self.copy_system, 'copy_manager') and hasattr(self.copy_system.copy_manager, 'trailing_manager'):
                    trailing_activations = stats.get('trailing_activations', 0)
            
                # Рассчитываем процент успешных копий (с защитой от деления на ноль)
                success_percent_text = "0"
                if daily_signals > 0:
                    success_percent = (daily_success / daily_signals * 100)
                    success_percent_text = f"{success_percent:.0f}"
            
                message = (
                    f"📅 *ДНЕВНОЙ ОТЧЕТ ({today})*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 **P&L сегодня:** {'+' if daily_pnl >= 0 else ''}{daily_pnl:.2f} USDT ({'+' if daily_pnl_percent >= 0 else ''}{daily_pnl_percent:.1f}%)\n"
                    f"📊 **Количество сигналов:** {daily_signals}\n"
                )
            
                # Добавляем строку об успешных копиях с защитой от деления на ноль
                if daily_signals > 0:
                    message += f"✅ **Успешных копий:** {daily_success} ({success_percent_text}% при {daily_signals} сигналах)\n\n"
                else:
                    message += f"✅ **Успешных копий:** {daily_success} (нет сигналов)\n\n"
            
                message += (
                    f"🎯 **ЛУЧШИЕ ПОЗИЦИИ:**\n"
                    f"{top_trades_text}\n"
                
                    f"📈 **КОПИРОВАНИЕ:**\n"
                    f"   Обработано сигналов: {daily_signals}\n"
                    f"   Успешных копий: {daily_success}\n"
                    f"   Средняя задержка: {avg_latency:.1f}s\n"
                    f"   Максимальная задержка: {max_latency:.1f}s\n\n"
                
                    f"🛡️ **РИСКИ:**\n"
                    f"   Макс просадка дня: {max_drawdown:.1f}%\n"
                    f"   Emergency активаций: {emergency_activations}\n"
                    f"   Trailing срабатываний: {trailing_activations}\n\n"
                
                    f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                )
        
            # Безопасная отправка сообщения с обработкой ошибок форматирования
            try:
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            except Exception as md_error:
                if "Can't parse entities" in str(md_error):
                    # Если ошибка парсинга Markdown, отправляем без форматирования
                    logger.warning(f"Markdown formatting error: {md_error}. Sending without formatting.")
                    await update.message.reply_text(message, parse_mode=None)
                else:
                    # Если другая ошибка, пробрасываем дальше
                    raise
        
        except Exception as e:
            logger.error(f"Daily report error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка дневного отчета: {e}")
    
    async def show_health_check_text(self, update):
        """🆕 Показать диагностику системы"""
        try:
            health_results = []
            
            # Проверка системы копирования
            if self.copy_system:
                health_results.append("✅ Copy System: Инициализирован")
                
                # Проверка базового монитора
                if hasattr(self.copy_system, 'base_monitor'):
                    health_results.append("✅ Base Monitor: Активен")
                    
                    # Тест API подключения
                    try:
                        balance = await self.copy_system.base_monitor.main_client.get_balance()
                        health_results.append(f"✅ API Connection: Работает (${balance:.2f})")
                    except Exception as e:
                        health_results.append(f"❌ API Connection: Ошибка ({str(e)[:30]})")
                else:
                    health_results.append("❌ Base Monitor: Не найден")
                
                # Проверка Kelly Calculator
                if hasattr(self.copy_system, 'kelly_calculator'):
                    health_results.append("✅ Kelly Calculator: Активен")
                else:
                    health_results.append("❌ Kelly Calculator: Не найден")
                
                # Проверка Copy Manager
                if hasattr(self.copy_system, 'copy_manager'):
                    health_results.append("✅ Copy Manager: Активен")
                    
                    # Проверка Trailing Manager
                    if hasattr(self.copy_system.copy_manager, 'trailing_manager'):
                        health_results.append("✅ Trailing Manager: Активен")
                    else:
                        health_results.append("❌ Trailing Manager: Не найден")
                else:
                    health_results.append("❌ Copy Manager: Не найден")
                
                # Проверка Drawdown Controller
                if hasattr(self.copy_system, 'drawdown_controller'):
                    health_results.append("✅ Drawdown Controller: Активен")
                else:
                    health_results.append("❌ Drawdown Controller: Не найден")
                
                # Проверка WebSocket
                if hasattr(self.copy_system, 'base_monitor') and hasattr(self.copy_system.base_monitor, 'websocket_manager'):
                    ws_manager = self.copy_system.base_monitor.websocket_manager
                    if hasattr(ws_manager, 'ws') and ws_manager.ws:
                        health_results.append("✅ WebSocket: Подключен")
                    else:
                        health_results.append("❌ WebSocket: Не подключен")
                else:
                    health_results.append("❌ WebSocket: Не найден")
                
                # Проверка системных статистик
                if hasattr(self.copy_system, 'system_stats'):
                    health_results.append("✅ System Stats: Активны")
                else:
                    health_results.append("❌ System Stats: Не найдены")
                
                # Проверка handler функций
                if hasattr(self.copy_system, 'process_copy_signal'):
                    health_results.append("✅ Signal Handler: Активен")
                else:
                    health_results.append("❌ Signal Handler: Не найден")
                    
            else:
                health_results.append("❌ Copy System: Не инициализирован")
            
            # Проверка Telegram Bot
            health_results.append(f"✅ Telegram Bot: Активен ({len(self.authorized_users)} пользователей)")
            
            # Проверка файловой системы
            try:
                test_file = "test_write.tmp"
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                health_results.append("✅ File System: Доступен")
            except Exception as e:
                health_results.append(f"❌ File System: Ошибка ({str(e)[:30]})")
            
            # Общий статус
            error_count = len([r for r in health_results if r.startswith("❌")])
            warning_count = len([r for r in health_results if r.startswith("⚠️")])
            
            if error_count == 0 and warning_count == 0:
                overall_status = "🟢 ОТЛИЧНО"
            elif error_count == 0:
                overall_status = "🟡 ХОРОШО"
            elif error_count <= 2:
                overall_status = "🟠 ЕСТЬ ПРОБЛЕМЫ"
            else:
                overall_status = "🔴 КРИТИЧЕСКИЕ ПРОБЛЕМЫ"
            
            message = (
                "🔧 *ДИАГНОСТИКА СИСТЕМЫ*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **ОБЩИЙ СТАТУС:** {overall_status}\n\n"
                
                f"📋 **РЕЗУЛЬТАТЫ ПРОВЕРКИ:**\n"
                f"{chr(10).join(['   ' + result for result in health_results])}\n\n"
                
                f"📊 **СТАТИСТИКА:**\n"
                f"   Ошибок: {error_count}\n"
                f"   Предупреждений: {warning_count}\n"
                f"   Успешных: {len(health_results) - error_count - warning_count}\n\n"
                
                f"⏰ Проверено: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка диагностики: {e}")
    
    async def show_sync_status_text(self, update):
        """🆕 Показать статус синхронизации"""
        try:
            if not self.copy_system:
                message = (
                    "🔄 *СТАТУС СИНХРОНИЗАЦИИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Система не инициализирована"
                )
            else:
                # Получаем статистику синхронизации
                stats = getattr(self.copy_system, 'system_stats', {})
                total_signals = stats.get('total_signals_processed', 0)
                successful = stats.get('successful_copies', 0)
                failed = stats.get('failed_copies', 0)
                
                success_rate = (successful / total_signals * 100) if total_signals > 0 else 0
                
                # Получаем данные о задержках
                avg_latency = stats.get('average_latency_ms', 0) / 1000  # переводим в секунды
                max_latency = stats.get('max_latency_ms', 0) / 1000
                
                # Проверяем статус WebSocket
                websocket_connected = False
                if hasattr(self.copy_system, 'base_monitor') and hasattr(self.copy_system.base_monitor, 'websocket_manager'):
                    ws_manager = self.copy_system.base_monitor.websocket_manager
                    websocket_connected = hasattr(ws_manager, 'ws') and ws_manager.ws
                
                # Получаем статус системы
                system_active = getattr(self.copy_system, 'system_active', False)
                copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
                
                # Получаем последние синхронизированные сделки
                last_syncs = []
                
                try:
                    if hasattr(self.copy_system, 'system_stats') and 'recent_copies' in stats:
                        recent_copies = stats['recent_copies']
                        for copy_info in recent_copies[:3]:  # Берем последние 3
                            time_str = copy_info.get('timestamp', '')
                            if isinstance(time_str, (int, float)):
                                try:
                                    time_str = datetime.fromtimestamp(time_str).strftime('%H:%M:%S')
                                except:
                                    time_str = str(time_str)
                            
                            symbol = copy_info.get('symbol', 'Unknown')
                            action = copy_info.get('action', 'Unknown')
                            size = copy_info.get('size', 0)
                            status = '✅' if copy_info.get('success', True) else '❌'
                            
                            action_str = f"{action} {'+' if size > 0 else ''}{size}" if action != 'Close' else 'Close'
                            last_syncs.append((time_str, symbol, action_str, status))
                except Exception as e:
                    logger.warning(f"Failed to get recent copies: {e}")
                
                # Если нет данных о последних копиях, создаем примерные данные
                if not last_syncs:
                    current_time = datetime.now()
                    last_syncs = [
                        (current_time.strftime('%H:%M:%S'), "Unknown", "No data", "⚠️")
                    ]
                
                message = (
                    "🔄 *СТАТУС СИНХРОНИЗАЦИИ*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚡ **ПРОИЗВОДИТЕЛЬНОСТЬ:**\n"
                    f"   Средняя задержка: {avg_latency:.3f}s\n"
                    f"   Максимальная задержка: {max_latency:.3f}s\n"
                    f"   Успешность: {success_rate:.1f}%\n"
                    f"   WebSocket: {'🟢 Подключен' if websocket_connected else '🔴 Отключен'}\n\n"
                    
                    f"📈 **ПОСЛЕДНИЕ СИНХРОНИЗАЦИИ:**\n"
                )
                
                for time_str, symbol, action, status in last_syncs:
                    message += f"   {time_str} {symbol} {action} {status}\n"
                
                message += (
                    f"\n📊 **СТАТИСТИКА:**\n"
                    f"   Обработано сигналов: {total_signals}\n"
                    f"   Успешных копий: {successful}\n"
                    f"   Ошибок копирования: {failed}\n"
                    f"   Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                    f"   Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
                    
                    f"❌ **ПРОБЛЕМЫ:**\n"
                    f"   {'Нет активных проблем' if success_rate > 95 and websocket_connected else 'Есть проблемы синхронизации'}\n\n"
                    
                    f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Sync status error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка статуса синхронизации: {e}")

    async def show_kelly_settings_text(self, update):
        """🆕 Показать настройки Kelly Criterion"""
        try:
            if not self.copy_system or not hasattr(self.copy_system, 'kelly_calculator'):
                message = (
                    "🎯 *НАСТРОЙКИ KELLY CRITERION*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "❌ Kelly Calculator не инициализирован\n\n"
                    "📊 **Команды настройки:**\n"
                    "`/set_kelly confidence 70` - установить уверенность 70%\n"
                    "`/set_kelly max_fraction 20` - максимальная доля 20%\n"
                    "`/set_kelly conservative 40` - консервативный фактор 40%"
                )
            else:
                kelly_calc = self.copy_system.kelly_calculator
                confidence = getattr(kelly_calc, 'confidence_threshold', 0.65) * 100
                max_fraction = getattr(kelly_calc, 'max_kelly_fraction', 0.25) * 100
                conservative = getattr(kelly_calc, 'conservative_factor', 0.5) * 100
                lookback = getattr(kelly_calc, 'lookback_period', 30)
                
                # Получаем дополнительные данные о Kelly расчетах
                win_probability = 0.0
                profit_loss_ratio = 0.0
                optimal_fraction = 0.0
                
                try:
                    if hasattr(kelly_calc, 'get_kelly_stats'):
                        kelly_stats = kelly_calc.get_kelly_stats()
                        win_probability = kelly_stats.get('win_probability', 0.0) * 100
                        profit_loss_ratio = kelly_stats.get('profit_loss_ratio', 0.0)
                        optimal_fraction = kelly_stats.get('optimal_fraction', 0.0) * 100
                except Exception as e:
                    logger.warning(f"Failed to get Kelly stats: {e}")
                
                message = (
                    "🎯 *НАСТРОЙКИ KELLY CRITERION*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 **ТЕКУЩИЕ ПАРАМЕТРЫ:**\n"
                    f"   Confidence Threshold: {confidence:.0f}%\n"
                    f"   Max Kelly Fraction: {max_fraction:.0f}%\n"
                    f"   Conservative Factor: {conservative:.0f}%\n"
                    f"   Lookback Period: {lookback} дней\n\n"
                    
                    f"📈 **ТЕКУЩИЕ РАСЧЕТЫ:**\n"
                    f"   Вероятность выигрыша: {win_probability:.1f}%\n"
                    f"   Соотношение выигрыш/проигрыш: {profit_loss_ratio:.2f}\n"
                    f"   Оптимальный размер: {optimal_fraction:.2f}%\n\n"
                    
                    "🔧 **КОМАНДЫ ИЗМЕНЕНИЯ:**\n"
                    "`/set_kelly confidence 70` - порог уверенности\n"
                    "`/set_kelly max_fraction 20` - максимальная доля\n"
                    "`/set_kelly conservative 40` - консервативный фактор\n"
                    "`/set_kelly lookback 30` - период анализа\n\n"
                    
                    "💡 **РЕКОМЕНДАЦИИ:**\n"
                    "   • Confidence: 60-70% (вероятность успеха)\n"
                    "   • Max Fraction: 20-25% (макс. размер позиции)\n"
                    "   • Conservative: 40-60% (уровень осторожности)\n"
                    "   • Lookback: 30-60 дней (период анализа)\n\n"
                    
                    f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
                )
            
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Kelly settings error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка настроек Kelly: {e}")
    
    # ================================
    # КОМАНДЫ УПРАВЛЕНИЯ КОПИРОВАНИЕМ
    # ================================
    
    async def copy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /copy - управление копированием"""
        sys_logger.log_telegram_command("/copy", update.effective_user.id)

        if not self.check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ Доступ запрещен")
            return
        
        try:
            if not self.copy_system:
                await update.message.reply_text("❌ Система не инициализирована")
                return
            
            args = context.args
            if not args:
                # Показываем текущий статус копирования
                system_active = getattr(self.copy_system, 'system_active', False)
                copy_enabled = getattr(self.copy_system, 'copy_enabled', False)
                
                message = (
                    "🔄 **УПРАВЛЕНИЕ КОПИРОВАНИЕМ**\n\n"
                    f"Система: {'🟢 Активна' if system_active else '🔴 Остановлена'}\n"
                    f"Копирование: {'✅ Включено' if copy_enabled else '❌ Выключено'}\n\n"
                    "Доступные команды:\n"
                    "`/copy start` - включить копирование\n"
                    "`/copy stop` - остановить копирование\n"
                    "`/copy restart` - перезапустить копирование\n"
                    "`/copy mode DEFAULT` - сменить режим копирования"
                )
                
                await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
                return
            
            command = args[0].lower()
            
            # Включение копирования
            if command == 'start':
                self.copy_system.system_active = True
                self.copy_system.copy_enabled = True
                
                if hasattr(self.copy_system, 'copy_manager'):
                    await self.copy_system.copy_manager.start_copying()
                
                await update.message.reply_text("✅ Копирование включено")
                await send_telegram_alert("🔄 Копирование было включено через бота")
            
            # Остановка копирования
            elif command == 'stop':
                self.copy_system.copy_enabled = False
                
                if hasattr(self.copy_system, 'copy_manager'):
                    await self.copy_system.copy_manager.stop_copying()
                
                await update.message.reply_text("❌ Копирование остановлено")
                await send_telegram_alert("🔄 Копирование было остановлено через бота")
            
            # Перезапуск копирования
            elif command == 'restart':
                # Сначала останавливаем
                self.copy_system.copy_enabled = False
                if hasattr(self.copy_system, 'copy_manager'):
                    await self.copy_system.copy_manager.stop_copying()
                
                await asyncio.sleep(1)  # Небольшая пауза
                
                # Затем запускаем снова
                self.copy_system.system_active = True
                self.copy_system.copy_enabled = True
                if hasattr(self.copy_system, 'copy_manager'):
                    await self.copy_system.copy_manager.start_copying()
                
                await update.message.reply_text("🔄 Копирование перезапущено")
                await send_telegram_alert("🔄 Копирование было перезапущено через бота")
            
            # Изменение режима копирования
            elif command == 'mode' and len(args) > 1:
                mode = args[1].upper()
                
                if not hasattr(self.copy_system, 'copy_manager'):
                    await update.message.reply_text("❌ Copy Manager недоступен")
                    return
                
                valid_modes = ['DEFAULT', 'AGGRESSIVE', 'CONSERVATIVE', 'CUSTOM']
                
                if mode not in valid_modes:
                    await update.message.reply_text(
                        f"❌ Неверный режим. Доступные режимы: {', '.join(valid_modes)}"
                    )
                    return
                
                # Устанавливаем режим копирования
                self.copy_system.copy_manager.copy_mode = mode
                
                # Дополнительные настройки в зависимости от режима
                if mode == 'AGGRESSIVE':
                    # Агрессивный режим: больше позиций, более высокие размеры
                    self.copy_system.copy_manager.max_positions = 15
                    self.copy_system.copy_manager.position_scaling = 1.5
                    
                    # Если есть Kelly Calculator, настраиваем его
                    if hasattr(self.copy_system, 'kelly_calculator'):
                        self.copy_system.kelly_calculator.max_kelly_fraction = 0.3
                        self.copy_system.kelly_calculator.conservative_factor = 0.4
                    
                elif mode == 'CONSERVATIVE':
                    # Консервативный режим: меньше позиций, меньшие размеры
                    self.copy_system.copy_manager.max_positions = 5
                    self.copy_system.copy_manager.position_scaling = 0.5
                    
                    # Если есть Kelly Calculator, настраиваем его
                    if hasattr(self.copy_system, 'kelly_calculator'):
                        self.copy_system.kelly_calculator.max_kelly_fraction = 0.15
                        self.copy_system.kelly_calculator.conservative_factor = 0.7
                    
                elif mode == 'DEFAULT':
                    # Стандартный режим: умеренные настройки
                    self.copy_system.copy_manager.max_positions = 10
                    self.copy_system.copy_manager.position_scaling = 1.0
                    
                    # Если есть Kelly Calculator, настраиваем его
                    if hasattr(self.copy_system, 'kelly_calculator'):
                        self.copy_system.kelly_calculator.max_kelly_fraction = 0.25
                        self.copy_system.kelly_calculator.conservative_factor = 0.5
                
                await update.message.reply_text(f"✅ Режим копирования изменен на: {mode}")
                await send_telegram_alert(f"🔄 Режим копирования был изменен на {mode} через бота")
            
            else:
                await update.message.reply_text(
                    "❓ Неизвестная команда\n"
                    "Доступные: start, stop, restart, mode"
                )
                
        except Exception as e:
            logger.error(f"Copy command error: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text(f"❌ Ошибка управления копированием: {e}")

    # ================================
    # РЕГИСТРАЦИЯ КОМАНД
    # ================================
    


    def register_commands(self, application):
        """
        🔧 ИСПРАВЛЕННЫЙ МЕТОД: Теперь всегда использует ensure_commands_registered()
        Регистрация всех команд бота
        """
        # 🚀 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Используем новый метод принудительной регистрации
        self.ensure_commands_registered(application)
        
        # ⚠️ ВАЖНО: /keys НЕ регистрируем здесь, чтобы не было двойной регистрации.
        # Меню ключей регистрируется:
        #  - в Stage2TelegramBot.start() (оригинальный tg_keys_menu, если модуль найден)
        #  - в integrated_launch_system._initialize_telegram_bot() сразу ПОСЛЕ start()
        #    (fallback, но только если оригинал не зарегистрирован).
        km_registered = application.bot_data.get("keys_menu_registered")
        km_fallback = application.bot_data.get("keys_menu_fallback", False)
        if km_registered:
            logger.debug("register_commands: /keys already registered (fallback=%s)", km_fallback)
        else:
            logger.debug("register_commands: /keys not registered yet — launcher guard will handle it")

# ==========================================
# ДИАГНОСТИЧЕСКАЯ ИНФОРМАЦИЯ
# ==========================================
logger.info("🔧 stage2_telegram_bot.py loaded with fixes:")
logger.info(f"   ✅ TELEGRAM_TOKEN: {'✓ Found' if TELEGRAM_TOKEN else '❌ Missing'}")
logger.info(f"   ✅ ADMIN_IDS: {len(ADMIN_IDS)} administrators")
logger.info(f"   ✅ TG_KEYS_MENU: {'✓ Available' if tg_keys_available else '❌ Fallback mode'}")
logger.info(f"   ✅ Configuration source: {TELEGRAM_TOKEN_SOURCE}")


# ================================
# ФУНКЦИЯ ЗАПУСКА БОТА
# ================================

async def run_stage2_telegram_bot(copy_system=None):
    """
    🔧 ИСПРАВЛЕННАЯ функция запуска Telegram Bot для Stage 2 v2.2
    PRODUCTION-READY версия для работы в интегрированной системе
    """
    try:
        logger.info("Starting Stage 2 Telegram Bot v2.2...")
        
        # Создаем экземпляр бота
        bot = Stage2TelegramBot(copy_system)
        
        # 🔧 ИСПРАВЛЕНО: НЕ используем asyncio.run() в production
        # Создаем приложение напрямую без вложенного event loop
        application = Application.builder().token(TELEGRAM_TOKEN).build()
        
        # Регистрируем команды
        bot.register_commands(application)
        
        # 🔧 PRODUCTION FIX: Инициализируем без создания нового loop
        await application.initialize()
        await application.start()
        
        bot.bot_active = True
        logger.info("✅ Stage 2 Telegram Bot v2.2 started successfully")
        
        # Отправляем уведомление о запуске
        await send_telegram_alert(
            "🤖 **STAGE 2 TELEGRAM BOT v2.2 ЗАПУЩЕН**\n"
            "✅ Расширенная система управления активна\n"
            "🆕 Добавлены дополнительные функции из плана интеграции:\n"
            "   • Автоматические уведомления\n"
            "   • Тестирование всех подключений\n"
            "   • Просмотр логов системы\n"
            "   • Управление настройками просадки\n"
            "   • Управление trailing stops\n"
            "   • Экспорт отчетов\n"
            "   • Backup/Restore настроек\n"
            "📱 Используйте /start для начала работы"
        )
        
        # 🔧 PRODUCTION FIX: Запускаем polling без создания нового loop
        await application.updater.start_polling(drop_pending_updates=True)
        
        # 🔧 PRODUCTION FIX: Ожидаем в текущем loop, не создаем новый
        try:
            # Работаем в текущем event loop
            while bot.bot_active:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Bot shutdown requested")
        finally:
            # Корректно останавливаем без создания нового loop
            bot.bot_active = False
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            logger.info("Stage 2 Telegram Bot stopped")
        
    except Exception as e:
        logger.error(f"Telegram Bot error: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    # 🔧 STANDALONE режим - используем asyncio.run() только если запускаются напрямую
    try:
        print("🤖 Запуск Telegram Bot для Этапа 2 v2.2 (тестовый режим)")
        print("🆕 НОВЫЕ ФУНКЦИИ v2.2:")
        print("   • Настройки Kelly Criterion")
        print("   • Детальная аналитика производительности") 
        print("   • Анализ рисков с рекомендациями")
        print("   • Дневные отчеты")
        print("   • Диагностика системы")
        print("   • Статус синхронизации")
        print("🔧 ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ИЗ ПЛАНА ИНТЕГРАЦИИ:")
        print("   • Автоматические уведомления")
        print("   • Тестирование всех подключений")
        print("   • Просмотр логов системы")
        print("   • Управление настройками просадки")
        print("   • Управление trailing stops")
        print("   • Экспорт отчетов")
        print("   • Backup/Restore настроек")
        
        # 🔧 ТОЛЬКО в standalone режиме используем asyncio.run()
        import asyncio
        asyncio.run(run_stage2_telegram_bot())
    except KeyboardInterrupt:
        print("\n🛑 Bot остановлен пользователем")
    except Exception as e:
        print(f"\n💥 Ошибка запуска бота: {e}")
        traceback.print_exc()

