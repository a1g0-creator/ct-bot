# -*- coding: utf-8 -*-
"""
ИСПРАВЛЕННЫЙ ФАЙЛ tg_keys_menu.py
С применением всех исправлений для методов CredentialsStore
"""

from __future__ import annotations
import asyncio
import os
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any, List
from enum import Enum
from telegram.error import BadRequest

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ContextTypes,
    ConversationHandler,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

# Настройка логирования
logger = logging.getLogger(__name__)

# ИСПРАВЛЕННЫЙ ИМПОРТ CredentialsStore - БЕЗ префикса app. в первой попытке!
# устойчивый импорт стора
try:
    from app.database_security_implementation import CredentialsStore
    logger.info("CredentialsStore imported from app.database_security_implementation")
except Exception:
    from database_security_implementation import CredentialsStore
    logger.info("CredentialsStore imported from database_security_implementation (top-level)")


# ИСПРАВЛЕННЫЙ ИМПОРТ админских ID - БЕЗ префикса app. в первой попытке!
try:
    from telegram_cfg import ADMIN_ONLY_IDS, TELEGRAM_CHAT_ID
except ImportError:
    try:
        from app.telegram_cfg import ADMIN_ONLY_IDS, TELEGRAM_CHAT_ID
    except ImportError:
        # Fallback на ENV
        ADMIN_ONLY_IDS = [int(x) for x in os.getenv("TELEGRAM_ADMIN_IDS", "").split(",") if x.strip().isdigit()]
        TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# FSM states
(MAIN_MENU, ACCOUNT_SELECT, ACCOUNT_MENU, WAIT_KEY, WAIT_SECRET, 
 CONFIRM_SAVE, MANAGE_MENU) = range(7)

class AccountType(Enum):
    """Типы аккаунтов"""
    TARGET = "TARGET"
    DONOR = "DONOR"
    CUSTOM = "CUSTOM"

@dataclass
class KeySession:
    """Сессия для работы с ключами"""
    selected_account_type: Optional[AccountType] = None
    selected_account_id: Optional[int] = None
    account_name: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    
    def clear_credentials(self):
        """Очистка введённых credentials"""
        self.api_key = None
        self.api_secret = None
    
    def get_display_name(self) -> str:
        """Получение отображаемого имени аккаунта"""
        if self.account_name:
            return self.account_name
        if self.selected_account_type == AccountType.TARGET:
            return f"Target Account (ID: {self.selected_account_id})"
        elif self.selected_account_type == AccountType.DONOR:
            return f"Donor Account (ID: {self.selected_account_id})"
        else:
            return f"Account ID: {self.selected_account_id}"

class AccountManager:
    """Менеджер для работы с аккаунтами"""
    
    @staticmethod
    def get_predefined_accounts() -> Dict[str, Dict]:
        """Получение предустановленных аккаунтов из ENV"""
        accounts = {}
        
        # Target account
        target_id = os.getenv("TARGET_ACCOUNT_ID", "1")
        if target_id:
            accounts["target"] = {
                "id": int(target_id),
                "name": "Target Account (Main)",
                "type": AccountType.TARGET,
                "env_key": "TARGET_ACCOUNT_ID"
            }
        
        # Donor account
        donor_id = os.getenv("DONOR_ACCOUNT_ID", "2")
        if donor_id:
            accounts["donor"] = {
                "id": int(donor_id),
                "name": "Donor Account (Source)",
                "type": AccountType.DONOR,
                "env_key": "DONOR_ACCOUNT_ID"
            }
        
        return accounts
    
    @staticmethod
    def get_all_configured_accounts() -> List[Dict]:
        """
        Возвращает только два предустановленных аккаунта (Target и Donor),
        помечая, есть ли у них сохранённые ключи в БД.
        """
        store = CredentialsStore()
        predefined = AccountManager.get_predefined_accounts()

        result: List[Dict] = []
        for key in ("target", "donor"):
            if key not in predefined:
                continue
            acc = predefined[key]
            has_creds = False
            try:
                creds = store.get_account_credentials(acc["id"])
                # считаем, что ключи есть, если вернулись 2 непустые строки
                has_creds = bool(creds and len(creds) >= 2 and creds[0] and creds[1])
            except Exception as e:
                logger.warning(f"get_account_credentials({acc['id']}) error: {e}")

            result.append({
                "id": acc["id"],
                "name": acc["name"],
                "type": acc["type"],
                "has_credentials": has_creds,
            })

        return result

def _is_admin(update: Update) -> bool:
    """Проверка админских прав"""
    uid = update.effective_user.id if update.effective_user else 0
    ids = set(ADMIN_ONLY_IDS) if ADMIN_ONLY_IDS else set()
    
    try:
        if TELEGRAM_CHAT_ID:
            ids.add(int(TELEGRAM_CHAT_ID))
    except:
        pass
    
    env_ids = os.getenv("TELEGRAM_ADMIN_IDS", "").split(",")
    for id_str in env_ids:
        if id_str.strip().isdigit():
            ids.add(int(id_str.strip()))
    
    logger.info(f"[/keys] Admin check: user_id={uid}, admin_ids={ids}, result={uid in ids}")
    return uid in ids

def _mask(s: Optional[str]) -> str:
    """Маскировка ключа"""
    if not s:
        return "—"
    if len(s) <= 8:
        return "***"
    return s[:4] + "…" + s[-4:]

async def _load_credentials(account_id: int) -> Tuple[Optional[str], Optional[str]]:
    """Загрузка credentials из БД"""
    try:
        store = CredentialsStore()
        creds = store.get_account_credentials(account_id)
        if creds and len(creds) >= 2:
            return creds[0], creds[1]
    except Exception as e:
        logger.error(f"Error loading credentials for account {account_id}: {e}")
    return None, None

async def _safe_edit_message(query, text, reply_markup=None, **kwargs):
    """
    ИСПРАВЛЕННАЯ функция безопасного редактирования сообщения:
    - если текст и клавиатура не меняются — просто отвечаем на callback и выходим
    - глушим BadRequest: 'Message is not modified'
    """
    msg = query.message
    prev_text = (getattr(msg, "text_html", None) or msg.text or "")
    same_text = (prev_text == text)

    def _mkd(m):
        try:
            return m.to_dict() if m else None
        except Exception:
            return None

    same_markup = (_mkd(msg.reply_markup) == _mkd(reply_markup))

    if same_text and same_markup:
        try:
            await query.answer("Без изменений")
        except Exception:
            pass
        return

    # ИСПРАВЛЕНИЕ: Устанавливаем parse_mode только если он не передан в kwargs
    if 'parse_mode' not in kwargs:
        kwargs['parse_mode'] = "HTML"

    try:
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            **kwargs
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            try:
                await query.answer("Без изменений")
            except Exception:
                pass
            return
        raise


# ===== ГЛАВНОЕ МЕНЮ =====

async def keys_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Точка входа - главное меню"""
    logger.info(f"[/keys] Called by user {update.effective_user.id if update.effective_user else 'unknown'}")
    
    if not _is_admin(update):
        await update.message.reply_text("⛔ Доступ запрещён. Вы не администратор.")
        return ConversationHandler.END
    
    # Инициализируем сессию
    context.user_data["keys_sess"] = KeySession()
    
    # Показываем главное меню
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрая настройка Target", callback_data="quick_target")],
        [InlineKeyboardButton("⚡ Быстрая настройка Donor", callback_data="quick_donor")],
        [InlineKeyboardButton("🔍 Проверить все аккаунты", callback_data="check_all")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="close")]
    ]
    
    txt = (
        "🔐 <b>Управление API ключами</b>\n\n"
        "Выберите действие:\n\n"
        "• <b>Быстрая настройка Target</b> - основной торговый аккаунт\n"
        "• <b>Быстрая настройка Donor</b> - аккаунт-источник сигналов\n"
        "• <b>Проверить все</b> - статус всех аккаунтов"
    )
    
    await update.message.reply_text(
        txt, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode='HTML'
    )
    return MAIN_MENU

# ===== ОБРАБОТЧИК ГЛАВНОГО МЕНЮ =====

async def main_menu_callback(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик кнопок главного меню"""
    query = update.callback_query
    await query.answer()

    sess: KeySession = context.user_data.get("keys_sess", KeySession())

    if query.data == "back_main":
        return await show_main_menu(update, context)

    if query.data == "close":
        await _safe_edit_message(query, "✅ Меню закрыто")
        context.user_data.clear()
        return ConversationHandler.END

    elif query.data == "quick_target":
        predefined = AccountManager.get_predefined_accounts()
        if "target" in predefined:
            sess.selected_account_type = AccountType.TARGET
            sess.selected_account_id = predefined["target"]["id"]
            sess.account_name = predefined["target"]["name"]
            context.user_data["keys_sess"] = sess
            return await show_account_menu(update, context)
        else:
            await _safe_edit_message(query, "❌ Target account не настроен в ENV")
            return MAIN_MENU

    elif query.data == "quick_donor":
        predefined = AccountManager.get_predefined_accounts()
        if "donor" in predefined:
            sess.selected_account_type = AccountType.DONOR
            sess.selected_account_id = predefined["donor"]["id"]
            sess.account_name = predefined["donor"]["name"]
            context.user_data["keys_sess"] = sess
            return await show_account_menu(update, context)
        else:
            await _safe_edit_message(query, "❌ Donor account не настроен в ENV")
            return MAIN_MENU

    elif query.data == "check_all":
        return await show_all_accounts_status(update, context)

    return MAIN_MENU

# ===== МЕНЮ КОНКРЕТНОГО АККАУНТА =====

async def show_account_menu(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать меню управления конкретным аккаунтом"""
    query = update.callback_query
    sess: KeySession = context.user_data.get("keys_sess", KeySession())

    # Загружаем текущие credentials
    api_key, api_secret = await _load_credentials(sess.selected_account_id)

    keyboard = [
        [InlineKeyboardButton("🔑 Задать API KEY",     callback_data="set_key")],
        [InlineKeyboardButton("🔐 Задать API SECRET",  callback_data="set_secret")],
        [InlineKeyboardButton("👁 Показать текущие",   callback_data="show_current")],
    ]
    if sess.api_key and sess.api_secret:
        keyboard.append([InlineKeyboardButton("💾 Сохранить и Применить", callback_data="save_creds")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_main")])

    txt = (
        f"⚙️ <b>Настройка: {sess.get_display_name()}</b>\n\n"
        f"Account ID: {sess.selected_account_id}\n"
        f"Тип: {sess.selected_account_type.value if sess.selected_account_type else 'Custom'}\n\n"
        f"<b>Текущий статус в БД:</b>\n"
        f"API KEY: <code>{_mask(api_key)}</code>\n"
        f"API SECRET: <code>{_mask(api_secret)}</code>\n\n"
    )
    if sess.api_key or sess.api_secret:
        txt += "<b>Новые данные (не сохранены):</b>\n"
        if sess.api_key:
            txt += f"NEW KEY: <code>{_mask(sess.api_key)}</code>\n"
        if sess.api_secret:
            txt += f"NEW SECRET: <code>{_mask(sess.api_secret)}</code>\n"

    await _safe_edit_message(
        query,
        txt,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML",
    )
    return ACCOUNT_MENU


# ===== ОБРАБОТЧИКИ КНОПОК =====

async def account_callback(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик выбора аккаунта и действий с ним"""
    query = update.callback_query
    await query.answer()

    sess: KeySession = context.user_data.get("keys_sess", KeySession())
    data = query.data

    # Навигация назад
    if data == "back_main":
        sess.clear_credentials()
        context.user_data["keys_sess"] = sess
        return await show_main_menu(update, context)

    # Действия с ключами
    elif data == "set_key":
        await _safe_edit_message(
            query,
            f"🔑 <b>Ввод API KEY для {sess.get_display_name()}</b>\n\n"
            "Отправьте API KEY от Bybit:\n"
            "(сообщение будет удалено для безопасности)",
        )
        context.user_data["awaiting"] = "key"
        return WAIT_KEY

    elif data == "set_secret":
        await _safe_edit_message(
            query,
            f"🔐 <b>Ввод API SECRET для {sess.get_display_name()}</b>\n\n"
            "Отправьте API SECRET от Bybit:\n"
            "(сообщение будет удалено для безопасности)",
        )
        context.user_data["awaiting"] = "secret"
        return WAIT_SECRET

    elif data == "show_current":
        api_key, api_secret = await _load_credentials(sess.selected_account_id)
        txt = (
            f"👁 <b>Текущие ключи для {sess.get_display_name()}</b>\n\n"
            f"Account ID: {sess.selected_account_id}\n"
            f"API KEY: <code>{_mask(api_key)}</code>\n"
            f"API SECRET: <code>{_mask(api_secret)}</code>"
        )
        await _safe_edit_message(query, txt)
        await asyncio.sleep(3)
        return await show_account_menu(update, context)

    elif data == "save_creds":
        if sess.api_key and sess.api_secret:
            try:
                # 1. Save credentials to the database
                store = CredentialsStore()
                store.set_account_credentials(
                    sess.selected_account_id,
                    sess.api_key,
                    sess.api_secret,
                )
                logger.info(f"[/keys] Credentials saved for account {sess.selected_account_id}")

                await _safe_edit_message(
                    query,
                    f"✅ <b>Ключи сохранены!</b>\n\n"
                    f"⏳ Применяю новые ключи и перезапускаю соединения...",
                )

                # 2. Trigger hot-reload in the main system
                system = context.application.bot_data.get("integrated_system")
                if system and hasattr(system, "reload_credentials_and_reconnect"):
                    # We run this as a background task so it doesn't block the bot
                    asyncio.create_task(system.reload_credentials_and_reconnect())
                    logger.info(f"[/keys] Hot-reload triggered for account {sess.selected_account_id}")
                else:
                    logger.warning("[/keys] Integrated system not found or has no reload method.")
                    await query.message.reply_text("⚠️ Ключи сохранены, но не удалось применить их автоматически. Может потребоваться перезапуск.")

                # 3. Clear session and return to menu
                sess.clear_credentials()
                context.user_data["keys_sess"] = sess

                await asyncio.sleep(3) # Give user time to read the message
                return await show_account_menu(update, context)

            except Exception as e:
                logger.error(f"[/keys] Error saving/applying credentials: {e}", exc_info=True)
                await _safe_edit_message(
                    query,
                    f"❌ <b>Ошибка сохранения или применения:</b>\n<code>{str(e)}</code>",
                )
                return ACCOUNT_MENU
        else:
            await _safe_edit_message(query, "⚠️ Нет данных для сохранения и применения")
            return ACCOUNT_MENU

    return ACCOUNT_MENU

# ===== ОБРАБОТКА ТЕКСТОВОГО ВВОДА =====

async def text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик текстового ввода ключей"""
    if not _is_admin(update):
        return ConversationHandler.END
    
    sess: KeySession = context.user_data.get("keys_sess", KeySession())
    text = (update.message.text or "").strip()
    awaiting = context.user_data.get("awaiting")
    
    logger.info(f"[/keys] Text input for account {sess.selected_account_id}, awaiting={awaiting}")
    
    if awaiting == "key":
        if len(text) < 8:
            await update.message.reply_text("⛔ API KEY слишком короткий (минимум 8 символов)")
            return WAIT_KEY
        
        sess.api_key = text
        context.user_data["keys_sess"] = sess
        context.user_data["awaiting"] = None
        
        # Удаляем сообщение с ключом для безопасности
        try:
            await update.message.delete()
            logger.info("[/keys] Message with API key deleted")
        except:
            pass
        
        # ИСПРАВЛЕНИЕ: Отправляем новое сообщение с меню вместо edit_message_text
        keyboard = [
            [InlineKeyboardButton("🔑 Задать API KEY", callback_data="set_key")],
            [InlineKeyboardButton("🔐 Задать API SECRET", callback_data="set_secret")],
            [InlineKeyboardButton("👁 Показать текущие", callback_data="show_current")],
        ]
        
        # Добавляем кнопку сохранения если есть оба ключа
        if sess.api_key and sess.api_secret:
            keyboard.append([InlineKeyboardButton("💾 Сохранить в БД", callback_data="save_creds")])
        
        # Добавляем кнопку применения если есть сохранённые ключи
        api_key_db, api_secret_db = await _load_credentials(sess.selected_account_id)
        if api_key_db and api_secret_db:
            keyboard.append([InlineKeyboardButton("✅ Применить без рестарта", callback_data="apply_hot")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_main")])
        
        txt = (
            f"✅ API KEY получен для {sess.get_display_name()}\n"
            f"Маска: <code>{_mask(text)}</code>\n\n"
            f"⚙️ <b>Настройка: {sess.get_display_name()}</b>\n\n"
            f"Account ID: {sess.selected_account_id}\n"
            f"Тип: {sess.selected_account_type.value if sess.selected_account_type else 'Custom'}\n\n"
            f"<b>Текущий статус в БД:</b>\n"
            f"API KEY: <code>{_mask(api_key_db)}</code>\n"
            f"API SECRET: <code>{_mask(api_secret_db)}</code>\n\n"
        )
        
        if sess.api_key or sess.api_secret:
            txt += "<b>Новые данные (не сохранены):</b>\n"
            if sess.api_key:
                txt += f"NEW KEY: <code>{_mask(sess.api_key)}</code>\n"
            if sess.api_secret:
                txt += f"NEW SECRET: <code>{_mask(sess.api_secret)}</code>\n"
        
        # ИСПРАВЛЕНИЕ: Используем reply_text вместо edit_message_text
        await update.message.reply_text(
            txt,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return ACCOUNT_MENU
    
    elif awaiting == "secret":
        if len(text) < 8:
            await update.message.reply_text("⛔ API SECRET слишком короткий (минимум 8 символов)")
            return WAIT_SECRET
        
        sess.api_secret = text
        context.user_data["keys_sess"] = sess
        context.user_data["awaiting"] = None
        
        # Удаляем сообщение с секретом для безопасности
        try:
            await update.message.delete()
            logger.info("[/keys] Message with API secret deleted")
        except:
            pass
        
        # ИСПРАВЛЕНИЕ: Отправляем новое сообщение с меню
        keyboard = [
            [InlineKeyboardButton("🔑 Задать API KEY", callback_data="set_key")],
            [InlineKeyboardButton("🔐 Задать API SECRET", callback_data="set_secret")],
            [InlineKeyboardButton("👁 Показать текущие", callback_data="show_current")],
        ]
        
        # Добавляем кнопку сохранения если есть оба ключа
        if sess.api_key and sess.api_secret:
            keyboard.append([InlineKeyboardButton("💾 Сохранить в БД", callback_data="save_creds")])
        
        # Добавляем кнопку применения если есть сохранённые ключи
        api_key_db, api_secret_db = await _load_credentials(sess.selected_account_id)
        if api_key_db and api_secret_db:
            keyboard.append([InlineKeyboardButton("✅ Применить без рестарта", callback_data="apply_hot")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_main")])
        
        txt = (
            f"✅ API SECRET получен для {sess.get_display_name()}\n"
            f"Маска: <code>{_mask(text)}</code>\n\n"
            f"⚙️ <b>Настройка: {sess.get_display_name()}</b>\n\n"
            f"Account ID: {sess.selected_account_id}\n"
            f"Тип: {sess.selected_account_type.value if sess.selected_account_type else 'Custom'}\n\n"
            f"<b>Текущий статус в БД:</b>\n"
            f"API KEY: <code>{_mask(api_key_db)}</code>\n"
            f"API SECRET: <code>{_mask(api_secret_db)}</code>\n\n"
        )
        
        if sess.api_key and sess.api_secret:
            txt += "<b>Новые данные (готовы к сохранению):</b>\n"
            if sess.api_key:
                txt += f"NEW KEY: <code>{_mask(sess.api_key)}</code>\n"
            if sess.api_secret:
                txt += f"NEW SECRET: <code>{_mask(sess.api_secret)}</code>\n"
            txt += "\n💡 <i>Нажмите '💾 Сохранить в БД' для сохранения</i>"
        
        # ИСПРАВЛЕНИЕ: Используем reply_text
        await update.message.reply_text(
            txt,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return ACCOUNT_MENU
    
    # Неожиданный текст
    await update.message.reply_text("⚠ Используйте кнопки меню для действий")
    return ACCOUNT_MENU

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

async def show_main_menu(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    sess: KeySession = context.user_data.get("keys_sess", KeySession())

    predefined = AccountManager.get_predefined_accounts()
    has_target = "target" in predefined
    has_donor  = "donor"  in predefined

    keyboard = []
    if has_target:
        keyboard.append([InlineKeyboardButton("🎯 Быстро: TARGET", callback_data="quick_target")])
    if has_donor:
        keyboard.append([InlineKeyboardButton("📡 Быстро: DONOR",  callback_data="quick_donor")])

    keyboard += [
        [InlineKeyboardButton("📋 Статус всех аккаунтов", callback_data="check_all")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="close")],
    ]

    txt = (
        "🧩 <b>BYBIT COPY TRADING BOT</b>\n"
        "Выберите действие:\n\n"
        "• Быстрая настройка TARGET/DONOR\n"
        "• Просмотр статуса всех аккаунтов\n"
    )

    await _safe_edit_message(
        query,
        txt,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML",
    )
    return MAIN_MENU


async def show_all_accounts_status(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать статус всех аккаунтов"""
    query = update.callback_query
    accounts = AccountManager.get_all_configured_accounts()

    txt = "🔍 <b>Статус всех аккаунтов</b>\n\n"
    for acc in accounts:
        status = "✅" if acc["has_credentials"] else "❌"
        txt += f"{status} <b>{acc['name']}</b>\n"
        txt += f"   ID: {acc['id']}, Тип: {acc['type'].value}\n"
        if acc["has_credentials"]:
            api_key, api_secret = await _load_credentials(acc["id"])
            txt += f"   KEY: <code>{_mask(api_key)}</code>\n"
            txt += f"   SECRET: <code>{_mask(api_secret)}</code>\n"
        else:
            txt += "   Ключи не настроены\n"
        txt += "\n"

    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_main")]]

    await _safe_edit_message(
        query,
        txt,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML",
    )
    return MAIN_MENU



# ===== РЕГИСТРАЦИЯ HANDLER'ОВ =====

def register_tg_keys_menu(app: Application, integrated_system: Any) -> None:
    """
    Регистрирует ConversationHandler для управления ключами.
    Порядок групп:
      -1: трассировка CBQ + общий лог апдейтов (block=False)
       1: общий CBQ-fallback (block=False)
       2: основной FSM /keys
    """
    logger.info("[/keys] Registering multi-account ConversationHandler")
    app.bot_data["integrated_system"] = integrated_system

    # --- РАННИЙ CBQ-ТРЕЙСЕР: снимает спиннер и логирует данные кнопок ---
    async def _cbq_prelog(update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = getattr(update, "callback_query", None)
        if not q:
            return
        try:
            # важно: отвечаем, чтобы убрать "часики" на кнопке
            await q.answer()
        except Exception:
            pass
        try:
            user_id = getattr(getattr(q, "from_user", None), "id", None)
            chat_id = getattr(getattr(q, "message", None), "chat_id", None)
            logger.info(
                "CBQ PRELOG: data=%r chat=%s user=%s state=%r",
                getattr(q, "data", None),
                chat_id,
                user_id,
                context.user_data.get("awaiting"),
            )
        except Exception:
            pass

    app.add_handler(
        CallbackQueryHandler(_cbq_prelog, pattern=".*", block=False),
        group=-1
    )

    # --- Универсальный логгер любых апдейтов (чтобы видеть, что вообще приходит) ---
    async def _dbg_log_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            uid = update.effective_user.id if update.effective_user else None
            has_cbq = bool(getattr(update, "callback_query", None))
            txt = getattr(getattr(update, "message", None), "text", None)
            context.application.logger.info(f"[tg] update: user={uid} has_cbq={has_cbq} text={txt!r}")
        except Exception:
            pass

    app.add_handler(MessageHandler(filters.ALL, _dbg_log_update), group=-1)

    # --- Основной диалог /keys (FSM) ---
    conv = ConversationHandler(
        entry_points=[CommandHandler("keys", keys_start)],
        states={
            MAIN_MENU:      [CallbackQueryHandler(main_menu_callback)],
            ACCOUNT_SELECT: [CallbackQueryHandler(account_callback)],
            ACCOUNT_MENU:   [CallbackQueryHandler(account_callback)],
            WAIT_KEY:       [MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler)],
            WAIT_SECRET:    [MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler)],
        },
        fallbacks=[CommandHandler("keys", keys_start)],
        name="multi_keys_fsm",
        persistent=False,
        # КЛЮЧЕВОЕ: нам нужны MessageHandler'ы для текста — оставляем False
        per_message=False
    )

    # FSM ставим в group=2
    app.add_handler(conv, group=2)

    # --- Общий CBQ-fallback вне FSM (не блокирует прохождение к FSM) ---
    app.add_handler(
        CallbackQueryHandler(main_menu_callback, block=False),
        group=1
    )

    app.bot_data["keys_menu_registered"] = True
    logger.info("[/keys] Multi-account ConversationHandler registered successfully")
