# trading_bot/telegram_cfg.py
# -*- coding: utf-8 -*-
"""
Telegram конфиг без 'светлых' секретов.
- Токен читаем из ENV TELEGRAM_TOKEN
- Админы: ADMIN_ONLY_IDS = { ... } ИЛИ из ENV ADMIN_ONLY_IDS="123,456"
"""

import os
from typing import Set

def _parse_bool(v: str, default: bool) -> bool:
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _parse_int_set(v: str) -> Set[int]:
    if not v:
        return set()
    return {int(x) for x in v.replace(" ", "").split(",") if x}

# === ОБЯЗАТЕЛЬНО ===
TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    # специально не падаем на импортe; упадём на старте бота с понятной ошибкой
    print("⚠️  TELEGRAM_TOKEN not set in environment")

# === Администраторы (доступ к /keys и пр.) ===
# Вариант 1: задать прямо здесь:
ADMIN_ONLY_IDS: Set[int] = {1753045590}
# Вариант 2 (дополнительно/вместо): из ENV ADMIN_ONLY_IDS="1753045590,111222333"
ADMIN_ONLY_IDS |= _parse_int_set(os.getenv("ADMIN_ONLY_IDS", ""))

# === Необязательные вещи ===
# Чат по умолчанию для уведомлений (если нужно). Можно оставить None.
_default_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_ID = int(_default_chat) if _default_chat.isdigit() else None

LOSS_ALERT_THRESHOLD = float(os.getenv("LOSS_ALERT_THRESHOLD", "0.06"))
NOTIFICATION_INTERVAL_HOURS = int(os.getenv("NOTIFICATION_INTERVAL_HOURS", "2"))
ENABLE_AUTO_NOTIFICATIONS = _parse_bool(os.getenv("ENABLE_AUTO_NOTIFICATIONS"), True)
ENABLE_RISK_ALERTS = _parse_bool(os.getenv("ENABLE_RISK_ALERTS"), True)
ENABLE_PERFORMANCE_REPORTS = _parse_bool(os.getenv("ENABLE_PERFORMANCE_REPORTS"), True)
COMMAND_COOLDOWN_SECONDS = int(os.getenv("COMMAND_COOLDOWN_SECONDS", "2"))

