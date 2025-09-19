# ---------- config.py (фрагмент: ключи/URL/окружение) ----------
from __future__ import annotations
import os
from typing import Optional, Tuple  # ← ДОБАВЛЕНО: импорт типов

# --- credentials store import for DB-first resolution ---
import logging
logger = logging.getLogger(__name__)

try:
    # боевой стор (шифрование + SQLAlchemy)
    from app.database_security_implementation import CredentialsStore
    logger.info("config: CredentialsStore imported from app.database_security_implementation")
except Exception:
    try:
        # legacy‑fallback (старый стор БЕЗ БД, только чтобы не падать)
        from app.crypto_store import CredentialsStore  # <= вот сюда и должен указывать фолбэк
        logger.warning("config: Fallback to app.crypto_store.CredentialsStore")
    except Exception:
        logger.exception("config: failed to import any CredentialsStore")
        CredentialsStore = None

# ============= SAFE MODE CONFIGURATION =============
# Настройки для retry механизма и безопасного режима
SAFE_MODE = {
    "api_retry_limit": 5,  # Максимальное количество попыток
    "api_backoff_base": 0.5,  # Базовая задержка в секундах
    "api_backoff_factor": 2.0,  # Множитель для экспоненциального backoff
    "api_backoff_max": 60.0,  # Максимальная задержка между попытками
    "websocket_retry_limit": 10,  # Попытки реконнекта WebSocket
    "websocket_timeout": 30.0,  # Таймаут WebSocket операций
    "rate_limit_buffer": 0.1,  # Буфер для rate limiting (10%)
    "emergency_stop": True,  # Включить аварийную остановку при критических ошибках
    "max_concurrent_orders": 10,  # Максимум одновременных ордеров
    "circuit_breaker_threshold": 5,  # Порог срабатывания circuit breaker
    "circuit_breaker_timeout": 300,  # Таймаут circuit breaker в секундах
}

# Для обратной совместимости можно добавить отдельные константы
API_RETRY_LIMIT = SAFE_MODE["api_retry_limit"]
API_BACKOFF_BASE = SAFE_MODE["api_backoff_base"]
API_BACKOFF_FACTOR = SAFE_MODE["api_backoff_factor"]
API_BACKOFF_MAX = SAFE_MODE["api_backoff_max"]


# Глобальная метка окружения
ENVIRONMENT = os.getenv("ENVIRONMENT", "prod").lower()
BYBIT_TESTNET = (ENVIRONMENT == "testnet")

# URL'ы как в работающем коде
SOURCE_API_URL = os.getenv("SOURCE_API_URL", "https://api.bybit.com")
MAIN_API_URL   = os.getenv("MAIN_API_URL",   "https://api.bybit.com")
SOURCE_WS_URL  = os.getenv("SOURCE_WS_URL",  "wss://stream.bybit.com/v5/private")
PUBLIC_WS_URL  = os.getenv("PUBLIC_WS_URL",  "wss://stream.bybit.com/v5/public/linear")

# Реквизиты (recv_window и т.д.) оставь свои
BYBIT_RECV_WINDOW = int(os.getenv("BYBIT_RECV_WINDOW", "20000"))

# ID аккаунтов из .env (по умолчанию 1=target, 2=donor-ro)
TARGET_ACCOUNT_ID = int(os.getenv("TARGET_ACCOUNT_ID", "1"))
DONOR_ACCOUNT_ID  = int(os.getenv("DONOR_ACCOUNT_ID",  "2"))

# ── Совместимость с более новой версией кода ──
DEFAULT_TRADE_ACCOUNT_ID = int(os.getenv("DEFAULT_TRADE_ACCOUNT_ID", str(TARGET_ACCOUNT_ID)))
# (опционально, на будущее)
DEFAULT_DONOR_ACCOUNT_ID = int(os.getenv("DEFAULT_DONOR_ACCOUNT_ID", str(DONOR_ACCOUNT_ID)))


# --- helpers для вытягивания ключей из БД (или ENV как фолбэк) ---
def _get_db_creds(account_id: int) -> Optional[Tuple[str, str]]:
    """
    Получение credentials из БД.
    Возвращает (api_key, api_secret) или None.
    """
    try:
        # ЖЁСТКОЕ хранилище с БД+шифрованием (как использует /keys)
        from app.database_security_implementation import CredentialsStore
        store = CredentialsStore()
        creds = store.get_account_credentials(account_id)
        # ожидание (key, secret) либо None
        if creds and all(creds):
            return creds
        return None
    except Exception as e:
        # не молчать — иначе «ключей нет» непонятно почему
        import logging
        logging.getLogger(__name__).exception("config._get_db_creds() failed for account_id=%s", account_id)
        return None


def _resolve(account_id: int, env_key_name: str, env_secret_name: str) -> Tuple[str, str]:  # ← ДОБАВЛЕНА типизация
    """
    Резолвинг credentials с приоритетом БД > ENV
    Args:
        account_id: ID аккаунта
        env_key_name: Имя переменной окружения для ключа
        env_secret_name: Имя переменной окружения для секрета
    Returns:
        Tuple[str, str]: (api_key, api_secret)
    """
    # 1) БД (основной источник)
    creds = _get_db_creds(account_id)
    if creds:
        return creds
    # 2) Фолбэк из ENV (на дев-машине можно временно задать)
    k = os.getenv(env_key_name, "")
    s = os.getenv(env_secret_name, "")
    return k, s

# ========= Runtime helpers for credentials =========

def get_api_credentials(account_id: int) -> Optional[Tuple[str, str]]:
    """
    Возвращает (api_key, api_secret) или None, если ничего не найдено.
    Порядок источников:
      1) База данных (горячая замена)
      2) ENV (MAIN_* для TARGET, SOURCE_* для DONOR; ACCOUNT_{id}_* для прочих)
      3) иначе None
    """
    # 1) DB (горячая замена)
    creds = _get_db_creds(account_id)
    if creds and all(creds):
        return creds

    # 2) ENV (роль-специфичные имена)
    if account_id == TARGET_ACCOUNT_ID:
        k = os.getenv("MAIN_API_KEY", "")
        s = os.getenv("MAIN_API_SECRET", "")
        if k and s:
            return k, s

    elif account_id == DONOR_ACCOUNT_ID:
        k = os.getenv("SOURCE_API_KEY", "")
        s = os.getenv("SOURCE_API_SECRET", "")
        if k and s:
            return k, s

    # 2b) Generic ENV для произвольных id
    k = os.getenv(f"ACCOUNT_{account_id}_API_KEY", "")
    s = os.getenv(f"ACCOUNT_{account_id}_API_SECRET", "")
    if k and s:
        return k, s

    # 3) Нет источника
    return None


def get_api_credentials_with_source(account_id: int) -> Tuple[str, str, str]:
    """
    То же самое, но с пометкой источника: 'DB' | 'ENV' | 'CONFIG' | 'NONE'.
    Удобно для логирования/диагностики.
    
    Returns:
        Tuple[str, str, str]: (api_key, api_secret, source)
    """
    creds = _get_db_creds(account_id)
    if creds and all(creds):
        return creds[0], creds[1], "DB"

    if account_id == TARGET_ACCOUNT_ID:
        env_k = os.getenv("MAIN_API_KEY", "")
        env_s = os.getenv("MAIN_API_SECRET", "")
        if env_k and env_s:
            return env_k, env_s, "ENV"
        # Если MAIN_API_KEY и MAIN_API_SECRET определены как переменные ниже
        # return MAIN_API_KEY, MAIN_API_SECRET, "CONFIG"
        return "", "", "NONE"  # ← ИЗМЕНЕНО: избегаем циклической зависимости

    if account_id == DONOR_ACCOUNT_ID:
        env_k = os.getenv("SOURCE_API_KEY", "")
        env_s = os.getenv("SOURCE_API_SECRET", "")
        if env_k and env_s:
            return env_k, env_s, "ENV"
        # Если SOURCE_API_KEY и SOURCE_API_SECRET определены как переменные ниже
        # return SOURCE_API_KEY, SOURCE_API_SECRET, "CONFIG"
        return "", "", "NONE"  # ← ИЗМЕНЕНО: избегаем циклической зависимости

    k = os.getenv(f"ACCOUNT_{account_id}_API_KEY", "")
    s = os.getenv(f"ACCOUNT_{account_id}_API_SECRET", "")
    if k and s:
        return k, s, "ENV"
    return "", "", "NONE"

# Копирование плеча с донора
COPY_LEVERAGE = os.getenv('COPY_LEVERAGE', 'true').lower() == 'true'

# -------- СНАЧАЛА вычисляем основные пары ключей --------
MAIN_API_KEY,   MAIN_API_SECRET   = _resolve(TARGET_ACCOUNT_ID, "MAIN_API_KEY",   "MAIN_API_SECRET")
SOURCE_API_KEY, SOURCE_API_SECRET = _resolve(DONOR_ACCOUNT_ID,  "SOURCE_API_KEY", "SOURCE_API_SECRET")

# -------- Потом публикуем алиасы совместимости --------
BYBIT_API_KEY    = MAIN_API_KEY     # так импортирует enhanced_trading_system_final_fixed.py
BYBIT_API_SECRET = MAIN_API_SECRET
# ---------- /конец фрагмента ----------
