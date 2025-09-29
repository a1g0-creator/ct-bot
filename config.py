def get_api_credentials(account_id: int):
    if account_id == 1:
        return "main_api_key", "main_api_secret"
    elif account_id == 2:
        return "source_api_key", "source_api_secret"
    return None

import os

import os

BYBIT_RECV_WINDOW = int(os.getenv("BYBIT_RECV_WINDOW", "50000"))
DEFAULT_TRADE_ACCOUNT_ID = 1
TARGET_ACCOUNT_ID = 1
DONOR_ACCOUNT_ID = 2

MAIN_API_KEY = "main_api_key"
MAIN_API_SECRET = "main_api_secret"
MAIN_API_URL = "https://api-demo.bybit.com"

SOURCE_API_KEY = "source_api_key"
SOURCE_API_SECRET = "source_api_secret"
SOURCE_API_URL = "https://api.bybit.com"

TELEGRAM_BOT_TOKEN = "dummy_token"
TELEGRAM_CHAT_ID = "dummy_chat_id"

# Account type for balance calculations (e.g., "UNIFIED", "DERIVATIVES")
BALANCE_ACCOUNT_TYPE = "UNIFIED"
