# -*- coding: utf-8 -*-
import asyncio
import hmac
import json
import logging
import os
import time
import websockets

# ==============================================================================
# WebSocket ТЕСТЕР ДЛЯ BYBIT V5 (приватный канал)
#
# Этот скрипт предназначен для быстрой проверки работоспособности
# приватного WebSocket-соединения с Bybit API v5.
#
# Что он делает:
# 1. Подключается к wss://stream.bybit.com/v5/private.
# 2. Выполняет аутентификацию с использованием API ключей.
# 3. Подписывается на ключевые топики: position, execution, order, wallet.
# 4. Входит в бесконечный цикл прослушивания и выводит все входящие
#    сообщения в консоль.
# 5. Автоматически отвечает на 'ping' от сервера, поддерживая соединение.
#
# Использование:
# 1. Установите зависимости:
#    pip install websockets
#
# 2. Установите переменные окружения (чтобы не хранить ключи в коде):
#    export BYBIT_API_KEY="ВАШ_API_КЛЮЧ"
#    export BYBIT_API_SECRET="ВАШ_API_СЕКРЕТ"
#
# 3. Запустите скрипт:
#    python ws_tester.py
#
# ==============================================================================

# Настройка логирования для вывода информации в консоль
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация ---
# URL для подключения к приватному WebSocket на mainnet
WS_URL = "wss://stream.bybit.com/v5/private"
# Топики для подписки
TOPICS = ["position", "execution", "order", "wallet"]

async def run_tester():
    """Главная функция для запуска тестера WebSocket."""

    # 1. Получение ключей из переменных окружения
    #    Это безопаснее, чем хранить их прямо в коде.
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")

    if not api_key or not api_secret:
        logging.error("Ошибка: Переменные окружения BYBIT_API_KEY и BYBIT_API_SECRET должны быть установлены.")
        return

    logging.info("Тестер запущен. Используются ключи из переменных окружения.")

    # 2. Бесконечный цикл для автоматического переподключения
    while True:
        try:
            # Используем async with для автоматического управления соединением
            async with websockets.connect(WS_URL) as ws:
                logging.info(f"Успешно подключились к {WS_URL}")

                # --- Аутентификация ---
                # Время истечения подписи (в миллисекундах)
                expires = int((time.time() + 60) * 1000)
                # Строка для подписи
                signature_payload = f"GET/realtime{expires}"
                # Создание подписи HMAC-SHA256
                signature = hmac.new(
                    api_secret.encode("utf-8"),
                    signature_payload.encode("utf-8"),
                    "sha256"
                ).hexdigest()

                # Сообщение для аутентификации
                auth_message = {
                    "op": "auth",
                    "args": [api_key, expires, signature],
                }

                # Отправка сообщения и ожидание ответа
                await ws.send(json.dumps(auth_message))
                auth_response = await ws.recv()
                logging.info(f"Ответ на аутентификацию: {auth_response}")

                # Проверка успешности аутентификации
                auth_data = json.loads(auth_response)
                if not auth_data.get("success"):
                    logging.error("Аутентификация не удалась. Проверьте ключи и их права.")
                    return  # Прерываем выполнение, если ключи неверны

                # --- Подписка на топики ---
                subscribe_message = {
                    "op": "subscribe",
                    "args": TOPICS,
                }
                await ws.send(json.dumps(subscribe_message))
                logging.info(f"Отправлен запрос на подписку: {TOPICS}")

                # --- Цикл прослушивания сообщений ---
                logging.info("Входим в режим прослушивания. Ожидание сообщений...")
                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    # Автоматический ответ на ping от сервера
                    if data.get("op") == "ping":
                        pong_message = {"op": "pong", "req_id": data.get("req_id")}
                        await ws.send(json.dumps(pong_message))
                        logging.info("Отправлен pong в ответ на ping.")
                        continue

                    # Вывод всех остальных сообщений
                    logging.info(f"Получено сообщение: {message}")

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"Соединение закрыто: {e}. Попытка переподключения через 5 секунд...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Произошла ошибка: {e}. Попытка переподключения через 10 секунд...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(run_tester())
    except KeyboardInterrupt:
        logging.info("Тестер остановлен пользователем.")
