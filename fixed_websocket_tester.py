#!/usr/bin/env python3
"""
Исправленный тестер WebSocket подключения к Bybit
Основан на официальной документации: https://bybit-exchange.github.io/docs/v5/ws/connect
"""

import asyncio
import websockets
import json
import hmac
import hashlib
import time
from datetime import datetime

# ==============================================================================
# ВСТАВЬТЕ СВОИ КЛЮЧИ ЗДЕСЬ
# ==============================================================================
API_KEY = "YOUR_API_KEY_HERE"
API_SECRET = "YOUR_API_SECRET_HERE"

# ==============================================================================
# ТЕСТЕР
# ==============================================================================

async def test_websocket_connection():
    """Тест WebSocket подключения с правильной аутентификацией"""
    
    # URL для private WebSocket (mainnet)
    uri = "wss://stream.bybit.com/v5/private"
    
    print("="*80)
    print("🚀 BYBIT WEBSOCKET TESTER (FIXED)")
    print("="*80)
    print(f"📍 Connecting to: {uri}")
    print(f"🔑 API Key: {API_KEY[:8]}...{API_KEY[-4:]}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    try:
        async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
            print("✅ Connected to WebSocket")
            
            # ==================================================================
            # ШАГ 1: АУТЕНТИФИКАЦИЯ
            # ==================================================================
            
            # Генерируем expires (текущее время + 1 час)
            expires = int((time.time() + 3600) * 1000)
            
            # ПРАВИЛЬНАЯ подпись согласно документации
            # signature = HMAC_SHA256(secret, 'GET/realtime' + expires)
            _val = f"GET/realtime{expires}"
            
            signature = hmac.new(
                bytes(API_SECRET, "utf-8"),
                bytes(_val, "utf-8"),
                hashlib.sha256
            ).hexdigest()
            
            # Формируем сообщение аутентификации
            auth_message = {
                "op": "auth",
                "args": [API_KEY, expires, signature]
            }
            
            print(f"\n📤 Sending authentication...")
            print(f"   Expires: {expires}")
            print(f"   Signature payload: 'GET/realtime{expires}'")
            
            await ws.send(json.dumps(auth_message))
            
            # Ждем ответ на аутентификацию
            auth_response = await asyncio.wait_for(ws.recv(), timeout=5)
            auth_data = json.loads(auth_response)
            
            print(f"\n📥 Auth response:")
            print(f"   {json.dumps(auth_data, indent=2)}")
            
            if auth_data.get('success') == True:
                print("\n✅ AUTHENTICATION SUCCESSFUL!")
            else:
                print(f"\n❌ AUTHENTICATION FAILED!")
                print(f"   Error: {auth_data.get('ret_msg', 'Unknown error')}")
                return
            
            # ==================================================================
            # ШАГ 2: ПОДПИСКА НА КАНАЛЫ
            # ==================================================================
            
            # Подписываемся на приватные каналы
            subscribe_message = {
                "op": "subscribe",
                "args": [
                    "position",      # Обновления позиций
                    "execution",     # Исполнение сделок
                    "order",         # Обновления ордеров
                    "wallet"         # Обновления баланса
                ]
            }
            
            print(f"\n📤 Subscribing to channels...")
            print(f"   Channels: {subscribe_message['args']}")
            
            await ws.send(json.dumps(subscribe_message))
            
            # Ждем подтверждение подписки
            sub_response = await asyncio.wait_for(ws.recv(), timeout=5)
            sub_data = json.loads(sub_response)
            
            print(f"\n📥 Subscribe response:")
            print(f"   {json.dumps(sub_data, indent=2)}")
            
            if sub_data.get('success') == True:
                print("\n✅ SUBSCRIPTION SUCCESSFUL!")
            else:
                print(f"\n❌ SUBSCRIPTION FAILED!")
                print(f"   Error: {sub_data.get('ret_msg', 'Unknown error')}")
                return
            
            # ==================================================================
            # ШАГ 3: СЛУШАЕМ СОБЫТИЯ
            # ==================================================================
            
            print("\n" + "="*80)
            print("⏳ WAITING FOR EVENTS...")
            print("   💡 TIP: Open/close/modify a position to see events")
            print("="*80 + "\n")
            
            # Счетчики событий
            event_count = {
                "position": 0,
                "execution": 0,
                "order": 0,
                "wallet": 0,
                "ping": 0,
                "pong": 0
            }
            
            # Начинаем отправку ping каждые 20 секунд
            async def send_ping():
                while True:
                    await asyncio.sleep(20)
                    ping_message = {"op": "ping"}
                    await ws.send(json.dumps(ping_message))
                    event_count["ping"] += 1
                    print(".", end="", flush=True)
            
            # Запускаем ping в фоне
            ping_task = asyncio.create_task(send_ping())
            
            try:
                while True:
                    # Получаем сообщение
                    message = await ws.recv()
                    data = json.loads(message)
                    
                    # Обрабатываем разные типы сообщений
                    if data.get('topic'):
                        # Это событие по подписке
                        topic = data['topic']
                        event_count[topic] = event_count.get(topic, 0) + 1
                        
                        print(f"\n🎯 EVENT #{event_count[topic]}: {topic.upper()}")
                        print(f"   Time: {datetime.now().strftime('%H:%M:%S')}")
                        
                        # Показываем данные события
                        event_data = data.get('data', [])
                        if isinstance(event_data, list):
                            for item in event_data[:2]:  # Показываем первые 2 элемента
                                if topic == "position":
                                    print(f"   📊 Position:")
                                    print(f"      Symbol: {item.get('symbol')}")
                                    print(f"      Side: {item.get('side')}")
                                    print(f"      Size: {item.get('size')}")
                                    print(f"      Entry: {item.get('avgPrice')}")
                                    print(f"      PnL: {item.get('unrealisedPnl')}")
                                elif topic == "order":
                                    print(f"   📝 Order:")
                                    print(f"      Symbol: {item.get('symbol')}")
                                    print(f"      Side: {item.get('side')}")
                                    print(f"      Price: {item.get('price')}")
                                    print(f"      Qty: {item.get('qty')}")
                                    print(f"      Status: {item.get('orderStatus')}")
                                elif topic == "execution":
                                    print(f"   ⚡ Execution:")
                                    print(f"      Symbol: {item.get('symbol')}")
                                    print(f"      Side: {item.get('side')}")
                                    print(f"      Price: {item.get('execPrice')}")
                                    print(f"      Qty: {item.get('execQty')}")
                                elif topic == "wallet":
                                    print(f"   💰 Wallet:")
                                    for coin in item.get('coin', [])[:1]:
                                        print(f"      {coin.get('coin')}: {coin.get('walletBalance')}")
                        
                    elif data.get('op') == 'pong':
                        # Ответ на ping
                        event_count["pong"] += 1
                        # Не печатаем, чтобы не засорять вывод
                        
                    elif data.get('ret_msg'):
                        # Системное сообщение
                        print(f"\n📨 System message: {data.get('ret_msg')}")
                        
                    else:
                        # Неизвестное сообщение
                        print(f"\n❓ Unknown message: {json.dumps(data, indent=2)[:200]}")
                        
            except KeyboardInterrupt:
                print("\n\n⏹️ Stopped by user")
            finally:
                ping_task.cancel()
                
            # ==================================================================
            # ИТОГИ
            # ==================================================================
            
            print("\n" + "="*80)
            print("📊 EVENT STATISTICS:")
            for event_type, count in event_count.items():
                if count > 0:
                    print(f"   {event_type}: {count}")
            print("="*80)
                    
    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n❌ Connection closed: {e}")
        
    except asyncio.TimeoutError:
        print(f"\n❌ Timeout waiting for response")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Главная функция"""
    
    # Проверка ключей
    if API_KEY == "YOUR_API_KEY_HERE":
        print("❌ ERROR: Please insert your API keys!")
        print("   Edit this file and replace:")
        print('   API_KEY = "YOUR_API_KEY_HERE" with your actual key')
        print('   API_SECRET = "YOUR_API_SECRET_HERE" with your actual secret')
        return
    
    # Запускаем тест
    await test_websocket_connection()


if __name__ == "__main__":
    print("🔌 Bybit WebSocket Tester (Based on Official Documentation)")
    print("📖 Docs: https://bybit-exchange.github.io/docs/v5/ws/connect")
    asyncio.run(main())
