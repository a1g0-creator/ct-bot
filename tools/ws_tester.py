import asyncio
import hmac
import json
import logging
import os
import time
import hashlib
import websockets

# --- Configuration ---
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get API credentials from environment variables
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

# Bybit WebSocket URL
WS_URL = "wss://stream.bybit.com/v5/private"

# Topics to subscribe to
SUBSCRIPTIONS = ["position", "execution", "order", "wallet"]

# --- Main WebSocket Client Class ---
class BybitWsTester:
    def __init__(self, api_key, api_secret):
        if not api_key or not api_secret:
            raise ValueError("API_KEY and API_SECRET must be set as environment variables.")
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws = None

    async def connect(self):
        """Connects to the WebSocket and performs authentication and subscription."""
        logging.info(f"Connecting to {WS_URL}...")
        try:
            self.ws = await websockets.connect(WS_URL, ping_interval=None)
            logging.info("WebSocket connection established.")
            await self.authenticate()
            await self.subscribe()
        except Exception as e:
            logging.error(f"Failed to connect: {e}")
            raise

    async def authenticate(self):
        """Generates and sends the authentication message."""
        logging.info("Authenticating...")
        expires = int((time.time() + 10) * 1000)
        payload = f"GET/realtime{expires}"
        signature = hmac.new(self.api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

        auth_msg = {
            "op": "auth",
            "args": [self.api_key, expires, signature]
        }
        await self.ws.send(json.dumps(auth_msg))

        response = await self.ws.recv()
        logging.info(f"Auth response: {response}")
        response_data = json.loads(response)
        if not response_data.get("success"):
            raise Exception(f"Authentication failed: {response_data.get('ret_msg')}")
        logging.info("Authentication successful.")

    async def subscribe(self):
        """Subscribes to the specified topics."""
        logging.info(f"Subscribing to topics: {SUBSCRIPTIONS}")
        sub_msg = {
            "op": "subscribe",
            "args": SUBSCRIPTIONS
        }
        await self.ws.send(json.dumps(sub_msg))
        response = await self.ws.recv()
        logging.info(f"Subscription response: {response}")

    async def send_ping(self):
        """Sends a ping to keep the connection alive."""
        ping_msg = {"op": "ping"}
        await self.ws.send(json.dumps(ping_msg))
        logging.info("Ping sent.")

    async def listen(self):
        """Listens for incoming messages and handles them."""
        logging.info("Listening for messages...")
        while True:
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                data = json.loads(message)
                if data.get("op") == "pong":
                    logging.info("Pong received.")
                else:
                    logging.info(f"Received message: {json.dumps(data, indent=2)}")
            except asyncio.TimeoutError:
                logging.info("No message received in 30s, sending ping.")
                await self.send_ping()
            except websockets.exceptions.ConnectionClosed as e:
                logging.error(f"Connection closed: {e}")
                break
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                break

async def main():
    """Main function to run the tester."""
    logging.info("Starting Bybit WebSocket Tester...")
    logging.info("Please set BYBIT_API_KEY and BYBIT_API_SECRET environment variables.")

    tester = BybitWsTester(API_KEY, API_SECRET)
    try:
        await tester.connect()
        await tester.listen()
    except Exception as e:
        logging.error(f"Tester stopped due to an error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Tester stopped by user.")
