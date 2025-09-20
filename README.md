# Bybit V5 Copy Trading Bot

This is a Python-based copy trading bot for the Bybit exchange, using their V5 API and WebSocket streams.

## Key Features

- Connects to Bybit's V5 private WebSocket for real-time position, order, execution, and wallet updates.
- Copies trades from a "donor" account to a "main" account.
- Performs REST API-based position reconciliation on startup to ensure state consistency.
- Includes a Telegram bot for diagnostics and manual control.

## Official Bybit WebSocket V5 Documentation

For detailed information on the WebSocket API, authentication, and subscriptions, refer to the official documentation:

[Bybit V5 WebSocket Documentation](https://bybit-exchange.github.io/docs/v5/ws/connect)
