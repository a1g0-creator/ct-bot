# Bybit V5 Copy Trading Bot

This is a Python-based copy trading bot for the Bybit exchange, using their V5 API and WebSocket streams.

## Key Features

- Connects to Bybit's V5 private WebSocket for real-time position, order, execution, and wallet updates.
- Copies trades from a "donor" account to a "main" account.
- Implements isolated margin mirroring.
- Supports configurable trailing stops.
- Performs REST API-based position reconciliation on startup to ensure state consistency.
- Includes a Telegram bot for diagnostics and manual control.

## Configuration

The bot is configured via environment variables. Create a `.env` file in the root directory or set these variables in your deployment environment.

### Core Settings
- `TELEGRAM_TOKEN`: Your Telegram bot token.
- `ADMIN_ONLY_IDS`: Comma-separated list of Telegram user IDs who are authorized to use the bot.
- `TARGET_ACCOUNT_ID`: The account ID (from the database) for the main trading account.
- `DONOR_ACCOUNT_ID`: The account ID for the read-only donor account.

### Trailing Stop Settings
- `TS_REF`: The reference price to use for calculating the trailing stop distance. Can be `entry` or `mark`. Defaults to `entry`.

### Isolated Margin Mirroring
- `MARGIN_MIRROR_ENABLED`: Set to `true` to enable mirroring of isolated margin adjustments. Defaults to `true`.
- `MARGIN_MIN_USDT`: The minimum proportional margin change (in USDT) to trigger a mirroring action. Defaults to `2.0`.
- `MARGIN_MAX_PCT_OF_EQUITY`: A cap on the margin to add, as a percentage of the main account's total equity. Defaults to `0.5` (50%).
- `MARGIN_DEBOUNCE_SEC`: The number of seconds to wait before mirroring another margin change for the same symbol. Defaults to `2`.

## Official Bybit WebSocket V5 Documentation

For detailed information on the WebSocket API, authentication, and subscriptions, refer to the official documentation:

[Bybit V5 WebSocket Documentation](https://bybit-exchange.github.io/docs/v5/ws/connect)
