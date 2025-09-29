# Test Plan for Copy Trading E2E Scenarios

This document outlines the detailed test plan for the five critical copy trading scenarios. The tests will be executed in a mocked environment to ensure deterministic results.

---

## S1: Margin Type (Cross/Isolated)

*   **Objective:** Verify that the follower's margin mode and leverage are correctly synchronized with the donor's before a trade is opened.
*   **Preconditions:**
    *   Donor account is configured for `BTCUSDT`.
    *   Donor sets margin mode to `Isolated` with `10x` leverage.
    *   Follower account is in `Cross` margin mode with `5x` leverage for `BTCUSDT`.
*   **Action:**
    1.  The test runner simulates a signal from the donor to open a `BUY` position in `BTCUSDT`.
    2.  The copy trading system receives the signal.
*   **Expected Outcome & Assertions:**
    1.  The system must call `EnhancedBybitClient.set_leverage` for the follower account with `symbol='BTCUSDT'` and `leverage='10'`.
    2.  The system must call the appropriate Bybit endpoint to set the margin mode to `Isolated` for `BTCUSDT` on the follower account.
    3.  *Crucially*, these two calls must happen *before* the `EnhancedBybitClient.place_order` call for the opening trade.
    4.  **Assert:** A mock of `set_leverage` was called with the correct parameters.
    5.  **Assert:** A mock of the set margin mode endpoint was called with the correct parameters.
    6.  **Assert:** The `place_order` call is made after the setup calls.

---

## S2: Trailing Stop for SELL

*   **Objective:** Ensure that a trailing stop on a donor's `SELL` (short) position is correctly mirrored on the follower's account.
*   **Preconditions:**
    *   Donor account opens a `SELL` position on `ETHUSDT` at a price of $2000.
    *   A trailing stop is attached with an activation price of $1950 and a distance of $10.
*   **Action:**
    1.  The test runner simulates the position opening signal from the donor.
    2.  The copy trading system processes the signal and opens the short position on the follower account.
*   **Expected Outcome & Assertions:**
    1.  The system should calculate the correct parameters for a trailing stop on a short position.
    2.  The system must call `EnhancedBybitClient.place_order` (or a dedicated trading stop endpoint) with the correct trailing stop parameters for the follower.
    3.  The `trailingStop` value should be calculated correctly based on the price and distance.
    4.  The `triggerDirection` for a short position's trailing stop should be correctly set (e.g., `1` for Rise).
    5.  **Assert:** The call to set the trailing stop contains the correct `trailingStop`, `activePrice`, and other relevant parameters for a short sale.

---

## S3: Side Inversion

*   **Objective:** Verify that a `SELL` trade on the donor account results in a `SELL` trade on the follower account, without being inverted to a `BUY`.
*   **Preconditions:**
    *   Donor account has no open position for `SOLUSDT`.
*   **Action:**
    1.  The test runner simulates a signal from the donor to open a `SELL` position in `SOLUSDT`.
    2.  The copy trading system processes the signal.
*   **Expected Outcome & Assertions:**
    1.  The system must call `EnhancedBybitClient.place_order` for the follower account.
    2.  The `side` parameter in the `place_order` call must be `Sell`.
    3.  The `reduceOnly` parameter should be `False` (as this is a new position).
    4.  The `positionIdx` should be correctly set for a short position (e.g., `2` in hedge mode).
    5.  **Assert:** The `side` parameter of the `place_order` call is `'Sell'`.
    6.  **Assert:** The final position on the follower's account is short.

---

## S4: Kelly and Risk Management

*   **Objective:** Check that the follower's position size is correctly calculated according to the Kelly Criterion and other risk settings.
*   **Preconditions:**
    *   `AdvancedKellyCalculator` is enabled with `conservative_factor=0.5`.
    *   Follower account has a balance of `$10,000`.
    *   The Kelly formula, based on historical data, recommends a fraction of `0.2` (20% of capital).
    *   The donor opens a position with a notional value of `$5000`.
    *   The exchange filter for `BTCUSDT` has `qtyStep=0.001` and `minOrderQty=0.001`.
*   **Action:**
    1.  The test runner simulates the `$5000` notional trade signal from the donor.
    2.  The copy trading system calculates the follower's position size.
*   **Expected Outcome & Assertions:**
    1.  The system calculates the base position size: `$10,000 * 0.2 (Kelly) = $2000`.
    2.  The system applies the `conservative_factor`: `$2000 * 0.5 = $1000`.
    3.  The system respects other risk parameters (e.g., `max_copy_size`).
    4.  The final quantity is correctly rounded down to the `qtyStep`.
    5.  **Assert:** The `qty` parameter in the `place_order` call matches the expected calculated and rounded value.
    6.  **Assert:** The calculated size does not violate any configured risk limits (e.g., `max_exposure_per_symbol`).

---

## S5: Web UI Display

*   **Objective:** Ensure the Web API provides correct and synchronized data for display in the frontend.
*   **Preconditions:**
    *   A `BUY` position of `0.5 BTCUSDT` has been successfully copied to the follower account.
    *   Entry price: $50,000. Leverage: 10x. Margin Mode: Isolated.
    *   Current Mark Price: $52,000.
*   **Action:**
    1.  The test runner makes a GET request to `/api/positions/open`.
    2.  The test runner makes a GET request to `/api/metrics`.
*   **Expected Outcome & Assertions:**
    1.  The `/api/positions/open` response contains an item for `BTCUSDT` with:
        *   `symbol`: "BTCUSDT"
        *   `side`: "Buy"
        *   `qty`: 0.5
        *   `entryPrice`: 50000
        *   `markPrice`: 52000
        *   `leverage`: 10
        *   `marginMode`: "Isolated"
        *   `unrealizedPnl`: 1000 ( (52000 - 50000) * 0.5 )
    2.  The `/api/metrics` response correctly reflects the current equity, including the unrealized P&L from the open position.
    3.  **Assert:** All fields in the API response match the expected values.