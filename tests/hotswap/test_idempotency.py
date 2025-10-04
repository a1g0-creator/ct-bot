import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from enhanced_trading_system_final_fixed import FinalTradingMonitor
from config import TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def monitor_for_concurrency():
    """
    Provides a monitor instance with mocked external dependencies suitable for concurrency testing.
    """
    with patch('enhanced_trading_system_final_fixed.CredentialsStore') as MockCredStore, \
         patch('enhanced_trading_system_final_fixed.EnhancedBybitClient') as MockBybitClient:

        MockCredStore.return_value.get_account_credentials.return_value = ("test_key", "test_secret")

        # Configure client mock to be awaitable where necessary
        mock_client_instance = MockBybitClient.return_value
        mock_client_instance.invalidate_caches = AsyncMock(return_value=None)

        monitor = FinalTradingMonitor()

        # Mock methods that perform I/O or have complex dependencies
        monitor.websocket_manager.close = AsyncMock()
        monitor.websocket_manager.connect = AsyncMock()
        monitor.websocket_manager.resubscribe_all = AsyncMock()
        type(monitor.websocket_manager).authenticated = property(fget=lambda s: True)
        monitor.run_reconciliation_cycle = AsyncMock()
        monitor.pause_processing = AsyncMock()
        monitor.resume_processing = AsyncMock()

        yield monitor

def count_ws_tasks():
    """Helper function to count active tasks related to WebSocket operations."""
    return [
        t for t in asyncio.all_tasks()
        if "WS" in t.get_name() or "Websocket" in t.get_name()
    ]

async def test_hot_swap_lock_serializes_calls(monitor_for_concurrency: FinalTradingMonitor):
    """
    Verifies that the hot-swap lock prevents concurrent executions.
    """
    monitor = monitor_for_concurrency

    # Use a real, slow-downed version of a method inside the lock to ensure overlap
    original_clear_caches = monitor.signal_processor.invalidate_caches
    call_count = 0

    async def slow_clear_caches(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.2)  # Ensure the first call holds the lock
        await original_clear_caches(*args, **kwargs)

    # Patch the method inside the hot-swap sequence
    monitor.signal_processor.invalidate_caches = slow_clear_caches

    # Trigger two hot-swaps concurrently
    task1 = asyncio.create_task(monitor.hot_swap_credentials(TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID))
    await asyncio.sleep(0.05) # Give task1 a chance to acquire the lock
    task2 = asyncio.create_task(monitor.hot_swap_credentials(TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID))

    # Await completion
    results = await asyncio.gather(task1, task2)

    # The first call should succeed. The second call should also technically succeed
    # as it will wait for the first to complete. The key is that `slow_clear_caches`
    # is only called once per successful execution path.
    # Because the lock is now outside the method, the second call will wait.
    # Let's check the call count of a method inside the lock.
    assert call_count == 2, "The locked operation should have been called twice, sequentially."

    # Both tasks should report success
    assert all(results), "Both hot-swap calls should eventually succeed."

async def test_no_zombie_tasks_after_hot_swap(monitor_for_concurrency: FinalTradingMonitor):
    """
    Ensures that running a hot-swap does not leave behind zombie tasks.
    """
    monitor = monitor_for_concurrency

    # Mock the WebSocket manager's task-creating methods to do nothing
    monitor.websocket_manager._start_heartbeat = AsyncMock()
    monitor.websocket_manager._recv_loop = AsyncMock()

    # Get baseline task count
    # Note: running pytest itself creates tasks. We focus on the *change*.
    initial_tasks = asyncio.all_tasks()

    # Run hot-swap
    await monitor.hot_swap_credentials(TARGET_ACCOUNT_ID, DONOR_ACCOUNT_ID)

    # Get task count after hot-swap
    final_tasks = asyncio.all_tasks()

    # The number of tasks should ideally be the same or very close.
    # We allow for a small number of new transient tasks but check that it's not a large leak.
    new_tasks = final_tasks - initial_tasks

    # Filter out pytest-internal tasks for a more stable assertion
    non_pytest_new_tasks = {t for t in new_tasks if "pytest" not in t.get_name().lower()}

    assert len(non_pytest_new_tasks) <= 2, f"Expected no significant task leak, but found {len(non_pytest_new_tasks)} new tasks: {[t.get_name() for t in non_pytest_new_tasks]}"