# risk_state_classes.py
from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Optional, Callable, Awaitable, Any


class RiskMode(Enum):
    NORMAL = "normal"
    RECOVERY = "recovery"
    EMERGENCY = "emergency"


class RiskDataContext:
    """
    Хранит последние валидные значения риска и правила их валидности.
    Используется DrawdownController/Stage2 для решений об алертах/блокировке.
    """

    def __init__(self, config: dict):
        # Конфиг может приходить в обёртке {'RISK': {...}, 'SAFE_MODE': {...}}
        risk_cfg = (config or {}).get("RISK", config or {})
        safe_cfg = (config or {}).get("SAFE_MODE", {})

        self.drawdown_limit: float = float(risk_cfg.get("drawdown_limit", 0.15))  # 15% по умолчанию
        self.data_stale_ttl_sec: float = float(safe_cfg.get("data_stale_ttl_sec", 10.0))
        self.risk_confirm_reads: int = int(safe_cfg.get("risk_confirm_reads", 2))
        self.risk_hysteresis: float = float(safe_cfg.get("risk_hysteresis", 0.01))  # 1%

        # Состояние данных
        self._last_equity: Optional[float] = None
        self._last_equity_ts: float = 0.0

        # Дневной high для дневной DD (его может вести внешняя логика; мы дополнительно храним)
        self._daily_high: Optional[float] = None
        self.daily_dd_percent: float = 0.0

        # Текущее "наблюдаемое" значение DD (может быть max(total,daily,position))
        self.dd_percent: float = 0.0

        # Подтверждения DD последовательными валидными чтениями
        self._confirm_counter: int = 0
        self._last_dd_over_limit: bool = False

        # Счётчик ошибок чтения — влияет на is_data_reliable()
        self._consecutive_failures: int = 0
        self._last_failure_ts: float = 0.0
        self._last_success_ts: float = 0.0

    # --- обновления данных ---

    def update_equity(self, equity: Optional[float]) -> None:
        """Запоминаем последнее валидное значение баланса (equity)."""
        if equity is None or equity <= 0:
            # не обновляем success-метки; валидности нет
            return
        self._last_equity = float(equity)
        self._last_equity_ts = time.time()
        self._consecutive_failures = 0
        self._last_success_ts = self._last_equity_ts

    def update_daily_dd(self, equity: float, daily_high: float) -> None:
        """Обновляем дневную просадку и текущее наблюдаемое значение DD."""
        if equity is None or equity <= 0 or daily_high is None or daily_high <= 0:
            return
        self._daily_high = max(daily_high, equity)
        self.daily_dd_percent = max(0.0, (self._daily_high - equity) / self._daily_high)
        # текущее наблюдаемое значение dd (если внешняя логика не передаёт total — используем дневную)
        self.dd_percent = max(self.dd_percent, self.daily_dd_percent)

    # --- валидность данных / подтверждение DD ---

    def is_data_reliable(self) -> bool:
        """Считаем данные надёжными, если:
        - есть последнее валидное значение equity и оно не устарело
        - нет серии отказов, делающей данные сомнительными
        """
        now = time.time()
        if self._last_equity is None:
            return False
        if (now - self._last_equity_ts) > self.data_stale_ttl_sec:
            return False
        # допускаем 1-2 сбоя подряд как «шум»
        if self._consecutive_failures >= 3:
            return False
        return True

    def register_failure(self) -> None:
        self._consecutive_failures += 1
        self._last_failure_ts = time.time()

    def register_success(self) -> None:
        self._consecutive_failures = 0
        self._last_success_ts = time.time()

    def dd_confirmed(self, positional_dd: Optional[float] = None) -> bool:
        """
        Подтверждение просадки: значение выше лимита N последовательных раз
        при надёжных данных. Используем гистерезис (—1%) для отлипания.
        """
        if positional_dd is not None:
            observed = max(self.dd_percent, float(positional_dd))
        else:
            observed = self.dd_percent

        over_limit = observed >= self.drawdown_limit
        hysteresis_ok = observed >= max(0.0, self.drawdown_limit - self.risk_hysteresis)

        if self.is_data_reliable() and hysteresis_ok:
            if over_limit:
                self._confirm_counter += 1
                self._last_dd_over_limit = True
            else:
                # если вернулись ниже порога-гистерезиса — сброс
                if not hysteresis_ok:
                    self._confirm_counter = 0
                    self._last_dd_over_limit = False
        else:
            # сомнительные данные не подтверждают
            self._confirm_counter = 0
            self._last_dd_over_limit = False

        return self._confirm_counter >= max(1, self.risk_confirm_reads)


class HealthSupervisor:
    """
    Небольшой оркестратор состояний: NORMAL/RECOVERY/EMERGENCY.
    Решает, можно ли открывать позиции и когда триггерить Emergency Stop.
    """

    def __init__(
        self,
        config: dict,
        risk_ctx: RiskDataContext,
        notifier: Optional[Callable[[str], Awaitable[Any] | Any]] = None,
    ):
        safe_cfg = (config or {}).get("SAFE_MODE", {})
        self.risk_ctx = risk_ctx
        self.notifier = notifier

        # эвристики для переключения режимов
        self.fail_window_sec = float(safe_cfg.get("fail_window_sec", 30.0))
        self.max_consecutive_failures_for_recovery = int(safe_cfg.get("max_failures_for_recovery", 3))

        self.mode: RiskMode = RiskMode.NORMAL
        self.copy_enabled: bool = True  # может управляться извне

        self._failure_log: list[float] = []

    # --- телеметрия API ---

    async def on_api_success(self) -> None:
        self.risk_ctx.register_success()
        # при устойчивом восстановлении возвращаем NORMAL
        if self.mode is not RiskMode.EMERGENCY:
            self.mode = RiskMode.NORMAL

    async def on_api_failure(self, reason: str = "") -> None:
        self.risk_ctx.register_failure()
        now = time.time()
        self._failure_log.append(now)
        # вычистим старые записи
        cutoff = now - self.fail_window_sec
        self._failure_log = [t for t in self._failure_log if t >= cutoff]

        if len(self._failure_log) >= self.max_consecutive_failures_for_recovery:
            # сеть/апи деградируют → уходим в RECOVERY, но не блокируем закрытия
            if self.mode is not RiskMode.EMERGENCY:
                self.mode = RiskMode.RECOVERY

    # --- решения по торговле ---

    def can_open_positions(self) -> bool:
        """Гейт на открытия: нельзя в EMERGENCY; в RECOVERY решает внешняя логика."""
        if not self.copy_enabled:
            return False
        if self.mode is RiskMode.EMERGENCY:
            return False
        # если данные ненадёжны — не блокируем открыть/закрыть «жизненно важное» действие,
        # блокировка открытий реализована в вызывающем коде по месту (см. Patch H/I).
        return True

    async def trigger_emergency_stop(self, reason: str = "Confirmed drawdown breach") -> None:
        self.mode = RiskMode.EMERGENCY
        self.copy_enabled = False
        if self.notifier:
            try:
                maybe_coro = self.notifier(
                    f"🛑 **EMERGENCY STOP**\nПричина: {reason}\nРежим: {self.mode.value}"
                )
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
            except Exception:
                # нельзя валить супервизор на отправке алерта
                pass
