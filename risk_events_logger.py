# app/risk_events_logger.py
"""
Логирование событий управления рисками
"""

from datetime import datetime
from typing import Optional
from decimal import Decimal

from db_session import SessionLocal
from db_models import RiskEvents
from sys_events_logger import sys_logger

import logging

logger = logging.getLogger(__name__)


class RiskEventType:
    """Типы событий рисков"""

    # Drawdown события
    DRAWDOWN_WARNING = "DRAWDOWN_WARNING"
    DRAWDOWN_CRITICAL = "DRAWDOWN_CRITICAL"
    DRAWDOWN_RECOVERY = "DRAWDOWN_RECOVERY"
    EMERGENCY_STOP = "EMERGENCY_STOP"

    # Kelly события
    KELLY_OVERRIDE = "KELLY_OVERRIDE"
    KELLY_REDUCED = "KELLY_REDUCED"

    # Position события
    POSITION_LIMIT = "POSITION_LIMIT"
    POSITION_REJECTED = "POSITION_REJECTED"

    # Trailing Stop события
    TRAILING_STOP_HIT = "TRAILING_STOP_HIT"
    TRAILING_ADJUSTED = "TRAILING_ADJUSTED"

    # Risk mode события
    RECOVERY_MODE_ON = "RECOVERY_MODE_ON"
    RECOVERY_MODE_OFF = "RECOVERY_MODE_OFF"


class RiskEventsLogger:
    """Класс для логирования событий рисков"""

    @staticmethod
    def log_risk_event(
        account_id: int,
        event: str,
        reason: Optional[str] = None,
        value: Optional[float] = None,
    ) -> bool:
        """
        Записывает событие риска в БД.

        Args:
            account_id: ID аккаунта (обычно 2 для основного)
            event: Тип события (из RiskEventType)
            reason: Причина/описание события
            value: Числовое значение (drawdown %, position size, etc)
        """
        try:
            with SessionLocal() as session:
                risk_event = RiskEvents(
                    account_id=account_id,
                    event=event,
                    reason=reason,
                    value=Decimal(str(value)) if value is not None else None,
                )

                session.add(risk_event)
                session.commit()

                logger.info(f"Risk event logged: {event} (value: {value})")

                # Дублируем в sys_events для общей наблюдаемости
                sys_logger.log_event(
                    "WARNING" if "WARNING" in event else "INFO",
                    "RiskManager",
                    f"Risk event: {event}",
                    {
                        "event": event,
                        "reason": reason,
                        "value": float(value) if value is not None else None,
                        "account_id": account_id,
                    },
                )

                return True

        except Exception as e:
            logger.error(f"Failed to log risk event: {e}")
            return False

    @staticmethod
    def log_drawdown_event(
        account_id: int,
        drawdown_percent: float,
        event_type: str,
        current_balance: Optional[float] = None,
        peak_balance: Optional[float] = None,
    ) -> bool:
        """Логирует события связанные с drawdown"""

        reason = f"Drawdown: {drawdown_percent:.2%}"
        if current_balance and peak_balance:
            reason += f" (current: ${current_balance:.2f}, peak: ${peak_balance:.2f})"

        return RiskEventsLogger.log_risk_event(
            account_id=account_id,
            event=event_type,
            reason=reason,
            value=drawdown_percent,
        )

    @staticmethod
    def log_position_rejection(
        account_id: int, symbol: str, requested_size: float, reason: str
    ) -> bool:
        """Логирует отклонение позиции риск-менеджментом"""

        return RiskEventsLogger.log_risk_event(
            account_id=account_id,
            event=RiskEventType.POSITION_REJECTED,
            reason=f"{symbol}: {reason}",
            value=requested_size,
        )

    @staticmethod
    def log_kelly_adjustment(
        account_id: int,
        original_size: float,
        adjusted_size: float,
        kelly_fraction: float,
    ) -> bool:
        """Логирует корректировку размера позиции по Kelly"""

        reduction_percent = (
            (1 - adjusted_size / original_size) if original_size > 0 else 0
        )

        return RiskEventsLogger.log_risk_event(
            account_id=account_id,
            event=RiskEventType.KELLY_REDUCED,
            reason=f"Kelly fraction: {kelly_fraction:.3f}, reduction: {reduction_percent:.1%}",
            value=adjusted_size,
        )

    @staticmethod
    def get_recent_events(
        account_id: Optional[int] = None,
        event_type: Optional[str] = None,
        limit: int = 20,
    ) -> list:
        """Получает последние события рисков"""
        try:
            with SessionLocal() as session:
                query = session.query(RiskEvents)

                if account_id:
                    query = query.filter_by(account_id=account_id)
                if event_type:
                    query = query.filter_by(event=event_type)

                events = query.order_by(RiskEvents.created_at.desc()).limit(limit).all()

                return [
                    {
                        "id": e.id,
                        "event": e.event,
                        "reason": e.reason,
                        "value": float(e.value) if e.value else None,
                        "created_at": e.created_at.isoformat(),
                    }
                    for e in events
                ]

        except Exception as e:
            logger.error(f"Failed to get risk events: {e}")
            return []


# Глобальный экземпляр
risk_events_logger = RiskEventsLogger()
