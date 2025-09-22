# app/orders_logger.py
"""
Логирование ордеров и их статусов
"""

import time
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

from app.db_session import SessionLocal
from app.db_models import OrdersLog
from app.sys_events_logger import sys_logger

import logging

logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    """Статусы ордеров"""

    PENDING = "PENDING"
    PLACED = "PLACED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class OrdersLogger:
    """Класс для логирования ордеров"""

    @staticmethod
    def log_order(
        account_id: int,
        symbol: str,
        side: str,
        qty: float,
        status: str,
        exchange_order_id: Optional[str] = None,
        attempt: int = 1,
        latency_ms: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Записывает информацию об ордере в БД.

        Args:
            account_id: ID аккаунта (1 для источника, 2 для основного)
            symbol: Торговая пара
            side: Buy/Sell
            qty: Размер ордера
            status: Статус ордера (PENDING, PLACED, FILLED, etc.)
            exchange_order_id: ID ордера на бирже
            attempt: Номер попытки
            latency_ms: Задержка выполнения в мс
            reason: Причина ошибки/отмены
        """
        try:
            with SessionLocal() as session:
                # Проверяем существование ордера по exchange_order_id
                if exchange_order_id:
                    existing = (
                        session.query(OrdersLog)
                        .filter_by(exchange_order_id=exchange_order_id)
                        .first()
                    )

                    if existing:
                        # Обновляем существующий
                        existing.status = status
                        existing.attempt = attempt
                        if latency_ms:
                            existing.latency_ms = latency_ms
                        if reason:
                            existing.reason = reason
                        session.commit()

                        logger.debug(f"Order updated: {exchange_order_id} -> {status}")
                        return True

                # Создаем новую запись
                order = OrdersLog(
                    account_id=account_id,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    status=status,
                    exchange_order_id=exchange_order_id or f"pending_{int(time.time()*1000)}",
                    attempt=attempt,
                    latency_ms=latency_ms,
                    reason=reason
                )

                session.add(order)
                session.commit()

                logger.info(f"Order logged: {symbol} {side} {qty} - {status}")

                # Логируем в sys_events
                sys_logger.log_event(
                    "INFO",
                    "OrdersLogger",
                    f"Order {status}: {symbol} {side}",
                    {
                        "symbol": symbol,
                        "side": side,
                        "qty": float(qty),
                        "status": status,
                        "account_id": account_id,
                        "attempt": attempt,
                        "exchange_order_id": exchange_order_id,
                    },
                )

                return True

        except Exception as e:
            logger.error(f"Failed to log order: {e}")
            return False

    @staticmethod
    def update_order_status(
        exchange_order_id: str,
        new_status: str,
        latency_ms: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> bool:
        """Обновляет статус существующего ордера"""
        try:
            with SessionLocal() as session:
                order = (
                    session.query(OrdersLog)
                    .filter_by(exchange_order_id=exchange_order_id)
                    .first()
                )

                if not order:
                    logger.warning(f"Order not found: {exchange_order_id}")
                    return False

                order.status = new_status
                if latency_ms:
                    order.latency_ms = latency_ms
                if reason:
                    order.reason = reason

                session.commit()

                logger.info(
                    f"Order status updated: {exchange_order_id} -> {new_status}"
                )
                return True

        except Exception as e:
            logger.error(f"Failed to update order status: {e}")
            return False

    @staticmethod
    def get_order_stats(
        symbol: Optional[str] = None, limit: int = 100
    ) -> Dict[str, Any]:
        """Получает статистику по ордерам"""
        try:
            with SessionLocal() as session:
                query = session.query(OrdersLog)
                if symbol:
                    query = query.filter_by(symbol=symbol)

                orders = query.order_by(OrdersLog.created_at.desc()).limit(limit).all()

                # Считаем статистику
                stats = {
                    "total": len(orders),
                    "pending": sum(1 for o in orders if o.status == "PENDING"),
                    "placed": sum(1 for o in orders if o.status == "PLACED"),
                    "filled": sum(1 for o in orders if o.status == "FILLED"),
                    "failed": sum(
                        1 for o in orders if o.status in ["FAILED", "REJECTED"]
                    ),
                    "avg_latency_ms": 0,
                    "retry_rate": 0,
                }

                # Средняя задержка
                latencies = [o.latency_ms for o in orders if o.latency_ms]
                if latencies:
                    stats["avg_latency_ms"] = sum(latencies) / len(latencies)

                # Процент retry
                retries = sum(1 for o in orders if o.attempt > 1)
                if orders:
                    stats["retry_rate"] = (retries / len(orders)) * 100

                return stats

        except Exception as e:
            logger.error(f"Failed to get order stats: {e}")
            return {}


# Глобальный экземпляр
orders_logger = OrdersLogger()
