# app/signals_logger.py
"""
Логирование торговых сигналов с дедупликацией
"""

import hashlib
import json
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.exc import IntegrityError

from db_session import SessionLocal
from db_models import SignalsLog
from sys_events_logger import sys_logger

import logging

logger = logging.getLogger(__name__)


class SignalsLogger:
    """Класс для записи и дедупликации торговых сигналов"""

    @staticmethod
    def generate_dedup_key(symbol: str, side: str, qty: float, timestamp: float) -> str:
        """
        Генерирует ключ дедупликации для сигнала.
        Использует 5-секундное окно для предотвращения дубликатов.
        """
        # Округляем timestamp до 5 секунд для группировки близких сигналов
        time_window = int(timestamp / 5) * 5

        # Создаем уникальный ключ
        key_string = f"{symbol}:{side}:{qty:.8f}:{time_window}"
        return hashlib.sha256(key_string.encode()).hexdigest()

    @staticmethod
    def log_signal(
        account_id: int,
        symbol: str,
        side: str,
        qty: float,
        ext_id: str,
        signal_data: Dict[str, Any],
        timestamp: Optional[float] = None,
    ) -> bool:
        """
        Записывает торговый сигнал в БД с дедупликацией.

        Args:
            account_id: ID аккаунта (1 для источника, 2 для основного)
            symbol: Торговая пара (BTCUSDT)
            side: Направление (Buy/Sell)
            qty: Размер позиции
            ext_id: Внешний ID (order_id или position_id)
            signal_data: Полные данные сигнала для parsed_json
            timestamp: Время получения сигнала

        Returns:
            True если сигнал записан, False если дубликат или ошибка
        """
        try:
            # Используем текущее время если не передано
            if timestamp is None:
                timestamp = datetime.now().timestamp()

            # Генерируем ключ дедупликации
            dedup_key = SignalsLogger.generate_dedup_key(symbol, side, qty, timestamp)

            with SessionLocal() as session:
                # Проверяем существование по dedup_key
                existing = (
                    session.query(SignalsLog).filter_by(dedup_key=dedup_key).first()
                )

                if existing:
                    logger.debug(f"Signal duplicate detected: {dedup_key}")
                    return False

                # Создаем новую запись
                signal = SignalsLog(
                    account_id=account_id,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    ext_id=ext_id,
                    received_at=datetime.fromtimestamp(timestamp),
                    dedup_key=dedup_key,
                    parsed_json=signal_data,
                )

                session.add(signal)
                session.commit()

                logger.info(
                    f"Signal logged: {symbol} {side} {qty} (dedup: {dedup_key[:8]}...)"
                )

                # Логируем в sys_events
                sys_logger.log_event(
                    "INFO",
                    "SignalsLogger",
                    f"New signal: {symbol} {side}",
                    {
                        "symbol": symbol,
                        "side": side,
                        "qty": float(qty),
                        "account_id": account_id,
                        "dedup_key": dedup_key[:8],
                    },
                )

                return True

        except IntegrityError as e:
            # Дубликат по ext_id или dedup_key
            logger.debug(f"Signal duplicate by constraint: {e}")
            return False

        except Exception as e:
            logger.error(f"Failed to log signal: {e}")
            sys_logger.log_error(
                "SignalsLogger",
                f"Signal logging failed: {str(e)}",
                {"symbol": symbol, "side": side, "error": str(e)},
            )
            return False

    @staticmethod
    def get_recent_signals(
        account_id: Optional[int] = None, symbol: Optional[str] = None, limit: int = 20
    ) -> list:
        """Получает последние сигналы из БД"""
        try:
            with SessionLocal() as session:
                query = session.query(SignalsLog)

                if account_id:
                    query = query.filter_by(account_id=account_id)
                if symbol:
                    query = query.filter_by(symbol=symbol)

                signals = (
                    query.order_by(SignalsLog.received_at.desc()).limit(limit).all()
                )

                return [
                    {
                        "id": s.id,
                        "symbol": s.symbol,
                        "side": s.side,
                        "qty": float(s.qty),
                        "ext_id": s.ext_id,
                        "received_at": s.received_at.isoformat(),
                        "account_id": s.account_id,
                    }
                    for s in signals
                ]

        except Exception as e:
            logger.error(f"Failed to get signals: {e}")
            return []


# Глобальный экземпляр
signals_logger = SignalsLogger()
