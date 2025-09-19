# app/balance_snapshots_logger.py
"""
Логирование снимков баланса для анализа динамики
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any

from app.db_session import SessionLocal
from app.db_models import BalanceSnapshots
from app.sys_events_logger import sys_logger

import logging

logger = logging.getLogger(__name__)


class BalanceSnapshotLogger:
    """Класс для логирования снимков баланса"""

    @staticmethod
    def log_balance_snapshot(
        account_id: int,
        asset: str,
        free: float,
        locked: float,
        equity: Optional[float] = None,
    ) -> bool:
        """
        Записывает снимок баланса в БД.

        Args:
            account_id: ID аккаунта (1 для источника, 2 для основного)
            asset: Валюта/актив (USDT, BTC и т.д.)
            free: Свободный баланс
            locked: Заблокированный баланс (в ордерах)
            equity: Эквити (баланс + нереализованный PnL)
        """
        try:
            # Рассчитываем эквити если не передано
            if equity is None:
                equity = free + locked

            with SessionLocal() as session:
                snapshot = BalanceSnapshots(
                    account_id=account_id,
                    asset=asset,
                    free=Decimal(str(free)),
                    locked=Decimal(str(locked)),
                    equity=Decimal(str(equity)),
                    ts=datetime.now(),
                )

                session.add(snapshot)
                session.commit()

                logger.debug(
                    f"Balance snapshot saved: {asset} free={free:.2f} locked={locked:.2f} equity={equity:.2f}"
                )
                return True

        except Exception as e:
            logger.error(f"Failed to log balance snapshot: {e}")
            return False

    @staticmethod
    def get_last_snapshot(
        account_id: int, asset: str = "USDT"
    ) -> Optional[Dict[str, Any]]:
        """Получает последний снимок баланса"""
        try:
            with SessionLocal() as session:
                snapshot = (
                    session.query(BalanceSnapshots)
                    .filter_by(account_id=account_id, asset=asset)
                    .order_by(BalanceSnapshots.ts.desc())
                    .first()
                )

                if snapshot:
                    return {
                        "account_id": snapshot.account_id,
                        "asset": snapshot.asset,
                        "free": float(snapshot.free),
                        "locked": float(snapshot.locked),
                        "equity": float(snapshot.equity),
                        "total": float(snapshot.free + snapshot.locked),
                        "timestamp": snapshot.ts.isoformat(),
                    }
                return None

        except Exception as e:
            logger.error(f"Failed to get last snapshot: {e}")
            return None

    @staticmethod
    def get_balance_history(
        account_id: int, asset: str = "USDT", hours: int = 24, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Получает историю балансов за период"""
        try:
            with SessionLocal() as session:
                since = datetime.now() - timedelta(hours=hours)

                snapshots = (
                    session.query(BalanceSnapshots)
                    .filter(
                        BalanceSnapshots.account_id == account_id,
                        BalanceSnapshots.asset == asset,
                        BalanceSnapshots.ts >= since,
                    )
                    .order_by(BalanceSnapshots.ts.desc())
                    .limit(limit)
                    .all()
                )

                return [
                    {
                        "free": float(s.free),
                        "locked": float(s.locked),
                        "equity": float(s.equity),
                        "total": float(s.free + s.locked),
                        "timestamp": s.ts.isoformat(),
                    }
                    for s in snapshots
                ]

        except Exception as e:
            logger.error(f"Failed to get balance history: {e}")
            return []

    @staticmethod
    def calculate_pnl_24h(account_id: int, asset: str = "USDT") -> Dict[str, float]:
        """Рассчитывает PnL за 24 часа"""
        try:
            with SessionLocal() as session:
                now = datetime.now()
                day_ago = now - timedelta(hours=24)

                # Снимок 24 часа назад
                snapshot_24h = (
                    session.query(BalanceSnapshots)
                    .filter(
                        BalanceSnapshots.account_id == account_id,
                        BalanceSnapshots.asset == asset,
                        BalanceSnapshots.ts <= day_ago,
                    )
                    .order_by(BalanceSnapshots.ts.desc())
                    .first()
                )

                # Текущий снимок
                current = (
                    session.query(BalanceSnapshots)
                    .filter_by(account_id=account_id, asset=asset)
                    .order_by(BalanceSnapshots.ts.desc())
                    .first()
                )

                if snapshot_24h and current:
                    start_equity = float(snapshot_24h.equity)
                    current_equity = float(current.equity)

                    pnl_absolute = current_equity - start_equity
                    pnl_percent = (
                        (pnl_absolute / start_equity * 100) if start_equity > 0 else 0
                    )

                    return {
                        "pnl_24h": pnl_absolute,
                        "pnl_24h_percent": pnl_percent,
                        "start_equity": start_equity,
                        "current_equity": current_equity,
                    }

                return {
                    "pnl_24h": 0.0,
                    "pnl_24h_percent": 0.0,
                    "start_equity": 0.0,
                    "current_equity": 0.0,
                }

        except Exception as e:
            logger.error(f"Failed to calculate 24h PnL: {e}")
            return {
                "pnl_24h": 0.0,
                "pnl_24h_percent": 0.0,
                "start_equity": 0.0,
                "current_equity": 0.0,
            }

    @staticmethod
    def cleanup_old_snapshots(days: int = 30) -> int:
        """Удаляет старые снимки"""
        try:
            with SessionLocal() as session:
                cutoff_date = datetime.now() - timedelta(days=days)

                deleted = (
                    session.query(BalanceSnapshots)
                    .filter(BalanceSnapshots.ts < cutoff_date)
                    .delete()
                )

                session.commit()

                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} old balance snapshots")
                    sys_logger.log_event(
                        "INFO",
                        "BalanceSnapshotLogger",
                        f"Cleaned up old snapshots",
                        {"deleted_count": deleted, "older_than_days": days},
                    )

                return deleted

        except Exception as e:
            logger.error(f"Failed to cleanup old snapshots: {e}")
            return 0


# Глобальный экземпляр
balance_logger = BalanceSnapshotLogger()
