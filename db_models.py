# trading_bot/app/db_models.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from typing import Optional
from enum import Enum

from sqlalchemy import (
    String, Integer, DateTime, Numeric, Text, UniqueConstraint, Index, ForeignKey, func
)
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# --- ДОБАВЛЕНО: совместимое перечисление уровней событий ---
class EventLevelEnum(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
# ------------------------------------------------------------

class Base(DeclarativeBase):
    pass

# ========== users ==========
class Users(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        # имена совпадают с чекером (любой like по имени пройдёт)
        UniqueConstraint("email", name="uq_users_email"),
    )

# ========== accounts ==========
class Accounts(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    env: Mapped[str] = mapped_column(String(16), nullable=False, default="testnet")  # 'prod' | 'testnet'
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    user = relationship("Users", lazy="joined")

    __table_args__ = (
        Index("ix_accounts_user_id", "user_id"),
    )

# ========== api_credentials ==========
class ApiCredentials(Base):
    __tablename__ = "api_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    enc_key: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    enc_secret: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    nonce: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    key_hint: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        UniqueConstraint("account_id", name="uq_api_credentials_account_id"),
        Index("ix_api_credentials_account_id", "account_id"),
    )

# ========== risk_profiles ==========
class RiskProfiles(Base):
    __tablename__ = "risk_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False, unique=True)
    daily_loss_limit: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    per_symbol_limit: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    equity_scale: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        UniqueConstraint("account_id", name="uq_risk_profiles_account_id"),
    )

# ========== signals_log ==========
class SignalsLog(Base):
    __tablename__ = "signals_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    side: Mapped[str] = mapped_column(String(12), nullable=False)  # 'BUY'|'SELL'
    qty: Mapped[float] = mapped_column(Numeric(36, 18), nullable=False)
    ext_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    # НОВОЕ: поле для дедупликации
    dedup_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    parsed_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        UniqueConstraint("ext_id", name="uq_signals_ext_id"),
        UniqueConstraint("dedup_key", name="uq_signals_dedup_key"),  # НОВОЕ
        Index("ix_signals_log_account_id", "account_id"),
        Index("ix_signals_log_symbol", "symbol"),
        Index("ix_signals_log_dedup_key", "dedup_key"),  # НОВОЕ
    )


# ========== orders_log ==========
class OrdersLog(Base):
    __tablename__ = "orders_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    side: Mapped[str] = mapped_column(String(12), nullable=False)
    qty: Mapped[float] = mapped_column(Numeric(36, 18), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    exchange_order_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    # НОВЫЕ поля для ретраев
    attempt: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        UniqueConstraint("exchange_order_id", name="uq_orders_exchange_id"),
        Index("ix_orders_log_account_id", "account_id"),
        Index("ix_orders_log_symbol", "symbol"),
        Index("ix_orders_log_status", "status"),  # НОВОЕ
    )

# ========== risk_events ==========
class RiskEvents(Base):
    __tablename__ = "risk_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    event: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    value: Mapped[Optional[float]] = mapped_column(Numeric(36, 18), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        Index("ix_risk_events_account_id", "account_id"),
    )

class BalanceSnapshots(Base):
    __tablename__ = "balance_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.id"), nullable=False)
    asset: Mapped[str] = mapped_column(String(20), nullable=False)  # 'USDT', 'BTC', etc
    free: Mapped[float] = mapped_column(Numeric(36, 18), nullable=False)
    locked: Mapped[float] = mapped_column(Numeric(36, 18), default=0, nullable=False)
    equity: Mapped[float] = mapped_column(Numeric(36, 18), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    account = relationship("Accounts", lazy="joined")

    __table_args__ = (
        Index("ix_balance_snapshots_account_id", "account_id"),
        Index("ix_balance_snapshots_ts", "ts"),
        Index("ix_balance_snapshots_account_ts", "account_id", "ts"),  # Составной индекс
    )

# ========== sys_events ==========
class SysEvents(Base):
    __tablename__ = "sys_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False)       # INFO|WARN|ERROR
    component: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(String(255), nullable=False)
    details_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_sys_events_component", "component"),
        Index("ix_sys_events_level", "level"),
    )
