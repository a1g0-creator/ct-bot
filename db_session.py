# app/db_session.py
# -*- coding: utf-8 -*-
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# 1) Поддерживаем оба имени переменной, плюс PSQL_URL (для psql CLI)
DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("DB_URL")
    or os.getenv("PSQL_URL")    # не обязательно, но вдруг ты его используешь
)

if not DB_URL:
    raise RuntimeError("DATABASE_URL is not set (also checked DB_URL, PSQL_URL)")

# 2) Делаем коннект «живучим»
engine = create_engine(
    DB_URL,
    future=True,
    pool_pre_ping=True,     # прозванивать соединение перед запросом
    pool_recycle=1800,      # раз в 30 минут реюз соединения
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)

def check_db_health() -> bool:
    """Простой self-check подключения к БД."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.exception("DB health check failed: %s", e)
        return False

