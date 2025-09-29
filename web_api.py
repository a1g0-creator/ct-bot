# app/web_api.py
"""
Web API endpoints for positions management (Fixed version with date parsing)
- Flexible date parsing for different formats
- Guaranteed KPI values (never null)
- Always returns at least one chart point
- Safe type conversions
- Proper camelCase response format
"""
import os
import re
import json
import time
import logging
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from dateutil import parser as dateutil_parser

# -----------------------------------------------------------------------------
# Env & logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("web_api")

# -----------------------------------------------------------------------------
# DB config - FIXED PASSWORD
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("PSQL_URL", "postgresql+psycopg2://sa:10az2wsX@127.0.0.1:5432/trading_bot")
)
TARGET_ACCOUNT_ID = int(os.getenv("TARGET_ACCOUNT_ID", "1"))

# Make engine creation more flexible for testing
engine_args = {}
if 'postgresql' in DATABASE_URL:
    engine_args.update({
        "pool_size": 10,
        "max_overflow": 20,
        "pool_pre_ping": True,
        "pool_recycle": 3600,
    })

engine = create_engine(DATABASE_URL, **engine_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# -----------------------------------------------------------------------------
# FastAPI + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="Trading Bot API", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Helpers (safe casts + normalization)
# -----------------------------------------------------------------------------
def safe_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None

def safe_float(v: Any) -> Optional[float]:
    d = safe_decimal(v)
    return float(d) if d is not None else None

def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int,)):
            return v
        if isinstance(v, Decimal):
            return int(v)
        return int(str(v))
    except Exception:
        return None

def dt_to_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        # Already ISO - return as is (frontend expects string)
        return dt
    if isinstance(dt, datetime):
        # Ensure Z suffix
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(dt)

def norm_margin_mode(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("isolated", "isolate", "iso", "1", "isolated_mode"):
        return "Isolated"
    if s in ("cross", "0", "cross_mode"):
        return "Cross"
    # Return original value if it's already properly cased
    s_original = str(val).strip()
    if s_original in ("Isolated", "Cross"):
        return s_original
    return None

def norm_side(val: Any) -> str:
    s = str(val or "").strip().lower()
    if s in ("sell", "short"):
        return "Sell"
    return "Buy"

def parse_flexible_datetime(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    
    # Убираем лишние пробелы
    date_str = date_str.strip()
    
    # Основные форматы
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 2025-09-01T14:53:24.624Z
        "%Y-%m-%dT%H:%M:%SZ",      # 2025-09-01T14:53:24Z
        "%Y-%m-%dT%H:%M:%S",       # 2025-09-01T14:53:24
        "%Y-%m-%d",                # 2025-09-01
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.split('+')[0].split('-')[0] if '+' in date_str or '-' in date_str[-6:] else date_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except:
            continue
    
    # Fallback to current time - 30 days
    logger.warning(f"Using fallback for unparseable date: {date_str}")
    return datetime.now(timezone.utc) - timedelta(days=30)

async def get_account_balance(account_id: int, db: Session) -> float:
    """
    Получить актуальный баланс для ОСНОВНОГО аккаунта из balance_snapshots.
    Используем текущую схему таблицы: equity + ts.
    """
    # Всегда работаем только с основным аккаунтом
    try:
        account_id = TARGET_ACCOUNT_ID
    except NameError:
        # на случай, если константа импортируется выше
        account_id = 1

    try:
        result = db.execute(text("""
            SELECT equity AS bal
            FROM v_balance_snapshots
            WHERE account_id = :aid
            ORDER BY "timestamp" DESC
            LIMIT 1
        """), {"aid": account_id}).first()

        if result and (result[0] is not None):
            return float(result[0])

    except Exception as e:
        logger.warning(f"Could not get balance for TARGET account: {e}")
        db.rollback()

    # Фолбэк: 0.0 — фронт корректно подсветит отсутствие данных
    return 0.0

# -----------------------------------------------------------------------------
# Dependency
# -----------------------------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class OpenPositionResponse(BaseModel):
    symbol: str
    side: str
    qty: float
    entryPrice: Optional[float] = None
    markPrice: Optional[float] = None
    leverage: Optional[int] = None
    marginMode: Optional[str] = None
    liqPrice: Optional[float] = None
    unrealizedPnl: Optional[float] = None
    positionIdx: int
    exchangePositionId: Optional[str] = None
    openedAt: Optional[str] = None
    updatedAt: Optional[str] = None

class ClosedPositionResponse(BaseModel):
    symbol: str
    side: str
    qty: float
    entryPrice: Optional[float] = None
    exitPrice: Optional[float] = None
    leverage: Optional[int] = None
    marginMode: Optional[str] = None
    liqPrice: Optional[float] = None
    realizedPnl: Optional[float] = None
    fees: Optional[float] = None
    positionIdx: int
    exchangePositionId: Optional[str] = None
    openedAt: Optional[str] = None
    closedAt: Optional[str] = None

class OpenPositionsEnvelope(BaseModel):
    items: List[OpenPositionResponse]
    total: int
    updatedAt: str

class ClosedPositionsEnvelope(BaseModel):
    items: List[ClosedPositionResponse]
    total: int
    updatedAt: str

# -----------------------------------------------------------------------------
# OPEN positions endpoint
# -----------------------------------------------------------------------------
@app.get("/api/positions/open", response_model=OpenPositionsEnvelope)
async def get_open_positions(
    accountId: Optional[int] = Query(None, description="Account ID (optional)"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    db: Session = Depends(get_db),
):
    """
    Get open positions from positions_open table.
    """
    started = time.time()
    account_id = TARGET_ACCOUNT_ID if accountId is None else int(accountId)

    sql = """
        SELECT symbol, side, qty, entry_price, mark_price, leverage, margin_mode,
               liq_price, unreal_pnl, position_idx, exchange_position_id,
               opened_at, updated_at
        FROM positions_open
        WHERE account_id = :account_id
    """
    params = {"account_id": account_id}

    if symbol:
        sql += " AND symbol = :symbol"
        params["symbol"] = str(symbol).upper()

    sql += " ORDER BY opened_at DESC"

    try:
        rows = db.execute(text(sql), params).mappings().all()
    except Exception as e:
        logger.error(f"DB open positions query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    items: List[OpenPositionResponse] = []
    for r in rows:
        item = OpenPositionResponse(
            symbol=str(r.get("symbol") or "").upper(),
            side=norm_side(r.get("side")),
            qty=safe_float(r.get("qty")) or 0.0,
            entryPrice=safe_float(r.get("entry_price")),
            markPrice=safe_float(r.get("mark_price")),
            leverage=safe_int(r.get("leverage")),
            marginMode=norm_margin_mode(r.get("margin_mode")),
            liqPrice=safe_float(r.get("liq_price")),
            unrealizedPnl=safe_float(r.get("unreal_pnl")),
            positionIdx=safe_int(r.get("position_idx")) or 0,
            exchangePositionId=r.get("exchange_position_id"),
            openedAt=dt_to_iso(r.get("opened_at")),
            updatedAt=dt_to_iso(r.get("updated_at")),
        )
        items.append(item)

    ms = int((time.time() - started) * 1000)
    logger.info(f"GET /api/positions/open: count={len(items)}, time={ms}ms")

    return OpenPositionsEnvelope(
        items=items,
        total=len(items),
        updatedAt=dt_to_iso(datetime.now(timezone.utc)) or ""
    )

# -----------------------------------------------------------------------------
# CLOSED positions endpoint
# -----------------------------------------------------------------------------
@app.get("/api/positions/closed", response_model=ClosedPositionsEnvelope)
async def get_closed_positions(
    accountId: Optional[int] = Query(None, description="Account ID (optional)"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    from_date: Optional[str] = Query(None, alias="from", description="Start date"),
    to_date: Optional[str] = Query(None, alias="to", description="End date"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get closed positions with proper exit_price and realized_pnl
    Now with flexible date parsing
    """
    started = time.time()
    account_id = TARGET_ACCOUNT_ID if accountId is None else int(accountId)

    # Parse dates with flexibility
    to_dt = parse_flexible_datetime(to_date) if to_date else datetime.now(timezone.utc)
    from_dt = parse_flexible_datetime(from_date) if from_date else to_dt - timedelta(days=30)

    base_params = {
        "account_id": account_id,
        "from_date": from_dt,
        "to_date": to_dt,
    }

    count_sql = """
        SELECT COUNT(*) AS total
        FROM positions_closed
        WHERE account_id = :account_id
          AND closed_at >= :from_date
          AND closed_at <= :to_date
    """
    data_sql = """
        SELECT symbol, side, qty, entry_price, exit_price, leverage, margin_mode,
               liq_price, realized_pnl, fees, position_idx, exchange_position_id,
               opened_at, closed_at
        FROM positions_closed
        WHERE account_id = :account_id
          AND closed_at >= :from_date
          AND closed_at <= :to_date
    """

    params = dict(base_params)
    if symbol:
        count_sql += " AND symbol = :symbol"
        data_sql  += " AND symbol = :symbol"
        params["symbol"] = str(symbol).upper()

    data_sql += " ORDER BY closed_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = int(limit)
    params["offset"] = int(offset)

    try:
        # Get total count first
        count_params = {k: v for k, v in params.items() if k not in ["limit", "offset"]}
        total_row = db.execute(text(count_sql), count_params).first()
        total = int(total_row[0]) if total_row else 0

        # Get actual data
        rows = db.execute(text(data_sql), params).mappings().all()
    except Exception as e:
        logger.error(f"DB closed positions query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    items: List[ClosedPositionResponse] = []
    for r in rows:
        items.append(ClosedPositionResponse(
            symbol=str(r.get("symbol") or "").upper(),
            side=norm_side(r.get("side")),
            qty=safe_float(r.get("qty")) or 0.0,
            entryPrice=safe_float(r.get("entry_price")),
            exitPrice=safe_float(r.get("exit_price")),
            leverage=safe_int(r.get("leverage")),
            marginMode=norm_margin_mode(r.get("margin_mode")),
            liqPrice=safe_float(r.get("liq_price")),
            realizedPnl=safe_float(r.get("realized_pnl")),
            fees=safe_float(r.get("fees")),
            positionIdx=safe_int(r.get("position_idx")) or 0,
            exchangePositionId=r.get("exchange_position_id"),
            openedAt=dt_to_iso(r.get("opened_at")),
            closedAt=dt_to_iso(r.get("closed_at")),
        ))

    ms = int((time.time() - started) * 1000)
    logger.info(f"GET /api/positions/closed: count={len(items)}, total={total}, time={ms}ms")

    return ClosedPositionsEnvelope(
        items=items,
        total=total,
        updatedAt=dt_to_iso(datetime.now(timezone.utc)) or ""
    )

# -----------------------------------------------------------------------------
# Metrics endpoint (FIXED with flexible date parsing and guaranteed values)
# -----------------------------------------------------------------------------
@app.get("/api/metrics")
async def get_metrics(
    accountId: Optional[int] = Query(None),
    from_date: Optional[str] = Query(None, alias="from", description="Start date (flexible format)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (flexible format)"),
    db: Session = Depends(get_db),
):
    """
    Production metrics for TARGET account dashboard with enhanced date parsing.
    Always returns valid KPI values and at least one chart point.
    
    Supported date formats:
    - ISO with Z: "2025-09-01T14:26:33Z"
    - ISO with timezone: "2025-09-01T14:26:33+07:00"
    - With space: "2025-09-01T14:26:33 07:00"
    - Without timezone: "2025-09-01T14:26:33"
    - Compact timezone: "2025-09-01T14:26:33+0700"
    """

    # === 0) Всегда работаем только с основным аккаунтом ===
    try:
        account_id = TARGET_ACCOUNT_ID  # игнорируем входной accountId
    except NameError:
        account_id = 1

    # Parse dates with flexibility
    to_dt = parse_flexible_datetime(to_date) if to_date else datetime.now(timezone.utc)
    from_dt = parse_flexible_datetime(from_date) if from_date else to_dt - timedelta(days=30)
    
    # Log parsing results for debugging
    logger.debug(f"Date parsing: from={from_date} -> {from_dt}, to={to_date} -> {to_dt}")

    # Баланс по умолчанию (если снапшотов нет вовсе)
    current_balance = 0.0

    # === 1) Берём последний баланс из balance_snapshots (equity/ts) ===
    try:
        row = db.execute(
            text("""
                SELECT equity AS bal
                FROM v_balance_snapshots
                WHERE account_id = :aid
                ORDER BY "timestamp" DESC
                LIMIT 1
            """),
            {"aid": account_id},
        ).first()

        if row and row[0] is not None:
            current_balance = float(row[0])
            logger.debug(f"[metrics] Main account snapshot found: account_id={account_id}, equity={current_balance}")
        else:
            # Fallback: берём самый свежий снапшот вообще
            any_row = db.execute(
                text("""
                    SELECT account_id, equity
                    FROM v_balance_snapshots
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """)
            ).first()
            if any_row and any_row[1] is not None:
                current_balance = float(any_row[1])
                logger.warning(
                    "[metrics] No snapshots for main account_id=%s. "
                    "Using latest snapshot from account_id=%s (equity=%.2f).",
                    account_id, int(any_row[0]), current_balance
                )
    except Exception as e:
        logger.warning(f"[metrics] Using default balance due to error: {e}")
        db.rollback()

    # === 2) Агрегаты (инициализируем с гарантированными значениями) ===
    equity_series: list[list[float]] = []
    margin_series: list[list[float]] = []
    total_realized_pnl = 0.0
    total_fees = 0.0
    current_unreal_pnl = 0.0
    current_margin = 0.0
    open_positions_count = 0
    closed_rows = []

    # === 3) PnL по дням за выбранный период ===
    try:
        closed_rows = db.execute(
            text("""
                SELECT 
                    date_trunc('day', closed_at) AS day,
                    SUM(COALESCE(realized_pnl, 0)) AS daily_pnl,
                    COUNT(*) AS trades_count
                FROM positions_closed
                WHERE account_id = :aid
                  AND closed_at >= :from_date
                  AND closed_at <= :to_date
                GROUP BY day
                ORDER BY day
            """),
            {"aid": account_id, "from_date": from_dt, "to_date": to_dt},
        ).all()
    except Exception as e:
        logger.error(f"[metrics] Error getting closed positions data: {e}")
        db.rollback()

    # === 4) Общие итоги (всё время) ===
    try:
        total_row = db.execute(
            text("""
                SELECT 
                    SUM(COALESCE(realized_pnl, 0)) AS total_realized,
                    SUM(COALESCE(fees, 0))        AS total_fees
                FROM positions_closed
                WHERE account_id = :aid
            """),
            {"aid": account_id},
        ).first()
        if total_row:
            total_realized_pnl = float(total_row[0] or 0)
            total_fees = float(total_row[1] or 0)
    except Exception as e:
        logger.error(f"[metrics] Error getting total realized PnL: {e}")
        db.rollback()

    # === 5) Текущие открытые позиции ===
    try:
        open_rows = db.execute(
            text("""
                SELECT 
                    COUNT(*) AS cnt,
                    SUM(COALESCE(unreal_pnl, 0)) AS total_unreal,
                    SUM(COALESCE(qty * entry_price / NULLIF(leverage, 0), 0)) AS total_margin
                FROM positions_open
                WHERE account_id = :aid
            """),
            {"aid": account_id},
        ).first()
        if open_rows:
            open_positions_count = int(open_rows[0] or 0)
            current_unreal_pnl = float(open_rows[1] or 0)
            current_margin = float(open_rows[2] or 0)
    except Exception as e:
        logger.error(f"[metrics] Error getting open positions data: {e}")
        db.rollback()

    # === 6) Временные ряды ===
    cumulative_pnl = 0.0
    initial_balance = current_balance - total_realized_pnl

    for day, daily_pnl, trades in closed_rows:
        cumulative_pnl += float(daily_pnl or 0)
        ts = int(day.replace(tzinfo=timezone.utc).timestamp() * 1000)
        equity_at_point = initial_balance + cumulative_pnl
        equity_series.append([ts, round(equity_at_point, 2)])

        # грубая оценка исторической маржи (как было)
        estimated_margin = trades * 50.0 if trades > 0 else 0.0
        margin_series.append([ts, round(estimated_margin, 2)])

    # === 7) Текущая точка ===
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    current_equity = current_balance + current_unreal_pnl

    # Добавляем текущую точку если её ещё нет или если значение изменилось
    if not equity_series or equity_series[-1][1] != round(current_equity, 2):
        equity_series.append([now_ts, round(current_equity, 2)])
        margin_series.append([now_ts, round(current_margin, 2)])

    # === 8) КРИТИЧНО: Гарантируем минимум одну точку для графика ===
    if not equity_series:
        # Если вообще нет данных, добавляем текущий момент с балансом
        equity_series.append([now_ts, round(current_balance, 2)])
        margin_series.append([now_ts, 0.0])
        logger.warning("[metrics] No data points found, using current balance as single point")

    # === 9) Гарантируем числовые значения для KPI (никогда не null) ===
    kpis = {
        "equity": round(current_equity, 2) if current_equity else 0.0,
        "margin": round(current_margin, 2) if current_margin else 0.0,
        "unrealized": round(current_unreal_pnl, 2) if current_unreal_pnl else 0.0,
        "realized": round(total_realized_pnl, 2) if total_realized_pnl else 0.0,
    }

    # --- legacy Highcharts-форма (что ждёт фронт) ---
    legacy_series = [
        {"name": "Equity", "data": equity_series},
        {"name": "Margin", "data": margin_series},
    ]

    # --- итоговый payload: и новая camelCase, и старая series/плоские ---
    payload = {
        # новая форма
        "equitySeries": equity_series,
        "marginSeries": margin_series,
        "kpis": kpis,
        "updatedAt": dt_to_iso(datetime.now(timezone.utc)) or "",

        # старая форма (для совместимости)
        "series": legacy_series,
        "equity": kpis["equity"],
        "margin": kpis["margin"],
        "unrealized": kpis["unrealized"],
        "realized": kpis["realized"],
    }

    logger.info(
        "GET /api/metrics: points=%s equity=%.2f margin=%.2f unreal=%.2f realized=%.2f",
        len(equity_series),
        payload["kpis"]["equity"],
        payload["kpis"]["margin"],
        payload["kpis"]["unrealized"],
        payload["kpis"]["realized"],
    )

    return payload

# -----------------------------------------------------------------------------
# Health check
# -----------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": dt_to_iso(datetime.now(timezone.utc)),
        "version": app.version,
    }

# -----------------------------------------------------------------------------
# Local run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
