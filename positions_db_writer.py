import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Union, Tuple, Set
import asyncio

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError


# --- add near imports / top of file ---
try:
    # если у вас TARGET_ACCOUNT_ID объявлен в этих модулях
    from app.config import TARGET_ACCOUNT_ID as _TARGET_ID
except Exception:
    try:
        from config import TARGET_ACCOUNT_ID as _TARGET_ID
    except Exception:
        _TARGET_ID = 1  # безопасный дефолт

def _target_id() -> int:
    """Всегда возвращаем ID основного аккаунта."""
    return int(_TARGET_ID) if isinstance(_TARGET_ID, int) else 1


# ===================== HELPER FUNCTIONS =====================

def safe_decimal(value: Any) -> Optional[Decimal]:
    """Безопасное преобразование в Decimal"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value or value.lower() in ('none', 'null', ''):
                return None
        return Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError):
        return None


def safe_int(value: Any, default: int = 0) -> int:
    """Безопасное преобразование в int"""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return default
        return int(float(str(value)))
    except (ValueError, TypeError):
        return default


def _normalize_timestamp(ts: Any) -> Optional[datetime]:
    """Нормализация таймстемпа с поддержкой миллисекунд"""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    
    try:
        ts_int = int(ts)
        # Если больше 10^10, то это миллисекунды
        if ts_int > 10**10:
            ts_int = ts_int // 1000
        return datetime.fromtimestamp(ts_int, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _ensure_json_str(data: Any) -> str:
    """Гарантированное преобразование в JSON-строку"""
    if data is None:
        return '{}'
    if isinstance(data, str):
        try:
            json.loads(data)
            return data
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"raw": data})
    try:
        return json.dumps(data, default=str)
    except (TypeError, ValueError):
        return '{}'


def _parse_json_if_str(data: Any) -> Dict[str, Any]:
    """Безопасный парсинг JSON из строки или возврат dict"""
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _norm_side(side: Any) -> str:
    """Нормализация стороны позиции"""
    if not side:
        return "Buy"
    side_str = str(side).strip().capitalize()
    if side_str in ("Buy", "Long", "1"):
        return "Buy"
    elif side_str in ("Sell", "Short", "2", "-1"):
        return "Sell"
    return "Buy"


def _normalize_margin_mode(mode: Any) -> str:
    """Нормализация margin mode для соответствия CHECK constraint в БД"""
    if not mode:
        return "Cross"
    mode_str = str(mode).strip().upper()
    if "ISOLATED" in mode_str or mode_str == "1":
        return "Isolated"
    return "Cross"


def _quantize(value: Decimal, precision: str = "0.00000001") -> Decimal:
    """Квантизация Decimal с заданной точностью"""
    if value is None:
        return Decimal("0")
    return value.quantize(Decimal(precision), rounding=ROUND_HALF_UP)


def _now_utc() -> datetime:
    """Текущее время в UTC"""
    return datetime.now(timezone.utc)


# ===================== MAIN CLASS =====================

class PositionsDBWriter:
    """
    Класс для записи позиций в БД с поддержкой:
    - Ленивой инициализации SessionLocal
    - Устойчивой обработки различных форматов данных
    - Полной нормализации полей для UI
    - Идемпотентного закрытия позиций
    """
    
    def __init__(self, session_factory=None, logger=None):
        """
        Args:
            session_factory: Фабрика сессий SQLAlchemy (опционально)
            logger: Логгер (опционально)
        """
        self._session_factory = session_factory
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def set_session_factory(self, session_factory):
        """Установка фабрики сессий извне"""
        self._session_factory = session_factory
        self.logger.info("Session factory set externally")
    
    @property
    def SessionLocal(self):
        """
        Ленивое получение фабрики сессий.
        Пытается использовать установленную, либо импортировать из стандартных мест.
        """
        if self._session_factory is None:
            self._init_session_factory()
        return self._session_factory
    
    def _init_session_factory(self):
        """Ленивая инициализация фабрики сессий"""
        if self._session_factory is not None:
            return
        
        # Пытаемся импортировать из стандартных мест
        try:
            from app.db_session import SessionLocal
            self._session_factory = SessionLocal
            self.logger.info("Session factory initialized from app.db_session")
            return
        except ImportError:
            pass
        
        try:
            from db_session import SessionLocal
            self._session_factory = SessionLocal
            self.logger.info("Session factory initialized from db_session")
            return
        except ImportError:
            pass
        
        # Если не нашли - критическая ошибка
        error_msg = (
            "Failed to initialize session factory. "
            "Please set it explicitly via set_session_factory() "
            "or ensure app.db_session.SessionLocal exists"
        )
        self.logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    @contextmanager
    def _session(self):
        """
        Контекстный менеджер для работы с сессией.
        Автоматически коммитит или откатывает транзакцию.
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            session.close()

    def save_balance_snapshot(self, account_id: int, balance_data: Dict[str, Any]) -> None:
        """
        ВАЖНО: Теперь ИСПОЛЬЗУЕМ переданный account_id, 
        а НЕ вызываем _target_id() который возвращает неправильное значение
        """
        try:
            with self._session() as db:
                # БЫЛО: aid = _target_id()  
                # СТАЛО: используем переданный account_id напрямую
                aid = account_id  # <-- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ!
            
                upnl = safe_decimal(
                    balance_data.get("unrealizedPnl")
                    or balance_data.get("unrealisedPnl")
                    or balance_data.get("unrealized")
                ) or Decimal("0")

                # БАЗА: только wallet (без UPNL)
                wallet_base = safe_decimal(
                    balance_data.get("totalWalletBalance")
                    or balance_data.get("walletBalance")
                    or balance_data.get("availableBalance")
                    or balance_data.get("balance")
                )

                total_equity = safe_decimal(
                    balance_data.get("totalEquity")
                    or balance_data.get("equity")
                )

                # если дали только equity — превращаем её в wallet_base
                if wallet_base is None and total_equity is not None:
                    wallet_base = total_equity - upnl

                if wallet_base is None:
                    self.logger.warning("save_balance_snapshot: no wallet fields in data, skip (account=%s)", aid)
                    return

                asset = str(balance_data.get("asset") or balance_data.get("currency") or "USDT").upper()
                free = safe_decimal(balance_data.get("availableBalance")) or Decimal("0")
                locked = safe_decimal(
                    balance_data.get("totalInitialMargin")
                    or balance_data.get("used_margin")
                ) or Decimal("0")

                ts_naive = _now_utc().replace(tzinfo=None)

                db.execute(text("""
                    INSERT INTO balance_snapshots (account_id, asset, free, locked, equity, ts)
                    VALUES (:aid, :asset, :free, :locked, :equity, :ts)
                """), {
                    "aid": aid,  # Теперь используется переданный account_id
                    "asset": asset,
                    "free": free,
                    "locked": locked,
                    "equity": wallet_base,  # Храним БАЗУ (wallet без UPNL)
                    "ts": ts_naive,
                })

                self.logger.info(
                    "Balance snapshot saved (account_id=%d): asset=%s base=%s free=%s locked=%s ts=%s",
                    aid,  # Логируем какой account_id реально используется
                    asset, wallet_base, free, locked, ts_naive
                )
        except Exception as e:
            self.logger.error("Failed to save balance snapshot: %s", e)
    
    def save_wallet_event(self, account_id: int, event_data: Dict[str, Any]) -> None:
        """
        Сохраняет wallet событие (если таблица есть). Пишем всегда в TARGET.
        """
        try:
            with self._session() as db:
                aid = _target_id()  # только основной аккаунт
                event_type = event_data.get("topic", "wallet")

                total_wallet = safe_decimal(
                    event_data.get("totalWalletBalance") or
                    event_data.get("data", {}).get("totalWalletBalance")
                )
                available = safe_decimal(
                    event_data.get("availableBalance") or
                    event_data.get("data", {}).get("availableBalance")
                )

                db.execute(text("""
                    INSERT INTO wallet_events (
                        account_id, event_type, total_wallet_balance,
                        available_balance, raw, created_at
                    ) VALUES (
                        :account_id, :event_type, :total_wallet,
                        :available, CAST(:raw AS jsonb), :created_at
                    )
                """), {
                    "account_id": aid,
                    "event_type": event_type,
                    "total_wallet": total_wallet,
                    "available": available,
                    "raw": _ensure_json_str(event_data),
                    "created_at": _now_utc(),
                })

                self.logger.debug("Wallet event saved: account=%s type=%s", aid, event_type)

        except Exception as e:
            # если таблицы нет — просто лог (как и раньше)
            self.logger.error(f"Failed to save wallet event: {e}")
    
    def get_latest_balance(self, account_id: int) -> Optional[float]:
        """
        Возвращает последний equity основного аккаунта из balance_snapshots
        по схеме (equity, ts).
        """
        try:
            with self._session() as db:
                aid = _target_id()  # только основной аккаунт
                row = db.execute(text("""
                    SELECT equity
                    FROM balance_snapshots
                    WHERE account_id = :aid
                    ORDER BY ts DESC
                    LIMIT 1
                """), {"aid": aid}).first()

                if row and row[0] is not None:
                    return float(row[0])

        except Exception as e:
            self.logger.error(f"Failed to get latest balance: {e}")

        return None

    def normalize_open(self, src: Dict[str, Any]) -> Dict[str, Any]:
        """
        Унифицированная нормализация открытой позиции:
        - Поддерживает REST/WS payload
        - Вытягивает leverage, margin_mode, liq_price, entry_price, mark_price из любых доступных полей
        """
        def first(keys):
            for k in keys:
                v = src.get(k)
                dv = safe_decimal(v)
                if dv is not None:
                    return dv
            return None

        qty_dec = safe_decimal(src.get("qty", src.get("size"))) or Decimal("0")

        leverage = first(["leverage", "leverageE2"])
        margin_mode = _normalize_margin_mode(
            src.get("marginMode") or src.get("margin_mode") or src.get("tradeMode")
        )
        liq_price = first(["liq_price", "liqPrice", "liquidationPrice"])
        entry_price = first(["entry_price", "entryPrice", "avgPrice", "avgEntryPrice", "price"])
        mark_price  = first(["mark_price", "markPrice", "lastPrice", "indexPrice"])

        return {
            "account_id": safe_int(src.get("account_id", 1), 1),
            "symbol": str(src.get("symbol", "")).upper(),
            "side": _norm_side(src.get("side")),
            "qty": qty_dec,
            "entry_price": entry_price,
            "mark_price": mark_price,
            "leverage": leverage,
            "margin_mode": margin_mode,
            "liq_price": liq_price,
            "unreal_pnl": safe_decimal(
                src.get("unreal_pnl", src.get("unrealisedPnl", src.get("unrealizedPnl")))
            ),
            "position_idx": safe_int(src.get("position_idx", src.get("positionIdx")), 0),
            "exchange_position_id": (
                src.get("exchange_position_id") or src.get("positionId") or src.get("position_id")
            ),
            "raw": _ensure_json_str(src.get("raw", src)),
            "opened_at": _normalize_timestamp(
                src.get("opened_at") or src.get("createdTime") or src.get("created_at")
            ) or _now_utc(),
            "updated_at": _normalize_timestamp(src.get("updated_at")) or _now_utc(),
        }
    
    async def upsert_position(self, data: Dict[str, Any], account_id: Optional[int] = None) -> None:
        """
        Асинхронная обертка для update_position_sync (alias для совместимости).
        """
        await self.update_position(data, account_id)

    def _upsert_open_position_sync(self, db: Session, norm: Dict[str, Any], leverage: Optional[Decimal] = None) -> None:
        """
        Upserts an open position, allowing an explicit leverage override.
        """
        try:
            # If an explicit leverage is passed, it overrides the one in 'norm'.
            if leverage is not None:
                norm['leverage'] = leverage

            db.execute(text("""
                INSERT INTO positions_open (
                    account_id, symbol, side, qty,
                    entry_price, mark_price, leverage, margin_mode,
                    liq_price, unreal_pnl, position_idx, exchange_position_id,
                    raw, opened_at, updated_at
                ) VALUES (
                    :account_id, :symbol, :side, :qty,
                    :entry_price, :mark_price, :leverage, :margin_mode,
                    :liq_price, :unreal_pnl, :position_idx, :exchange_position_id,
                    CAST(:raw AS jsonb), :opened_at, :updated_at
                )
                ON CONFLICT (account_id, symbol, position_idx) DO UPDATE SET
                    side = EXCLUDED.side,
                    qty = EXCLUDED.qty,
                    entry_price = COALESCE(EXCLUDED.entry_price, positions_open.entry_price),
                    mark_price = COALESCE(EXCLUDED.mark_price, positions_open.mark_price),
                    leverage = EXCLUDED.leverage, -- Always take the new leverage value
                    margin_mode = COALESCE(EXCLUDED.margin_mode, positions_open.margin_mode),
                    liq_price = COALESCE(EXCLUDED.liq_price, positions_open.liq_price),
                    unreal_pnl = EXCLUDED.unreal_pnl,
                    exchange_position_id = COALESCE(EXCLUDED.exchange_position_id, positions_open.exchange_position_id),
                    raw = EXCLUDED.raw,
                    updated_at = EXCLUDED.updated_at
            """), norm)
            
            self.logger.debug(
                "Upserted open position: account_id=%s, symbol=%s, idx=%s, qty=%s, leverage=%s",
                norm.get("account_id"), norm.get("symbol"), 
                norm.get("position_idx"), norm.get("qty"), norm.get('leverage')
            )
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to upsert open position: {e}")
            raise
    
    def _close_position_sync(self, db: Session, data: Dict[str, Any], already_in_tx: bool = False, leverage: Optional[Decimal] = None) -> None:
        """
        Moves a position from open to closed, allowing an explicit leverage override.
        """
        # 1) читаем текущую open-позицию
        cur = db.execute(text("""
            SELECT id, account_id, symbol, side, qty, entry_price, leverage, margin_mode,
                   liq_price, position_idx, exchange_position_id, raw, opened_at
            FROM positions_open
            WHERE account_id=:account_id AND symbol=:symbol AND position_idx=:position_idx
            LIMIT 1
        """), {
            "account_id": safe_int(data.get("account_id"), 1),
            "symbol": str(data.get("symbol", "")).upper(),
            "position_idx": safe_int(data.get("position_idx"), 0),
        }).mappings().first()

        if not cur:
            self.logger.warning(
                "Position to close not found: account_id=%s, symbol=%s, idx=%s",
                data.get("account_id"), data.get("symbol"), data.get("position_idx")
            )
            return

        existing = dict(cur)
        entry_price = safe_decimal(existing.get("entry_price"))
        qty         = safe_decimal(existing.get("qty")) or Decimal("0")
        side        = _norm_side(existing.get("side"))

        # 2) подготавливаем raw
        raw_close = _parse_json_if_str(data.get("raw", {}))
        raw_open  = _parse_json_if_str(existing.get("raw", {}))

        def _first_decimal(d: Dict[str, Any], keys: List[str]) -> Optional[Decimal]:
            for k in keys:
                v = d.get(k)
                x = safe_decimal(v)
                if x is not None:
                    return x
            return None

        # 3) exit_price с фоллбеками
        exit_price = safe_decimal(data.get("mark_price"))
        if exit_price is None:
            exit_price = _first_decimal(raw_close, [
                "exitPrice", "avgExitPrice", "avgPrice", "markPrice", "lastPrice", "price", "fillPrice",
                "closePrice", "execPrice"
            ])
        if exit_price is None:
            exit_price = _first_decimal(raw_open, [
                "exitPrice", "avgExitPrice", "avgPrice", "markPrice", "lastPrice", "price", "fillPrice"
            ])
        if exit_price is None:
            exit_price = entry_price

        # 4) fees
        fees = _first_decimal(raw_close, [
            "fees", "totalFee", "cumFee", "commission", "execFee", "closeFee"
        ])

        # 5) lev/mode/liq с фоллбеком
        if leverage is None: # Only if not explicitly provided
            leverage_close = _first_decimal(raw_close, ["leverage", "leverageE2"])
            leverage = leverage_close if leverage_close is not None else safe_decimal(existing.get("leverage"))

        margin_mode_close = _normalize_margin_mode(
            raw_close.get("marginMode") or raw_close.get("margin_mode") or raw_close.get("tradeMode")
        )
        margin_mode = margin_mode_close or _normalize_margin_mode(existing.get("margin_mode"))
        liq_close = _first_decimal(raw_close, ["liqPrice", "liquidationPrice"])
        liq_price = liq_close if liq_close is not None else safe_decimal(existing.get("liq_price"))

        # 6) realized_pnl
        realized_pnl = _first_decimal(raw_close, [
            "realizedPnl", "realisedPnl", "closedPnl", "cumRealisedPnl", "cumRealizedPnl"
        ])
        if realized_pnl is None and all([entry_price, exit_price]) and qty > 0:
            if side == "Buy":
                realized_pnl = (exit_price - entry_price) * qty
            else:
                realized_pnl = (entry_price - exit_price) * qty
        if realized_pnl is not None:
            realized_pnl = _quantize(realized_pnl, "0.00000001")

        # 7) insert в closed
        db.execute(text("""
            INSERT INTO positions_closed (
                account_id, symbol, side, qty,
                entry_price, exit_price, leverage, margin_mode,
                liq_price, realized_pnl, fees, exchange_position_id,
                raw_open, raw_close, opened_at, closed_at, position_idx
            ) VALUES (
                :account_id, :symbol, :side, :qty,
                :entry_price, :exit_price, :leverage, :margin_mode,
                :liq_price, :realized_pnl, :fees, :exchange_position_id,
                CAST(:raw_open AS jsonb), CAST(:raw_close AS jsonb),
                :opened_at, :closed_at, :position_idx
            )
            ON CONFLICT DO NOTHING
        """), {
            "account_id": existing["account_id"],
            "symbol": existing["symbol"],
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "leverage": leverage,
            "margin_mode": margin_mode,
            "liq_price": liq_price,
            "realized_pnl": realized_pnl,
            "fees": fees,
            "exchange_position_id": existing.get("exchange_position_id"),
            "raw_open": _ensure_json_str(raw_open),
            "raw_close": _ensure_json_str(raw_close),
            "opened_at": existing.get("opened_at"),
            "closed_at": _now_utc(),
            "position_idx": existing.get("position_idx", 0),
        })

        # 8) удаляем из open
        db.execute(text("DELETE FROM positions_open WHERE id=:id"), {"id": existing["id"]})

        self.logger.info(
            "Closed position: account_id=%s, symbol=%s, idx=%s, entry=%s, exit=%s, liq=%s, lev=%s, mode=%s, fees=%s, realized_pnl=%s",
            existing["account_id"], existing["symbol"], existing.get("position_idx", 0),
            entry_price, exit_price, liq_price, leverage, margin_mode, fees, realized_pnl
        )

    # ============================================================================
    # НОВЫЙ МЕТОД: Идемпотентное закрытие позиции
    # ============================================================================
    def close_position_idempotent(self, position_data: Dict[str, Any]) -> None:
        """
        Идемпотентное закрытие позиции с UPSERT логикой.
        Использует уникальный ключ: (account_id, symbol, position_idx, side, closed_at_sec)
        """
        try:
            with self._session() as db:
                account_id = position_data.get('account_id', 1)
                symbol = str(position_data.get('symbol', '')).upper()
                position_idx = int(position_data.get('position_idx', 0))
                side = position_data.get('side', 'Buy')
                
                # Генерируем временной бакет для идемпотентности (секунды)
                closed_at_sec = position_data.get('closed_at_sec', int(time.time()))
                
                # Сначала ищем открытую позицию для получения entry данных
                open_position = db.execute(text("""
                    SELECT id, entry_price, qty, leverage, margin_mode, liq_price, 
                           opened_at, exchange_position_id, raw
                    FROM positions_open
                    WHERE account_id = :account_id 
                    AND symbol = :symbol 
                    AND position_idx = :position_idx
                    LIMIT 1
                """), {
                    'account_id': account_id,
                    'symbol': symbol,
                    'position_idx': position_idx
                }).first()
                
                if not open_position:
                    # Если открытой позиции нет, пробуем обновить существующую закрытую
                    self._update_existing_closed_position(db, position_data, closed_at_sec)
                    return
                
                # Подготавливаем данные для UPSERT
                entry_price = float(open_position[1] or 0)
                qty = float(open_position[2] or position_data.get('qty', 0))
                leverage = int(open_position[3] or position_data.get('leverage', 1))
                margin_mode = open_position[4] or position_data.get('margin_mode', 'cross')
                liq_price = float(open_position[5] or 0)
                opened_at = open_position[6]
                exchange_position_id = open_position[7] or position_data.get('exchange_position_id')
                raw_open = open_position[8] or {}
                
                # Данные закрытия
                exit_price = float(position_data.get('exit_price', 
                                 position_data.get('markPrice', 
                                 position_data.get('lastPrice', 0))))
                
                realized_pnl = float(position_data.get('realized_pnl', 
                                   position_data.get('realisedPnl', 
                                   position_data.get('cumRealisedPnl', 0))))
                
                fees = float(position_data.get('fees', 
                           position_data.get('totalFee', 
                           position_data.get('cumFee', 0))))
                
                raw_close = position_data.get('raw', position_data)
                closed_at = datetime.now(timezone.utc)
                
                # ИДЕМПОТЕНТНЫЙ UPSERT с уникальным ключом
                db.execute(text("""
                    INSERT INTO positions_closed (
                        account_id, symbol, side, qty,
                        entry_price, exit_price, leverage, margin_mode,
                        liq_price, realized_pnl, fees, exchange_position_id,
                        raw_open, raw_close, opened_at, closed_at, 
                        position_idx, closed_at_sec
                    ) VALUES (
                        :account_id, :symbol, :side, :qty,
                        :entry_price, :exit_price, :leverage, :margin_mode,
                        :liq_price, :realized_pnl, :fees, :exchange_position_id,
                        :raw_open::jsonb, :raw_close::jsonb,
                        :opened_at, :closed_at, 
                        :position_idx, :closed_at_sec
                    )
                    ON CONFLICT (account_id, symbol, position_idx, side, closed_at_sec) 
                    DO UPDATE SET
                        qty = GREATEST(EXCLUDED.qty, positions_closed.qty),
                        exit_price = COALESCE(NULLIF(EXCLUDED.exit_price, 0), positions_closed.exit_price),
                        realized_pnl = CASE 
                            WHEN ABS(EXCLUDED.realized_pnl) > ABS(positions_closed.realized_pnl) 
                            THEN EXCLUDED.realized_pnl 
                            ELSE positions_closed.realized_pnl 
                        END,
                        fees = GREATEST(EXCLUDED.fees, positions_closed.fees),
                        leverage = COALESCE(NULLIF(EXCLUDED.leverage, 0), positions_closed.leverage),
                        raw_close = EXCLUDED.raw_close,
                        updated_at = NOW()
                """), {
                    'account_id': account_id,
                    'symbol': symbol,
                    'side': side,
                    'qty': qty,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'leverage': leverage,
                    'margin_mode': margin_mode,
                    'liq_price': liq_price,
                    'realized_pnl': realized_pnl,
                    'fees': fees,
                    'exchange_position_id': exchange_position_id,
                    'raw_open': json.dumps(raw_open) if isinstance(raw_open, dict) else raw_open,
                    'raw_close': json.dumps(raw_close) if isinstance(raw_close, dict) else raw_close,
                    'opened_at': opened_at,
                    'closed_at': closed_at,
                    'position_idx': position_idx,
                    'closed_at_sec': closed_at_sec
                })
                
                # Удаляем из открытых позиций
                db.execute(text("""
                    DELETE FROM positions_open 
                    WHERE id = :id
                """), {'id': open_position[0]})
                
                db.commit()
                
                self.logger.info(
                    f"DB_UPSERT_CLOSED OK: {symbol}/{position_idx}/{side}/t{closed_at_sec} "
                    f"exit={exit_price:.2f} pnl={realized_pnl:.2f}"
                )
                
        except Exception as e:
            self.logger.error(f"DB_UPSERT_CLOSED ERR: {e}", exc_info=True)
            raise
    
    def _update_existing_closed_position(self, db: Session, position_data: Dict, closed_at_sec: int):
        """
        Обновление существующей закрытой позиции (если открытой уже нет)
        """
        try:
            account_id = position_data.get('account_id', 1)
            symbol = str(position_data.get('symbol', '')).upper()
            position_idx = int(position_data.get('position_idx', 0))
            side = position_data.get('side', 'Buy')
            
            exit_price = float(position_data.get('exit_price', 0))
            realized_pnl = float(position_data.get('realized_pnl', 0))
            
            # Пробуем обновить существующую запись в том же временном окне
            result = db.execute(text("""
                UPDATE positions_closed SET
                    exit_price = CASE 
                        WHEN :exit_price > 0 AND :exit_price != exit_price 
                        THEN :exit_price 
                        ELSE exit_price 
                    END,
                    realized_pnl = CASE 
                        WHEN ABS(:realized_pnl) > ABS(realized_pnl) 
                        THEN :realized_pnl 
                        ELSE realized_pnl 
                    END,
                    raw_close = :raw_close::jsonb,
                    updated_at = NOW()
                WHERE account_id = :account_id
                AND symbol = :symbol
                AND position_idx = :position_idx
                AND side = :side
                AND closed_at_sec = :closed_at_sec
            """), {
                'account_id': account_id,
                'symbol': symbol,
                'position_idx': position_idx,
                'side': side,
                'closed_at_sec': closed_at_sec,
                'exit_price': exit_price,
                'realized_pnl': realized_pnl,
                'raw_close': json.dumps(position_data)
            })
            
            if result.rowcount > 0:
                self.logger.info(f"Updated existing closed position: {symbol}/{position_idx}")
            
        except Exception as e:
            self.logger.error(f"Update existing closed position error: {e}")

    # ============================================================================
    # НОВЫЙ МЕТОД: Обновление цены выхода для закрытой позиции
    # ============================================================================
    def update_closed_position_price(self, account_id: int, symbol: str, position_idx: int, exec_price: float):
        """
        Уточнение цены выхода для закрытой позиции (из execution события)
        """
        try:
            with self._session() as db:
                # Находим последнюю закрытую позицию в пределах последних 5 минут
                result = db.execute(text("""
                    UPDATE positions_closed SET
                        exit_price = CASE 
                            WHEN :exec_price > 0 THEN :exec_price 
                            ELSE exit_price 
                        END,
                        updated_at = NOW()
                    WHERE account_id = :account_id
                    AND symbol = :symbol
                    AND position_idx = :position_idx
                    AND closed_at > NOW() - INTERVAL '5 minutes'
                    AND (exit_price = 0 OR exit_price IS NULL OR ABS(exit_price - :exec_price) > 0.01)
                """), {
                    'account_id': account_id,
                    'symbol': symbol.upper(),
                    'position_idx': position_idx,
                    'exec_price': exec_price
                })
                
                if result.rowcount > 0:
                    self.logger.info(f"Updated exit price for {symbol}: {exec_price}")
                    
        except Exception as e:
            self.logger.error(f"Update exit price error: {e}")

    async def reconcile_positions(self, *args, **kwargs) -> None:
        """
        Асинхронная универсальная сверка открытых позиций (REST).
        """
        # --- Устойчивый разбор аргументов ---
        positions = None
        account_id = None
        
        # Обработка позиционных аргументов
        if args:
            if len(args) == 1:
                arg = args[0]
                if isinstance(arg, (list, tuple)):
                    positions = arg
                elif isinstance(arg, int):
                    account_id = arg
                    positions = []
                else:
                    positions = arg
                    
            elif len(args) >= 2:
                arg1, arg2 = args[0], args[1]
                
                if isinstance(arg1, (list, tuple)) and isinstance(arg2, (int, str, type(None))):
                    positions = arg1
                    account_id = safe_int(arg2) if arg2 is not None else None
                elif isinstance(arg1, (int, str)) and isinstance(arg2, (list, tuple)):
                    account_id = safe_int(arg1)
                    positions = arg2
                elif isinstance(arg1, (list, tuple)):
                    positions = arg1
                    account_id = safe_int(arg2) if arg2 is not None else None
                else:
                    positions = arg1 if not isinstance(arg1, int) else []
                    account_id = safe_int(arg2) if arg2 else safe_int(arg1) if isinstance(arg1, int) else None
        
        # Обработка именованных аргументов
        if "positions" in kwargs:
            positions = kwargs["positions"]
        if "account_id" in kwargs:
            account_id = safe_int(kwargs["account_id"]) if kwargs["account_id"] is not None else None
        
        # Валидация
        if positions is None:
            self.logger.warning("reconcile_positions called without positions, using empty list")
            positions = []
        
        if not isinstance(positions, (list, tuple)):
            if isinstance(positions, int):
                self.logger.error(f"reconcile_positions: positions is int({positions}), expected list")
                positions = []
            else:
                try:
                    positions = list(positions)
                except (TypeError, ValueError):
                    self.logger.error(f"reconcile_positions: cannot iterate positions of type {type(positions)}")
                    positions = []
        
        if account_id is None:
            if positions and isinstance(positions[0], dict):
                account_id = safe_int(positions[0].get("account_id"), 1)
            else:
                account_id = 1
        else:
            account_id = safe_int(account_id, 1)
        
        self.logger.debug(f"reconcile_positions: processing {len(positions)} positions for account_id={account_id}")
        
        # Запускаем синхронную работу с БД в executor
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        await loop.run_in_executor(None, self._reconcile_positions_sync, positions, account_id)
    
    def _reconcile_positions_sync(self, positions: list, account_id: int) -> None:
        """
        Синхронная часть reconcile_positions для работы с БД.
        """
        try:
            with self._session() as db:
                seen_keys = set()
                
                for raw_pos in positions:
                    try:
                        if not isinstance(raw_pos, dict):
                            self.logger.warning(f"Skipping non-dict position: {type(raw_pos)}")
                            continue
                        
                        norm = self.normalize_open({**raw_pos, "account_id": account_id})
                        symbol = str(norm.get("symbol", "")).upper()
                        idx = safe_int(norm.get("position_idx"), 0)
                        qty = safe_decimal(norm.get("qty")) or Decimal("0")
                        
                        if qty > 0:
                            seen_keys.add((symbol, idx))
                            self._upsert_open_position_sync(db, norm)
                        else:
                            db.execute(text("""
                                DELETE FROM positions_open
                                WHERE account_id=:aid AND symbol=:sym AND position_idx=:idx
                            """), {"aid": account_id, "sym": symbol, "idx": idx})
                            self.logger.debug(f"Deleted zero-qty position: {account_id}/{symbol}/{idx}")
                    except Exception as e:
                        self.logger.exception(f"reconcile_positions: failed to process item: {e}")
                
                # Удаление исчезнувших позиций
                try:
                    existing = db.execute(text("""
                        SELECT symbol, position_idx FROM positions_open WHERE account_id=:aid
                    """), {"aid": account_id}).mappings().all()
                    
                    to_delete = []
                    for row in existing:
                        key = (row["symbol"], safe_int(row["position_idx"], 0))
                        if key not in seen_keys:
                            to_delete.append(key)
                    
                    for sym, idx in to_delete:
                        db.execute(text("""
                            DELETE FROM positions_open
                            WHERE account_id=:aid AND symbol=:sym AND position_idx=:idx
                        """), {"aid": account_id, "sym": sym, "idx": idx})
                        self.logger.debug(f"Deleted disappeared position: {account_id}/{sym}/{idx}")
                    
                    if to_delete:
                        self.logger.info(f"Cleaned up {len(to_delete)} disappeared positions for account_id={account_id}")
                        
                except Exception as e:
                    self.logger.exception(f"reconcile_positions: cleanup failed: {e}")
                
                self.logger.info(f"Reconciled positions: account_id={account_id}, open_now={len(seen_keys)}")
                
        except Exception as e:
            self.logger.exception(f"reconcile_positions: critical error: {e}")
    
    async def update_position(self, data: Dict[str, Any], account_id: Optional[int] = None) -> None:
        """
        Асинхронная обертка для update_position_sync.
        Сохраняет совместимость со старым API.
        """
        if account_id is not None and "account_id" not in data:
            data = {**data, "account_id": account_id}
        
        # Запускаем синхронный метод в executor для совместимости
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Если нет event loop, выполняем синхронно
            self.update_position_sync(data)
            return
            
        await loop.run_in_executor(None, self.update_position_sync, data)

    def update_position_sync(self, data: Dict[str, Any]) -> None:
        """
        Синхронное обновление позиции (открытие/обновление/закрытие).
        Использует контекстный менеджер _session для автоматического управления транзакциями.
        """
        if not data:
            self.logger.warning("update_position_sync called with empty data")
            return
        
        try:
            with self._session() as db:
                self._update_position_sync(db, data, already_in_tx=True)
        except Exception as e:
            self.logger.error(f"update_position_sync error: {e}")
            raise
    
    def _update_position_sync(self, db: Session, data: Dict[str, Any], already_in_tx: bool = False) -> None:
        """
        Внутренний метод обновления позиции.
        Определяет тип операции и вызывает соответствующий метод.
        """
        try:
            # Определяем тип операции
            qty = safe_decimal(data.get("qty", data.get("size")))
            
            if qty is None or qty == 0:
                # Закрытие позиции
                self._close_position_sync(db, data, already_in_tx)
            else:
                # Открытие или обновление
                norm = self.normalize_open(data)
                self._upsert_open_position_sync(db, norm)
                
        except Exception as e:
            self.logger.exception(f"_update_position_sync failed: {e}")
            if not already_in_tx:
                raise

    async def log_open(self, data: Dict[str, Any], leverage: Optional[int] = None):
        """Asynchronously logs the opening/modification of a position with a specific leverage."""
        if not data:
            self.logger.warning("log_open called with empty data")
            return

        leverage_decimal = Decimal(str(leverage)) if leverage is not None else None

        def sync_task():
            with self._session() as db:
                norm = self.normalize_open(data)
                self._upsert_open_position_sync(db, norm, leverage=leverage_decimal)
                self.logger.info(f"DB_LOG_OPEN: Position {norm.get('symbol')} logged to DB with leverage={leverage}.")

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sync_task)
        except Exception as e:
            self.logger.error(f"log_open async error: {e}", exc_info=True)

    async def log_close(self, data: Dict[str, Any], leverage: Optional[int] = None):
        """Asynchronously logs the closing of a position with a specific leverage."""
        if not data:
            self.logger.warning("log_close called with empty data")
            return

        leverage_decimal = Decimal(str(leverage)) if leverage is not None else None

        def sync_task():
            with self._session() as db:
                self._close_position_sync(db, data, leverage=leverage_decimal)
                self.logger.info(f"DB_LOG_CLOSE: Position {data.get('symbol')} closure logged to DB with leverage={leverage}.")

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sync_task)
        except Exception as e:
            self.logger.error(f"log_close async error: {e}", exc_info=True)


# ===================== GLOBAL INSTANCE =====================

# Создаём глобальный экземпляр для обратной совместимости
# Ленивая инициализация SessionLocal произойдёт при первом использовании
positions_writer = PositionsDBWriter()

# Экспортируем для обратной совместимости
__all__ = ['PositionsDBWriter', 'positions_writer']