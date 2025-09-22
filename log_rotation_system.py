# trading_bot/log_rotation_system.py
from __future__ import annotations
import os, gzip, shutil, logging, time
from pathlib import Path
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

_DEFAULT_FMT = "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def setup_logging(
    base_dir: Path | str,
    level: str | int = "INFO",
    retention_days: int = 30,
    logger_name: str | None = None,
) -> logging.Logger:
    """
    Единая настройка логирования:
    - Файлы в base_dir
    - Один «текущий» файл bot.log, каждые сутки ротация на bot_YYYYMMDD.log.gz
    - Ретеншен по дням (ручной, чтобы не зависеть от внутренней логики backupCount)
    """
    base_dir = _ensure_dir(Path(base_dir))
    lvl = level if isinstance(level, int) else getattr(logging, str(level).upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)

    # Убираем старые FileHandler, чтобы не плодить дубли
    for h in list(root.handlers):
        if isinstance(h, logging.FileHandler):
            root.removeHandler(h)

    formatter = logging.Formatter(_DEFAULT_FMT, _DEFAULT_DATEFMT)
    current_file = base_dir / "bot.log"

    handler = TimedRotatingFileHandler(
        filename=current_file,
        when="midnight",
        interval=1,
        backupCount=0,        # чистим вручную (см. enforce_retention)
        encoding="utf-8",
        delay=True,
        utc=False,
    )

    # Имена ротаций: bot_YYYYMMDD.log.gz
    def _namer(default_name: str) -> str:
        # default_name: /path/bot.log.YYYY-mm-dd
        date_part = default_name.rsplit(".", 1)[-1]
        try:
            d = datetime.strptime(date_part, "%Y-%m-%d").strftime("%Y%m%d")
        except Exception:
            d = datetime.now().strftime("%Y%m%d")
        return str(Path(default_name).parent / f"bot_{d}.log")

    def _rotator(source: str, dest: str) -> None:
        gz = dest + ".gz"
        with open(source, "rb") as f_in, gzip.open(gz, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        try:
            os.remove(source)
        except FileNotFoundError:
            pass

    handler.namer = _namer
    handler.rotator = _rotator
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Консоль — для локального запуска (systemd пишет в journald)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    # Первичная зачистка по ретеншену
    enforce_retention(base_dir, retention_days)

    lg = logging.getLogger(logger_name or "trading_bot")
    lg.info("Logging initialized → %s (level=%s, retention=%sd)", base_dir, lvl, retention_days)
    return lg

def enforce_retention(base_dir: Path | str, retention_days: int = 30) -> int:
    """Удаляем архивы bot_YYYYMMDD.log.gz старше retention_days."""
    base_dir = Path(base_dir)
    cutoff = time.time() - retention_days * 86400
    removed = 0
    for p in base_dir.glob("bot_*.log.gz"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
                removed += 1
        except FileNotFoundError:
            pass
    return removed
