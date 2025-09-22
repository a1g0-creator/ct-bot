# app/sys_events_logger.py
"""
Централизованный логгер для записи событий в sys_events
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import contextmanager

from app.db_session import SessionLocal
from app.db_models import SysEvents

logger = logging.getLogger(__name__)

class SystemEventLogger:
    """Класс для централизованного логирования системных событий в БД"""

    @staticmethod
    def log_event(
        level: str,
        component: str, 
        message: str,
        details: Optional[Dict[str, Any]] = None,
        session=None
    ) -> bool:
        """
        Записывает событие в sys_events
        
        Args:
            level: INFO, WARN, ERROR
            component: Компонент системы (Stage1, Stage2, TelegramBot, etc)
            message: Сообщение события
            details: Дополнительные данные в JSON
            session: Существующая сессия БД или None
        
        Returns:
            True если успешно записано
        """
        try:
            # Убираем sensitive данные из details
            if details:
                details = SystemEventLogger._mask_sensitive_data(details.copy())

            if session:
                # Используем переданную сессию
                event = SysEvents(
                    level=level,
                    component=component,
                    message=message,
                    details_json=details,
                    created_at=datetime.now()
                )
                session.add(event)
                return True
            else:
                # Создаем новую сессию
                with SessionLocal() as db_session:
                    event = SysEvents(
                        level=level,
                        component=component,
                        message=message,
                        details_json=details,
                        created_at=datetime.now()
                    )
                    db_session.add(event)
                    db_session.commit()
                return True

        except Exception as e:
            logger.error(f"Failed to log event to sys_events: {e}")
            return False

    @staticmethod
    def _mask_sensitive_data(data: dict) -> dict:
        """Маскирует чувствительные данные"""
        sensitive_keys = ['api_key', 'api_secret', 'secret', 'password', 'token', 'key']

        for key in data:
            if any(s in key.lower() for s in sensitive_keys):
                if isinstance(data[key], str) and len(data[key]) > 8:
                    data[key] = data[key][:4] + "***" + data[key][-4:]
                else:
                    data[key] = "***masked***"
            elif isinstance(data[key], dict):
                data[key] = SystemEventLogger._mask_sensitive_data(data[key])

        return data

    @staticmethod
    def log_startup(component: str, details: Optional[dict] = None):
        """Логирует запуск компонента"""
        return SystemEventLogger.log_event(
            "INFO", 
            component, 
            f"{component} started",
            details
        )

    @staticmethod
    def log_shutdown(component: str, details: Optional[dict] = None):
        """Логирует остановку компонента"""
        return SystemEventLogger.log_event(
            "INFO",
            component,
            f"{component} shutdown completed", 
            details
        )

    @staticmethod
    def log_error(component: str, error: str, details: Optional[dict] = None):
        """Логирует ошибку"""
        if details is None:
            details = {}
        details['error'] = str(error)
        return SystemEventLogger.log_event(
            "ERROR",
            component,
            f"Error in {component}",
            details
        )

    @staticmethod
    def log_warning(component: str, message: str, details: Optional[dict] = None):
        """Логирует предупреждение"""
        return SystemEventLogger.log_event(
            "WARNING", 
            component, 
            message, 
            details
        )

    @staticmethod
    def log_reconnect(component: str, service: str, attempt: int = 1):
        """Логирует попытку реконнекта"""
        return SystemEventLogger.log_event(
            "WARNING",
            component,
            f"Reconnecting to {service}",
            {"service": service, "attempt": attempt},
        )

    @staticmethod
    def log_telegram_command(command: str, user_id: int, success: bool = True):
        """Логирует выполнение Telegram команды"""
        return SystemEventLogger.log_event(
            "INFO" if success else "WARNING",
            "TelegramBot",
            f"Command {command} executed",
            {"command": command, "user_id": user_id, "success": success},
        )

# Создаем глобальный экземпляр
sys_logger = SystemEventLogger()
