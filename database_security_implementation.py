#!/usr/bin/env python3
"""
🔐 ПРОФЕССИОНАЛЬНАЯ БАЗА ДАННЫХ И СИСТЕМА БЕЗОПАСНОСТИ
Версия 2.0 - Production-Ready Trading System Database (ИСПРАВЛЕННАЯ)

🎯 КОМПОНЕНТЫ:
- ✅ PostgreSQL с SQLAlchemy 2.0+ и Alembic миграции
- ✅ AES-GCM шифрование API ключей
- ✅ Comprehensive аудит и логирование
- ✅ Multi-environment поддержка (testnet/prod)
- ✅ Production-ready session management
- ✅ Enterprise-grade безопасность

📁 СТРУКТУРА:
project_root/
  db/
    alembic.ini
    env.py
    versions/
  app/
    db_models.py
    db_session.py
    crypto_store.py
    config_loader.py
"""

import os
import sys
import asyncio
import logging
import secrets
import base64
from pathlib import Path
from datetime import datetime as _dt
from typing import Optional, Dict, Any, List, Tuple
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- УСТОЙЧИВЫЕ ИМПОРТЫ БД (без EventLevelEnum) ---
try:
    # Пакетный запуск
    from app.db_session import SessionLocal
    from app.db_models import ApiCredentials, SysEvents
except Exception:
    try:
        # Топ-левел запуск
        from db_session import SessionLocal
        from db_models import ApiCredentials, SysEvents
    except Exception as e:
        logger.exception("DB imports failed")
        raise


# ================================
# СОЗДАНИЕ СТРУКТУРЫ ПРОЕКТА
# ================================

def create_project_structure():
    """Создание структуры каталогов проекта"""
    base_path = Path.cwd()
    
    # Создаем основные директории
    directories = [
        'db',
        'db/versions',
        'app',
        'logs',
        'backups'
    ]
    
    for directory in directories:
        dir_path = base_path / directory
        dir_path.mkdir(exist_ok=True, parents=True)
        logger.info(f"✅ Создана директория: {directory}")
    
    return base_path


# ================================
# КЛАСС ДЛЯ РАБОТЫ С ШИФРОВАНИЕМ
# ================================

class CredentialsStore:
    """
    Enhanced credentials store по ТЗ с интеграцией в базу данных
    Шифрует/расшифровывает ключи с записью в БД
    """
    
    def __init__(self):
        """Инициализация с мастер-ключом из BOT_MASTER_KEY"""
        key_b64 = os.getenv("BOT_MASTER_KEY")
        if not key_b64:
            raise ValueError("BOT_MASTER_KEY is required")
            
        # Обработка разных форматов ключа
        if key_b64 == "DEV_RANDOM":
            # Для разработки - случайный ключ
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            self._key = AESGCM(os.urandom(32))
            logger.warning("Using DEV_RANDOM key - not for production!")
        else:
            try:
                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                # Попытка декодировать как base64
                key_bytes = base64.b64decode(key_b64)
                if len(key_bytes) != 32:
                    raise ValueError("Decoded key must be 32 bytes")
                self._key = AESGCM(key_bytes)
                logger.info("Using base64 decoded master key")
            except Exception:
                # Если не base64, используем первые 32 байта как UTF-8
                key_bytes = key_b64.encode("utf-8")[:32]
                if len(key_bytes) < 32:
                    # Дополняем до 32 байт
                    key_bytes = key_bytes.ljust(32, b'\0')
                self._key = AESGCM(key_bytes)
                logger.info("Using UTF-8 encoded master key")
    
    def encrypt_pair(self, api_key: str, api_secret: str) -> Tuple[bytes, bytes, bytes]:
        """
        Шифрование пары API ключ/секрет
        
        Args:
            api_key: API ключ
            api_secret: API секрет
            
        Returns:
            tuple[bytes, bytes, bytes]: (enc_key, enc_secret, nonce)
        """
        try:
            nonce = secrets.token_bytes(12)  # 96-bit nonce для GCM
            enc_key = self._key.encrypt(nonce, api_key.encode('utf-8'), None)
            enc_secret = self._key.encrypt(nonce, api_secret.encode('utf-8'), None)
            
            logger.debug("Credentials encrypted successfully")
            return enc_key, enc_secret, nonce
            
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise
    
    def decrypt_pair(self, enc_key: bytes, enc_secret: bytes, nonce: bytes) -> Tuple[str, str]:
        """
        Расшифровка пары API ключ/секрет
        
        Args:
            enc_key: Зашифрованный ключ
            enc_secret: Зашифрованный секрет
            nonce: Nonce
            
        Returns:
            tuple[str, str]: (api_key, api_secret)
        """
        try:
            key = self._key.decrypt(nonce, enc_key, None).decode('utf-8')
            secret = self._key.decrypt(nonce, enc_secret, None).decode('utf-8')
            
            logger.debug("Credentials decrypted successfully")
            return key, secret
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise
    
    def set_account_credentials(self, account_id: int, api_key: str, api_secret: str):
        """
        Сохраняет зашифрованные credentials в таблице ApiCredentials.
        Пишет запись в SysEvents. Без МОКов.
        """
        if not account_id:
            raise ValueError("account_id is required")

        # Устойчивый импорт на уровне функции (избегаем циклов)
        try:
            from app.db_session import SessionLocal as _SessionLocal
            from app.db_models import ApiCredentials as _ApiCredentials, SysEvents as _SysEvents
        except Exception:
            try:
                from db_session import SessionLocal as _SessionLocal
                from db_models import ApiCredentials as _ApiCredentials, SysEvents as _SysEvents
            except Exception as e:
                logger.exception("DB imports failed inside set_account_credentials")
                raise

        try:
            enc_key, enc_secret, nonce = self.encrypt_pair(api_key, api_secret)

            with _SessionLocal() as session:
                row = session.query(_ApiCredentials).filter_by(account_id=account_id).first()
                if row is None:
                    row = _ApiCredentials(
                        account_id=account_id,
                        enc_key=enc_key,
                        enc_secret=enc_secret,
                        nonce=nonce,
                        key_hint=api_key[:8] + "*" * (len(api_key) - 8) if len(api_key) > 8 else api_key
                    )
                    session.add(row)
                    action = "created"
                else:
                    row.enc_key = enc_key
                    row.enc_secret = enc_secret
                    row.nonce = nonce
                    row.key_hint = api_key[:8] + "*" * (len(api_key) - 8) if len(api_key) > 8 else api_key
                    try:
                        from datetime import datetime as _dt
                        row.updated_at = _dt.utcnow()
                    except Exception:
                        pass
                    action = "updated"

                event = _SysEvents(
                    level="INFO",                        # <--- было EventLevelEnum.INFO
                    component="CredentialsStore",
                    message=f"Credentials {action}",
                    details_json={"account_id": account_id, "action": action}
                )
                session.add(event)
                session.commit()

            logger.info("Credentials %s for account %s", action, account_id)

        except Exception as e:
            logger.exception("Failed to set credentials for account %s: %s", account_id, e)
            raise


    
    def get_account_credentials(self, account_id: int) -> Optional[Tuple[str, str]]:
        """
        Возвращает (api_key, api_secret) или None, если записи нет.
        """
        if not account_id:
            raise ValueError("account_id is required")

        try:
            from app.db_session import SessionLocal as _SessionLocal
            from app.db_models import ApiCredentials as _ApiCredentials
        except Exception:
            try:
                from db_session import SessionLocal as _SessionLocal
                from db_models import ApiCredentials as _ApiCredentials
            except Exception as e:
                logger.exception("DB imports failed inside get_account_credentials")
                raise

        try:
            with _SessionLocal() as session:
                row = session.query(_ApiCredentials).filter_by(account_id=account_id).first()
                if not row:
                    logger.debug("No credentials found for account %s", account_id)
                    return None

                api_key, api_secret = self.decrypt_pair(row.enc_key, row.enc_secret, row.nonce)

                try:
                    from datetime import datetime as _dt
                    row.last_used = _dt.utcnow()
                    session.commit()
                except Exception:
                    pass

                logger.debug("Credentials retrieved for account %s", account_id)
                return api_key, api_secret

        except Exception as e:
            logger.exception("Failed to get credentials for account %s: %s", account_id, e)
            raise


    
    def delete_account_credentials(self, account_id: int) -> bool:
        """
        Удаляет credentials по account_id. Возвращает True если удалено, False если не найдено.
        Ошибки не подавляются.
        """
        if not account_id:
            raise ValueError("account_id is required")

        try:
            from app.db_session import SessionLocal as _SessionLocal
            from app.db_models import ApiCredentials as _ApiCredentials, SysEvents as _SysEvents
        except Exception:
            try:
                from db_session import SessionLocal as _SessionLocal
                from db_models import ApiCredentials as _ApiCredentials, SysEvents as _SysEvents
            except Exception as e:
                logger.exception("DB imports failed inside delete_account_credentials")
                raise

        try:
            with _SessionLocal() as session:
                row = session.query(_ApiCredentials).filter_by(account_id=account_id).first()
                if not row:
                    return False

                session.delete(row)
                event = _SysEvents(
                    level="INFO",                        # <--- было EventLevelEnum.INFO
                    component="CredentialsStore",
                    message="Credentials deleted",
                    details_json={"account_id": account_id}
                )
                session.add(event)
                session.commit()

            logger.info("Credentials deleted for account %s", account_id)
            return True

        except Exception as e:
            logger.exception("Failed to delete credentials for account %s: %s", account_id, e)
            raise


    
    def list_accounts_with_credentials(self) -> List[int]:
        """
        Список account_id, имеющих сохранённые credentials.
        """
        try:
            from app.db_session import SessionLocal as _SessionLocal
            from app.db_models import ApiCredentials as _ApiCredentials
        except Exception:
            try:
                from db_session import SessionLocal as _SessionLocal
                from db_models import ApiCredentials as _ApiCredentials
            except Exception as e:
                logger.exception("DB imports failed inside list_accounts_with_credentials")
                raise

        try:
            with _SessionLocal() as session:
                rows = session.query(_ApiCredentials.account_id).all()
                return [r[0] for r in rows]
        except Exception as e:
            logger.exception("Failed to list accounts with credentials: %s", e)
            raise



# ================================
# КЛАСС ДЛЯ РАБОТЫ С КРИПТОГРАФИЕЙ
# ================================

class CryptoStore:
    """
    Professional-grade encryption store для API credentials
    Использует AES-GCM для authenticated encryption
    """
    
    def __init__(self, master_key: Optional[str] = None):
        """
        Инициализация с мастер-ключом из environment
        
        Args:
            master_key: Мастер-ключ (если не указан, берется из BOT_MASTER_KEY)
        """
        self.master_key = master_key or os.getenv("BOT_MASTER_KEY")
        
        if not self.master_key:
            raise ValueError("BOT_MASTER_KEY environment variable not set")
        
        if len(self.master_key) < 32:
            raise ValueError("Master key must be at least 32 characters long")
        
        self._aesgcm = None
        self._initialize_cipher()
    
    def _initialize_cipher(self):
        """Инициализация AES-GCM шифра"""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            
            # Выводим ключ из мастер-пароля с использованием PBKDF2
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,  # 256-bit ключ
                salt=b'bybit_trading_salt_2024',  # Константная соль для воспроизводимости
                iterations=100000,  # OWASP рекомендация
            )
            
            key = kdf.derive(self.master_key.encode('utf-8'))
            self._aesgcm = AESGCM(key)
            
            logger.info("Crypto store initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize crypto store: {e}")
            raise
    
    def encrypt_credentials(self, api_key: str, api_secret: str) -> Tuple[bytes, bytes, bytes]:
        """
        Шифрование API credentials
        
        Args:
            api_key: API ключ для шифрования
            api_secret: API секрет для шифрования
        
        Returns:
            Tuple[bytes, bytes, bytes]: (encrypted_key, encrypted_secret, nonce)
        """
        try:
            # Генерируем уникальный nonce для каждого шифрования
            nonce = secrets.token_bytes(12)  # 96-bit nonce для GCM
            
            # Шифруем API ключ и секрет
            encrypted_key = self._aesgcm.encrypt(nonce, api_key.encode('utf-8'), None)
            encrypted_secret = self._aesgcm.encrypt(nonce, api_secret.encode('utf-8'), None)
            
            logger.debug("API credentials encrypted successfully")
            return encrypted_key, encrypted_secret, nonce
            
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise
    
    def decrypt_credentials(self, enc_key: bytes, enc_secret: bytes, nonce: bytes) -> Tuple[str, str]:
        """
        Расшифровка API credentials
        
        Args:
            enc_key: Зашифрованный API ключ
            enc_secret: Зашифрованный API секрет
            nonce: Nonce, использованный при шифровании
        
        Returns:
            Tuple[str, str]: (api_key, api_secret)
        """
        try:
            # Расшифровываем данные
            api_key = self._aesgcm.decrypt(nonce, enc_key, None).decode('utf-8')
            api_secret = self._aesgcm.decrypt(nonce, enc_secret, None).decode('utf-8')
            
            logger.debug("API credentials decrypted successfully")
            return api_key, api_secret
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise
    
    def encrypt_json_data(self, data: Dict[str, Any]) -> Tuple[bytes, bytes]:
        """
        Шифрование JSON данных
        
        Args:
            data: Словарь для шифрования
        
        Returns:
            Tuple[bytes, bytes]: (encrypted_data, nonce)
        """
        try:
            nonce = secrets.token_bytes(12)
            json_data = json.dumps(data, ensure_ascii=False)
            encrypted_data = self._aesgcm.encrypt(nonce, json_data.encode('utf-8'), None)
            
            return encrypted_data, nonce
            
        except Exception as e:
            logger.error(f"JSON encryption failed: {e}")
            raise
    
    def decrypt_json_data(self, encrypted_data: bytes, nonce: bytes) -> Dict[str, Any]:
        """
        Расшифровка JSON данных
        
        Args:
            encrypted_data: Зашифрованные данные
            nonce: Nonce
        
        Returns:
            Dict[str, Any]: Расшифрованный словарь
        """
        try:
            decrypted_data = self._aesgcm.decrypt(nonce, encrypted_data, None)
            json_str = decrypted_data.decode('utf-8')
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"JSON decryption failed: {e}")
            raise
    
    def generate_key_hint(self, api_key: str) -> str:
        """
        Генерация подсказки для API ключа (первые 8 символов)
        
        Args:
            api_key: API ключ
        
        Returns:
            str: Подсказка (первые 8 символов)
        """
        if len(api_key) < 8:
            return api_key
        return api_key[:8] + "*" * (len(api_key) - 8)
    
    @staticmethod
    def generate_master_key() -> str:
        """
        Генерация нового мастер-ключа
        
        Returns:
            str: Безопасный мастер-ключ
        """
        # Генерируем 64-символьный ключ (512 бит)
        key_bytes = secrets.token_bytes(32)
        return base64.urlsafe_b64encode(key_bytes).decode('ascii')
    
    def test_encryption(self) -> bool:
        """
        Тест шифрования/расшифровки
        
        Returns:
            bool: True если тест прошел успешно
        """
        try:
            test_key = "test_api_key_12345"
            test_secret = "test_api_secret_67890"
            
            # Шифруем
            enc_key, enc_secret, nonce = self.encrypt_credentials(test_key, test_secret)
            
            # Расшифровываем
            dec_key, dec_secret = self.decrypt_credentials(enc_key, enc_secret, nonce)
            
            # Проверяем
            if dec_key == test_key and dec_secret == test_secret:
                logger.info("Encryption test passed")
                return True
            else:
                logger.error("Encryption test failed: data mismatch")
                return False
                
        except Exception as e:
            logger.error(f"Encryption test failed: {e}")
            return False


# ================================
# UTILITY FUNCTIONS
# ================================

def create_crypto_store() -> CryptoStore:
    """Factory function для создания CryptoStore"""
    try:
        return CryptoStore()
    except Exception as e:
        logger.error(f"Failed to create crypto store: {e}")
        raise

def create_credentials_store() -> CredentialsStore:
    """Factory function для создания CredentialsStore"""
    try:
        return CredentialsStore()
    except Exception as e:
        logger.error(f"Failed to create credentials store: {e}")
        raise

def encrypt_api_credentials(api_key: str, api_secret: str) -> Dict[str, bytes]:
    """
    Удобная функция для шифрования API credentials
    
    Returns:
        Dict содержащий enc_key, enc_secret, nonce
    """
    crypto = create_crypto_store()
    enc_key, enc_secret, nonce = crypto.encrypt_credentials(api_key, api_secret)
    
    return {
        'enc_key': enc_key,
        'enc_secret': enc_secret,
        'nonce': nonce,
        'key_hint': crypto.generate_key_hint(api_key)
    }

def decrypt_api_credentials(enc_key: bytes, enc_secret: bytes, nonce: bytes) -> Tuple[str, str]:
    """Удобная функция для расшифровки API credentials"""
    crypto = create_crypto_store()
    return crypto.decrypt_credentials(enc_key, enc_secret, nonce)


# ================================
# TESTING FUNCTIONS
# ================================

def test_credentials_store():
    """Минимальные тесты для CredentialsStore (без MOCK)."""
    try:
        store = CredentialsStore()

        # Тест encrypt/decrypt
        test_key = "test_api_key_12345"
        test_secret = "test_api_secret_67890"

        enc_key, enc_secret, nonce = store.encrypt_pair(test_key, test_secret)
        dec_key, dec_secret = store.decrypt_pair(enc_key, enc_secret, nonce)
        assert dec_key == test_key, "Decrypted key mismatch"
        assert dec_secret == test_secret, "Decrypted secret mismatch"
        print("✅ Encrypt/decrypt test passed")

        # Тест с базой данных (если check_db_health проходит)
        if _db_health_check():
            test_account_id = 999_999

            # Сохраняем
            store.set_account_credentials(test_account_id, test_key, test_secret)

            # Читаем
            api = store.get_account_credentials(test_account_id)
            assert api is not None, "Credentials not found in DB"
            assert api[0] == test_key, "Retrieved key mismatch"
            assert api[1] == test_secret, "Retrieved secret mismatch"

            # Удаляем
            assert store.delete_account_credentials(test_account_id) is True, "Delete returned False"
            print("✅ Database integration test passed")
        else:
            print("⚠️ Database health check failed — DB tests skipped")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False



def check_db_health():
    """Проверка здоровья БД (без заглушек)."""
    try:
        return _db_health_check()
    except Exception as e:
        logger.exception("DB health check failed: %s", e)
        return False



# ================================
# MAIN EXECUTION
# ================================

if __name__ == "__main__":
    print("🔐 TESTING DATABASE SECURITY IMPLEMENTATION")
    print("=" * 60)
    
    # Устанавливаем тестовый мастер-ключ если не установлен
    if not os.getenv("BOT_MASTER_KEY"):
        test_key = CryptoStore.generate_master_key()
        os.environ["BOT_MASTER_KEY"] = test_key
        print(f"Generated test master key: {test_key[:16]}...")
    
    # Тест CryptoStore
    print("\n📦 Testing CryptoStore...")
    try:
        crypto = create_crypto_store()
        if crypto.test_encryption():
            print("✅ CryptoStore test passed")
        else:
            print("❌ CryptoStore test failed")
    except Exception as e:
        print(f"❌ CryptoStore error: {e}")
    
    # Тест CredentialsStore
    print("\n🔑 Testing CredentialsStore...")
    if test_credentials_store():
        print("✅ CredentialsStore test passed")
    else:
        print("❌ CredentialsStore test failed")
    
    print("\n" + "=" * 60)
    print("✨ Testing completed!")
    
    # Проверка здоровья БД
    print("\n🗄️ Checking database health...")
    if check_db_health():
        print("✅ Database is healthy")
    else:
        print("⚠️ Database is not available or not configured")
    
    print("\n📌 Note: For production use, set BOT_MASTER_KEY environment variable")
    print("   with a secure 32+ character key")