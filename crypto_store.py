# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, base64, binascii, secrets
from datetime import datetime
from typing import Optional, Tuple

from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes, constant_time
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .db_session import SessionLocal
from .db_models import ApiCredentials, SysEvents

PBKDF2_ITERS = 200_000

def _b64_try(s: str) -> Optional[bytes]:
    try:
        return base64.b64decode(s, validate=True)
    except Exception:
        return None

def _hex_try(s: str) -> Optional[bytes]:
    try:
        return binascii.unhexlify(s)
    except Exception:
        return None

def _derive_master_key(raw: str) -> bytes:
    """
    Получить 32-байтовый мастер-ключ:
      - если raw похоже на base64 -> decode
      - если raw похоже на hex    -> decode
      - иначе считаем 'парольной фразой' и делаем PBKDF2-HMAC(SHA256)
    """
    b = _b64_try(raw)
    if b is None:
        b = _hex_try(raw)
    if b is not None:
        if len(b) != 32:
            raise ValueError("BOT_MASTER_KEY after decode must be 32 bytes")
        return b

    salt_env = os.getenv("BOT_MASTER_SALT", "")
    salt = _b64_try(salt_env) or _hex_try(salt_env) or salt_env.encode("utf-8")
    if not salt:
        raise ValueError("Provide BOT_MASTER_SALT for PBKDF2 when using passphrase BOT_MASTER_KEY")
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=PBKDF2_ITERS)
    return kdf.derive(raw.encode("utf-8"))

def _hkdf_expand(master: bytes, info: bytes) -> bytes:
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=info)
    return hkdf.derive(master)

class CredentialsStore:
    """
    AES-GCM с одним nonce на запись и двумя ПОДКЛЮЧАМИ (HKDF):
      - key_enc_key     = HKDF(master, info=b"api_key")
      - key_enc_secret  = HKDF(master, info=b"api_secret")
    Это устраняет риск 'nonce reuse' в пределах одной записи без изменения схемы.
    """
    def __init__(self) -> None:
        self._master_key: Optional[bytes] = None
        self._k_api_key: Optional[bytes] = None
        self._k_api_secret: Optional[bytes] = None

    def init(self) -> None:
        raw = os.getenv("BOT_MASTER_KEY")
        if not raw:
            raise RuntimeError("BOT_MASTER_KEY is required")
        master = _derive_master_key(raw)
        self._master_key = master
        self._k_api_key = _hkdf_expand(master, b"api_key")
        self._k_api_secret = _hkdf_expand(master, b"api_secret")

    def encrypt_pair(self, api_key: str, api_secret: str) -> tuple[bytes, bytes, bytes]:
        if self._k_api_key is None:
            self.init()
        assert self._k_api_key and self._k_api_secret
        nonce = secrets.token_bytes(12)
        aead_key = AESGCM(self._k_api_key)
        aead_sec = AESGCM(self._k_api_secret)
        enc_key = aead_key.encrypt(nonce, api_key.encode("utf-8"), None)
        enc_sec = aead_sec.encrypt(nonce, api_secret.encode("utf-8"), None)
        return enc_key, enc_sec, nonce

    def decrypt_pair(self, enc_key: bytes, enc_secret: bytes, nonce: bytes) -> tuple[str, str]:
        if self._k_api_key is None:
            self.init()
        assert self._k_api_key and self._k_api_secret
        aead_key = AESGCM(self._k_api_key)
        aead_sec = AESGCM(self._k_api_secret)
        key = aead_key.decrypt(nonce, enc_key, None).decode("utf-8")
        sec = aead_sec.decrypt(nonce, enc_secret, None).decode("utf-8")
        return key, sec

    @staticmethod
    def _mask_hint(api_key: str) -> str:
        tail = api_key[-4:] if len(api_key) >= 4 else api_key
        return f"***{tail}"

    def set_account_credentials(self, account_id: int, api_key: str, api_secret: str) -> None:
        enc_key, enc_secret, nonce = self.encrypt_pair(api_key, api_secret)
        hint = self._mask_hint(api_key)
        with SessionLocal() as session:
            row = session.query(ApiCredentials).filter_by(account_id=account_id).one_or_none()
            action = "updated" if row else "created"
            if row is None:
                row = ApiCredentials(account_id=account_id, enc_key=enc_key, enc_secret=enc_secret,
                                     nonce=nonce, key_hint=hint)
                session.add(row)
            else:
                row.enc_key = enc_key
                row.enc_secret = enc_secret
                row.nonce = nonce
                row.key_hint = hint
                row.updated_at = datetime.utcnow()
            session.add(SysEvents(level="INFO", component="CredentialsStore",
                                  message=f"Credentials {action}",
                                  details_json={"account_id": account_id}))
            session.commit()

    def get_account_credentials(self, account_id: int) -> Optional[tuple[str, str]]:
        with SessionLocal() as session:
            row = session.query(ApiCredentials).filter_by(account_id=account_id).one_or_none()
            if not row:
                return None
            return self.decrypt_pair(row.enc_key, row.enc_secret, row.nonce)
