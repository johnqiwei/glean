"""
Feishu integration client.

Handles authenticating as a tenant bot, sending rich-text messages,
message signature verification, and payload decryption.
"""

import base64
import hashlib
import json
from datetime import UTC, datetime
from typing import Any
import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from glean_core import get_logger
from glean_core.schemas.config import FeishuConfig

logger = get_logger(__name__)


class AESCipher:
    """
    Feishu payload decryption utility using AES-256-CBC.
    """

    def __init__(self, key: str) -> None:
        # Feishu key is the SHA-256 hash of the configured encrypt_key
        self.key = hashlib.sha256(key.encode("utf-8")).digest()

    def decrypt(self, encrypt_text: str) -> str:
        encrypt_bytes = base64.b64decode(encrypt_text)
        # IV is the first 16 bytes of the decrypted string
        iv = encrypt_bytes[:16]
        cipher_text = encrypt_bytes[16:]
        
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        decrypted_bytes = decryptor.update(cipher_text) + decryptor.finalize()
        # Remove PKCS7 padding
        padding_len = decrypted_bytes[-1]
        decrypted_text = decrypted_bytes[:-padding_len].decode("utf-8")
        return decrypted_text


class FeishuBotClient:
    """
    Client wrapper for Feishu open APIs.
    """

    def __init__(self, config: FeishuConfig) -> None:
        self.config = config
        self._tenant_token: str | None = None
        self._token_expires_at: float = 0.0

    async def get_tenant_access_token(self) -> str:
        """
        Get or refresh the tenant access token.
        """
        now = datetime.now(UTC).timestamp()
        if self._tenant_token and now < self._token_expires_at:
            return self._tenant_token

        if not self.config.app_id or not self.config.app_secret:
            raise ValueError("Feishu app_id and app_secret must be configured")

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.config.app_id,
            "app_secret": self.config.app_secret,
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0:
                    self._tenant_token = data["tenant_access_token"]
                    # Expire token slightly earlier (e.g. 5 minutes) for safety
                    self._token_expires_at = now + data["expire"] - 300
                    return self._tenant_token
                else:
                    raise ValueError(f"Failed to fetch tenant token: {data.get('msg')}")
            except Exception as e:
                logger.exception("Failed to fetch Feishu tenant access token")
                raise

    async def send_rich_text_message(
        self,
        chat_id: str,
        title: str,
        content: list[list[dict[str, Any]]],
    ) -> str:
        """
        Send a rich text (post) message to a Feishu chat.
        
        Args:
            chat_id: Feishu Chat ID.
            title: Title of the post message.
            content: Rich text element structure.
            
        Returns:
            The message ID of the sent message.
        """
        token = await self.get_tenant_access_token()
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        # Structure the content payload as a JSON-serialized string
        msg_content = {
            "zh_cn": {
                "title": title,
                "content": content,
            }
        }
        
        payload = {
            "receive_id": chat_id,
            "msg_type": "post",
            "content": json.dumps(msg_content, ensure_ascii=False),
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0:
                    message_id = data["data"]["message_id"]
                    logger.info("Sent Feishu rich text message successfully", extra={"message_id": message_id})
                    return str(message_id)
                else:
                    raise ValueError(f"Failed to send Feishu message: {data.get('msg')}")
            except Exception as e:
                logger.exception("Feishu send_rich_text_message failed", extra={"chat_id": chat_id})
                raise

    async def reply_text_message(self, message_id: str, text: str) -> str:
        """
        Reply to a specific message in Feishu.
        """
        token = await self.get_tenant_access_token()
        url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        payload = {
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0:
                    reply_id = data["data"]["message_id"]
                    return str(reply_id)
                else:
                    raise ValueError(f"Failed to reply to Feishu message: {data.get('msg')}")
            except Exception as e:
                logger.exception("Feishu reply_text_message failed", extra={"message_id": message_id})
                raise

    def verify_signature(
        self,
        timestamp: str,
        nonce: str,
        signature: str,
        body: bytes,
    ) -> bool:
        """
        Verify the signature of Feishu callbacks.
        
        For security, the subscription event signature includes a timestamp, nonce,
        configured encrypt_key, and request body.
        """
        if not self.config.encrypt_key:
            # If encrypt_key is not configured, we cannot verify signature this way.
            # Usually we log a warning or allow passing if config.encrypt_key is blank.
            logger.warning("Feishu encrypt_key not configured, skipping signature verification")
            return True

        # Signature algorithm: SHA256(timestamp + nonce + encrypt_key + body)
        content_to_sign = timestamp.encode("utf-8") + nonce.encode("utf-8") + self.config.encrypt_key.encode("utf-8") + body
        computed_sig = hashlib.sha256(content_to_sign).hexdigest()
        return computed_sig == signature

    def decrypt_payload(self, encrypt_text: str) -> dict[str, Any]:
        """
        Decrypt encrypted Feishu event payload.
        """
        if not self.config.encrypt_key:
            raise ValueError("Feishu encrypt_key must be configured to decrypt payloads")
        cipher = AESCipher(self.config.encrypt_key)
        decrypted_str = cipher.decrypt(encrypt_text)
        return json.loads(decrypted_str)  # type: ignore[no-any-return]
