# app/payments/leptage_signing.py
"""
Leptage API request signing using EC secp256r1 + SHA256 ECDSA.

This module handles all cryptographic operations for signing Leptage API requests.
Follows the pattern from their Java example using secp256r1 elliptic curve.
"""

import hashlib
import json
import time
import os
import binascii
import hmac
from typing import Dict, Any, Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, utils
import base64


class LeptageRequestSigner:
    """
    Signs Leptage API requests using ECDSA secp256r1 + SHA256.

    Leptage uses EC key pairs (secp256r1) to sign all API requests.
    This ensures request authenticity and integrity.
    """

    def __init__(self, api_key_hex: str, api_secret_hex: str):
        """
        Initialize signer with hex-encoded EC keys.

        Args:
            api_key_hex: Public key as hex string (from generate_leptage_keys.py)
            api_secret_hex: Private key as hex string (from generate_leptage_keys.py)

        Raises:
            RuntimeError: If keys cannot be loaded
        """
        self.api_key_hex = api_key_hex
        self.api_secret_hex = api_secret_hex

        # Reconstruct private key from DER hex bytes
        try:
            private_der = binascii.unhexlify(api_secret_hex)
            self.private_key = serialization.load_der_private_key(
                private_der, password=None
            )
        except Exception as e:
            raise RuntimeError(f"[LEPTAGE] Failed to load private key from hex: {e}")

    def sign_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        timestamp: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Create signed request headers for Leptage API.

        This generates:
        - X-API-Key: Your public key (hex)
        - X-Signature: ECDSA signature of the request (hex)
        - X-Timestamp: Request timestamp for replay protection
        - Content-Type: application/json

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path (e.g. /v1/address/deposit)
            body: Request body dict (if POST/PUT)
            timestamp: Unix timestamp (auto-generated if not provided)

        Returns:
            Dict of headers to include in HTTP request

        Example:
            headers = signer.sign_request("POST", "/v1/address/deposit", {"chain": "ethereum"})
            # headers = {
            #     "X-API-Key": "3059...",
            #     "X-Signature": "3081...",
            #     "X-Timestamp": "1234567890",
            #     "Content-Type": "application/json"
            # }
        """
        if timestamp is None:
            timestamp = int(time.time())

        # Build message to sign
        # Standard pattern: METHOD|PATH|TIMESTAMP|BODY_HASH
        if body:
            # Sort keys for consistent JSON representation
            body_str = json.dumps(body, separators=(",", ":"), sort_keys=True)
            body_hash = hashlib.sha256(body_str.encode()).hexdigest()
        else:
            body_hash = hashlib.sha256(b"").hexdigest()

        # Construct the message (adjust format if Leptage specifies different pattern)
        message = f"{method}|{path}|{timestamp}|{body_hash}"

        # Sign the message
        signature_bytes = self._sign_message(message.encode())
        signature_hex = binascii.hexlify(signature_bytes).decode()

        # Return signed headers
        return {
            "X-API-Key": self.api_key_hex,
            "X-Signature": signature_hex,
            "X-Timestamp": str(timestamp),
            "Content-Type": "application/json",
        }

    def _sign_message(self, message: bytes) -> bytes:
        """
        Sign a message using ECDSA secp256r1 + SHA256.

        Args:
            message: Raw message bytes to sign

        Returns:
            Raw signature bytes (DER-encoded by default)

        Note:
            If Leptage requires raw (r, s) format instead of DER, this can be
            converted using utils.decode_dss_signature() and re-encoded.
        """
        try:
            signature_der = self.private_key.sign(
                message, ec.ECDSA(hashes.SHA256())
            )
            return signature_der
        except Exception as e:
            raise RuntimeError(f"[LEPTAGE] Failed to sign message: {e}")

    def verify_signature(self, message: bytes, signature: bytes) -> bool:
        """
        Verify a signature (used for testing or webhook verification).

        Args:
            message: Original message bytes
            signature: Signature bytes to verify

        Returns:
            True if signature is valid, False otherwise
        """
        try:
            public_key = self.private_key.public_key()
            public_key.verify(signature, message, ec.ECDSA(hashes.SHA256()))
            return True
        except Exception:
            return False


class LeptageWebhookVerifier:
    """
    Verifies webhooks signed by Leptage.

    Leptage sends webhooks with a signature header for verification.
    """

    def __init__(self, webhook_secret: str):
        """
        Initialize verifier with webhook secret.

        Args:
            webhook_secret: Secret provided by Leptage for webhook HMAC
        """
        self.webhook_secret = webhook_secret

    def verify_webhook(self, payload: bytes, signature: str) -> bool:
        """
        Verify a webhook signature using HMAC-SHA256.

        Args:
            payload: Raw webhook payload bytes
            signature: Signature from X-Leptage-Signature header (hex or base64)

        Returns:
            True if signature is valid, False otherwise
        """
        if not self.webhook_secret:
            # If no secret configured, allow (for dev)
            return True

        try:
            # Compute expected signature
            expected = hmac.new(
                self.webhook_secret.encode("utf-8"),
                payload,
                hashlib.sha256,
            ).hexdigest()

            # Compare (timing-safe)
            return hmac.compare_digest(expected, signature or "")
        except Exception as e:
            print(f"[LEPTAGE WEBHOOK] Verification failed: {e}")
            return False


def get_signed_headers(
    method: str = "POST",
    path: str = "/",
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Helper function to get signed headers for a Leptage API request.

    Reads API credentials from environment and returns ready-to-use headers.

    Args:
        method: HTTP method
        path: API endpoint path
        body: Request body dict

    Returns:
        Headers dict ready for requests.post(url, json=body, headers=headers)

    Raises:
        RuntimeError: If credentials not configured in environment

    Example:
        headers = get_signed_headers("POST", "/v1/address/deposit", {"chain": "ethereum"})
        response = requests.post(
            "https://api1.uat.planckage.cc/openapi/v1/address/deposit",
            json={"chain": "ethereum"},
            headers=headers
        )
    """
    api_key = os.getenv("LEPTAGE_API_KEY", "").strip()
    api_secret = os.getenv("LEPTAGE_API_SECRET", "").strip()

    if not api_key or not api_secret:
        raise RuntimeError(
            "[LEPTAGE] LEPTAGE_API_KEY and LEPTAGE_API_SECRET not configured in environment"
        )

    signer = LeptageRequestSigner(api_key, api_secret)
    return signer.sign_request(method, path, body)


def get_webhook_verifier() -> LeptageWebhookVerifier:
    """
    Get a webhook verifier instance using credentials from environment.

    Returns:
        LeptageWebhookVerifier instance

    Example:
        verifier = get_webhook_verifier()
        is_valid = verifier.verify_webhook(raw_body, signature_from_header)
    """
    webhook_secret = os.getenv("LEPTAGE_WEBHOOK_SECRET", "").strip() or None
    return LeptageWebhookVerifier(webhook_secret or "")
