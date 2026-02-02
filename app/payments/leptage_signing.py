# app/payments/leptage_signing.py
"""
Leptage API request signing and webhook verification.

API Authentication:
- ECDSA P-256 (secp256r1, prime256v1) with SHA256withECDSA
- Headers:
    X-API-KEY      : Public Key (API Key, hex DER)
    X-API-SIGNATURE: Signature (hex DER)
    X-API-NONCE    : Timestamp in ms
- String to sign:
    METHOD + /openapi + PATH + NONCE + PARAMS
  where:
    - METHOD is uppercased (GET/POST/...)
    - PATH is resource path WITHOUT /openapi (e.g. /v1/balance)
    - NONCE is X-API-NONCE (ms)
    - PARAMS:
        * GET:  key=val&key2=val2 (sorted by key, asc)
        * POST: compact JSON (no spaces/newlines)
        * None: empty string

Webhook Authentication:
- HMAC-SHA256 using Webhook Secret
- Headers:
    X-HOOK-SIGNATURE: Signature (hex)
    X-HOOK-NONCE    : Timestamp in ms
- String to sign:
    NONCE + WEBHOOK_URL + PARAMS
  where:
    - WEBHOOK_URL is the full callback URL you registered
    - PARAMS is compact JSON body (no spaces/newlines)
"""

import hashlib
import json
import time
import os
import binascii
import hmac
from typing import Dict, Any, Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


class LeptageRequestSigner:
    """
    Signs Leptage API requests using ECDSA P-256 (secp256r1) + SHA256.

    Spec:
      - X-API-KEY      : public key hex (API Key)
      - X-API-NONCE    : timestamp in ms
      - X-API-SIGNATURE: ECDSA signature over:
            METHOD + "/openapi" + PATH + NONCE + PARAMS
    """

    def __init__(self, api_key_hex: str, api_secret_hex: str):
        """
        Initialize signer with hex-encoded EC keys.

        Args:
            api_key_hex   : Public key as hex string (from generate_leptage_keys.py)
            api_secret_hex: Private key as hex string (from generate_leptage_keys.py)

        Raises:
            RuntimeError: If keys cannot be loaded
        """
        self.api_key_hex = api_key_hex
        self.api_secret_hex = api_secret_hex

        try:
            private_der = binascii.unhexlify(api_secret_hex)
            self.private_key = serialization.load_der_private_key(
                private_der, password=None
            )
        except Exception as e:
            raise RuntimeError(f"[LEPTAGE] Failed to load private key from hex: {e}")

    def _build_params_string(
        self,
        method: str,
        body_or_params: Optional[Dict[str, Any]],
    ) -> str:
        """
        Build the 'request parameters' string as per docs.

        GET:
            name=someone&age=21  -> sorted by key -> age=21&name=someone
        POST:
            {"name": "someone", "age": 21}
            -> {"name":"someone","age":21} (compact JSON)
        None:
            ""
        """
        if not body_or_params:
            return ""

        method_up = method.upper()

        if method_up == "GET":
            items = sorted(body_or_params.items(), key=lambda x: x[0])
            return "&".join(f"{k}={v}" for k, v in items)

        # POST JSON (and others treated as JSON)
        json_str = json.dumps(body_or_params, separators=(",", ":"), sort_keys=True)
        return json_str

    def _build_string_to_sign(
        self,
        method: str,
        path: str,
        nonce_ms: int,
        body_or_params: Optional[Dict[str, Any]],
    ) -> str:
        """
        Construct string:
            METHOD + /openapi + PATH + NONCE + PARAMS

        Examples from docs:

          GET:
            Request params: name=someone&age=21 -> age=21&name=someone
            Method        : GET
            Path          : /v1/balance
            Nonce         : 1741240848495

            String:
              GET/openapi/v1/balance1741240848495age=21&name=someone

          POST:
            Request params: {"name":"someone","age":21}
            Method        : POST
            Path          : /v1/balance
            Nonce         : 1741240918899

            String:
              POST/openapi/v1/balance1741240918899{"name":"someone","age":21}
        """
        method_up = method.upper()
        params_str = self._build_params_string(method_up, body_or_params)

        if not path.startswith("/"):
            path = "/" + path

        resource = f"/openapi{path}"
        return f"{method_up}{resource}{nonce_ms}{params_str}"

    def _sign_bytes(self, data: bytes) -> str:
        """
        Sign bytes with ECDSA P-256 + SHA256 and return DER hex string.
        """
        signature_der = self.private_key.sign(
            data,
            ec.ECDSA(hashes.SHA256()),
        )
        return binascii.hexlify(signature_der).decode()

    def sign_request(
        self,
        method: str,
        path: str,
        body_or_params: Optional[Dict[str, Any]] = None,
        nonce_ms: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Create signed headers for a Leptage API request.

        Args:
            method       : HTTP method ("GET", "POST", ...)
            path         : resource path WITHOUT /openapi prefix
                           e.g. "/v1/balance", "/v1/address/deposit"
            body_or_params: dict of GET query params or POST JSON body
            nonce_ms     : timestamp in ms (if None, auto-generate)

        Returns:
            Headers dict with X-API-KEY, X-API-NONCE, X-API-SIGNATURE
        """
        if nonce_ms is None:
            nonce_ms = int(time.time() * 1000)

        string_to_sign = self._build_string_to_sign(
            method,
            path,
            nonce_ms,
            body_or_params,
        )
        signature_hex = self._sign_bytes(string_to_sign.encode("utf-8"))

        return {
            "X-API-KEY": self.api_key_hex,
            "X-API-NONCE": str(nonce_ms),
            "X-API-SIGNATURE": signature_hex,
            "Content-Type": "application/json",
        }


class LeptageWebhookVerifier:
    """
    Verify Leptage webhooks using HMAC-SHA256 as per docs.

    Spec:
      - X-HOOK-SIGNATURE: HMAC-SHA256 result (hex, case-insensitive)
      - X-HOOK-NONCE    : timestamp in ms
      - String to sign  : NONCE + WEBHOOK_URL + PARAMS
            where PARAMS is compact JSON body (no spaces/newlines)
    """

    def __init__(self, webhook_secret: str, webhook_url: str):
        """
        Args:
            webhook_secret: Webhook Secret provided by Leptage
            webhook_url   : Full webhook URL registered with Leptage
        """
        self.webhook_secret = webhook_secret or ""
        self.webhook_url = webhook_url

    def _compact_body(self, body_bytes: bytes) -> str:
        """
        Remove spaces and newlines from JSON body as per docs.
        """
        text = body_bytes.decode("utf-8")
        return text.replace(" ", "").replace("\n", "").replace("\r", "")

    def compute_signature(self, nonce: str, body_bytes: bytes) -> str:
        """
        Compute HMAC-SHA256 hex signature:
            NONCE + WEBHOOK_URL + COMPACT_BODY
        """
        compact_body = self._compact_body(body_bytes)
        to_sign = f"{nonce}{self.webhook_url}{compact_body}"
        digest = hmac.new(
            self.webhook_secret.encode("utf-8"),
            to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return digest

    def verify_webhook(self, headers: Dict[str, str], body_bytes: bytes) -> bool:
        """
        Verify incoming webhook based on headers and raw body.
        """
        if not self.webhook_secret:
            # No secret configured -> reject
            return False

        nonce = headers.get("X-HOOK-NONCE") or headers.get("x-hook-nonce")
        received_sig = headers.get("X-HOOK-SIGNATURE") or headers.get("x-hook-signature")

        if not nonce or not received_sig:
            return False

        expected = self.compute_signature(str(nonce), body_bytes)
        return hmac.compare_digest(expected.lower(), received_sig.lower())


def get_signed_headers(
    method: str = "POST",
    path: str = "/",
    body_or_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Helper function to get signed headers for a Leptage API request.
    """
    api_key = os.getenv("LEPTAGE_API_KEY", "").strip()
    api_secret = os.getenv("LEPTAGE_API_SECRET", "").strip()

    if not api_key or not api_secret:
        raise RuntimeError(
            "[LEPTAGE] LEPTAGE_API_KEY and LEPTAGE_API_SECRET not configured in environment"
        )

    signer = LeptageRequestSigner(api_key, api_secret)
    return signer.sign_request(method, path, body_or_params)


def get_signed_headers_v2(
    method: str,
    url: str,
    body_or_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Strictly mirror Leptage's Java demo signing:

      - url: path including /openapi prefix, e.g. "/openapi/v1/txns/deposit"
      - GET: signStr = METHOD + url + nonce + (sorted key=value&...)
      - POST: signStr = METHOD + url + nonce + compact sorted JSON body

    Returns headers:
      X-API-KEY, X-API-NONCE, X-API-SIGNATURE, Content-Type
    """
    api_key = os.getenv("LEPTAGE_API_KEY", "").strip()
    api_secret = os.getenv("LEPTAGE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise RuntimeError(
            "[LEPTAGE] LEPTAGE_API_KEY and LEPTAGE_API_SECRET not configured in environment"
        )

    nonce_ms = int(time.time() * 1000)
    method_up = method.upper()

    # Build PARAMS string exactly like the Java demo
    if not body_or_params:
        params_str = ""
    else:
        if method_up == "GET":
            items = sorted(body_or_params.items(), key=lambda x: x[0])
            # key=value&key2=value2
            params_str = "&".join(f"{k}={v}" for k, v in items)
        else:
            # POST: compact JSON with sorted keys
            params_str = json.dumps(body_or_params, separators=(",", ":"), sort_keys=True)
            print(f"[DEBUG] Compact JSON body: {params_str}")

    sign_str = f"{method_up}{url}{nonce_ms}{params_str}"
    print(f"[DEBUG] String to sign: {sign_str}")

    # Sign with ECDSA P-256 + SHA256, DER hex
    signer = LeptageRequestSigner(api_key, api_secret)
    signature_hex = signer._sign_bytes(sign_str.encode("utf-8"))

    print(f"[DEBUG] Signature (hex): {signature_hex}")

    return {
        "X-API-KEY": api_key,
        "X-API-NONCE": str(nonce_ms),
        "X-API-SIGNATURE": signature_hex,
        "Content-Type": "application/json",
    }


def get_webhook_verifier() -> LeptageWebhookVerifier:
    """
    Build a webhook verifier instance based on environment and known URL.
    """
    webhook_secret = os.getenv("LEPTAGE_WEBHOOK_SECRET", "").strip()
    # UAT webhook URL you registered with Leptage:
    webhook_url = "https://googleads-ex2w.onrender.com/api/webhooks/leptage"
    return LeptageWebhookVerifier(webhook_secret, webhook_url)
