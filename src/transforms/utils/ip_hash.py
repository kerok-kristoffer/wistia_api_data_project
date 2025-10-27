# src/transforms/utils/ip_hash.py
import hmac
import hashlib
from typing import Optional


def hmac_sha256_hex(value: Optional[str], key: str) -> Optional[str]:
    """
    Privacy: HMAC-SHA256 over the raw IP; returns lowercase hex digest.
    None/empty input -> None.
    """
    if not value:
        return None
    return hmac.new(
        key.encode("utf-8"), value.encode("utf-8"), hashlib.sha256
    ).hexdigest()
