# tests/transforms/test_ip_hash.py
from transforms.utils.ip_hash import hmac_sha256_hex


def test_hmac_is_deterministic_and_length_is_64_hex():
    key = "supersecret"
    ip = "1.2.3.4"
    d1 = hmac_sha256_hex(ip, key)
    d2 = hmac_sha256_hex(ip, key)
    assert d1 == d2
    assert len(d1) == 64
    assert d1 != hmac_sha256_hex("5.6.7.8", key)
    assert d1 != hmac_sha256_hex(ip, "anotherkey")


def test_hmac_none_for_missing_value():
    assert hmac_sha256_hex(None, "x") is None
    assert hmac_sha256_hex("", "x") is None
