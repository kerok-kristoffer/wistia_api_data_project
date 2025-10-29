from __future__ import annotations
import json
import os
import boto3
from botocore.exceptions import ClientError


def load_wistia_secret() -> dict:
    """
    Returns a dict: {"api_token": str, "media_ids": List[str], ...maybe ip hmac...}
    Pulls from Secrets Manager if WISTIA_SECRET_ARN set; else from env.
    """
    arn = os.getenv("WISTIA_SECRET_ARN")
    if not arn:
        token = os.getenv("WISTIA_API_TOKEN")
        media_ids_val = os.getenv("MEDIA_IDS", "")
        media_ids = [x.strip() for x in media_ids_val.split(",") if x.strip()]
        if not token:
            raise RuntimeError(
                "Missing WISTIA_API_TOKEN; set WISTIA_SECRET_ARN for prod "
                "or WISTIA_API_TOKEN for local/tests."
            )
        return {"api_token": token, "media_ids": media_ids}

    sm = boto3.client("secretsmanager")
    try:
        resp = sm.get_secret_value(SecretId=arn)
    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Failed to read secret {arn}: {msg}") from e

    secret = (resp.get("SecretString") or "").strip()
    try:
        obj = json.loads(secret)
        token = obj.get("WISTIA_API_TOKEN") or obj.get("api_token")
        media_ids_val = obj.get("MEDIA_IDS") or obj.get("media_ids") or []
        if isinstance(media_ids_val, str):
            media_ids = [x.strip() for x in media_ids_val.split(",") if x.strip()]
        elif isinstance(media_ids_val, list):
            media_ids = [str(x).strip() for x in media_ids_val if str(x).strip()]
        else:
            media_ids = []
        if not token:
            raise ValueError("Secret missing 'WISTIA_API_TOKEN'/'api_token'")
        out = {"api_token": token, "media_ids": media_ids}
        # pass through optional extras if present
        for k in ("VISITOR_IP_HMAC_KEY", "ip_hash_key", "hmac_secret"):
            if k in obj:
                out["ip_hash_key"] = obj[k]
        return out
    except json.JSONDecodeError:
        if not secret:
            raise RuntimeError(f"Secret {arn} is empty")
        return {"api_token": secret, "media_ids": []}


def merge_wistia_secret_into_env(secret: dict) -> None:
    token = secret.get("api_token")
    if token:
        os.environ.setdefault("WISTIA_API_TOKEN", token)

    media_ids = secret.get("media_ids")
    if media_ids:
        os.environ.setdefault("MEDIA_IDS", ",".join(media_ids))

    ip_hmac = secret.get("ip_hash_key") or secret.get("hmac_secret")
    if ip_hmac:
        os.environ.setdefault("VISITOR_IP_HMAC_KEY", str(ip_hmac))
