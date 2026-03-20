import os
from dataclasses import dataclass
from collections import defaultdict
from functools import lru_cache
from typing import Any
from urllib.parse import quote


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    endpoint: str
    public_base_url: str


def _required_env(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise RuntimeError(f"Missing required R2 env: {name}")
    return value


def load_r2_config_from_env() -> R2Config:
    account_id = _required_env("R2_ACCOUNT_ID")
    access_key_id = _required_env("R2_ACCESS_KEY_ID")
    secret_access_key = _required_env("R2_SECRET_ACCESS_KEY")
    bucket_name = _required_env("R2_BUCKET_NAME")
    endpoint_raw = _required_env("R2_ENDPOINT")
    public_base_url = _required_env("R2_PUBLIC_BASE_URL")

    endpoint = endpoint_raw.replace("ACCOUNT_ID", account_id).strip().rstrip("/")
    if "ACCOUNT_ID" in endpoint:
        raise RuntimeError("R2_ENDPOINT still contains ACCOUNT_ID placeholder")

    return R2Config(
        account_id=account_id,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket_name=bucket_name,
        endpoint=endpoint,
        public_base_url=public_base_url.rstrip("/"),
    )


@lru_cache(maxsize=1)
def _r2_client() -> Any:
    cfg = load_r2_config_from_env()
    try:
        import boto3
        from botocore.config import Config
    except Exception as exc:
        raise RuntimeError(
            "boto3/botocore are required for R2 support. Add boto3 to requirements."
        ) from exc
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def _normalize_object_key(object_key: str) -> str:
    normalized = str(object_key or "").strip().lstrip("/")
    if not normalized:
        raise ValueError("object_key is required")
    return normalized


def r2_public_url(object_key: str) -> str:
    cfg = load_r2_config_from_env()
    normalized_key = _normalize_object_key(object_key)
    escaped_key = quote(normalized_key, safe="/-_.~")
    return f"{cfg.public_base_url}/{escaped_key}"


def r2_exists(object_key: str) -> bool:
    cfg = load_r2_config_from_env()
    key = _normalize_object_key(object_key)
    client = _r2_client()
    try:
        from botocore.exceptions import ClientError
    except Exception as exc:
        raise RuntimeError(
            "boto3/botocore are required for R2 support. Add boto3 to requirements."
        ) from exc
    try:
        client.head_object(Bucket=cfg.bucket_name, Key=key)
        return True
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code", "")).strip()
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def r2_put_bytes(
    object_key: str,
    data: bytes,
    *,
    content_type: str = "audio/mpeg",
    cache_control: str = "public, max-age=31536000, immutable",
) -> None:
    cfg = load_r2_config_from_env()
    key = _normalize_object_key(object_key)
    payload = bytes(data or b"")
    if not payload:
        raise ValueError("data must be non-empty bytes")
    client = _r2_client()
    client.put_object(
        Bucket=cfg.bucket_name,
        Key=key,
        Body=payload,
        ContentType=content_type,
        CacheControl=cache_control,
    )


def r2_delete_object(object_key: str) -> bool:
    cfg = load_r2_config_from_env()
    key = _normalize_object_key(object_key)
    client = _r2_client()
    try:
        from botocore.exceptions import ClientError
    except Exception as exc:
        raise RuntimeError(
            "boto3/botocore are required for R2 support. Add boto3 to requirements."
        ) from exc
    try:
        client.delete_object(Bucket=cfg.bucket_name, Key=key)
        return True
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code", "")).strip()
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def r2_bucket_usage_summary(
    *,
    prefix_depth: int = 1,
    min_prefix_bytes: int = 0,
    max_prefixes: int = 50,
) -> dict[str, Any]:
    cfg = load_r2_config_from_env()
    client = _r2_client()
    paginator = client.get_paginator("list_objects_v2")
    depth = max(1, int(prefix_depth or 1))
    min_bytes = max(0, int(min_prefix_bytes or 0))
    limit = max(1, int(max_prefixes or 50))
    total_objects = 0
    total_bytes = 0
    by_prefix: dict[str, dict[str, int]] = defaultdict(lambda: {"objects": 0, "bytes": 0})

    for page in paginator.paginate(Bucket=cfg.bucket_name):
        for obj in page.get("Contents") or []:
            key = str(obj.get("Key") or "").strip()
            if not key:
                continue
            size_bytes = int(obj.get("Size") or 0)
            total_objects += 1
            total_bytes += size_bytes
            key_parts = [part for part in key.split("/") if part]
            prefix = "/".join(key_parts[:depth]) if key_parts else "(root)"
            entry = by_prefix[prefix]
            entry["objects"] += 1
            entry["bytes"] += size_bytes

    prefixes = [
        {
            "prefix": prefix,
            "objects": int(values.get("objects") or 0),
            "bytes": int(values.get("bytes") or 0),
        }
        for prefix, values in by_prefix.items()
        if int(values.get("bytes") or 0) >= min_bytes
    ]
    prefixes.sort(key=lambda item: (-int(item["bytes"]), item["prefix"]))
    return {
        "bucket_name": cfg.bucket_name,
        "total_objects": total_objects,
        "total_bytes": total_bytes,
        "prefix_depth": depth,
        "prefixes": prefixes[:limit],
    }
