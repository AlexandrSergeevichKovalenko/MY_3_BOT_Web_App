import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode

import requests


BASE_URL = "https://backendwebbackendserverpy-production.up.railway.app"
BOT_TOKEN = os.environ["TELEGRAM_Deutsch_BOT_TOKEN"]
USER_ID = 117649764
USERNAME = "AlexandrSergeevichKovalenko"
FIRST_NAME = "Oleksandr"
LAST_NAME = "Kovalenko"
TOPIC = "Nebensaetze mit weil, dass, wenn, obwohl"
LEVEL = "b2"


def _build_telegram_login_payload() -> dict:
    auth_date = str(int(time.time()))
    payload = {
        "id": str(USER_ID),
        "first_name": FIRST_NAME,
        "last_name": LAST_NAME,
        "username": USERNAME,
        "auth_date": auth_date,
    }
    data_check_string = "\n".join(f"{key}={payload[key]}" for key in sorted(payload.keys()))
    secret_key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    payload["hash"] = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return payload


def main() -> None:
    session = requests.Session()
    login_payload = _build_telegram_login_payload()
    auth_resp = session.post(
        f"{BASE_URL}/api/web/auth/telegram",
        json=login_payload,
        timeout=30,
    )
    auth_resp.raise_for_status()
    auth_body = auth_resp.json()
    init_data = str(auth_body.get("initData") or "").strip()
    if not init_data:
        raise RuntimeError(f"Missing initData in auth response: {auth_body}")

    request_payload = {
        "initData": init_data,
        "topic": TOPIC,
        "level": LEVEL,
        "force_new_session": True,
    }
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    start_resp = session.post(
        f"{BASE_URL}/api/webapp/start",
        json=request_payload,
        timeout=120,
    )
    body = start_resp.json()
    print(
        json.dumps(
            {
                "started_at_utc": started_at,
                "auth_status": auth_resp.status_code,
                "start_status": start_resp.status_code,
                "response": body,
                "init_data_preview": urlencode({"initData": init_data[:120]})[:160],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
