import hashlib
import hmac
import json
import sys
import time
from urllib import parse, request


def _post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _build_init_data(*, bot_token: str, user_id: int, username: str, first_name: str) -> str:
    auth_date = int(time.time())
    user_payload = json.dumps(
        {
            "id": int(user_id),
            "first_name": first_name,
            "username": username,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    payload = {
        "auth_date": str(auth_date),
        "user": user_payload,
    }
    data_check_string = "\n".join(f"{k}={payload[k]}" for k in sorted(payload))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    payload["hash"] = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return parse.urlencode(payload)


def main() -> None:
    if len(sys.argv) != 7:
        raise SystemExit("usage: tts_route_proof.py <base_url> <bot_token> <user_id> <username> <first_name> <text>")
    base_url, bot_token, user_id_raw, username, first_name, text = sys.argv[1:]
    user_id = int(user_id_raw)
    init_data = _build_init_data(
        bot_token=bot_token,
        user_id=user_id,
        username=username,
        first_name=first_name,
    )
    generate_result = _post_json(
        f"{base_url.rstrip('/')}/api/webapp/tts/generate",
        {
            "initData": init_data,
            "text": text,
            "language": "de-DE",
        },
    )
    print(json.dumps({"ok": True, "generate": generate_result}))


if __name__ == "__main__":
    main()
