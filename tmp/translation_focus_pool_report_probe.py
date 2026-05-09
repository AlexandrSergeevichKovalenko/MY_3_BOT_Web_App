import os
import socket


def main() -> None:
    body = b'{"force": true}'
    token = os.environ["AUDIO_DISPATCH_TOKEN"]
    headers = (
        "POST /api/admin/send-translation-focus-pool-report HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"X-Admin-Token: {token}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    sock = socket.socket()
    sock.settimeout(60)
    sock.connect(("127.0.0.1", 8080))
    sock.sendall(headers + body)
    chunks: list[bytes] = []
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
    except Exception as exc:  # noqa: BLE001
        print(type(exc).__name__, str(exc))
    finally:
        sock.close()
    print(b"".join(chunks).decode("utf-8", "ignore"))


if __name__ == "__main__":
    main()
