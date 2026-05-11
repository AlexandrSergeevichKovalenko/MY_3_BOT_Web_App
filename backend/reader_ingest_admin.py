import argparse
import json

from backend.backend_server import _process_reader_library_ingest_job
from backend.database import (
    get_reader_library_document,
    get_reader_library_document_ingest_state,
)


def _build_payload(*, user_id: int, document_id: int, source_lang: str, target_lang: str) -> dict:
    ingest_state = get_reader_library_document_ingest_state(
        user_id=int(user_id),
        document_id=int(document_id),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    if not ingest_state:
        raise SystemExit(f"document {document_id} not found")
    raw_payload = dict(ingest_state.get("ingest_payload") or {})
    if not raw_payload:
        raise SystemExit(f"document {document_id} has no ingest_payload")
    payload = {
        "user_id": int(user_id),
        "document_id": int(document_id),
        "source_lang": source_lang,
        "target_lang": target_lang,
        "input_text": str(raw_payload.get("input_text") or ""),
        "input_url": str(raw_payload.get("input_url") or ""),
        "file_name": str(raw_payload.get("file_name") or ""),
        "file_mime": str(raw_payload.get("file_mime") or ""),
        "file_content_b64": "",
        "upload_tmp_path": "",
        "upload_r2_object_key": str(raw_payload.get("upload_r2_object_key") or ""),
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Process one reader ingest document directly.")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--document-id", type=int, required=True)
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    args = parser.parse_args()

    payload = _build_payload(
        user_id=args.user_id,
        document_id=args.document_id,
        source_lang=str(args.source_lang or "ru").strip().lower() or "ru",
        target_lang=str(args.target_lang or "de").strip().lower() or "de",
    )
    _process_reader_library_ingest_job(**payload)
    document = get_reader_library_document(
        user_id=args.user_id,
        document_id=args.document_id,
        source_lang=payload["source_lang"],
        target_lang=payload["target_lang"],
        include_content=False,
    )
    print(json.dumps(document or {}, ensure_ascii=False))


if __name__ == "__main__":
    main()
