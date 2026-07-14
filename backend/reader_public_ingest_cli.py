"""CLI to (re)build the curated public-domain reader library.

    python -m backend.reader_public_ingest_cli --dry-run          # resolve+extract, no DB writes
    python -m backend.reader_public_ingest_cli                     # ingest all 20 books
    python -m backend.reader_public_ingest_cli --slug heidi --slug verwandlung

Dry-run still hits Project Gutenberg (to verify each title resolves and report the
extracted char count) but writes nothing. Requires the same DB/R2 env as the app.
"""

import argparse
import json
import logging


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest curated public-domain German books into the shared reader library.")
    parser.add_argument("--slug", action="append", help="Ingest only this catalog slug (repeatable). Default: all.")
    parser.add_argument("--source-lang", default="ru", help="Reader source (native) language. Default: ru.")
    parser.add_argument("--target-lang", default="de", help="Reader target (study) language. Default: de.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve + extract but do not write to the DB.")
    parser.add_argument("--audio-pregen", action="store_true", help="Instead of ingesting text, pre-generate shared audio for already-ingested public books (from the Standard free bucket).")
    parser.add_argument("--audio-max-pages", type=int, default=None, help="Cap pages synthesized this run (audio-pregen only).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.audio_pregen:
        # Imported lazily so --help works without spinning up the full server module.
        from backend.backend_server import run_public_library_audio_pregen

        summary = run_public_library_audio_pregen(
            target_lang=args.target_lang,
            max_pages=args.audio_max_pages,
            dry_run=args.dry_run,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    from backend.backend_server import ingest_public_library_catalog

    summary = ingest_public_library_catalog(
        slugs=args.slug,
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("failed", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
