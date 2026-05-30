"""
Regression smoke tests — critical flows that have broken before.

Each test here corresponds to a confirmed past regression and pins the
behavior that was broken.  Tests are intentionally narrow: they cover
exactly the thing that broke, not a complete integration scenario.

Run with:
    python -m pytest backend/tests/test_regression_smoke.py -v
"""
import os
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch, call

import backend.backend_server as server
import backend.background_jobs as jobs


# ──────────────────────────────────────────────────────────────────────────────
# 1. ReaderLibraryProvider: value object must include readerArchiveOpen
# ──────────────────────────────────────────────────────────────────────────────

class ReaderSectionValueObjectTests(unittest.TestCase):
    """
    Regression: commit c3223141 introduced ReaderLibraryProvider but the
    value object in ReaderSection.jsx omitted readerArchiveOpen.
    validateReaderLibraryValue() throws on every library open.

    These tests run without a browser — they read the source files directly.
    """

    def test_reader_section_passes_readerarchiveopen_to_provider(self):
        """ReaderSection.jsx value object forwarded to provider must include readerArchiveOpen."""
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "components", "ReaderSection.jsx",
        )
        with open(os.path.normpath(src_path)) as fh:
            src = fh.read()
        # The value object between <ReaderLibraryProvider value={{...}}> and the
        # closing }}>  must contain readerArchiveOpen.
        provider_start = src.find("<ReaderLibraryProvider")
        self.assertGreater(provider_start, 0, "ReaderLibraryProvider not found in ReaderSection.jsx")
        provider_block = src[provider_start: provider_start + 3000]
        self.assertIn(
            "readerArchiveOpen",
            provider_block,
            "readerArchiveOpen is missing from the ReaderLibraryProvider value object "
            "(regression: causes validateReaderLibraryValue to throw on library open)",
        )

    def test_required_keys_all_present_in_reader_section_value_object(self):
        """All REQUIRED_READER_LIBRARY_KEYS must be forwarded by ReaderSection.jsx."""
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "components", "ReaderSection.jsx",
        )
        provider_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "providers", "ReaderLibraryProvider.jsx",
        )
        with open(os.path.normpath(src_path)) as fh:
            reader_src = fh.read()
        with open(os.path.normpath(provider_path)) as fh:
            provider_src = fh.read()

        # Extract REQUIRED_READER_LIBRARY_KEYS from provider source.
        import re
        keys_block_match = re.search(
            r"REQUIRED_READER_LIBRARY_KEYS\s*=\s*\[(.*?)\]",
            provider_src,
            re.DOTALL,
        )
        self.assertIsNotNone(keys_block_match, "Could not parse REQUIRED_READER_LIBRARY_KEYS")
        raw_keys = re.findall(r"'([^']+)'", keys_block_match.group(1))
        self.assertGreater(len(raw_keys), 0)

        # Find the provider value block in ReaderSection.jsx.
        provider_start = reader_src.find("<ReaderLibraryProvider")
        self.assertGreater(provider_start, 0)
        # Take 4000 chars — large enough to cover the whole value object.
        provider_block = reader_src[provider_start: provider_start + 4000]

        missing = [k for k in raw_keys if k not in provider_block]
        self.assertEqual(
            missing, [],
            f"Keys required by ReaderLibraryProvider are missing from ReaderSection.jsx: {missing}",
        )


# ──────────────────────────────────────────────────────────────────────────────
# 2. Shortcut delivery: OCR noise routing returns 0, does NOT send Telegram msg
# ──────────────────────────────────────────────────────────────────────────────

class ShortcutDeliveryOcrRoutingTests(unittest.TestCase):
    """
    _run_shortcut_lookup_delivery must respect OCR v2 routing.

    skip_all_noise  → returns 0, zero Telegram sends.
    proceed         → calls _shortcut_split_blocks and sends ≥1 message.
    """

    def _run_delivery(self, text, *, routing, pass_list=None, noise_list=None, blocks=None):
        from backend.ocr_pipeline import OcrCandidate, LearnabilityScore

        if pass_list is None:
            pass_list = []
        if noise_list is None:
            noise_list = []
        if blocks is None:
            blocks = []

        dummy_archetype = MagicMock()
        dummy_archetype.archetype = "vocabulary"
        dummy_archetype.confidence = 0.9
        dummy_archetype.german_target_count = 1
        dummy_archetype.support_language_count = 1

        dummy_grouped_payload = MagicMock()
        dummy_grouped_payload.candidate_count = 1
        dummy_grouped_payload.german_group_count = 1
        dummy_grouped_payload.groups = []

        dummy_structured = MagicMock()
        dummy_structured.payload_id = 1
        dummy_structured.archetype = "vocabulary"
        dummy_structured.grouped_units = []
        dummy_structured.german_target_detected = True
        dummy_structured.extraction_priority = "high"
        dummy_structured.confidence = 0.9
        dummy_structured.support_languages = []

        sends = []

        def _fake_send(user_id, text, **_kw):
            sends.append(text)

        with ExitStack() as stack:
            stack.enter_context(patch.object(server, "classify_archetype", return_value=dummy_archetype))
            stack.enter_context(patch.object(server, "group_spatially", return_value=[]))
            stack.enter_context(patch.object(server, "segment_candidates", return_value=pass_list + noise_list))
            stack.enter_context(patch.object(server, "route_candidates", return_value=(routing, pass_list, noise_list)))
            stack.enter_context(patch.object(server, "_shortcut_split_blocks", return_value=blocks))
            stack.enter_context(patch.object(server, "_send_private_message", side_effect=_fake_send))
            result = server._run_shortcut_lookup_delivery(
                user_id=111,
                text=text,
                source="shortcut",
            )
        return result, sends

    def test_skip_all_noise_returns_zero_and_sends_nothing(self):
        """Pure noise text: OCR routing skip_all_noise → 0 messages, 0 sends."""
        result, sends = self._run_delivery(
            "hgjkasdhgjksd",
            routing="skip_all_noise",
            pass_list=[],
        )
        self.assertEqual(result, 0)
        self.assertEqual(sends, [])

    def test_proceed_routing_sends_one_message_per_block(self):
        """Learnable text: OCR routing proceed → one Telegram send per block."""
        from backend.ocr_pipeline import OcrCandidate, LearnabilityScore

        fake_cand = MagicMock()
        fake_cand.text = "sich abfinden mit"
        fake_cand.token_estimate = 5
        fake_cand.detected_languages = ["de"]
        fake_score = MagicMock()
        fake_score.score = 0.65
        fake_score.label = "likely_learnable"
        fake_score.breakdown = {}

        result, sends = self._run_delivery(
            "sich abfinden mit",
            routing="proceed",
            pass_list=[(fake_cand, fake_score)],
            blocks=[("sich abfinden mit", "sich abfinden mit")],
        )
        self.assertEqual(result, 1)
        self.assertEqual(len(sends), 1)
        self.assertIn("sich abfinden mit", sends[0])

    def test_empty_blocks_after_proceed_sends_nothing(self):
        """LLM split returns no blocks after proceed routing → 0 sends (not a crash)."""
        fake_cand = MagicMock()
        fake_cand.text = "abc"
        fake_cand.token_estimate = 2
        fake_cand.detected_languages = []
        fake_score = MagicMock()
        fake_score.score = 0.3
        fake_score.label = "likely_learnable"
        fake_score.breakdown = {}

        result, sends = self._run_delivery(
            "abc",
            routing="proceed",
            pass_list=[(fake_cand, fake_score)],
            blocks=[],
        )
        self.assertEqual(result, 0)
        self.assertEqual(sends, [])


# ──────────────────────────────────────────────────────────────────────────────
# 3. Shortcut worker: processing status DB failure must NOT block delivery
# ──────────────────────────────────────────────────────────────────────────────

class ShortcutWorkerDeliveryResilienceTests(unittest.TestCase):
    """
    Regression: commit 9d73c3db put set_status("processing") inside the same
    try/except as row loading and added `raise`.  Any transient DB error during
    status bookkeeping aborted delivery; row stayed at "queued" forever.

    Fix (f7f75b66): status update has its own try/except; delivery continues.
    This class pins that contract.
    """

    def _run_worker(self, *, set_status_side_effect=None):
        import backend.database as db_module

        row = {
            "ingest_id": 42, "status": "queued", "job_id": "j1",
            "duplicate_count": 0, "sent_prompt_count": 0,
            "accepted_at": None, "completed_at": None, "error_code": None,
            "normalized_text_full": "sich abfinden mit",
            "normalized_text_sha256": "",
            "normalized_text_size_bytes": 18,
        }
        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row))
            stack.enter_context(patch.object(
                db_module, "shortcut_ingest_request_set_status",
                side_effect=set_status_side_effect,
            ))
            stack.enter_context(patch(
                "backend.backend_server._run_shortcut_lookup_delivery",
                side_effect=lambda **kw: delivery_calls.append(kw) or 2,
            ))
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="sich abfinden mit",
                source="shortcut",
                ingest_key="abc123",
                ingest_id=42,
            )
        return delivery_calls

    def test_happy_path_delivery_called(self):
        """Normal path: delivery is called and returns sent count."""
        calls = self._run_worker()
        self.assertEqual(len(calls), 1)

    def test_processing_status_db_failure_does_not_block_delivery(self):
        """DB error on set_status(processing) must not abort delivery."""
        def _fail_on_processing(*, status, **_kw):
            if status == "processing":
                raise RuntimeError("DB write failed")

        calls = self._run_worker(set_status_side_effect=_fail_on_processing)
        self.assertEqual(len(calls), 1, "delivery must still be called despite processing status DB failure")


# ──────────────────────────────────────────────────────────────────────────────
# 4. YouTube startup: open_section=false must NOT open YouTube section
# ──────────────────────────────────────────────────────────────────────────────

class YouTubeStartupNavigationTests(unittest.TestCase):
    """
    Regression: commit 4626b672 fixed executeYoutubeCommand which called
    ensureSectionVisible('youtube') even when open_section=false was set.
    This caused the app to open on YouTube on every startup.
    """

    def test_app_jsx_does_not_call_ensuresectionvisible_on_open_section_false(self):
        """App.jsx open_section=false path must NOT call ensureSectionVisible."""
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "App.jsx",
        )
        with open(os.path.normpath(src_path)) as fh:
            src = fh.read()

        # Find the block that handles request_video_selection.
        idx = src.find("request_video_selection")
        self.assertGreater(idx, 0, "request_video_selection not found in App.jsx")

        # The 1500-char window around the handler should NOT contain
        # ensureSectionVisible('youtube') inside the open_section===false branch.
        # The only legal call to openSingleSectionAndScroll in this block is
        # guarded by `payload?.open_section !== false`.
        handler_block = src[idx: idx + 1500]
        # If the else-branch ensureSectionVisible regression is re-introduced,
        # there will be a second call to ensureSectionVisible('youtube') after
        # the openSingleSectionAndScroll call.
        occurrences = handler_block.count("ensureSectionVisible('youtube')")
        self.assertEqual(
            occurrences, 0,
            "ensureSectionVisible('youtube') found in request_video_selection handler — "
            "this re-introduces the YouTube startup navigation regression",
        )


# ──────────────────────────────────────────────────────────────────────────────
# 5. HomeDashboardTiles: locked section props are accepted by the component
# ──────────────────────────────────────────────────────────────────────────────

class HomeDashboardTilesLockedSectionTests(unittest.TestCase):
    """
    Verify structural wiring: HomeDashboardTiles accepts lockedSectionKeys and
    onLockedSection and passes them to DashTile rows.
    """

    def test_homedashboardtiles_accepts_locked_section_props(self):
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "components", "HomeDashboardTiles.jsx",
        )
        with open(os.path.normpath(src_path)) as fh:
            src = fh.read()
        self.assertIn("lockedSectionKeys", src, "lockedSectionKeys prop missing from HomeDashboardTiles")
        self.assertIn("onLockedSection", src, "onLockedSection prop missing from HomeDashboardTiles")
        # DashTile rows must check lockedSectionKeys before opening.
        self.assertIn("lockedSectionKeys?.has?.(", src)
        self.assertIn("onLockedSection?.(", src)

    def test_app_jsx_passes_locked_section_keys_to_homedashboardtiles(self):
        src_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "frontend", "src", "App.jsx",
        )
        with open(os.path.normpath(src_path)) as fh:
            src = fh.read()
        # Find the JSX usage (not the lazy() import declaration).
        # We look for <HomeDashboardTiles which is the render call.
        hdt_idx = src.find("<HomeDashboardTiles")
        self.assertGreater(hdt_idx, 0, "<HomeDashboardTiles JSX not found in App.jsx")
        hdt_block = src[hdt_idx: hdt_idx + 800]
        self.assertIn("lockedSectionKeys=", hdt_block)
        self.assertIn("onLockedSection=", hdt_block)


if __name__ == "__main__":
    unittest.main()
