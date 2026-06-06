import importlib

import backend.tts_generation as tts_generation


def test_german_tts_default_matches_current_app_voice():
    assert tts_generation._TTS_VOICES["de"] == "de-DE-Neural2-C"
    assert tts_generation._normalize_tts_voice_name(None, "de") == "de-DE-Neural2-C"


def test_german_tts_default_can_be_overridden_by_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_TTS_VOICE_DE", "de-DE-Standard-A")
    reloaded = importlib.reload(tts_generation)
    try:
        assert reloaded._TTS_VOICES["de"] == "de-DE-Standard-A"
        assert reloaded._normalize_tts_voice_name(None, "de") == "de-DE-Standard-A"
    finally:
        monkeypatch.delenv("GOOGLE_TTS_VOICE_DE", raising=False)
        importlib.reload(tts_generation)
