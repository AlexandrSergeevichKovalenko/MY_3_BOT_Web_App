"""
OCR evaluation fixtures — real-world-representative screenshot payloads.

Each fixture has:
  raw       — raw OCR text exactly as extracted from the image
  must_keep — substrings that MUST appear in the cleaned output
  must_drop — substrings that must NOT appear in the cleaned output
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OcrFixture:
    name: str
    raw: str
    must_keep: tuple[str, ...]
    must_drop: tuple[str, ...]


# --------------------------------------------------------------------------
# 1. Instagram Reel — German vocabulary post
# --------------------------------------------------------------------------
INSTAGRAM_VOCABULARY_REEL = OcrFixture(
    name="instagram_vocabulary_reel",
    raw=(
        "@germanlinguist\n"
        'Das Verb "aufgeben" bedeutet:\n'
        "1. aufhören / to give up\n"
        "2. abschicken / to mail, submit\n"
        "3. auferlegen / to impose\n"
        "#deutsch #germanwords #lernen #sprachenlernen\n"
        "❤️ 2.4K   💬 89   ↗️ Share\n"
        "Original Audio\n"
    ),
    must_keep=(
        'Das Verb "aufgeben" bedeutet:',
        "aufhören",
        "to give up",
        "abschicken",
        "auferlegen",
    ),
    must_drop=(
        "@germanlinguist",
        "#deutsch",
        "#germanwords",
        "Original Audio",
        "2.4K",
        "Share",
    ),
)


# --------------------------------------------------------------------------
# 2. TikTok — German subtitle dump with duet label and view count
# --------------------------------------------------------------------------
TIKTOK_SUBTITLE_DUMP = OcrFixture(
    name="tiktok_subtitle_dump",
    raw=(
        "duet with @sprachcoach_berlin\n"
        "Ich habe gestern ein Buch gelesen,\n"
        "das sehr interessant war.\n"
        "Man kann viel durch Lesen lernen.\n"
        "3:42\n"
        "1.1M views\n"
        "#deutsch #bücher #lesen\n"
    ),
    must_keep=(
        "Ich habe gestern ein Buch gelesen",
        "das sehr interessant war.",
        "Man kann viel durch Lesen lernen.",
    ),
    must_drop=(
        "duet with",
        "@sprachcoach_berlin",
        "3:42",
        "1.1M views",
        "#deutsch",
        "#lesen",
    ),
)


# --------------------------------------------------------------------------
# 3. Mixed Russian/German — language learning post
# --------------------------------------------------------------------------
MIXED_RUSSIAN_GERMAN = OcrFixture(
    name="mixed_russian_german",
    raw=(
        "@berlin_russisch\n"
        "Das ist sehr wichtig — это очень важно\n"
        "Ich bin müde — я устал\n"
        "Guten Morgen — доброе утро\n"
        "#russisch #deutsch #zweisprachig #билингвизм\n"
        "подписчиков: 5,2K\n"
        "sponsored\n"
    ),
    must_keep=(
        "Das ist sehr wichtig",
        "это очень важно",
        "Ich bin müde",
        "я устал",
        "Guten Morgen",
        "доброе утро",
    ),
    must_drop=(
        "@berlin_russisch",
        "#russisch",
        "#deutsch",
        "5,2K",
        "sponsored",
    ),
)


# --------------------------------------------------------------------------
# 4. Emoji-heavy payload — isolated emoji lines mixed with real content
# --------------------------------------------------------------------------
EMOJI_HEAVY_PAYLOAD = OcrFixture(
    name="emoji_heavy_payload",
    raw=(
        "😂😂😂😂\n"
        "Ich lache so viel!\n"
        "🔥💯🎉🥳🎊🎈\n"
        "Das ist wirklich super!\n"
        "🇩🇪🇩🇪🇩🇪🇩🇪🇩🇪\n"
        "Deutsch ist eine schöne Sprache.\n"
    ),
    must_keep=(
        "Ich lache so viel!",
        "Das ist wirklich super!",
        "Deutsch ist eine schöne Sprache.",
    ),
    must_drop=(),  # emoji-only lines removed; mixed lines preserved
)


# --------------------------------------------------------------------------
# 5. OCR mistakes — mangled German quotes, excessive punctuation
# --------------------------------------------------------------------------
OCR_MISTAKES = OcrFixture(
    name="ocr_mistakes",
    raw=(
        ",,Ich bin so glücklich!,, sagte sie.\n"
        "Was ist das???\n"
        "Unglaublich!!!!\n"
        "Er sagte: ,,Das ist wunderbar.,,\n"
        "Wirklich.....\n"
    ),
    must_keep=(
        "Ich bin so glücklich",
        "Was ist das",
        "Unglaublich",
        "Das ist wunderbar",
        "Wirklich",
    ),
    must_drop=(),  # after fix: „ replaces ,,; punctuation normalized — no content dropped
)


# --------------------------------------------------------------------------
# 6. Pure UI garbage — nothing learnable
# --------------------------------------------------------------------------
PURE_UI_GARBAGE = OcrFixture(
    name="pure_ui_garbage",
    raw=(
        "@user123 @user456\n"
        "#hashtag1 #hashtag2 #hashtag3\n"
        "1.5K likes  234 comments  45 shares\n"
        "Original Audio\n"
        "duet with @someone\n"
        "sponsored\n"
        "0:30\n"
    ),
    must_keep=(),   # everything should be removed
    must_drop=(
        "@user123",
        "@user456",
        "#hashtag1",
        "1.5K likes",
        "234 comments",
        "Original Audio",
        "duet with",
        "sponsored",
    ),
)


# --------------------------------------------------------------------------
# 7. Multiline German subtitles — should pass through clean
# --------------------------------------------------------------------------
MULTILINE_GERMAN_SUBTITLES = OcrFixture(
    name="multiline_german_subtitles",
    raw=(
        "Wenn ich in Deutschland bin,\n"
        "spreche ich immer Deutsch.\n"
        "Aber manchmal ist es schwierig,\n"
        "weil die Grammatik kompliziert ist.\n"
        "Trotzdem macht es mir viel Spaß!\n"
    ),
    must_keep=(
        "Wenn ich in Deutschland bin",
        "spreche ich immer Deutsch.",
        "Aber manchmal ist es schwierig",
        "weil die Grammatik kompliziert ist.",
        "Trotzdem macht es mir viel Spaß!",
    ),
    must_drop=(),
)


# --------------------------------------------------------------------------
# 8. Clean learnable German text — no noise, must be unchanged
# --------------------------------------------------------------------------
CLEAN_GERMAN_TEXT = OcrFixture(
    name="clean_german_text",
    raw=(
        "Das ist ein guter Tag.\n"
        "Ich lerne Deutsch seit zwei Jahren.\n"
        "Die Sprache ist schön, aber schwer.\n"
    ),
    must_keep=(
        "Das ist ein guter Tag.",
        "Ich lerne Deutsch seit zwei Jahren.",
        "Die Sprache ist schön, aber schwer.",
    ),
    must_drop=(),
)


# --------------------------------------------------------------------------
# 9. Instagram story — counters on same line as text
# --------------------------------------------------------------------------
INSTAGRAM_STORY_MIXED = OcrFixture(
    name="instagram_story_mixed",
    raw=(
        "Heute habe ich 3 neue Wörter gelernt!\n"
        "12.3K likes\n"
        "Wortschatz des Tages: die Freude (joy)\n"
        "See more\n"
        "#wortschatz #deutschlernen\n"
    ),
    must_keep=(
        "Heute habe ich 3 neue Wörter gelernt!",
        "Wortschatz des Tages: die Freude (joy)",
    ),
    must_drop=(
        "12.3K likes",
        "See more",
        "#wortschatz",
        "#deutschlernen",
    ),
)


# --------------------------------------------------------------------------
# 10. TikTok with whitespace noise — tabs, CRLF, repeated spaces
# --------------------------------------------------------------------------
TIKTOK_WHITESPACE_NOISE = OcrFixture(
    name="tiktok_whitespace_noise",
    raw=(
        "Guten\tMorgen!\r\n"
        "Wie  geht  es  Ihnen?\r\n"
        "  Das ist eine Frage.\n"
        "\n\n\n"
        "Bitte antworten Sie.\n"
    ),
    must_keep=(
        "Guten Morgen!",
        "Wie geht es Ihnen?",
        "Das ist eine Frage.",
        "Bitte antworten Sie.",
    ),
    must_drop=(),
)


ALL_FIXTURES: tuple[OcrFixture, ...] = (
    INSTAGRAM_VOCABULARY_REEL,
    TIKTOK_SUBTITLE_DUMP,
    MIXED_RUSSIAN_GERMAN,
    EMOJI_HEAVY_PAYLOAD,
    OCR_MISTAKES,
    PURE_UI_GARBAGE,
    MULTILINE_GERMAN_SUBTITLES,
    CLEAN_GERMAN_TEXT,
    INSTAGRAM_STORY_MIXED,
    TIKTOK_WHITESPACE_NOISE,
)


# =============================================================================
# v2 Learnability fixtures — already-cleaned text (after OCR pipeline v1)
# scored at the candidate level.
# =============================================================================

@dataclass(frozen=True, slots=True)
class LearnabilityFixture:
    name: str
    text: str            # already-cleaned text (output of OCR pipeline v1)
    expected_label: str  # "likely_learnable" | "likely_noise" | "uncertain"
    score_min: float     # minimum acceptable score
    score_max: float     # maximum acceptable score


# --------------------------------------------------------------------------
# 1. Obvious garbage — pure numeric / engagement residual
# --------------------------------------------------------------------------
NOISE_PURE_NUMERIC = LearnabilityFixture(
    name="noise_pure_numeric",
    text="89",
    expected_label="likely_noise",
    score_min=-1.0, score_max=-0.21,
)

NOISE_EMOJI_ONLY = LearnabilityFixture(
    name="noise_emoji_only",
    text="😂😂😂😂",
    expected_label="likely_noise",
    score_min=-1.0, score_max=-0.21,
)

NOISE_ENGAGEMENT_RESIDUAL = LearnabilityFixture(
    name="noise_engagement_residual",
    text="1.2K likes",
    expected_label="likely_noise",
    score_min=-1.0, score_max=-0.21,
)

NOISE_SINGLE_CTA = LearnabilityFixture(
    name="noise_single_cta",
    text="Follow",
    expected_label="likely_noise",
    score_min=-1.0, score_max=-0.21,
)

NOISE_DOUBLE_CTA = LearnabilityFixture(
    name="noise_double_cta",
    text="Follow\nSubscribe",
    expected_label="likely_noise",
    score_min=-1.0, score_max=-0.21,
)

# --------------------------------------------------------------------------
# 2. Borderline subtitle fragments — expected uncertain
# --------------------------------------------------------------------------
UNCERTAIN_SHORT_SENTENCE = LearnabilityFixture(
    name="uncertain_short_sentence",
    text="Okay!",
    expected_label="uncertain",
    score_min=-0.19, score_max=0.19,
)

UNCERTAIN_MIXED_NUMBER_TEXT = LearnabilityFixture(
    name="uncertain_mixed_number_text",
    text="3:42 Ich lerne",
    expected_label="uncertain",
    score_min=-0.19, score_max=0.19,
)

UNCERTAIN_REACTION_MEME = LearnabilityFixture(
    name="uncertain_reaction_meme",
    text="😂😂 so funny lol 😂😂",
    expected_label="uncertain",
    score_min=-0.19, score_max=0.19,
)

# --------------------------------------------------------------------------
# 3. Clearly learnable — normal German phrases and sentences
# --------------------------------------------------------------------------
LEARNABLE_GERMAN_SENTENCE = LearnabilityFixture(
    name="learnable_german_sentence",
    text="Das ist ein guter Tag.",
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)

LEARNABLE_GERMAN_VERB_SENTENCE = LearnabilityFixture(
    name="learnable_german_verb_sentence",
    text="Ich möchte Deutsch lernen.",
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)

LEARNABLE_MULTILINE_SUBTITLE = LearnabilityFixture(
    name="learnable_multiline_subtitle",
    text="Wenn ich in Deutschland bin,\nspreche ich immer Deutsch.",
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)

LEARNABLE_RUSSIAN_PHRASE = LearnabilityFixture(
    name="learnable_russian_phrase",
    text="Привет, как дела? Всё хорошо.",
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)

LEARNABLE_PEDAGOGICAL = LearnabilityFixture(
    name="learnable_pedagogical",
    text="aufgeben — to give up, to abandon",
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)

LEARNABLE_DENSE_SUBTITLES = LearnabilityFixture(
    name="learnable_dense_subtitles",
    text=(
        "Ich lerne jeden Tag neue Wörter.\n"
        "Das macht mir viel Freude.\n"
        "Deutsch ist eine schöne Sprache."
    ),
    expected_label="likely_learnable",
    score_min=0.2, score_max=1.0,
)


ALL_LEARNABILITY_FIXTURES: tuple[LearnabilityFixture, ...] = (
    NOISE_PURE_NUMERIC,
    NOISE_EMOJI_ONLY,
    NOISE_ENGAGEMENT_RESIDUAL,
    NOISE_SINGLE_CTA,
    NOISE_DOUBLE_CTA,
    UNCERTAIN_SHORT_SENTENCE,
    UNCERTAIN_MIXED_NUMBER_TEXT,
    UNCERTAIN_REACTION_MEME,
    LEARNABLE_GERMAN_SENTENCE,
    LEARNABLE_GERMAN_VERB_SENTENCE,
    LEARNABLE_MULTILINE_SUBTITLE,
    LEARNABLE_RUSSIAN_PHRASE,
    LEARNABLE_PEDAGOGICAL,
    LEARNABLE_DENSE_SUBTITLES,
)
