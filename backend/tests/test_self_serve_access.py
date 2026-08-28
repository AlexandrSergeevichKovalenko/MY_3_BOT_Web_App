"""Self-serve access: an invite link IS the invitation, /deny is still final.

Covers:
  * brand-new user on /start: access granted, admin gets a «+1», onboarding continues
  * admin-denied user on /start: no grant, closed-door reply, onboarding NOT shown
  * already-allowed user: no grant attempt, no admin notification
  * referral attribution runs BEFORE the grant (it only counts not-yet-allowed users)
  * /start payload → «откуда пришёл» label
  * webapp guard: first Mini-App open self-serve-grants; denied user still gets 403
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


def _make_update(*, user_id=555, chat_type="private", text="/start"):
    message = SimpleNamespace(text=text, reply_text=AsyncMock(), chat_id=user_id)
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id, username="newbie", first_name="New", last_name=None),
        effective_chat=SimpleNamespace(id=user_id, type=chat_type, title=None),
        effective_message=message,
        callback_query=None,
        poll_answer=None,
        my_chat_member=None,
    )


def _make_context(args=None):
    return SimpleNamespace(
        args=list(args or []),
        user_data={},
        bot=SimpleNamespace(username="testbot", send_message=AsyncMock()),
    )


class StartSelfServeAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_user_is_granted_and_admin_notified(self):
        import bot_3
        update = _make_update()
        ctx = _make_context(["ref_777"])
        grant = Mock(return_value=True)
        with patch.object(bot_3, "_maybe_capture_referral", AsyncMock()), \
             patch.object(bot_3, "is_telegram_user_allowed", Mock(return_value=False)), \
             patch.object(bot_3, "auto_grant_telegram_user", grant), \
             patch.object(bot_3, "_notify_admins_new_user", AsyncMock()) as notify, \
             patch.object(bot_3, "_ensure_reply_keyboard_delivered", AsyncMock()), \
             patch.object(bot_3, "_onboarding_enabled", Mock(return_value=True)), \
             patch.object(bot_3, "_send_onboarding_prompt", AsyncMock()) as onboarding:
            await bot_3.start(update, ctx)
        grant.assert_called_once()
        self.assertEqual(grant.call_args.args[0], 555)
        self.assertIn("реферальная ссылка", grant.call_args.args[2])
        notify.assert_awaited_once()
        onboarding.assert_awaited_once()

    async def test_denied_user_gets_closed_door_and_no_onboarding(self):
        import bot_3
        update = _make_update(user_id=666)
        ctx = _make_context()
        with patch.object(bot_3, "_maybe_capture_referral", AsyncMock()), \
             patch.object(bot_3, "is_telegram_user_allowed", Mock(return_value=False)), \
             patch.object(bot_3, "auto_grant_telegram_user", Mock(return_value=False)), \
             patch.object(bot_3, "_notify_admins_new_user", AsyncMock()) as notify, \
             patch.object(bot_3, "_send_onboarding_prompt", AsyncMock()) as onboarding:
            await bot_3.start(update, ctx)
        notify.assert_not_awaited()
        onboarding.assert_not_awaited()
        update.effective_message.reply_text.assert_awaited_once()

    async def test_existing_user_is_not_regranted(self):
        import bot_3
        update = _make_update(user_id=777)
        ctx = _make_context()
        grant = Mock()
        with patch.object(bot_3, "_maybe_capture_referral", AsyncMock()), \
             patch.object(bot_3, "is_telegram_user_allowed", Mock(return_value=True)), \
             patch.object(bot_3, "auto_grant_telegram_user", grant), \
             patch.object(bot_3, "_notify_admins_new_user", AsyncMock()) as notify, \
             patch.object(bot_3, "_ensure_reply_keyboard_delivered", AsyncMock()), \
             patch.object(bot_3, "_onboarding_enabled", Mock(return_value=True)), \
             patch.object(bot_3, "_send_onboarding_prompt", AsyncMock()):
            await bot_3.start(update, ctx)
        grant.assert_not_called()
        notify.assert_not_awaited()

    async def test_referral_capture_runs_before_grant(self):
        """Order matters: _maybe_capture_referral only attributes users who are NOT yet
        allowed, so granting first would silently kill referral counting."""
        import bot_3
        update = _make_update()
        ctx = _make_context(["ref_777"])
        calls = []
        with patch.object(bot_3, "_maybe_capture_referral", AsyncMock(side_effect=lambda *a, **k: calls.append("referral"))), \
             patch.object(bot_3, "is_telegram_user_allowed", Mock(return_value=False)), \
             patch.object(bot_3, "auto_grant_telegram_user", Mock(side_effect=lambda *a, **k: calls.append("grant") or True)), \
             patch.object(bot_3, "_notify_admins_new_user", AsyncMock()), \
             patch.object(bot_3, "_ensure_reply_keyboard_delivered", AsyncMock()), \
             patch.object(bot_3, "_onboarding_enabled", Mock(return_value=True)), \
             patch.object(bot_3, "_send_onboarding_prompt", AsyncMock()):
            await bot_3.start(update, ctx)
        self.assertEqual(calls, ["referral", "grant"])


class SourceLabelTests(unittest.TestCase):
    def test_labels(self):
        import bot_3
        cases = {
            (): "прямая ссылка",
            ("ref_42",): "реферальная ссылка (от 42)",
            ("razbor_9",): "ссылка на «Полный разбор»",
            ("dict",): "быстрый словарь",
        }
        for args, expected in cases.items():
            self.assertEqual(bot_3._auto_access_source_label(_make_context(args)), expected)


class WebappSelfServeGrantTests(unittest.TestCase):
    """Мини-апп — своя дверь: он открывается РАНЬШЕ, чем человек жмёт /start.

    Здесь стояло «invite share-cards open it directly (t.me/<bot>/app?startapp=…)
    and never send /start» — это неверно, проверено 28.08.2026. Ссылка-приглашение
    ведёт в чат с ботом: t.me/<bot>?start=ref_<id>. Подробный вердикт и способ
    перемерить — в докстроке `_grant_self_serve_webapp_access`.

    Дверь всё равно нужна: замер на живой базе показал, что 3 из 4 самостоятельных
    входов засчитаны именно здесь, а не в боте."""

    def test_first_open_grants_and_notifies(self):
        import backend.backend_server as server
        with patch.object(server, "auto_grant_telegram_user", Mock(return_value=True)) as grant, \
             patch.object(server, "_notify_admins_new_user_async", Mock()) as notify, \
             patch.object(server, "_cache_webapp_allowlist", Mock()) as cache:
            allowed = server._grant_self_serve_webapp_access(
                4242,
                {"id": 4242, "first_name": "Neu"},
                {"start_param": "ref_77"},
            )
        self.assertTrue(allowed)
        grant.assert_called_once()
        self.assertIn("реферальная ссылка (от 77)", grant.call_args.args[2])
        notify.assert_called_once()
        cache.assert_called_once_with(4242, True)

    def test_denied_user_stays_out_and_is_not_announced(self):
        import backend.backend_server as server
        with patch.object(server, "auto_grant_telegram_user", Mock(return_value=False)), \
             patch.object(server, "invalidate_telegram_user_allowed_cache", Mock()), \
             patch.object(server, "is_telegram_user_allowed", Mock(return_value=False)), \
             patch.object(server, "_notify_admins_new_user_async", Mock()) as notify, \
             patch.object(server, "_cache_webapp_allowlist", Mock()):
            allowed = server._grant_self_serve_webapp_access(4343, {"id": 4343}, None)
        self.assertFalse(allowed)
        notify.assert_not_called()

    def test_lost_race_still_lets_the_user_in(self):
        """Bot /start and the Mini App can grant the same brand-new user at once: the
        loser sees «not granted» but must NOT 403 — it re-reads the allow-list."""
        import backend.backend_server as server
        with patch.object(server, "auto_grant_telegram_user", Mock(return_value=False)), \
             patch.object(server, "invalidate_telegram_user_allowed_cache", Mock()), \
             patch.object(server, "is_telegram_user_allowed", Mock(return_value=True)), \
             patch.object(server, "_notify_admins_new_user_async", Mock()) as notify, \
             patch.object(server, "_cache_webapp_allowlist", Mock()):
            allowed = server._grant_self_serve_webapp_access(4444, {"id": 4444}, None)
        self.assertTrue(allowed)
        notify.assert_not_called()

    def test_denied_user_does_not_re_hit_the_db_on_every_request(self):
        """A denied client keeps retrying. Without the attempt memo each retry re-ran the
        denial lookup + recheck — a per-request DB path anyone denied could amplify."""
        import backend.backend_server as server
        uid = 555002
        server._HOTPATH_ALLOWLIST_CACHE.invalidate(server._allowlist_cache_key(uid))
        server._HOTPATH_ALLOWLIST_CACHE.invalidate(server._self_serve_attempt_memo_key(uid))
        grant = Mock(return_value=False)
        with patch.object(server, "auto_grant_telegram_user", grant), \
             patch.object(server, "invalidate_telegram_user_allowed_cache", Mock()), \
             patch.object(server, "is_telegram_user_allowed", Mock(return_value=False)) as allowed_lookup:
            for _ in range(50):
                self.assertFalse(server._is_webapp_user_allowed(uid))
        # One real attempt, then the memo answers — not 50 round-trips to the database.
        # Two allow-list reads are expected and constant: the initial cache-miss lookup
        # plus the post-grant recheck; both are then cached for the negative TTL.
        self.assertEqual(grant.call_count, 1)
        self.assertLessEqual(allowed_lookup.call_count, 2)

    def test_synthetic_load_test_users_are_never_auto_granted(self):
        import backend.backend_server as server
        with patch.object(server, "auto_grant_telegram_user", Mock(return_value=True)) as grant:
            self.assertFalse(server._grant_self_serve_webapp_access(9_937_001_842, {}, None))
        grant.assert_not_called()


class DmStartHintTests(unittest.TestCase):
    """Someone playing a group task who never opened the bot in a DM gets one soft nudge —
    the bot cannot write to them first, so without that tap they receive nothing."""

    def setUp(self):
        import backend.backend_server as server
        for uid in (61001, 61002, 61003):
            server._HOTPATH_ALLOWLIST_CACHE.invalidate(("dm_reachable", uid))

    def test_no_hint_for_a_normal_dm_user(self):
        import backend.backend_server as server
        with patch.object(server, "has_reply_keyboard_delivered", Mock(return_value=True)):
            self.assertIsNone(server._build_dm_start_hint(61001, "Anna"))

    def test_hint_carries_name_and_bot(self):
        import backend.backend_server as server
        with patch.object(server, "has_reply_keyboard_delivered", Mock(return_value=False)), \
             patch.object(server, "TELEGRAM_BOT_USERNAME", "test_bot"):
            hint = server._build_dm_start_hint(61002, "Anna")
        self.assertEqual(hint["name"], "Anna")
        self.assertEqual(hint["bot_username"], "test_bot")

    def test_db_failure_never_nags_a_normal_user(self):
        import backend.backend_server as server
        with patch.object(server, "has_reply_keyboard_delivered", Mock(side_effect=RuntimeError("db down"))):
            self.assertIsNone(server._build_dm_start_hint(61003, "Anna"))

    def test_reachability_is_cached(self):
        import backend.backend_server as server
        probe = Mock(return_value=True)
        with patch.object(server, "has_reply_keyboard_delivered", probe):
            for _ in range(25):
                server._is_dm_reachable(61001)
        self.assertEqual(probe.call_count, 1)


class DigestFormattingTests(unittest.TestCase):
    def test_empty_and_populated(self):
        import bot_3
        empty = bot_3._format_access_digest_text({"new_users": [], "total_real": 13})
        self.assertIn("новых пользователей нет", empty)
        self.assertIn("13", empty)
        populated = bot_3._format_access_digest_text({
            "new_users": [{"user_id": 5, "username": "Anna", "note": "self-serve access: реферальная ссылка (от 7)"}],
            "total_real": 14,
        })
        self.assertIn("Anna", populated)
        self.assertIn("реферальная ссылка (от 7)", populated)


if __name__ == "__main__":
    unittest.main()
