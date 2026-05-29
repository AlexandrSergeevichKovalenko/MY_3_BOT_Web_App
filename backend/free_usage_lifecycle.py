import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from backend.database import (
    build_free_limit_error,
    get_free_feature_limit_metadata,
    get_free_feature_usage_today,
    increment_free_feature_usage,
    resolve_entitlement,
)


@dataclass(frozen=True)
class FreeUsageLifecycleConfig:
    feature_key: str
    feature_title: str
    operation_kind: str


class FreeUsageLifecycleAbort(Exception):
    def __init__(self, payload: dict[str, Any], status: int = 503):
        super().__init__(str(payload.get("error") or "free_usage_lifecycle_abort"))
        self.payload = payload
        self.status = int(status)


FREE_USAGE_LIFECYCLE_REGISTRY: dict[str, FreeUsageLifecycleConfig] = {
    "dictionary_save_new_item": FreeUsageLifecycleConfig(
        feature_key="dictionary_lookup_save_daily",
        feature_title="Словарь",
        operation_kind="count_new_dictionary_item",
    ),
    "translation_session_create": FreeUsageLifecycleConfig(
        feature_key="translation_daily_sets",
        feature_title="Переводы",
        operation_kind="count_new_translation_session",
    ),
    "fsrs_card_review": FreeUsageLifecycleConfig(
        feature_key="fsrs_card_review_daily",
        feature_title="Тренировка карточек",
        operation_kind="count_successful_fsrs_review",
    ),
    "shortcut_ingest_save": FreeUsageLifecycleConfig(
        feature_key="shortcut_ingest_save_daily",
        feature_title="Shortcut сохранение слов",
        operation_kind="count_new_shortcut_ingest_item",
    ),
}


def _state_error(*, feature: str, feature_title: str, exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "error": "free_usage_state_unavailable",
        "feature": feature,
        "feature_title": feature_title,
        "message": "Не удалось проверить лимит бесплатного тарифа.",
        "detail": exc.__class__.__name__,
    }


def _format_limit(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    numeric = float(value)
    return int(numeric) if numeric.is_integer() else numeric


def get_free_usage_lifecycle_config(lifecycle_key: str) -> FreeUsageLifecycleConfig:
    key = str(lifecycle_key or "").strip().lower()
    config = FREE_USAGE_LIFECYCLE_REGISTRY.get(key)
    if not config:
        raise ValueError(f"Missing free usage lifecycle config for {key!r}")
    return config


def begin_free_usage_lifecycle(
    *,
    lifecycle_key: str,
    user_id: int,
    route: str,
    object_id: Any = None,
    session_id: Any = None,
    requested_units: float = 1.0,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int | None]:
    try:
        config = get_free_usage_lifecycle_config(lifecycle_key)
        meta = get_free_feature_limit_metadata(config.feature_key)
        if not meta:
            raise ValueError(f"Missing free feature metadata for {config.feature_key!r}")
        entitlement = resolve_entitlement(user_id=int(user_id), now_ts_utc=datetime.now(timezone.utc), tz="Europe/Vienna")
        effective_mode = str(entitlement.get("effective_mode") or "").strip().lower()
        if effective_mode not in {"free", "trial", "pro"}:
            raise ValueError(f"Unsupported effective_mode: {effective_mode!r}")
        limit_value = float(meta.get("free_limit"))
        feature_title = str(meta.get("title") or config.feature_title or config.feature_key).strip()
        if effective_mode != "free":
            logging.info(
                "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
                int(user_id),
                config.feature_key,
                effective_mode,
                config.operation_kind,
                "allow",
                route,
                object_id,
                session_id,
                None,
                _format_limit(limit_value),
            )
            return {
                "feature": config.feature_key,
                "feature_title": feature_title,
                "effective_mode": effective_mode,
                "operation_kind": config.operation_kind,
                "skip_increment": True,
                "limit": limit_value,
                "used": None,
            }, None, None
        used = float(get_free_feature_usage_today(int(user_id), config.feature_key, tz="Europe/Vienna"))
    except Exception as exc:
        feature = "unknown"
        feature_title = "unknown"
        operation_kind = "unknown"
        try:
            config = get_free_usage_lifecycle_config(lifecycle_key)
            feature = config.feature_key
            feature_title = config.feature_title
            operation_kind = config.operation_kind
        except Exception:
            pass
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            "unknown",
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return None, _state_error(feature=feature, feature_title=feature_title, exc=exc), 503

    requested = max(0.0, float(requested_units or 0.0))
    if used + requested > limit_value:
        logging.info(
            "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
            int(user_id),
            config.feature_key,
            effective_mode,
            config.operation_kind,
            "block",
            route,
            object_id,
            session_id,
            _format_limit(used),
            _format_limit(limit_value),
        )
        return None, build_free_limit_error(
            config.feature_key,
            used=used,
            limit=limit_value,
            reset_at=entitlement.get("reset_at"),
            tz="Europe/Vienna",
        ), 429
    logging.info(
        "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
        int(user_id),
        config.feature_key,
        effective_mode,
        config.operation_kind,
        "allow",
        route,
        object_id,
        session_id,
        _format_limit(used),
        _format_limit(limit_value),
    )
    return {
        "feature": config.feature_key,
        "feature_title": feature_title,
        "effective_mode": effective_mode,
        "operation_kind": config.operation_kind,
        "skip_increment": False,
        "limit": limit_value,
        "used": used,
    }, None, None


def begin_free_usage_lifecycle_tx(
    *,
    cursor,
    lifecycle_key: str,
    user_id: int,
    route: str,
    object_id: Any = None,
    session_id: Any = None,
    requested_units: float = 1.0,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int | None]:
    if cursor is None:
        exc = ValueError("Transaction-aware lifecycle requires an explicit cursor")
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            "unknown",
            "unknown",
            "unknown",
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return None, _state_error(feature="unknown", feature_title="unknown", exc=exc), 503
    try:
        config = get_free_usage_lifecycle_config(lifecycle_key)
        meta = get_free_feature_limit_metadata(config.feature_key)
        if not meta:
            raise ValueError(f"Missing free feature metadata for {config.feature_key!r}")
        entitlement = resolve_entitlement(
            user_id=int(user_id),
            now_ts_utc=datetime.now(timezone.utc),
            tz="Europe/Vienna",
            cursor=cursor,
        )
        effective_mode = str(entitlement.get("effective_mode") or "").strip().lower()
        if effective_mode not in {"free", "trial", "pro"}:
            raise ValueError(f"Unsupported effective_mode: {effective_mode!r}")
        limit_value = float(meta.get("free_limit"))
        feature_title = str(meta.get("title") or config.feature_title or config.feature_key).strip()
        if effective_mode != "free":
            logging.info(
                "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
                int(user_id),
                config.feature_key,
                effective_mode,
                config.operation_kind,
                "allow",
                route,
                object_id,
                session_id,
                None,
                _format_limit(limit_value),
            )
            return {
                "feature": config.feature_key,
                "feature_title": feature_title,
                "effective_mode": effective_mode,
                "operation_kind": config.operation_kind,
                "skip_increment": True,
                "limit": limit_value,
                "used": None,
            }, None, None
        used = float(get_free_feature_usage_today(int(user_id), config.feature_key, tz="Europe/Vienna", cursor=cursor))
    except Exception as exc:
        feature = "unknown"
        feature_title = "unknown"
        operation_kind = "unknown"
        try:
            config = get_free_usage_lifecycle_config(lifecycle_key)
            feature = config.feature_key
            feature_title = config.feature_title
            operation_kind = config.operation_kind
        except Exception:
            pass
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            "unknown",
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return None, _state_error(feature=feature, feature_title=feature_title, exc=exc), 503

    requested = max(0.0, float(requested_units or 0.0))
    if used + requested > limit_value:
        logging.info(
            "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
            int(user_id),
            config.feature_key,
            effective_mode,
            config.operation_kind,
            "block",
            route,
            object_id,
            session_id,
            _format_limit(used),
            _format_limit(limit_value),
        )
        return None, build_free_limit_error(
            config.feature_key,
            used=used,
            limit=limit_value,
            reset_at=entitlement.get("reset_at"),
            tz="Europe/Vienna",
        ), 429
    logging.info(
        "free_usage_lifecycle_check user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
        int(user_id),
        config.feature_key,
        effective_mode,
        config.operation_kind,
        "allow",
        route,
        object_id,
        session_id,
        _format_limit(used),
        _format_limit(limit_value),
    )
    return {
        "feature": config.feature_key,
        "feature_title": feature_title,
        "effective_mode": effective_mode,
        "operation_kind": config.operation_kind,
        "skip_increment": False,
        "limit": limit_value,
        "used": used,
    }, None, None


def finish_free_usage_lifecycle_success(
    *,
    user_id: int,
    usage_state: dict[str, Any] | None,
    route: str,
    idempotency_seed: str,
    object_id: Any = None,
    session_id: Any = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, int | None]:
    state = dict(usage_state or {})
    feature = str(state.get("feature") or "").strip().lower()
    feature_title = str(state.get("feature_title") or feature or "unknown").strip()
    effective_mode = str(state.get("effective_mode") or "").strip().lower()
    operation_kind = str(state.get("operation_kind") or "unknown").strip()
    if not feature or not operation_kind or effective_mode not in {"free", "trial", "pro"}:
        exc = ValueError("Missing free usage lifecycle state")
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature or "unknown",
            effective_mode or "unknown",
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return _state_error(feature=feature or "unknown", feature_title=feature_title, exc=exc), 503
    if effective_mode != "free" or bool(state.get("skip_increment")):
        logging.info(
            "free_usage_lifecycle_skip user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "skip_non_free",
            route,
            object_id,
            session_id,
        )
        return None, None
    seed = str(idempotency_seed or "").strip()
    if not seed:
        exc = ValueError("Missing free usage lifecycle increment idempotency seed")
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return _state_error(feature=feature, feature_title=feature_title, exc=exc), 503
    try:
        increment_free_feature_usage(
            user_id=int(user_id),
            feature_key=feature,
            idempotency_key=f"free_usage:{feature}:{int(user_id)}:{seed}",
            source_lang=source_lang,
            target_lang=target_lang,
            metadata={"route": route, **(metadata if isinstance(metadata, dict) else {})},
        )
    except Exception as exc:
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "increment_error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        return _state_error(feature=feature, feature_title=feature_title, exc=exc), 503
    logging.info(
        "free_usage_lifecycle_increment user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
        int(user_id),
        feature,
        effective_mode,
        operation_kind,
        "increment",
        route,
        object_id,
        session_id,
        _format_limit(state.get("used")),
        _format_limit(state.get("limit")),
    )
    return None, None


def finish_free_usage_lifecycle_success_tx(
    *,
    cursor,
    user_id: int,
    usage_state: dict[str, Any] | None,
    route: str,
    idempotency_seed: str,
    object_id: Any = None,
    session_id: Any = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    if cursor is None:
        exc = ValueError("Transaction-aware lifecycle requires an explicit cursor")
        raise FreeUsageLifecycleAbort(_state_error(feature="unknown", feature_title="unknown", exc=exc), 503)
    state = dict(usage_state or {})
    feature = str(state.get("feature") or "").strip().lower()
    feature_title = str(state.get("feature_title") or feature or "unknown").strip()
    effective_mode = str(state.get("effective_mode") or "").strip().lower()
    operation_kind = str(state.get("operation_kind") or "unknown").strip()
    if not feature or not operation_kind or effective_mode not in {"free", "trial", "pro"}:
        exc = ValueError("Missing free usage lifecycle state")
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature or "unknown",
            effective_mode or "unknown",
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        raise FreeUsageLifecycleAbort(_state_error(feature=feature or "unknown", feature_title=feature_title, exc=exc), 503)
    if effective_mode != "free" or bool(state.get("skip_increment")):
        logging.info(
            "free_usage_lifecycle_skip user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "skip_non_free",
            route,
            object_id,
            session_id,
        )
        return
    seed = str(idempotency_seed or "").strip()
    if not seed:
        exc = ValueError("Missing free usage lifecycle increment idempotency seed")
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        raise FreeUsageLifecycleAbort(_state_error(feature=feature, feature_title=feature_title, exc=exc), 503)
    try:
        increment_free_feature_usage(
            user_id=int(user_id),
            feature_key=feature,
            idempotency_key=f"free_usage:{feature}:{int(user_id)}:{seed}",
            source_lang=source_lang,
            target_lang=target_lang,
            metadata={"route": route, **(metadata if isinstance(metadata, dict) else {})},
            cursor=cursor,
        )
    except Exception as exc:
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            feature,
            effective_mode,
            operation_kind,
            "increment_error",
            route,
            object_id,
            session_id,
            exc.__class__.__name__,
        )
        raise FreeUsageLifecycleAbort(_state_error(feature=feature, feature_title=feature_title, exc=exc), 503)
    logging.info(
        "free_usage_lifecycle_increment user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s used=%s limit=%s",
        int(user_id),
        feature,
        effective_mode,
        operation_kind,
        "increment",
        route,
        object_id,
        session_id,
        _format_limit(state.get("used")),
        _format_limit(state.get("limit")),
    )


def run_free_usage_lifecycle(
    *,
    lifecycle_key: str,
    user_id: int,
    route: str,
    operation: Callable[[], Any],
    classify_success: Callable[[Any], dict[str, Any]],
    idempotency_seed: Callable[[Any], str] | str,
    source_lang: str | None = None,
    target_lang: str | None = None,
    metadata: Callable[[Any], dict[str, Any]] | dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any] | None, int | None, dict[str, Any] | None, dict[str, Any] | None]:
    state, payload, status = begin_free_usage_lifecycle(
        lifecycle_key=lifecycle_key,
        user_id=int(user_id),
        route=route,
    )
    if payload:
        return None, payload, status, state, None
    config = get_free_usage_lifecycle_config(lifecycle_key)
    try:
        logging.info(
            "free_usage_lifecycle_execute user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s",
            int(user_id),
            config.feature_key,
            str((state or {}).get("effective_mode") or "unknown"),
            config.operation_kind,
            "execute",
            route,
            None,
            None,
        )
        result = operation()
    except Exception as exc:
        logging.error(
            "free_usage_lifecycle_error user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s error=%s",
            int(user_id),
            config.feature_key,
            str((state or {}).get("effective_mode") or "unknown"),
            config.operation_kind,
            "operation_error",
            route,
            None,
            None,
            exc.__class__.__name__,
        )
        raise
    outcome = dict(classify_success(result) or {})
    object_id = outcome.get("object_id")
    session_id = outcome.get("session_id")
    if not bool(outcome.get("success")):
        logging.info(
            "free_usage_lifecycle_skip user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s",
            int(user_id),
            config.feature_key,
            str((state or {}).get("effective_mode") or "unknown"),
            config.operation_kind,
            "skip_failed_operation",
            route,
            object_id,
            session_id,
        )
        return result, None, None, state, outcome
    if not bool(outcome.get("count_usage")):
        logging.info(
            "free_usage_lifecycle_skip user_id=%s feature=%s effective_mode=%s operation_kind=%s decision=%s route=%s object_id=%s session_id=%s",
            int(user_id),
            config.feature_key,
            str((state or {}).get("effective_mode") or "unknown"),
            config.operation_kind,
            str(outcome.get("skip_reason") or "skip_no_count"),
            route,
            object_id,
            session_id,
        )
        return result, None, None, state, outcome
    seed_value = idempotency_seed(result) if callable(idempotency_seed) else str(idempotency_seed or "")
    metadata_value = metadata(result) if callable(metadata) else metadata
    increment_payload, increment_status = finish_free_usage_lifecycle_success(
        user_id=int(user_id),
        usage_state=state,
        route=route,
        idempotency_seed=seed_value,
        object_id=object_id,
        session_id=session_id,
        source_lang=source_lang,
        target_lang=target_lang,
        metadata=metadata_value if isinstance(metadata_value, dict) else None,
    )
    if increment_payload:
        return result, increment_payload, increment_status, state, outcome
    return result, None, None, state, outcome
