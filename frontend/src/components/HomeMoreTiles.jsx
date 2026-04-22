import React, { useCallback, useMemo, useState } from 'react';
import './HomeDashboardTiles.css';

function MoreTile({ def, label, onClick }) {
  const [state, setState] = useState('idle');
  const isDisabled = Boolean(def.disabled);

  const handleDown = useCallback(() => {
    if (isDisabled) return;
    setState('pressed');
  }, [isDisabled]);

  const handleUp = useCallback(() => {
    if (isDisabled) return;
    setState('pop');
    onClick?.();
    window.setTimeout(() => setState('idle'), 380);
  }, [isDisabled, onClick]);

  const handleLeave = useCallback(() => {
    setState((prev) => (prev === 'pressed' ? 'idle' : prev));
  }, []);

  return (
    <button
      type="button"
      className={`hdt-tile ${def.cls} ${isDisabled ? 'hdt-disabled' : ''} ${state === 'pressed' ? 'hdt-pressed' : ''} ${state === 'pop' ? 'hdt-pop' : ''}`}
      onPointerDown={handleDown}
      onPointerUp={handleUp}
      onPointerLeave={handleLeave}
      onPointerCancel={handleLeave}
      disabled={isDisabled}
      aria-label={label}
    >
      {def.badge && (
        <span className="hdt-badge">
          {def.badge}
        </span>
      )}
      <span className="hdt-icon">{def.emoji}</span>
      <span className="hdt-label">{label}</span>
    </button>
  );
}

export default function HomeMoreTiles({
  tr,
  uiLang,
  openSection,
  canViewEconomics = false,
  isSkillTrainingReady = false,
  refs = {},
}) {
  const currentUiLang = uiLang === 'de' ? 'de' : 'ru';

  const tileDefs = useMemo(() => {
    const defs = [
      { key: 'su', labelRu: 'Подписка\nи тариф', labelDe: 'Abo\nund Tarif', emoji: '💳', cls: 'hdt-indigo', sectionKey: 'subscription', refKey: 'billingRef' },
      { key: 'gu', labelRu: 'Как\nпользоваться', labelDe: 'So\nbenutzt du es', emoji: '❓', cls: 'hdt-cyan', sectionKey: 'guide', refKey: 'guideRef' },
      { key: 'mo', labelRu: 'Фильмы\nи сцены', labelDe: 'Filme\nund Szenen', emoji: '🎬', cls: 'hdt-red', sectionKey: 'movies', refKey: 'moviesRef' },
      { key: 'di', labelRu: 'Словарь\nи поиск', labelDe: 'Wörterbuch\nund Suche', emoji: '📚', cls: 'hdt-emerald', sectionKey: 'dictionary', refKey: 'dictionaryRef' },
      { key: 'sp', labelRu: 'Техподдержка\nи связь', labelDe: 'Support\nund Kontakt', emoji: '🛟', cls: 'hdt-amber', sectionKey: 'support', refKey: 'supportRef' },
      { key: 'an', labelRu: 'Аналитика\nпрогресса', labelDe: 'Analytics\nund Fortschritt', emoji: '📊', cls: 'hdt-blue', sectionKey: 'analytics', refKey: 'analyticsRef' },
      { key: 'sk', labelRu: 'Тренировка\nнавыка', labelDe: 'Skill-\nTraining', emoji: '🧩', cls: 'hdt-violet', sectionKey: 'skill_training', refKey: 'skillTrainingRef', disabled: !isSkillTrainingReady, badge: !isSkillTrainingReady ? tr('Позже', 'Spaeter') : '' },
    ];
    if (canViewEconomics) {
      defs.splice(6, 0, { key: 'ec', labelRu: 'Экономика\nи лимиты', labelDe: 'Kosten\nund Limits', emoji: '💹', cls: 'hdt-green', sectionKey: 'economics', refKey: 'economicsRef' });
    }
    return defs;
  }, [canViewEconomics, isSkillTrainingReady, tr]);

  const handleTile = useCallback((def) => {
    if (def.disabled) return;
    openSection?.(def.sectionKey, refs[def.refKey] || null);
  }, [openSection, refs]);

  return (
    <div className="hdt-root" ref={refs.homeMoreRef || null}>
      <div className="hdt-more-card">
        <div className="hdt-more-title">{tr('Остальные функции', 'Weitere Funktionen')}</div>
        <div className="hdt-more-sub">
          {tr('Здесь только разделы, которых нет на главном экране.', 'Hier sind nur Bereiche, die nicht auf dem Startbildschirm liegen.')}
        </div>
      </div>

      <div className="hdt-section-head">
        <span className="hdt-section-label">{tr('Дополнительно', 'Zusätzlich')}</span>
        <span className="hdt-section-hint">{tr('Все оставшиеся разделы', 'Alle übrigen Bereiche')}</span>
      </div>

      <div className="hdt-grid">
        {tileDefs.map((def) => {
          const label = currentUiLang === 'de' ? def.labelDe : def.labelRu;
          return (
            <MoreTile
              key={def.key}
              def={def}
              label={label}
              onClick={() => handleTile(def)}
            />
          );
        })}
      </div>
    </div>
  );
}
