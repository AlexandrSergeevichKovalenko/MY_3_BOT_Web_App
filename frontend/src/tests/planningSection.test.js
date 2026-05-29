import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import PlanningSection, { PLANNING_BOUNDARY_METRICS } from '../components/PlanningSection.jsx';
import { PlanningContext } from '../providers/PlanningProvider.jsx';

const tr = (ru) => ru;

const planningContextValue = {
  planAnalyticsPeriod: 'week',
  setPlanAnalyticsPeriod: vi.fn(),
  planAnalyticsLoading: false,
  weeklyPlan: {
    week: { start_date: '2026-05-25', end_date: '2026-05-31', days_elapsed: 5, days_total: 7 },
    plan: { translations_goal: 10, learned_words_goal: 20, agent_minutes_goal: 15, reading_minutes_goal: 30 },
    metrics: {
      translations: { goal: 10, actual: 5, completion_percent: 50 },
      learned_words: { goal: 20, actual: 8, completion_percent: 40 },
      agent_minutes: { goal: 15, actual: 3, completion_percent: 20 },
      reading_minutes: { goal: 30, actual: 12, completion_percent: 40 },
    },
    snapshot_saved_at: '2026-05-29T10:00:00.000Z',
  },
  weeklyPlanCollapsed: false,
  setWeeklyPlanCollapsed: vi.fn(),
  weeklyPlanDraft: {
    translations_goal: '10',
    learned_words_goal: '20',
    agent_minutes_goal: '15',
    reading_minutes_goal: '30',
  },
  setWeeklyPlanDraft: vi.fn(),
  weeklyPlanSaving: false,
  weeklyPlanLoading: false,
  weeklyPlanError: '',
  weeklyPlanSnapshotTone: 'snapshot',
  planAnalyticsMetrics: {},
  planAnalyticsRange: null,
  planAnalyticsError: '',
  weeklyMetricExpanded: {},
  setWeeklyMetricExpanded: vi.fn(),
  todayPlan: {
    date: '2026-05-29',
    total_minutes: 10,
    items: [{ id: 1, title: 'Translate', task_type: 'translation', estimated_minutes: 10, status: 'todo', payload: {} }],
    snapshot_saved_at: '2026-05-29T10:00:00.000Z',
  },
  todayPlanLoadedOnce: true,
  todayPlanLoading: false,
  todayPlanError: '',
  todayPlanSnapshotTone: 'snapshot',
  skillReport: {
    groups: [{ skills: [{ skill_id: 'word_order_v2_rule', name: 'V2', mastery: 40, errors_7d: 2, has_data: true }] }],
    total_skills: 1,
    skill_training_status: {},
    snapshot_saved_at: '2026-05-29T10:00:00.000Z',
  },
  skillReportLoadedOnce: true,
  skillReportLoading: false,
  skillReportError: '',
  skillReportSnapshotTone: 'snapshot',
  loadTodayPlan: vi.fn(),
  loadSkillReport: vi.fn(),
  loadWeeklyPlan: vi.fn(),
  homeSkillTrainingStatusMap: {},
};

const baseProps = {
  tr,
  uiLang: 'ru',
  sectionRefs: {},
  visiblePanels: { weeklyPlan: true, todayPlan: true, skillReport: true },
  todayItemLoading: {},
  todayTimerNowMs: Date.now(),
  saveWeeklyPlan: vi.fn(),
  regenerateTodayPlan: vi.fn(),
  startTodayTask: vi.fn(),
  submitTodayVideoFeedback: vi.fn(),
  getTodayItemDisplayElapsedSeconds: vi.fn(() => 0),
  getTodayItemProgressPercent: vi.fn(() => 0),
  getTodayTranslationProgress: vi.fn(() => ({ completedCount: 0, targetCount: 7 })),
  getTodayItemTitle: vi.fn((item) => item.title || ''),
  formatCompactTimer: vi.fn(() => '00:00'),
  skillPracticeLoading: {},
  startSkillPractice: vi.fn(),
  resumeSkillPractice: vi.fn(),
  getStoredSkillTrainingSnapshot: vi.fn(() => null),
  getLocalizedSkillDisplayName: vi.fn((skill) => skill.name || skill.skill_id),
  getLocalizedSkillTopicLabel: vi.fn(() => ''),
};

function renderPlanningSection({ mounted = true, contextValue = planningContextValue } = {}) {
  if (!mounted) return '';
  return renderToStaticMarkup(React.createElement(
    PlanningContext.Provider,
    { value: contextValue },
    React.createElement(PlanningSection, baseProps),
  ));
}

describe('PlanningSection', () => {
  it('renders planning panels from PlanningContext', () => {
    const html = renderPlanningSection();
    expect(html).toContain('План на неделю');
    expect(html).toContain('Задачи на сегодня');
    expect(html).toContain('Карта навыков');
  });

  it('free users never render PlanningSection when boundary is not mounted', () => {
    expect(renderPlanningSection({ mounted: false })).toBe('');
  });

  it('paid users render PlanningSection when boundary is mounted', () => {
    expect(renderPlanningSection({ mounted: true })).toContain('weekly-plan-panel');
  });

  it('fails explicitly without planning context', () => {
    expect(() => renderToStaticMarkup(React.createElement(PlanningSection, baseProps))).toThrow('PlanningSection requires PlanningContext');
  });

  it('does not declare planning useState ownership in App.jsx', () => {
    const appSource = readFileSync(resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).not.toContain('const HomeScreenSection');
    expect(appSource).not.toContain('weekly-plan-panel');
    expect(appSource).not.toContain('today-plan-panel');
    expect(appSource).not.toContain('skill-report-panel');
  });

  it('exposes boundary completion metrics', () => {
    expect(PLANNING_BOUNDARY_METRICS).toEqual(expect.objectContaining({
      provider_name: 'planning',
      rendering_lines_removed: expect.any(Number),
      remaining_references: expect.any(Array),
    }));
  });
});
