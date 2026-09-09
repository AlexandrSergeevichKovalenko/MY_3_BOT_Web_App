# -*- coding: utf-8 -*-
"""Перепись полей ввода, до которых может не добраться палец при поднятой клавиатуре.

┌─ ПРОВЕРЕНО 09.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────────┐
│ Повод: 09.09 в карточке слова «Новости дня»/стендапа форма «Сохранить по-своему»    │
│ стояла ВНУТРИ карточки, вписанной в высоту экрана, и клавиатура срезала верхнее     │
│ поле с немецкой фразой. Починено (окно поверх экрана). Владелец спросил: сколько    │
│ таких мест ещё.                                                                    │
│                                                                                    │
│ Сырое число: 113 вхождений <input>/<textarea>/contentEditable. Из них 35 — галочки, │
│ переключатели, ползунки, выбор файла: клавиатуры не вызывают. Клавиатурных — 78.    │
│ Разложение 78 на 09.09.2026:                                                        │
│   25 — в прибитом слое (position: fixed): подъём над клавиатурой уже сделан;        │
│   38 — обычная прокручиваемая страница: поле поднимает общий обработчик focusin;    │
│   15 — сначала выглядели подозрительными, разобраны поимённо (см. ниже).            │
│                                                                                    │
│ ЛОЖНАЯ ТРЕВОГА, из-за которой 15 выглядели как 15 находок: у .webapp-page один      │
│ общий className со ВСЕМИ состояниями сразу (is-flashcards, is-home-bento,           │
│ is-reader-immersive, is-dictionary-layout…), они подставляются по условию. Любой    │
│ разбор по тексту читает их как включённые, и правило вида «в режиме карточек        │
│ .webapp-main вписан в экран и режет» примеряется ко всему приложению. Поэтому все   │
│ 15 указывали на ОДНУ строку — объявление .webapp-page в App.jsx.                    │
│                                                                                    │
│ Разбор этих 15 по настоящим экранам:                                                │
│   7 — калькулятор экономики: сервер пускает туда только владельца                   │
│       (ECONOMICS_ADMIN_TELEGRAM_ID, backend_server.py);                             │
│   2 — форма входа «Telegram ID» на экране логина;                                   │
│   2 — практика по теории и по навыку: названы в комментарии обработчика клавиатуры  │
│       от 28.08.2026 как уже обслуживаемые;                                          │
│   1 — сообщение в поддержку: обычная страница;                                      │
│   1 — поле initData: живёт только в dev-сборке (import.meta.env.DEV), в прод не     │
│       попадает вовсе;                                                               │
│   2 — словарь: большое поле поиска и «Название» при создании папки. ТОЛЬКО у них    │
│       архитектура та же, что у сломанной карточки (страница вписана в экран,        │
│       цепочка overflow: hidden, один назначенный прокручиваемый блок).              │
│                                                                                     │
│ РЕШЕНИЕ ВЛАДЕЛЬЦА 09.09.2026: эти два поля НЕ ТРОГАЕМ. Оба короткие и стоят         │
│ наверху своего экрана; в карточке новости фразу вытолкнули за край ещё и две        │
│ кнопки под полями, здесь их нет. Вернуться, если владелец поймает это руками.       │
│                                                                                     │
│ Экранов, построенных «в размер экрана», девять: тренажёр карточек и FSRS, главный   │
│ экран-бенто, читалка в погружении, кинозал YouTube, новость дня и стендап,          │
│ библиотека словаря, поиск в словаре, экран-гид, урок. Поля ввода есть только в трёх │
│ (новость дня — починена, два словарных — выше). Интерактивы из чата сюда не входят: │
│ у них своя подгонка под видимую часть (frontend/src/answer/fitCard.js).             │
│                                                                                     │
│ ГРАНИЦЫ ЭТОЙ ПРОВЕРКИ, чтобы не выдавать её за большее: разбор читает КОД, а не     │
│ экран. Он доказал, что видит известный дефект (проверка ниже), но не может сказать, │
│ рвёт ли те два словарных поля на живом телефоне: это зависит от высот соседних      │
│ блоков, шрифта и размера клавиатуры.                                                │
│                                                                                     │
│ КАК ПЕРЕМЕРИТЬ:                                                                     │
│   python3 scripts/keyboard_field_audit.py frontend/src                               │
│ КАК ПРОВЕРИТЬ САМ ПРИБОР (обязательно перед тем, как верить его выводу):            │
│   C=$(git log --format=%H -1 --grep="Своя версия слова правится в окне")            │
│   git archive $C^ frontend/src | tar -x -C /tmp/before                              │
│   python3 scripts/keyboard_field_audit.py /tmp/before/frontend/src                   │
│   На коде ДО правки в корзине C обязаны стоять два поля worldnews-card-own-input     │
│   (тот самый дефект, который владелец видел глазами); на нынешнем коде их там нет.  │
└─────────────────────────────────────────────────────────────────────────────────────┘

Правило отбора: у поля есть предок, вписанный в высоту экрана (100dvh/100vh/--app-height)
И режущий содержимое (overflow hidden/clip), причём между полем и этим предком нет
прокручиваемого блока, и предок не прибит к экрану (position: fixed — там подъём над
клавиатурой делает сам слой).

Селекторы сопоставляются со всей цепочкой предков; className читается и с соседних строк
(в этом репозитории атрибуты часто перенесены на следующую строку после <div).
"""

import re, os, io, sys, json, collections

SRC = sys.argv[1] if len(sys.argv) > 1 else 'frontend/src'

rules = []
for root, _, files in os.walk(SRC):
    for fn in files:
        if not fn.endswith('.css'): continue
        path = os.path.join(root, fn)
        s = io.open(path, encoding='utf-8').read()
        for m in re.finditer(r'([^{}/][^{}]*)\{([^{}]*)\}', s):
            sel, body = m.group(1), m.group(2)
            if '@' in sel or ':root' in sel: continue
            line = s[:m.start()].count('\n') + 1
            for part in sel.split(','):
                part = re.sub(r'::?[a-z-]+(\([^)]*\))?', '', part).strip()
                if not part or '.' not in part: continue
                comps = [set(re.findall(r'\.([A-Za-z0-9_-]+)', c))
                         for c in re.split(r'[ >+~]+', part) if c.strip()]
                comps = [c for c in comps if c]
                if comps: rules.append((path, line, comps, body))

CAP    = re.compile(r'(?:max-)?height\s*:[^;]*(100dvh|100vh|--app-height)')
CLIP   = re.compile(r'overflow(-y)?\s*:\s*(hidden|clip)')
SCROLL = re.compile(r'overflow(-y)?\s*:\s*(auto|scroll)')
FIX    = re.compile(r'position\s*:\s*fixed')

def matches(comps, level, outer):
    if not comps[-1] <= level: return False
    need = comps[:-1][::-1]; i = 0
    for lv in outer:
        if i < len(need) and need[i] <= lv: i += 1
    return i == len(need)

def props(level, outer):
    return ''.join(b + ';' for _p, _l, c, b in rules if matches(c, level, outer))

def classes_at(lines, i):
    """Классы тега, открытого на строке i: атрибуты бывают перенесены на следующие."""
    buf, j = lines[i], i
    while '>' not in buf and j - i < 12 and j + 1 < len(lines):
        j += 1; buf += '\n' + lines[j]
    out = set()
    for m in re.finditer(r'className=(?:"([^"]*)"|\{`([^`]*)`|\{\s*\'([^\']*)\')', buf):
        out |= set(re.findall(r'[A-Za-z][A-Za-z0-9_-]*', m.group(1) or m.group(2) or m.group(3)))
    return out

rows = []
for root, _, files in os.walk(SRC):
    for fn in files:
        if not fn.endswith('.jsx'): continue
        p = os.path.join(root, fn)
        lines = io.open(p, encoding='utf-8').read().split('\n')
        for i, l in enumerate(lines):
            if not re.search(r'<input|<textarea|contentEditable', l): continue
            typ = re.search(r'type=(?:"|\{\')([a-z]+)', '\n'.join(lines[i:i+9]))
            typ = typ.group(1) if typ else ('textarea' if '<textarea' in l else 'text')
            if typ in ('checkbox','radio','range','file','hidden','color','submit','button'): continue
            own = classes_at(lines, i)
            levels, cur = [], len(l) - len(l.lstrip())
            for j in range(i - 1, -1, -1):
                lj = lines[j]
                if not lj.strip(): continue
                ij = len(lj) - len(lj.lstrip())
                if ij < cur and re.search(r'<[a-zA-Z]', lj):
                    cur = ij
                    levels.append((j + 1, classes_at(lines, j)))
                    if ij == 0: break
            rows.append(dict(file=p, line=i + 1, type=typ, own=sorted(own), levels=levels))

buckets = collections.defaultdict(list)
for r in rows:
    marks = []
    for k, (ln, cls) in enumerate(r['levels']):
        if not cls: continue
        pr = props(cls, [c for _l, c in r['levels'][k+1:]])
        marks.append(dict(line=ln, cls=sorted(cls)[:3], cap=bool(CAP.search(pr)),
                          clip=bool(CLIP.search(pr)), scroll=bool(SCROLL.search(pr)),
                          fixed=bool(FIX.search(pr))))
    own_pr = props(set(r['own']), [c for _l, c in r['levels']])
    verdict, why = 'D. обычная прокручиваемая страница', None
    scroller = bool(SCROLL.search(own_pr))
    for m in marks:
        if m['fixed']:
            verdict, why = 'A. прибитый слой — подъём над клавиатурой есть', m; break
        if m['cap'] and m['clip']:
            verdict = ('B. режет, но поле внутри прокручиваемой части' if scroller
                       else 'C. ПОДОЗРЕНИЕ: вписан в экран, режет, прокрутки до поля нет')
            why = m; break
        if m['scroll']: scroller = True
    r['bucket'], r['why'] = verdict, why
    buckets[verdict].append(r)

for b in sorted(buckets):
    print(f'\n===== {b}: {len(buckets[b])} =====')
    for r in buckets[b]:
        w = r['why']
        tail = f"  ← {'/'.join(w['cls'])} (стр. {w['line']})" if w else ''
        print(f"  {r['file']}:{r['line']} [{r['type']}] {' '.join(r['own'][:2])}{tail}")
print()
print('ИТОГО полей:', len(rows))
