VALID_CATEGORIES = [
    'Nouns', 'Articles & Determiners', 'Cases', 'Pronouns',
    'Verbs', 'Voice (Active/Passive)', 'Tenses', 'Moods',
    'Adjectives', 'Adverbs', 'Prepositions', 'Conjunctions',
    'Word Order', 'Negation', 'Particles',
    'Clauses & Sentence Types', 'Infinitive & Participles',
    'Punctuation', 'Orthography & Spelling',
    'Other mistake'
]

VALID_SUBCATEGORIES = {
    'Nouns': [
        'Pluralization', 'Compound Nouns', 'Declension Errors',
        'Noun Capitalization', 'Noun-Verb Confusion'
    ],

    'Articles & Determiners': [
        'Definite Articles (der/die/das)', 'Indefinite Articles (ein/eine)',
        'Negation Article (kein)', 'Possessive Determiners (mein/dein...)',
        'Demonstratives (dieser/jener)', 'Quantifiers (jeder/mancher/viele...)',
        'Article Omission/Redundancy'
    ],

    'Cases': [
        'Nominative', 'Accusative', 'Dative', 'Genitive',
        'Case after Preposition',          # общий “зонтик”
        'Akkusativ + Preposition', 'Dative + Preposition', 'Genitive + Preposition',
        'Two-way Prepositions (Wechselpräpositionen)',
        'Case Agreement in Noun Phrase'    # согласование падежа внутри группы
    ],

    'Pronouns': [
        'Personal Pronouns', 'Reflexive Pronouns',
        'Relative Pronouns', 'Possessive Pronouns',
        'Demonstrative Pronouns', 'Indefinite Pronouns (man/jemand/etwas)',
        'Pronoun Reference (wrong antecedent)'
    ],

    'Verbs': [
        'Conjugation', 'Weak Verbs', 'Strong Verbs', 'Mixed Verbs',
        'Separable Verbs', 'Inseparable Prefix Verbs',
        'Reflexive Verbs', 'Auxiliary Verbs (sein/haben/werden)',
        'Modal Verbs', 'Verb Valency (missing object/complement)',
        'Verb Placement in Main Clause', 'Verb Placement in Subordinate Clause',
        'Infinitive Form Errors'
    ],

    'Voice (Active/Passive)': [
        'Vorgangspassiv (werden + Partizip II)',
        'Zustandspassiv (sein + Partizip II)',
        'Passive with Modal Verbs',
        'Passive Word Order',
        'Agent Phrase (von/durch) misuse',
        'Active-Passive Confusion'
    ],

    'Tenses': [
        'Present (Präsens)',
        'Simple Past (Präteritum)',
        'Present Perfect (Perfekt)',
        'Past Perfect (Plusquamperfekt)',
        'Future 1 (Futur I)',
        'Future 2 (Futur II)',
        'Sequence of Tenses',
        'Tense Choice (context mismatch)'
    ],

    'Moods': [
        'Indicative',
        'Imperative',
        'Subjunctive 1 (Konjunktiv I)',
        'Subjunctive 2 (Konjunktiv II)',
        'Konjunktiv II: würde-Form',
        'Irrealis / Hypothetical',
        'Politeness Forms',
        'Reported Speech (Indirekte Rede)'
    ],

    'Adjectives': [
        'Endings', 'Weak Declension', 'Strong Declension', 'Mixed Declension',
        'Adjective Placement', 'Comparative', 'Superlative',
        'Incorrect Adjective Case Agreement',
        'Adjective vs Participle Confusion'
    ],

    'Adverbs': [
        'Adverb Placement', 'Multiple Adverbs (TEKAMOLO)',
        'Incorrect Adverb Usage', 'Sentence Adverbs (leider, vielleicht...)'
    ],

    'Prepositions': [
        'Accusative Prepositions', 'Dative Prepositions', 'Genitive Prepositions',
        'Two-way Prepositions', 'Incorrect Preposition Usage',
        'Preposition Omission/Redundancy',
        'Fixed Prepositional Phrases (idiomatic)'
    ],

    'Conjunctions': [
        'Coordinating (und/aber/oder/denn)',
        'Subordinating (weil/dass/ob/wenn...)',
        'Correlative Conjunctions (entweder...oder / sowohl...als auch)',
        'Incorrect Use of Conjunctions'
    ],

    'Word Order': [
        'Standard', 'Inverted',
        'Verb-Second Rule (V2)', 'Verb-First (Questions/Imperative)',
        'Position of Negation', 'Position of Time/Manner/Place',
        'Incorrect Order in Subordinate Clause',
        'Incorrect Order with Modal Verb',
        'Placement of Separable Prefix',
        'Placement of Participle (Perfekt/Passive)'
    ],

    'Negation': [
        'nicht vs kein', 'Negation Placement',
        'Double Negation', 'Negation with Pronouns/Quantifiers'
    ],

    'Particles': [
        'Modal Particles (doch/ja/mal/halt/eben)',
        'Focus Particles (nur/auch/sogar)',
        'Antwortpartikeln (ja/nein/doch)',
        'Particle Misuse/Omission'
    ],

    'Clauses & Sentence Types': [
        'Main vs Subordinate Clause',
        'Relative Clauses',
        'Indirect Questions',
        'Conditionals (wenn/falls)',
        'Purpose Clauses (damit/um...zu)',
        'Concessive Clauses (obwohl)',
        'Infinitive Clauses vs dass-clause',
        'Question Formation'
    ],

    'Infinitive & Participles': [
        'zu + Infinitive',
        'um...zu',
        'Infinitive with Modal Verbs',
        'Partizip I',
        'Partizip II',
        'Participial Constructions',
        'Gerund-like Constructions (beim + Infinitive)'
    ],

    'Punctuation': [
        'Comma in Subordinate Clause',
        'Comma in Relative Clause',
        'Comma with Infinitive Group',
        'Question Mark / Exclamation',
        'Quotation Marks'
    ],

    'Orthography & Spelling': [
        'Capitalization', 'ß/ss', 'Umlauts (ä/ö/ü)',
        'Hyphenation', 'Common Spelling Errors'
    ],

    'Other mistake': [
        'Unclassified mistake'
    ]
}

VALID_CATEGORIES_lower = [cat.lower() for cat in VALID_CATEGORIES]
VALID_SUBCATEGORIES_lower = {k.lower(): [v.lower() for v in values] for k, values in VALID_SUBCATEGORIES.items()}