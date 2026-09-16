WITH слова AS (
  SELECT u.id AS unit_id, s.id AS sense_id, u.kind, u.display AS de,
         NULLIF(BTRIM(CONCAT_WS(' · ', s.label, s.note)), '') AS пояснение,
         string_agg(ru.display, '; ' ORDER BY l.rank, ru.display) AS ru
  FROM bt_3_lex_senses s
  JOIN bt_3_lex_units u  ON u.id = s.unit_id AND u.lang='de' AND u.kind IN ('word','collocation')
  JOIN bt_3_lex_links l  ON l.from_unit = u.id AND l.sense_id = s.id
  JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang='ru'
  GROUP BY 1,2,3,4,5
),
предложения AS (
  SELECT u.id AS unit_id, NULL::bigint AS sense_id, u.kind, u.display AS de,
         NULL::text AS пояснение,
         string_agg(ru.display, '; ' ORDER BY l.rank, ru.display) AS ru
  FROM bt_3_lex_units u
  JOIN bt_3_lex_links l  ON l.from_unit = u.id
  JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang='ru'
  WHERE u.lang='de' AND u.kind='sentence'
  GROUP BY 1,2,3,4,5
),
выборка AS (
  (SELECT * FROM слова        WHERE kind='word'        ORDER BY random() LIMIT 40)
  UNION ALL
  (SELECT * FROM слова        WHERE kind='collocation' ORDER BY random() LIMIT 30)
  UNION ALL
  (SELECT * FROM предложения                           ORDER BY random() LIMIT 30)
)
SELECT json_agg(json_build_object(
         'unit_id', unit_id, 'sense_id', sense_id, 'kind', kind,
         'de', de, 'sense', пояснение, 'ru', ru))::text
FROM выборка;
