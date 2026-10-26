-- Output fingerprint of the six destinations, for equivalence checks between implementations.
-- exact:   every column including generated surrogate keys.
-- logical: surrogate keys replaced by natural keys (raw_id for table1 rows, table3 attributes), so two
--          implementations that assign identity values in a different order still compare equal.
WITH t1 AS (SELECT t.*, t3.text_column_1 AS t3k1, t3.text_column_3 AS t3k3, t3.text_column_4 AS t3k4
            FROM public.table1 t LEFT JOIN public.table3 t3 ON t3.id_column_1 = t.id_column_4)
SELECT 'table1' AS tbl, count(*) AS n,
       md5(string_agg(t::text, '|' ORDER BY id_column_1)) AS exact,
       md5(string_agg(row(id_column_2, text_column_1, datetime_column_1, text_column_2, integer_column_1, integer_column_2,
           integer_column_3, text_column_3, id_column_3, t3k1, t3k3, t3k4, id_column_5, integer_column_4, integer_column_5,
           id_column_6, integer_column_6, integer_column_7)::text, '|' ORDER BY id_column_3)) AS logical
FROM t1 t
UNION ALL
SELECT 'table2', count(*), md5(string_agg(t::text, '|' ORDER BY t.id_column_1)),
       md5(string_agg(row(m.id_column_3, t.integer_column_1, t.integer_column_2, t.text_column_1, t.integer_column_3,
           t.integer_column_4, t.integer_column_5, t.integer_column_6)::text, '|' ORDER BY m.id_column_3, t.integer_column_1))
FROM public.table2 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2
UNION ALL
SELECT 'table3', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)),
       md5(string_agg(row(text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4)::text, '|'
           ORDER BY row(text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4)::text))
FROM public.table3 t
UNION ALL
SELECT 'table4', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)), md5(string_agg(t::text, '|' ORDER BY id_column_1))
FROM public.table4 t
UNION ALL
SELECT 'table5', count(*), md5(string_agg(t::text, '|' ORDER BY t.id_column_1)),
       md5(string_agg(row(t.numeric_column_1, t.numeric_column_2, t.integer_column_1, t.integer_column_2, m.id_column_3,
           t.id_column_3)::text, '|' ORDER BY m.id_column_3, t.numeric_column_1))
FROM public.table5 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2
UNION ALL
SELECT 'table6', count(*), md5(string_agg(t::text, '|' ORDER BY t.id_column_1)),
       md5(string_agg(row(m.id_column_3, t.id_column_3)::text, '|' ORDER BY m.id_column_3, t.id_column_3))
FROM public.table6 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2
ORDER BY 1;
SELECT to_regclass('public.pgdm_table_row_counts') IS NOT NULL AS has_rc \gset
\if :has_rc
SELECT 'row_counts', count(*), md5(string_agg(row(schema_name, table_name, row_count)::text, '|' ORDER BY schema_name, table_name))
FROM public.pgdm_table_row_counts;
\endif
