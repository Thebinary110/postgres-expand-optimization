-- Fingerprint for large outputs (string_agg of a whole table exceeds PostgreSQL's 1 GB limit beyond ~10M rows):
-- rows are hashed per bucket of 1M ordering-key values, then the bucket hashes are hashed in order.
-- exact:   all columns, ordered and bucketed by the surrogate key.
-- logical: surrogate keys replaced by natural keys, ordered and bucketed by the natural key.
WITH t1 AS (SELECT t.*, t3.text_column_1 AS t3k1, t3.text_column_3 AS t3k3, t3.text_column_4 AS t3k4
            FROM public.table1 t LEFT JOIN public.table3 t3 ON t3.id_column_1 = t.id_column_4),
t1x AS (SELECT id_column_1 / 1000000 AS b, md5(string_agg(t::text, '|' ORDER BY id_column_1)) AS h
        FROM public.table1 t GROUP BY 1),
t1l AS (SELECT id_column_3 / 1000000 AS b, md5(string_agg(row(id_column_2, text_column_1, datetime_column_1, text_column_2,
            integer_column_1, integer_column_2, integer_column_3, text_column_3, id_column_3, t3k1, t3k3, t3k4, id_column_5,
            integer_column_4, integer_column_5, id_column_6, integer_column_6, integer_column_7)::text, '|' ORDER BY id_column_3)) AS h
        FROM t1 GROUP BY 1),
t2 AS (SELECT t.*, m.id_column_3 AS raw FROM public.table2 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2),
t2x AS (SELECT id_column_1 / 1000000 AS b, md5(string_agg(row(id_column_1, id_column_2, integer_column_1, integer_column_2,
            text_column_1, integer_column_3, integer_column_4, integer_column_5, integer_column_6)::text, '|' ORDER BY id_column_1)) AS h
        FROM t2 GROUP BY 1),
t2l AS (SELECT raw / 1000000 AS b, md5(string_agg(row(raw, integer_column_1, integer_column_2, text_column_1, integer_column_3,
            integer_column_4, integer_column_5, integer_column_6)::text, '|' ORDER BY raw, integer_column_1)) AS h
        FROM t2 GROUP BY 1),
t5 AS (SELECT t.*, m.id_column_3 AS raw FROM public.table5 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2),
t5x AS (SELECT id_column_1 / 1000000 AS b, md5(string_agg(row(id_column_1, numeric_column_1, numeric_column_2, integer_column_1,
            integer_column_2, id_column_2, id_column_3)::text, '|' ORDER BY id_column_1)) AS h FROM t5 GROUP BY 1),
t5l AS (SELECT raw / 1000000 AS b, md5(string_agg(row(numeric_column_1, numeric_column_2, integer_column_1, integer_column_2, raw,
            id_column_3)::text, '|' ORDER BY raw, numeric_column_1)) AS h FROM t5 GROUP BY 1),
t6 AS (SELECT t.*, m.id_column_3 AS raw FROM public.table6 t JOIN public.table1 m ON m.id_column_1 = t.id_column_2),
t6x AS (SELECT id_column_1 / 1000000 AS b, md5(string_agg(row(id_column_1, id_column_2, id_column_3)::text, '|'
            ORDER BY id_column_1)) AS h FROM t6 GROUP BY 1),
t6l AS (SELECT raw / 1000000 AS b, md5(string_agg(row(raw, id_column_3)::text, '|' ORDER BY raw, id_column_3)) AS h
        FROM t6 GROUP BY 1)
SELECT 'table1', (SELECT count(*) FROM public.table1), (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t1x),
       (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t1l)
UNION ALL SELECT 'table2', (SELECT count(*) FROM public.table2), (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t2x),
       (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t2l)
UNION ALL SELECT 'table3', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)),
       md5(string_agg(row(text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4)::text, '|'
           ORDER BY row(text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4)::text)) FROM public.table3 t
UNION ALL SELECT 'table4', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)),
       md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table4 t
UNION ALL SELECT 'table5', (SELECT count(*) FROM public.table5), (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t5x),
       (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t5l)
UNION ALL SELECT 'table6', (SELECT count(*) FROM public.table6), (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t6x),
       (SELECT md5(string_agg(h, '' ORDER BY b)) FROM t6l)
UNION ALL SELECT 'row_counts', count(*), md5(string_agg(row(schema_name, table_name, row_count)::text, '|'
           ORDER BY schema_name, table_name)), NULL FROM public.pgdm_table_row_counts;
