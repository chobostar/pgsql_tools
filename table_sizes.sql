CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.table_sizes AS 
 SELECT (t.table_schema::text || '.'::text) || t.table_name::text AS table_name,
    pg_size_pretty(pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)) AS table_size,
    pg_size_pretty(idx.size) AS index_size,
    pg_size_pretty(pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)::numeric + idx.size) AS total_size,
    pt.tablespace
   FROM information_schema.tables t
     LEFT JOIN ( SELECT (pi.schemaname::text || '.'::text) || pi.tablename::text AS table_name,
            'INDEXES'::text AS obj_type,
            sum(pg_table_size(((pi.schemaname::text || '.'::text) || quote_ident(pi.indexname::text))::regclass)) AS size
           FROM pg_indexes pi
          WHERE pi.schemaname <> 'pg_catalog'::name
          GROUP BY ((pi.schemaname::text || '.'::text) || pi.tablename::text)) idx ON ((t.table_schema::text || '.'::text) || t.table_name::text) = idx.table_name
     LEFT JOIN pg_tables pt ON ((t.table_schema::text || '.'::text) || t.table_name::text) = ((pt.schemaname::text || '.'::text) || pt.tablename::text)
  WHERE t.table_schema::text <> 'pg_catalog'::text
  ORDER BY (pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)::numeric + COALESCE(idx.size, 0::numeric)) DESC;

COMMENT ON VIEW _query_stats.table_sizes
  IS 'Размеры таблиц вместе со всеми его индексами';
