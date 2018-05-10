CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.suspect_indexes AS 
 WITH query AS (
         SELECT (t.table_schema::text || '.'::text) || t.table_name::text AS table_name,
            pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass) AS table_size,
            idx.size AS index_size,
            pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)::numeric + idx.size AS total_size,
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
        )
 SELECT query.table_name,
    pg_size_pretty(query.table_size) AS table_size,
    pg_size_pretty(query.index_size) AS index_size,
    pg_size_pretty(query.total_size) AS total_size,
    query.tablespace,
    query.index_size / query.table_size::numeric AS idx_and_tbl_size_ratio
   FROM query
  WHERE query.index_size > query.table_size::numeric AND query.table_size > 0
  ORDER BY query.index_size DESC;

COMMENT ON VIEW _query_stats.suspect_indexes
  IS 'Таблицы, где размер индексов больше, чем размер таблиц';

