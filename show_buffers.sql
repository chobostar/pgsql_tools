CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.show_buffers AS 
 SELECT (ns.nspname::text || '.'::text) || c.relname::text AS relname,
    c.relkind,
    pg_size_pretty(count(*) * 8192::bigint) AS cached_size,
    count(*) * 100::bigint / (( SELECT count(1) AS count
           FROM pg_buffercache)) AS "%_of_total_cache",
    pg_size_pretty(count(*) FILTER (WHERE b.isdirty) * 8192::bigint) AS dirty_size,
    100 * count(*) FILTER (WHERE b.isdirty) / count(1) AS "%_dirty"
   FROM pg_buffercache b
     JOIN pg_class c ON b.relfilenode = c.relfilenode
     JOIN pg_namespace ns ON ns.oid = c.relnamespace
  GROUP BY ns.nspname, c.relname, c.relkind
  ORDER BY (count(*)) DESC
 LIMIT 50;

COMMENT ON VIEW _query_stats.show_buffers
  IS 'Просмотр содержимого shared_buffers.
 relname - имя отношения
 relkind - тип отношения (r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table)
 cached_size - размер закешированной части
 %_of_total_cache - какая доля от общего кеша
 dirty_size - размер грязных страниц в кеше
 %_dirty - доля грязных страниц';
