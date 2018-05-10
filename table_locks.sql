CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.table_locks AS 
 SELECT pg_namespace.nspname AS schemaname,
    pg_class.relname AS tablename,
    pg_locks.mode AS lock_type,
    age(now(), pg_stat_activity.query_start) AS time_running,
    pg_stat_activity.query
   FROM pg_class
     JOIN pg_locks ON pg_locks.relation = pg_class.oid
     JOIN pg_database ON pg_database.oid = pg_locks.database
     JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
     JOIN pg_stat_activity ON pg_stat_activity.pid = pg_locks.pid
  WHERE pg_class.relkind = 'r'::"char" AND pg_database.datname = current_database();

COMMENT ON VIEW _query_stats.table_locks
  IS 'Блокировки на таблицах';

