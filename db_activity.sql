CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.db_activity AS 
 SELECT clock_timestamp() - pg_stat_activity.xact_start AS ts_age,
    pg_stat_activity.state,
    clock_timestamp() - pg_stat_activity.query_start AS query_age,
    clock_timestamp() - pg_stat_activity.state_change AS change_age,
    pg_stat_activity.datname,
    pg_stat_activity.pid,
    pg_stat_activity.usename,
    COALESCE(pg_stat_activity.wait_event_type = 'Lock'::text, false) AS waiting,
    pg_stat_activity.client_addr,
    pg_stat_activity.client_port,
    pg_stat_activity.query
   FROM pg_stat_activity
  WHERE ((clock_timestamp() - pg_stat_activity.xact_start) > '00:00:00.1'::interval OR (clock_timestamp() - pg_stat_activity.query_start) > '00:00:00.1'::interval AND pg_stat_activity.state = 'idle in transaction (aborted)'::text) AND pg_stat_activity.pid <> pg_backend_pid()
  ORDER BY (COALESCE(pg_stat_activity.xact_start, pg_stat_activity.query_start));
