CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.replica_lag AS 
 SELECT pg_stat_replication.client_addr AS client,
    pg_stat_replication.usename AS "user",
    pg_stat_replication.application_name AS name,
    pg_stat_replication.state,
    pg_stat_replication.sync_state AS mode,
    (pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.sent_lsn) / 1024::numeric)::integer AS pending_kb,
    (pg_wal_lsn_diff(pg_stat_replication.sent_lsn, pg_stat_replication.write_lsn) / 1024::numeric)::integer AS write_kb,
    (pg_wal_lsn_diff(pg_stat_replication.write_lsn, pg_stat_replication.flush_lsn) / 1024::numeric)::integer AS flush_kb,
    (pg_wal_lsn_diff(pg_stat_replication.flush_lsn, pg_stat_replication.replay_lsn) / 1024::numeric)::integer AS replay_kb,
    pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.replay_lsn)::integer / 1024 AS total_lag_kb
   FROM pg_stat_replication;

COMMENT ON VIEW _query_stats.replica_lag
  IS 'Лаги репликации и возможные его причины. pending - сеть :: write/flush - диски :: replay - диски/CPU';

