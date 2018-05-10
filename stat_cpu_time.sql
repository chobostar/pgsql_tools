CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.stat_cpu_time AS 
 WITH s AS (
         SELECT sum(pg_stat_statements.total_time) AS t,
            sum(pg_stat_statements.blk_read_time + pg_stat_statements.blk_write_time) AS iot,
            sum(pg_stat_statements.total_time - pg_stat_statements.blk_read_time - pg_stat_statements.blk_write_time) AS cput,
            sum(pg_stat_statements.calls) AS s,
            sum(pg_stat_statements.rows) AS r
           FROM pg_stat_statements
          WHERE true
        ), _pg_stat_statements AS (
         SELECT pg_stat_statements.dbid,
            regexp_replace(pg_stat_statements.query, '\?(, ?\?)+'::text, '?'::text) AS query,
            sum(pg_stat_statements.total_time) AS total_time,
            sum(pg_stat_statements.blk_read_time) AS blk_read_time,
            sum(pg_stat_statements.blk_write_time) AS blk_write_time,
            sum(pg_stat_statements.calls) AS calls,
            sum(pg_stat_statements.rows) AS rows
           FROM pg_stat_statements
          WHERE true
          GROUP BY pg_stat_statements.dbid, pg_stat_statements.query
        )
 SELECT 100 AS time_percent,
    100 AS iotime_percent,
    100 AS cputime_percent,
    s.t / 1000::double precision * '00:00:01'::interval AS total_time,
    (s.cput * 1000::double precision / s.s::double precision)::numeric(20,2) AS avg_cpu_time_microsecond,
    (s.iot * 1000::double precision / s.s::double precision)::numeric(20,2) AS avg_io_time_microsecond,
    s.s AS calls,
    100 AS calls_percent,
    s.r AS rows,
    100 AS row_percent,
    'all'::name AS database,
    'total'::text AS query
   FROM s
UNION ALL
 SELECT (100::double precision * _pg_stat_statements.total_time / (( SELECT s.t
           FROM s)))::numeric(20,2) AS time_percent,
    (100::double precision * (_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT s.iot
           FROM s)))::numeric(20,2) AS iotime_percent,
    (100::double precision * (_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT s.cput
           FROM s)))::numeric(20,2) AS cputime_percent,
    _pg_stat_statements.total_time / 1000::double precision * '00:00:01'::interval AS total_time,
    ((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) * 1000::double precision / _pg_stat_statements.calls::double precision)::numeric(20,2) AS avg_cpu_time_microsecond,
    ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) * 1000::double precision / _pg_stat_statements.calls::double precision)::numeric(20,2) AS avg_io_time_microsecond,
    _pg_stat_statements.calls,
    (100::numeric * _pg_stat_statements.calls / (( SELECT s.s
           FROM s)))::numeric(20,2) AS calls_percent,
    _pg_stat_statements.rows,
    (100::numeric * _pg_stat_statements.rows / (( SELECT s.r
           FROM s)))::numeric(20,2) AS row_percent,
    ( SELECT pg_database.datname
           FROM pg_database
          WHERE pg_database.oid = _pg_stat_statements.dbid) AS database,
    _pg_stat_statements.query
   FROM _pg_stat_statements
  WHERE ((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT s.cput
           FROM s))) >= 0.005::double precision
UNION ALL
 SELECT (100::double precision * sum(_pg_stat_statements.total_time) / (( SELECT s.t
           FROM s)))::numeric(20,2) AS time_percent,
    (100::double precision * sum(_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT s.iot
           FROM s)))::numeric(20,2) AS iotime_percent,
    (100::double precision * sum(_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT s.cput
           FROM s)))::numeric(20,2) AS cputime_percent,
    sum(_pg_stat_statements.total_time) / 1000::double precision * '00:00:01'::interval AS total_time,
    (sum(_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) * 1000::double precision / sum(_pg_stat_statements.calls)::double precision)::numeric(10,3) AS avg_cpu_time_microsecond,
    (sum(_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) * 1000::double precision / sum(_pg_stat_statements.calls)::double precision)::numeric(10,3) AS avg_io_time_microsecond,
    sum(_pg_stat_statements.calls) AS calls,
    (100::numeric * sum(_pg_stat_statements.calls) / (( SELECT s.s
           FROM s)))::numeric(20,2) AS calls_percent,
    sum(_pg_stat_statements.rows) AS rows,
    (100::numeric * sum(_pg_stat_statements.rows) / (( SELECT s.r
           FROM s)))::numeric(20,2) AS row_percent,
    'all'::name AS database,
    'other'::text AS query
   FROM _pg_stat_statements
  WHERE ((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT s.cput
           FROM s))) < 0.005::double precision
  ORDER BY 3 DESC;

COMMENT ON VIEW _query_stats.stat_cpu_time
  IS 'display queries running >= 0.05 seconds (IO time is not taking into account). Require pg_stat_statements extension enabled and optionally track_io_timings enabled in postgresql.conf.

Columns:

time_percent - total query runtime measured in %, relative to the runtime of all queries;

iotime_percent - query time spent on block IO in %, relative to the runtime of all queries;

cputime_percent - query runtime (without time spent on block IO) in %, relative to the runtime of all queries;

total_time - total runtime of this query;

avg_time - average runtime for this query;

avg_io_time - average time spent on IO for this query;

calls - numbers of calls for this query;

calls_percent - numbers of calls for this query in %, relative to the all queries calls;

rows - number of rows was returned by this query;

row_percent - row was returned by this query in %, relative to the all rows returned by all others queries;

query - query text

Note: all queries which runtime less 0.05 seconds, accounts into dedicated ''other'' query.

https://github.com/dataegret/pg-utils
';
