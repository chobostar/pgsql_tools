-- PostgreSQL 9.6

CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

CREATE OR REPLACE VIEW _query_stats.btree_bloat AS 
 WITH data AS (
         SELECT current_database() AS current_database,
            sub.nspname AS schemaname,
            sub.tblname,
            sub.idxname,
            sub.bs * sub.relpages::bigint::numeric AS real_size,
            sub.bs * (sub.relpages::double precision - sub.est_pages)::bigint::numeric AS extra_size,
            100::double precision * (sub.relpages::double precision - sub.est_pages) / sub.relpages::double precision AS extra_ratio,
            sub.fillfactor,
            sub.bs::double precision * (sub.relpages::double precision - sub.est_pages_ff) AS bloat_size,
            100::double precision * (sub.relpages::double precision - sub.est_pages_ff) / sub.relpages::double precision AS bloat_ratio,
            sub.is_na
           FROM ( SELECT COALESCE(1::double precision + ceil(s2.reltuples / floor((s2.bs - s2.pageopqdata::numeric - s2.pagehdr::numeric)::double precision / (4::numeric + s2.nulldatahdrwidth)::double precision)), 0::double precision) AS est_pages,
                    COALESCE(1::double precision + ceil(s2.reltuples / floor(((s2.bs - s2.pageopqdata::numeric - s2.pagehdr::numeric) * s2.fillfactor::numeric)::double precision / (100::double precision * (4::numeric + s2.nulldatahdrwidth)::double precision))), 0::double precision) AS est_pages_ff,
                    s2.bs,
                    s2.nspname,
                    s2.table_oid,
                    s2.tblname,
                    s2.idxname,
                    s2.relpages,
                    s2.fillfactor,
                    s2.is_na
                   FROM ( SELECT s1.maxalign,
                            s1.bs,
                            s1.nspname,
                            s1.tblname,
                            s1.idxname,
                            s1.reltuples,
                            s1.relpages,
                            s1.relam,
                            s1.table_oid,
                            s1.fillfactor,
                            ((s1.index_tuple_hdr_bm + s1.maxalign -
                                CASE
                                    WHEN (s1.index_tuple_hdr_bm % s1.maxalign) = 0 THEN s1.maxalign
                                    ELSE s1.index_tuple_hdr_bm % s1.maxalign
                                END)::double precision + s1.nulldatawidth + s1.maxalign::double precision -
                                CASE
                                    WHEN s1.nulldatawidth = 0::double precision THEN 0
                                    WHEN (s1.nulldatawidth::integer % s1.maxalign) = 0 THEN s1.maxalign
                                    ELSE s1.nulldatawidth::integer % s1.maxalign
                                END::double precision)::numeric AS nulldatahdrwidth,
                            s1.pagehdr,
                            s1.pageopqdata,
                            s1.is_na
                           FROM ( SELECT i.nspname,
                                    i.tblname,
                                    i.idxname,
                                    i.reltuples,
                                    i.relpages,
                                    i.relam,
                                    a.attrelid AS table_oid,
                                    current_setting('block_size'::text)::numeric AS bs,
                                    i.fillfactor,
CASE
 WHEN version() ~ 'mingw32'::text OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64'::text THEN 8
 ELSE 4
END AS maxalign,
                                    24 AS pagehdr,
                                    16 AS pageopqdata,
CASE
 WHEN max(COALESCE(s.null_frac, 0::real)) = 0::double precision THEN 2
 ELSE 2 + (32 + 8 - 1) / 8
END AS index_tuple_hdr_bm,
                                    sum((1::double precision - COALESCE(s.null_frac, 0::real)) * COALESCE(s.avg_width, 1024)::double precision) AS nulldatawidth,
                                    max(
CASE
 WHEN a.atttypid = 'name'::regtype::oid THEN 1
 ELSE 0
END) > 0 AS is_na
                                   FROM pg_attribute a
                                     JOIN ( SELECT pg_namespace.nspname,
    tbl.relname AS tblname,
    idx.relname AS idxname,
    idx.reltuples,
    idx.relpages,
    idx.relam,
    pg_index.indrelid,
    pg_index.indexrelid,
    pg_index.indkey::smallint[] AS attnum,
    COALESCE("substring"(array_to_string(idx.reloptions, ' '::text), 'fillfactor=([0-9]+)'::text)::smallint::integer, 90) AS fillfactor
   FROM pg_index
     JOIN pg_class idx ON idx.oid = pg_index.indexrelid
     JOIN pg_class tbl ON tbl.oid = pg_index.indrelid
     JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
  WHERE pg_index.indisvalid AND tbl.relkind = 'r'::"char" AND idx.relpages > 0) i ON a.attrelid = i.indexrelid
                                     JOIN pg_stats s ON s.schemaname = i.nspname AND (s.tablename = i.tblname AND s.attname::text = pg_get_indexdef(a.attrelid, a.attnum::integer, true) OR s.tablename = i.idxname AND s.attname = a.attname)
                                     JOIN pg_type t ON a.atttypid = t.oid
                                  WHERE a.attnum > 0
                                  GROUP BY i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid, (current_setting('block_size'::text)::numeric), i.fillfactor) s1) s2
                     JOIN pg_am am ON s2.relam = am.oid
                  WHERE am.amname = 'btree'::name) sub
        )
 SELECT data.current_database,
    data.schemaname,
    data.tblname,
    data.idxname,
    data.real_size,
    pg_size_pretty(data.real_size) AS real_size_pretty,
    data.extra_size,
    pg_size_pretty(data.extra_size) AS extra_size_pretty,
    data.extra_ratio AS "extra_ratio, %",
    data.bloat_size,
    pg_size_pretty(data.bloat_size::numeric) AS bloat_size_pretty,
    data.bloat_ratio AS "bloat_ratio, %",
    data.fillfactor,
    data.is_na,
    data.real_size::double precision - data.bloat_size AS live_data_size
   FROM data
  ORDER BY data.bloat_size DESC;

COMMENT ON VIEW _query_stats.btree_bloat
  IS 'enhanced version of https://github.com/ioguix/pgsql-bloat-estimation/blob/master/btree/btree_bloat.sql

WARNING: executed with a non-superuser role, the query inspect only index on tables you are granted to read.
WARNING: rows with is_na =''t'' are known to have bad statistics ("name" type is not supported).
This query is compatible with PostgreSQL 8.2 and after
';


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


CREATE OR REPLACE VIEW _query_stats.replication_lag AS 
 SELECT s.client_addr,
    s.sent_offset::double precision - (s.replay_offset::double precision - ((s.sent_xlog - s.replay_xlog) * 255)::double precision * (16::double precision ^ 6::double precision)) AS byte_lag
   FROM ( SELECT pg_stat_replication.client_addr,
            (('x'::text || lpad(split_part(pg_stat_replication.sent_location::text, '/'::text, 1), 8, '0'::text)))::bit(32)::bigint AS sent_xlog,
            (('x'::text || lpad(split_part(pg_stat_replication.replay_location::text, '/'::text, 1), 8, '0'::text)))::bit(32)::bigint AS replay_xlog,
            (('x'::text || lpad(split_part(pg_stat_replication.sent_location::text, '/'::text, 2), 8, '0'::text)))::bit(32)::bigint AS sent_offset,
            (('x'::text || lpad(split_part(pg_stat_replication.replay_location::text, '/'::text, 2), 8, '0'::text)))::bit(32)::bigint AS replay_offset
           FROM pg_stat_replication) s;


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


CREATE OR REPLACE VIEW _query_stats.stat_io_time AS 
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
  WHERE ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT s.iot
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
  WHERE ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT s.iot
           FROM s))) < 0.005::double precision
  ORDER BY 2 DESC;

COMMENT ON VIEW _query_stats.stat_io_time
  IS 'VIEW for viewing queries with IO time more or equal 0.02 seconds. Also require pg_stat_statements and track_io_timings in postgresql.conf.

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

Note: all queries which runtime less 0.02 seconds, accounts into dedicated ''other'' query.
';


CREATE OR REPLACE VIEW _query_stats.stat_total AS 
 WITH pg_stat_statements_normalized AS (
         SELECT pg_stat_statements.userid,
            pg_stat_statements.dbid,
            pg_stat_statements.queryid,
            pg_stat_statements.query,
            pg_stat_statements.calls,
            pg_stat_statements.total_time,
            pg_stat_statements.min_time,
            pg_stat_statements.max_time,
            pg_stat_statements.mean_time,
            pg_stat_statements.stddev_time,
            pg_stat_statements.rows,
            pg_stat_statements.shared_blks_hit,
            pg_stat_statements.shared_blks_read,
            pg_stat_statements.shared_blks_dirtied,
            pg_stat_statements.shared_blks_written,
            pg_stat_statements.local_blks_hit,
            pg_stat_statements.local_blks_read,
            pg_stat_statements.local_blks_dirtied,
            pg_stat_statements.local_blks_written,
            pg_stat_statements.temp_blks_read,
            pg_stat_statements.temp_blks_written,
            pg_stat_statements.blk_read_time,
            pg_stat_statements.blk_write_time,
            translate(regexp_replace(regexp_replace(regexp_replace(regexp_replace(pg_stat_statements.query, '\?(::[a-zA-Z_]+)?( *, *\?(::[a-zA-Z_]+)?)+'::text, '?'::text, 'g'::text), '\$[0-9]+(::[a-zA-Z_]+)?( *, *\$[0-9]+(::[a-zA-Z_]+)?)*'::text, '$N'::text, 'g'::text), '--.*$'::text, ''::text, 'ng'::text), '/\*.*?\*/'::text, ''::text, 'g'::text), '
'::text, ''::text) AS query_normalized
           FROM pg_stat_statements
          WHERE (pg_stat_statements.dbid IN ( SELECT pg_database.oid
                   FROM pg_database
                  WHERE current_database() = 'postgres'::name OR pg_database.datname = current_database()))
        ), totals AS (
         SELECT sum(pg_stat_statements.total_time) AS total_time,
            sum(pg_stat_statements.blk_read_time + pg_stat_statements.blk_write_time) AS io_time,
            sum(pg_stat_statements.total_time - pg_stat_statements.blk_read_time - pg_stat_statements.blk_write_time) AS cpu_time,
            sum(pg_stat_statements.calls) AS ncalls,
            sum(pg_stat_statements.rows) AS total_rows
           FROM pg_stat_statements
          WHERE (pg_stat_statements.dbid IN ( SELECT pg_database.oid
                   FROM pg_database
                  WHERE current_database() = 'postgres'::name OR pg_database.datname = current_database()))
        ), _pg_stat_statements AS (
         SELECT ( SELECT pg_database.datname
                   FROM pg_database
                  WHERE pg_database.oid = p.dbid) AS database,
            ( SELECT pg_roles.rolname
                   FROM pg_roles
                  WHERE pg_roles.oid = p.userid) AS username,
            "substring"(translate(replace((array_agg(p.query ORDER BY (length(p.query))))[1], '-- 
'::text, '--
'::text), '
'::text, ''::text), 1, 8192) AS query,
            sum(p.total_time) AS total_time,
            sum(p.blk_read_time) AS blk_read_time,
            sum(p.blk_write_time) AS blk_write_time,
            sum(p.calls) AS calls,
            sum(p.rows) AS rows
           FROM pg_stat_statements_normalized p
          WHERE true
          GROUP BY p.dbid, p.userid, (md5(p.query_normalized))
        ), totals_readable AS (
         SELECT to_char('00:00:00.001'::interval * totals.total_time, 'HH24:MI:SS'::text) AS total_time,
            (100::double precision * totals.io_time / totals.total_time)::numeric(20,2) AS io_time_percent,
            to_char(totals.ncalls, 'FM999,999,999,990'::text) AS total_queries,
            ( SELECT to_char(count(DISTINCT md5(_pg_stat_statements.query)), 'FM999,999,990'::text) AS to_char
                   FROM _pg_stat_statements) AS unique_queries
           FROM totals
        ), statements AS (
         SELECT 100::double precision * _pg_stat_statements.total_time / (( SELECT totals.total_time
                   FROM totals)) AS time_percent,
            100::double precision * (_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT GREATEST(totals.io_time, 1::double precision) AS "greatest"
                   FROM totals)) AS io_time_percent,
            100::double precision * (_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT totals.cpu_time
                   FROM totals)) AS cpu_time_percent,
            to_char('00:00:00.001'::interval * _pg_stat_statements.total_time, 'HH24:MI:SS'::text) AS total_time,
            (_pg_stat_statements.total_time::numeric / _pg_stat_statements.calls)::numeric(20,2) AS avg_time,
            ((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time)::numeric / _pg_stat_statements.calls)::numeric(20,2) AS avg_cpu_time,
            ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time)::numeric / _pg_stat_statements.calls)::numeric(20,2) AS avg_io_time,
            to_char(_pg_stat_statements.calls, 'FM999,999,999,990'::text) AS calls,
            (100::numeric * _pg_stat_statements.calls / (( SELECT totals.ncalls
                   FROM totals)))::numeric(20,2) AS calls_percent,
            to_char(_pg_stat_statements.rows, 'FM999,999,999,990'::text) AS rows,
            (100::numeric * _pg_stat_statements.rows / (( SELECT totals.total_rows
                   FROM totals)))::numeric(20,2) AS row_percent,
            _pg_stat_statements.database,
            _pg_stat_statements.username,
            _pg_stat_statements.query
           FROM _pg_stat_statements
          WHERE ((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT totals.cpu_time
                   FROM totals))) >= 0.01::double precision OR ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT GREATEST(totals.io_time, 1::double precision) AS "greatest"
                   FROM totals))) >= 0.01::double precision OR (_pg_stat_statements.calls / (( SELECT totals.ncalls
                   FROM totals))) >= 0.02 OR (_pg_stat_statements.rows / (( SELECT totals.total_rows
                   FROM totals))) >= 0.02
        UNION ALL
         SELECT (100::numeric * sum(_pg_stat_statements.total_time)::numeric)::double precision / (( SELECT totals.total_time
                   FROM totals)) AS time_percent,
            (100::numeric * sum(_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time)::numeric)::double precision / (( SELECT GREATEST(totals.io_time, 1::double precision) AS "greatest"
                   FROM totals)) AS io_time_percent,
            (100::numeric * sum(_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time)::numeric)::double precision / (( SELECT totals.cpu_time
                   FROM totals)) AS cpu_time_percent,
            to_char('00:00:00.001'::interval * sum(_pg_stat_statements.total_time), 'HH24:MI:SS'::text) AS total_time,
            (sum(_pg_stat_statements.total_time)::numeric / sum(_pg_stat_statements.calls))::numeric(20,2) AS avg_time,
            (sum(_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time)::numeric / sum(_pg_stat_statements.calls))::numeric(20,2) AS avg_cpu_time,
            (sum(_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time)::numeric / sum(_pg_stat_statements.calls))::numeric(20,2) AS avg_io_time,
            to_char(sum(_pg_stat_statements.calls), 'FM999,999,999,990'::text) AS calls,
            (100::numeric * sum(_pg_stat_statements.calls) / (( SELECT totals.ncalls
                   FROM totals)))::numeric(20,2) AS calls_percent,
            to_char(sum(_pg_stat_statements.rows), 'FM999,999,999,990'::text) AS rows,
            (100::numeric * sum(_pg_stat_statements.rows) / (( SELECT totals.total_rows
                   FROM totals)))::numeric(20,2) AS row_percent,
            'all'::name AS database,
            'all'::name AS username,
            'other'::text AS query
           FROM _pg_stat_statements
          WHERE NOT (((_pg_stat_statements.total_time - _pg_stat_statements.blk_read_time - _pg_stat_statements.blk_write_time) / (( SELECT totals.cpu_time
                   FROM totals))) >= 0.01::double precision OR ((_pg_stat_statements.blk_read_time + _pg_stat_statements.blk_write_time) / (( SELECT GREATEST(totals.io_time, 1::double precision) AS "greatest"
                   FROM totals))) >= 0.01::double precision OR (_pg_stat_statements.calls / (( SELECT totals.ncalls
                   FROM totals))) >= 0.02 OR (_pg_stat_statements.rows / (( SELECT totals.total_rows
                   FROM totals))) >= 0.02)
        ), statements_readable AS (
         SELECT row_number() OVER (ORDER BY s.time_percent DESC) AS pos,
            to_char(s.time_percent, 'FM990.0'::text) || '%'::text AS time_percent,
            to_char(s.io_time_percent, 'FM990.0'::text) || '%'::text AS io_time_percent,
            to_char(s.cpu_time_percent, 'FM990.0'::text) || '%'::text AS cpu_time_percent,
            to_char(s.avg_io_time * 100::numeric / COALESCE(NULLIF(s.avg_time, 0::numeric), 1::numeric), 'FM990.0'::text) || '%'::text AS avg_io_time_percent,
            s.total_time,
            s.avg_time,
            s.avg_cpu_time,
            s.avg_io_time,
            s.calls,
            s.calls_percent,
            s.rows,
            s.row_percent,
            s.database,
            s.username,
            s.query
           FROM statements s
          WHERE s.calls IS NOT NULL
        )
 SELECT (((((((((((((((((((((((('total time:	'::text || totals_readable.total_time) || ' (IO: '::text) || totals_readable.io_time_percent) || '%)
'::text) || 'total queries:	'::text) || totals_readable.total_queries) || ' (unique: '::text) || totals_readable.unique_queries) || ')
'::text) || 'report for '::text) || (( SELECT
                CASE
                    WHEN current_database() = 'postgres'::name THEN 'all databases'::text
                    ELSE current_database()::text || ' database'::text
                END AS "case"))) || ', version 0.9.5'::text) || ' @ PostgreSQL '::text) || (( SELECT pg_settings.setting
           FROM pg_settings
          WHERE pg_settings.name = 'server_version'::text))) || '
tracking '::text) || (( SELECT pg_settings.setting
           FROM pg_settings
          WHERE pg_settings.name = 'pg_stat_statements.track'::text))) || ' '::text) || (( SELECT pg_settings.setting
           FROM pg_settings
          WHERE pg_settings.name = 'pg_stat_statements.max'::text))) || ' queries, utilities '::text) || (( SELECT pg_settings.setting
           FROM pg_settings
          WHERE pg_settings.name = 'pg_stat_statements.track_utility'::text))) || ', logging '::text) || (( SELECT
                CASE
                    WHEN pg_settings.setting = '0'::text THEN 'all'::text
                    WHEN pg_settings.setting = '-1'::text THEN 'none'::text
                    WHEN pg_settings.setting::integer > 1000 THEN (pg_settings.setting::numeric / 1000::numeric)::numeric(20,1) || 's+'::text
                    ELSE pg_settings.setting || 'ms+'::text
                END AS "case"
           FROM pg_settings
          WHERE pg_settings.name = 'log_min_duration_statement'::text))) || ' queries
'::text) || (( SELECT COALESCE(string_agg(((('WARNING: database '::text || pg_database.datname::text) || ' must be vacuumed within '::text) || to_char(2147483647 - age(pg_database.datfrozenxid), 'FM999,999,999,990'::text)) || ' transactions'::text, '
'::text ORDER BY (age(pg_database.datfrozenxid)) DESC) || '
'::text, ''::text) AS "coalesce"
           FROM pg_database
          WHERE (2147483647 - age(pg_database.datfrozenxid)) < 200000000))) || '
'::text
   FROM totals_readable
UNION ALL
( SELECT (((((((((((((((((((((((((((((('=============================================================================================================
'::text || 'pos:'::text) || statements_readable.pos) || '	 total time: '::text) || statements_readable.total_time) || ' ('::text) || statements_readable.time_percent) || ', CPU: '::text) || statements_readable.cpu_time_percent) || ', IO: '::text) || statements_readable.io_time_percent) || ')	 calls: '::text) || statements_readable.calls) || ' ('::text) || statements_readable.calls_percent) || '%)	 avg_time: '::text) || statements_readable.avg_time) || 'ms (IO: '::text) || statements_readable.avg_io_time_percent) || ')
'::text) || 'user: '::text) || statements_readable.username::text) || '	 db: '::text) || statements_readable.database::text) || '	 rows: '::text) || statements_readable.rows) || ' ('::text) || statements_readable.row_percent) || '%)'::text) || '	 query:
'::text) || statements_readable.query) || '
'::text
   FROM statements_readable
  ORDER BY statements_readable.pos);


CREATE OR REPLACE VIEW _query_stats.table_bloat AS 
 SELECT current_database() AS current_database,
    sml.schemaname,
    sml.tablename,
    round(
        CASE
            WHEN sml.otta = 0::double precision THEN 0.0::double precision
            ELSE sml.relpages::double precision / sml.otta
        END::numeric, 1) AS tbloat,
        CASE
            WHEN sml.relpages::double precision < sml.otta THEN 0::numeric
            ELSE sml.bs * (sml.relpages::double precision - sml.otta)::bigint::numeric
        END AS wastedbytes,
    sml.iname,
    round(
        CASE
            WHEN sml.iotta = 0::double precision OR sml.ipages = 0 THEN 0.0::double precision
            ELSE sml.ipages::double precision / sml.iotta
        END::numeric, 1) AS ibloat,
        CASE
            WHEN sml.ipages::double precision < sml.iotta THEN 0::double precision
            ELSE sml.bs::double precision * (sml.ipages::double precision - sml.iotta)
        END AS wastedibytes
   FROM ( SELECT rs.schemaname,
            rs.tablename,
            cc.reltuples,
            cc.relpages,
            rs.bs,
            ceil(cc.reltuples * ((rs.datahdr + rs.ma::numeric -
                CASE
                    WHEN (rs.datahdr % rs.ma::numeric) = 0::numeric THEN rs.ma::numeric
                    ELSE rs.datahdr % rs.ma::numeric
                END)::double precision + rs.nullhdr2 + 4::double precision) / (rs.bs::double precision - 20::double precision)) AS otta,
            COALESCE(c2.relname, '?'::name) AS iname,
            COALESCE(c2.reltuples, 0::real) AS ituples,
            COALESCE(c2.relpages, 0) AS ipages,
            COALESCE(ceil(c2.reltuples * (rs.datahdr - 12::numeric)::double precision / (rs.bs::double precision - 20::double precision)), 0::double precision) AS iotta
           FROM ( SELECT foo.ma,
                    foo.bs,
                    foo.schemaname,
                    foo.tablename,
                    (foo.datawidth + (foo.hdr + foo.ma -
                        CASE
                            WHEN (foo.hdr % foo.ma) = 0 THEN foo.ma
                            ELSE foo.hdr % foo.ma
                        END)::double precision)::numeric AS datahdr,
                    foo.maxfracsum * (foo.nullhdr + foo.ma -
                        CASE
                            WHEN (foo.nullhdr % foo.ma::bigint) = 0 THEN foo.ma::bigint
                            ELSE foo.nullhdr % foo.ma::bigint
                        END)::double precision AS nullhdr2
                   FROM ( SELECT s.schemaname,
                            s.tablename,
                            constants.hdr,
                            constants.ma,
                            constants.bs,
                            sum((1::double precision - s.null_frac) * s.avg_width::double precision) AS datawidth,
                            max(s.null_frac) AS maxfracsum,
                            constants.hdr + (( SELECT 1 + count(*) / 8
                                   FROM pg_stats s2
                                  WHERE s2.null_frac <> 0::double precision AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename)) AS nullhdr
                           FROM pg_stats s,
                            ( SELECT ( SELECT current_setting('block_size'::text)::numeric AS current_setting) AS bs,
CASE
 WHEN "substring"(foo_1.v, 12, 3) = ANY (ARRAY['8.0'::text, '8.1'::text, '8.2'::text]) THEN 27
 ELSE 23
END AS hdr,
CASE
 WHEN foo_1.v ~ 'mingw32'::text THEN 8
 ELSE 4
END AS ma
                                   FROM ( SELECT version() AS v) foo_1) constants
                          GROUP BY s.schemaname, s.tablename, constants.hdr, constants.ma, constants.bs) foo) rs
             JOIN pg_class cc ON cc.relname = rs.tablename
             JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'::name
             LEFT JOIN pg_index i ON i.indrelid = cc.oid
             LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid) sml
  ORDER BY (
        CASE
            WHEN sml.relpages::double precision < sml.otta THEN 0::numeric
            ELSE sml.bs * (sml.relpages::double precision - sml.otta)::bigint::numeric
        END) DESC;

COMMENT ON VIEW _query_stats.table_bloat
  IS 'https://github.com/bucardo/check_postgres';


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


CREATE OR REPLACE VIEW _query_stats.table_sizes AS 
 SELECT (t.table_schema::text || '.'::text) || t.table_name::text AS table_name,
    pg_size_pretty(pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)) AS table_size,
    pg_size_pretty(idx.size) AS index_size,
    pg_size_pretty(pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)::numeric + idx.size) AS total_size,
    pt.tablespace
   FROM information_schema.tables t
     LEFT JOIN ( SELECT (pi.schemaname::text || '.'::text) || pi.tablename::text AS table_name,
            'INDEXES' AS obj_type,
            sum(pg_table_size(((pi.schemaname::text || '.'::text) || quote_ident(pi.indexname::text))::regclass)) AS size
           FROM pg_indexes pi
          WHERE pi.schemaname <> 'pg_catalog'::name
          GROUP BY ((pi.schemaname::text || '.'::text) || pi.tablename::text)) idx ON ((t.table_schema::text || '.'::text) || t.table_name::text) = idx.table_name
     LEFT JOIN pg_tables pt ON ((t.table_schema::text || '.'::text) || t.table_name::text) = ((pt.schemaname::text || '.'::text) || pt.tablename::text)
  WHERE t.table_schema::text <> 'pg_catalog'::text
  ORDER BY (pg_table_size(((t.table_schema::text || '.'::text) || t.table_name::text)::regclass)::numeric + COALESCE(idx.size, 0::numeric)) DESC;

COMMENT ON VIEW _query_stats.table_sizes
  IS 'Размеры таблиц вместе со всеми его индексами';


