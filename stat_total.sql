CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

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

