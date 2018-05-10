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
