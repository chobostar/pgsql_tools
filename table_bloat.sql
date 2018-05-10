CREATE SCHEMA IF NOT EXISTS _query_stats;
COMMENT ON SCHEMA _query_stats IS 'Мониторинг БД';

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
