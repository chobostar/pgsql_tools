Friendly human readable view for some pg_stat_*

pgadmin3_compatibility.sql - add PostgreSQL 10 support for pgAdmin III (pgAdmin 3)


- https://github.com/chobostar/pgsql_tools/blob/master/pg_repack_disable_superuser_check.patch
Patched https://github.com/reorg/pg_repack
- Removed checks for superuser
- When creating extension-a it grants all permissions to database owner
- Supposed to be used in managed postgresql paired with https://github.com/dimitri/pgextwlist
