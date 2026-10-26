-- Fingerprint of the version registry (control database) after an expansion, without volatile values
-- (ids, timestamps, host name): the versions and their dependencies.
SELECT 'versions', count(*), md5(string_agg(row(database_name, schema_name, table_name, version_code, version_title,
           created_by, sql_recipe, restored_from_version, raw_dump_path, raw_hash, raw_schema, operation_kind,
           regexp_replace(version_history_log, '^(Data/Hora|Computador):.*$', '', 'gn'))::text, '|'
           ORDER BY database_name, schema_name, table_name, version_code))
FROM app_control.table_versions
UNION ALL
SELECT 'dependencies', count(*), md5(string_agg(row(v.table_name, v.version_code, d.dependency_kind,
           d.source_database_name, d.source_schema_name, d.source_table_name, d.dependency_payload)::text, '|'
           ORDER BY v.table_name, v.version_code, d.dependency_kind, d.source_table_name))
FROM app_control.table_version_dependencies d JOIN app_control.table_versions v ON v.id = d.table_version_id;
