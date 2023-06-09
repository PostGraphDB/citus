DO $$
declare
citus_schemas_create_query text;
BEGIN
citus_schemas_create_query=$CSCQ$
    CREATE OR REPLACE VIEW %I.citus_schemas AS
    SELECT
        schemaid::regnamespace AS schema_name,
        colocationid AS colocation_id,
        (
            SELECT pg_size_pretty(sum(citus_total_relation_size(logicalrelid, fail_on_error:=false)))
            FROM pg_dist_partition pdp WHERE pdp.colocationid = ts.colocationid
        ) AS schema_size,
        pg_get_userbyid(nspowner) AS schema_owner
    FROM
        pg_dist_tenant_schema ts
    JOIN
        pg_namespace n ON (ts.schemaid = n.oid)
    ORDER BY
        schema_name;
$CSCQ$;

IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public') THEN
    EXECUTE format(citus_schemas_create_query, 'public');
    GRANT SELECT ON public.citus_schemas TO public;
ELSE
    EXECUTE format(citus_schemas_create_query, 'citus');
    ALTER VIEW citus.citus_schemas SET SCHEMA pg_catalog;
    GRANT SELECT ON pg_catalog.citus_schemas TO public;
END IF;

END;
$$;
