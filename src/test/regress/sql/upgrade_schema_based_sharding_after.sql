ALTER SCHEMA "tenant\'_1" RENAME TO tenant_1;
ALTER SCHEMA "tenant\'_2" RENAME TO tenant_2;

-- verify that colocation id is set even for empty tenant
SELECT colocationid > 0 FROM pg_dist_tenant_schema
WHERE schemaid::regnamespace::text = 'tenant_1';

-- verify the same on workers
SELECT result FROM run_command_on_workers($$
    SELECT colocationid > 0 FROM pg_dist_tenant_schema
    WHERE schemaid::regnamespace::text = 'tenant_1';
$$);

-- verify that colocation id is set for non-empty tenant
SELECT colocationid = (
    SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_2.test_table'::regclass
)
FROM pg_dist_tenant_schema
WHERE schemaid::regnamespace::text = 'tenant_2';

-- verify the same on workers
SELECT result FROM run_command_on_workers($$
    SELECT colocationid = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_2.test_table'::regclass
    )
    FROM pg_dist_tenant_schema
    WHERE schemaid::regnamespace::text = 'tenant_2';
$$);

CREATE TABLE tenant_1.tbl_1(a int, b text);
CREATE TABLE tenant_2.tbl_1(a int, b text);

-- Show that we can create further tenant tables in the tenant schemas
-- after pg upgrade.
SELECT COUNT(*)=2 FROM pg_dist_partition
WHERE logicalrelid IN ('tenant_1.tbl_1'::regclass, 'tenant_2.tbl_1'::regclass) AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;

SELECT colocationid = (
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'tenant_1.tbl_1'::regclass AND
          partmethod = 'n' AND repmodel = 's'
)
FROM pg_dist_tenant_schema
WHERE schemaid::regnamespace::text = 'tenant_1';

SELECT colocationid = (
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'tenant_2.tbl_1'::regclass AND
          partmethod = 'n' AND repmodel = 's'
)
FROM pg_dist_tenant_schema
WHERE schemaid::regnamespace::text = 'tenant_2';

-- rollback the changes made on following schemas to make this test idempotent
DROP TABLE tenant_1.tbl_1, tenant_2.tbl_1;
ALTER SCHEMA tenant_1 RENAME TO "tenant\'_1";
ALTER SCHEMA tenant_2 RENAME TO "tenant\'_2";

SET citus.enable_schema_based_sharding TO ON;

CREATE SCHEMA tenant_3;

-- Show that we can create furher tenant schemas after pg upgrade.
SELECT COUNT(*)=1 FROM pg_dist_tenant_schema WHERE schemaid::regnamespace::text = 'tenant_3';

-- drop the schema created in this test to this test idempotent
DROP SCHEMA tenant_3 CASCADE;

RESET citus.enable_schema_based_sharding;
