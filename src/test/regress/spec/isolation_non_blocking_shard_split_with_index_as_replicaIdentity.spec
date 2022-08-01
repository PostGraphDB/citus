setup
{
	SET citus.shard_count to 1;
	SET citus.shard_replication_factor to 1;
    SELECT setval('pg_dist_shardid_seq', 1500000);

	CREATE TABLE to_split_table (id int NOT NULL, value int);
	CREATE UNIQUE INDEX split_table_index ON to_split_table(id);
	ALTER TABLE to_split_table REPLICA IDENTITY USING INDEX split_table_index;

	SELECT create_distributed_table('to_split_table', 'id');
}

teardown
{
    DROP TABLE to_split_table CASCADE;
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

// cache all placements
step "s1-load-cache"
{
	-- Indirect way to load cache.
	TRUNCATE to_split_table;
}

step "s1-non-blocking-shard-split"
{
    	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500001,
		ARRAY['-1073741824'],
		ARRAY[2, 2],
		'force_logical');
}

step "s1-end"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-insert"
{
	SELECT get_shard_id_for_distribution_column('to_split_table', 123456789);
	INSERT INTO to_split_table VALUES (123456789, 1);
}

step "s2-insert-2"
{
	SELECT get_shard_id_for_distribution_column('to_split_table', 123456789);
	INSERT INTO to_split_table VALUES (1234567819, 1);
}

step "s2-update"
{
	UPDATE to_split_table SET value = 111 WHERE id = 123456789;
}

step "s2-delete"
{
	DELETE FROM to_split_table WHERE id = 123456789;
}

step "s2-select"
{
	SELECT count(*) FROM to_split_table WHERE id = 123456789;
}

step "s2-end"
{
	  COMMIT;
}

step "s2-print-cluster"
{
	-- row count per shard
	SELECT
		nodeport, shardid, success, result
	FROM
		run_command_on_placements('to_split_table', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;
	-- rows
	SELECT id, value FROM to_split_table ORDER BY id, value;
}


session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

##// nonblocking tests lie below ###

// move placement first
// the following tests show the non-blocking modifications while shard is being moved
// in fact, the shard move blocks the writes for a very short duration of time
// by using an advisory and allowing the other commands continue to run, we prevent
// the modifications to block on that blocking duration
//permutation "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-insert" "s3-release-advisory-lock" "s1-end" "s1-select" "s1-get-shard-distribution"


permutation "s1-load-cache" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s2-begin" "s1-non-blocking-shard-split" "s2-insert" "s2-end" "s2-print-cluster" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s1-load-cache" "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-update" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s1-load-cache" "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-delete" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"

permutation "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s2-begin" "s1-non-blocking-shard-split" "s2-insert" "s2-end" "s2-print-cluster" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-update" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-delete" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
