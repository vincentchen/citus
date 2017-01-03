--
-- MULTI_REPLICATE_REFERENCE_TABLE
--
-- Tests that check the metadata returned by the master node.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1370000;


CREATE TABLE tmp_shard_placement(
    shardid int8 NOT NULL,
    shardstate int4 NOT NULL,
    shardlength int8 NOT NULL,
    nodename text NOT NULL,
    nodeport int8 NOT NULL,
    placementid bigint NOT NULL
);

-- remove a node for testing purposes
INSERT INTO tmp_shard_placement (SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port);
DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT master_remove_node('localhost', :worker_2_port);


-- test adding new node with no reference tables
SELECT master_add_node('localhost', :worker_2_port);

-- verify nothing is replicated to the new node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;


-- test adding new node with a reference table which does not have any healthy placement
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_unhealthy(column1 int);
SELECT create_reference_table('replicate_reference_table_unhealthy');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1370000;

SELECT master_add_node('localhost', :worker_2_port);

-- verify nothing is replicated to the new node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;

DROP TABLE replicate_reference_table_unhealthy;


-- test replicating a reference table when a new node added
CREATE TABLE replicate_reference_table_valid(column1 int);
SELECT create_reference_table('replicate_reference_table_valid');

-- status before master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

SELECT master_add_node('localhost', :worker_2_port);

-- status after master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

DROP TABLE replicate_reference_table_valid;


-- test replicating a reference table when a new node added in TRANSACTION + ROLLBACK
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_rollback(column1 int);
SELECT create_reference_table('replicate_reference_table_rollback');

-- status before master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
ROLLBACK;

-- status after master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

DROP TABLE replicate_reference_table_rollback;


-- test replicating a reference table when a new node added in TRANSACTION + COMMIT
CREATE TABLE replicate_reference_table_commit(column1 int);
SELECT create_reference_table('replicate_reference_table_commit');

-- status before master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
COMMIT;

-- status after master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

DROP TABLE replicate_reference_table_commit;


-- test adding new node + upgrading another hash distributed table to reference table + creating new reference table in TRANSACTION
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_reference_one(column1 int);
SELECT create_reference_table('replicate_reference_table_reference_one');

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
CREATE TABLE replicate_reference_table_hash(column1 int);
SELECT create_distributed_table('replicate_reference_table_hash', 'column1');

CREATE TABLE replicate_reference_table_reference_two(column1 int);

-- status before master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);
SELECT * FROM pg_dist_partition WHERE logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_hash', 'replicate_reference_table_reference_two');

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
SELECT upgrade_to_reference_table('replicate_reference_table_hash');
SELECT create_reference_table('replicate_reference_table_reference_two');
COMMIT;

-- status after master_add_node
SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);
SELECT * FROM pg_dist_partition WHERE logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_hash', 'replicate_reference_table_reference_two');

DROP TABLE replicate_reference_table_reference_one;
DROP TABLE replicate_reference_table_hash;
DROP TABLE replicate_reference_table_reference_two;


-- reload pg_dist_shard_placement table

INSERT INTO pg_dist_shard_placement (SELECT * FROM tmp_shard_placement);
DROP TABLE tmp_shard_placement;
