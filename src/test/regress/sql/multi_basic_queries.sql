--
-- MULTI_BASIC_QUERIES
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 440000;


-- Execute simple sum, average, and count queries on data recently uploaded to
-- our partitioned table.

SELECT count(*) FROM lineitem;

SELECT sum(l_extendedprice) FROM lineitem;

SELECT avg(l_extendedprice) FROM lineitem;

-- Verify temp tables which are used for final result aggregation don't persist.
SELECT count(*) FROM pg_class WHERE relname LIKE 'pg_merge_job_%' AND relkind = 'r';
