/* citus--6.1-12--6.1-13.sql */

SET search_path = 'pg_catalog';

CREATE INDEX pg_dist_partition_partmethod_index
ON pg_dist_partition using btree(partmethod);

RESET search_path;
