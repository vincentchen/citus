/* citus--6.0-18--6.0-19.sql */

SET search_path = 'pg_catalog';

-- we don't need this constraint any more since reference tables 
-- wouldn't have partition columns, which we represent as NULL
ALTER TABLE pg_dist_partition ALTER COLUMN partkey DROP NOT NULL;

-- add the new distribution type
--ALTER TYPE citus.distribution_type ADD VALUE 'all' AFTER 'append';

RESET search_path;
