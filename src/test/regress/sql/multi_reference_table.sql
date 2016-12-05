ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1250000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1250000;

CREATE TABLE reference_table_test (value_1 int, value_2 float, value_3 text, value_4 timestamp);

-- user should not be able to create reference tables via master_create_distributed_table()
SELECT master_create_distributed_table('reference_table_test', 'value_1', 'all');

-- user should not be able to create reference tables via create_distributed_table()
SELECT create_distributed_table('reference_table_test', 'value_1', 'all');

-- insert some data, and make sure that cannot be create_distributed_table
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-05');

-- should error out given that there exists data
SELECT create_reference_table('reference_table_test');

TRUNCATE reference_table_test;

-- now should be able to create the reference table
SELECT create_reference_table('reference_table_test');

-- see that partkey is NULL
SELECT
	partmethod, (partkey IS NULL) as partkeyisnull, colocationid
FROM
	pg_dist_partition
WHERE
	logicalrelid = 'reference_table_test'::regclass;

-- now see that shard min/max values are NULL
SELECT
	shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
	pg_dist_shard
WHERE
	logicalrelid = 'reference_table_test'::regclass;
SELECT
	shardid, shardstate, nodename, nodeport
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'reference_table_test'::regclass);

-- now, execute some modification queries
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test VALUES (3, 3.0, '3', '2016-12-03');
INSERT INTO reference_table_test VALUES (4, 4.0, '4', '2016-12-04');
INSERT INTO reference_table_test VALUES (5, 5.0, '5', '2016-12-05');


-- most of the queries in this file are already tested on multi_router_planner.sql
-- However, for the sake of completeness we need to run similar tests with
-- reference tables as well

-- run some queries on top of the data
SELECT
	*
FROM
	reference_table_test;

SELECT
	*
FROM
	reference_table_test
WHERE
	value_1 = 1;

SELECT
	value_1,
	value_2
FROM
	reference_table_test
ORDER BY
	2 ASC LIMIT 3;

SELECT
	value_1, value_3
FROM
	reference_table_test
WHERE
	value_2 >= 4
ORDER BY
	2 LIMIT 3;

SELECT
	value_1, 15 * value_2
FROM
	reference_table_test
ORDER BY
	2 ASC
LIMIT 2;

SELECT
	value_1, 15 * value_2
FROM
	reference_table_test
ORDER BY
	2 ASC LIMIT 2 OFFSET 2;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_2 = 2 OR value_2 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_2 = 2 AND value_2 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_3 = '2' OR value_1 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	(
		value_3 = '2' OR value_1 = 3
	)
	AND FALSE;

SELECT
	*
FROM
	reference_table_test
WHERE
	value_2 IN
	(
		SELECT
			value_3::FLOAT
		FROM
			reference_table_test
	)
	AND value_1 < 3;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_3 IN
	(
		'1', '2'
	);

SELECT
	date_part('day', value_4)
FROM
	reference_table_test
WHERE
	value_3 IN
	(
		'5', '2'
	);

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 <= 2 AND value_2 >= 4;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 <= 20 AND value_2 >= 4;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 >= 5 AND value_2 <= random();

SELECT
	value_1
FROM
	reference_table_test
WHERE
	value_4 BETWEEN '2016-12-01' AND '2016-12-03';

SELECT
	value_1
FROM
	reference_table_test
WHERE
	FALSE;
SELECT
	value_1
FROM
	reference_table_test
WHERE
	int4eq(1, 2);

-- rename output name and do some operations
SELECT
	value_1 as id, value_2 * 15 as age
FROM
	reference_table_test;

-- queries with CTEs are supported
WITH some_data AS ( SELECT value_2, value_4 FROM reference_table_test WHERE value_2 >=3)
SELECT
	*
FROM
	some_data;

-- queries with CTEs are supported even if CTE is not referenced inside query
WITH some_data AS ( SELECT value_2, value_4 FROM reference_table_test WHERE value_2 >=3)
SELECT * FROM reference_table_test ORDER BY 1 LIMIT 1;

-- queries which involve functions in FROM clause are supported if it goes to a single worker.
SELECT
	*
FROM
	reference_table_test, position('om' in 'Thomas')
WHERE
	value_1 = 1;

SELECT
	*
FROM
	reference_table_test, position('om' in 'Thomas')
WHERE
	value_1 = 1 OR value_1 = 2;

-- set operations are supported
(SELECT * FROM reference_table_test WHERE value_1 = 1)
UNION
(SELECT * FROM reference_table_test WHERE value_1 = 3);

(SELECT * FROM reference_table_test WHERE value_1 = 1)
EXCEPT
(SELECT * FROM reference_table_test WHERE value_1 = 3);

(SELECT * FROM reference_table_test WHERE value_1 = 1)
INTERSECT
(SELECT * FROM reference_table_test WHERE value_1 = 3);

-- to make the tests more interested for aggregation tests, ingest some more data
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test VALUES (3, 3.0, '3', '2016-12-03');

-- some aggregations
SELECT
	value_4, SUM(value_2)
FROM
	reference_table_test
GROUP BY
	value_4
HAVING
	SUM(value_2) > 3
ORDER BY
	1;

SELECT
	value_4,
	value_3,
	SUM(value_2)
FROM
	reference_table_test
GROUP BY
	GROUPING sets ((value_4), (value_3))
ORDER BY 1, 2, 3;


-- distinct clauses also work fine
SELECT DISTINCT
	value_4
FROM
	reference_table_test
ORDER BY
	1;

-- window functions are also supported
SELECT
	value_4, RANK() OVER (PARTITION BY value_1 ORDER BY value_4)
FROM
	reference_table_test;

-- window functions are also supported
SELECT
	value_4, AVG(value_1) OVER (PARTITION BY value_4 ORDER BY value_4)
FROM
	reference_table_test;

SELECT
	count(DISTINCT CASE
			WHEN
				value_2 >= 3
			THEN
				value_2
			ELSE
				NULL
			END) as c
	FROM
		reference_table_test;

SELECT
	value_1,
	count(DISTINCT CASE
			WHEN
				value_2 >= 3
			THEN
				value_2
			ELSE
				NULL
			END) as c
	FROM
		reference_table_test
	GROUP BY
		value_1
	ORDER BY
		1;

-- selects inside a transaction works fine as well

BEGIN;
SELECT * FROM reference_table_test;
SELECT * FROM reference_table_test WHERE value_1 = 1;
END;

-- cursor queries also works fine
BEGIN;
DECLARE test_cursor CURSOR FOR
	SELECT *
		FROM reference_table_test
		WHERE value_1 = 1 OR value_1 = 2
		ORDER BY value_1;
FETCH test_cursor;
FETCH ALL test_cursor;
FETCH test_cursor; -- fetch one row after the last
END;

-- table creation queries inside can be router plannable
CREATE TEMP TABLE temp_reference_test as
	SELECT *
	FROM reference_table_test
	WHERE value_1 = 1;

-- all kinds of joins are supported among reference tables
-- first create two more tables
CREATE TABLE reference_table_test_second (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_second');

CREATE TABLE reference_table_test_third (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_third');

-- ingest some data to both tables
INSERT INTO reference_table_test_second VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test_second VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test_second VALUES (3, 3.0, '3', '2016-12-03');

INSERT INTO reference_table_test_third VALUES (4, 4.0, '4', '2016-12-04');
INSERT INTO reference_table_test_third VALUES (5, 5.0, '5', '2016-12-05');

-- some very basic tests
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_2
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_third t3
WHERE
	t1.value_2 = t3.value_2
ORDER BY
	1;

SELECT
	DISTINCT t2.value_1
FROM
	reference_table_test_second t2, reference_table_test_third t3
WHERE
	t2.value_2 = t3.value_2
ORDER BY
	1;

-- join on different columns and different data types via casts
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_1
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_3::int
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = date_part('day', t2.value_4)
ORDER BY
	1;

-- ingest a common row to see more meaningful results with joins involving 3 tables
INSERT INTO reference_table_test_third VALUES (3, 3.0, '3', '2016-12-03');

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2, reference_table_test_third t3
WHERE
	t1.value_2 = date_part('day', t2.value_4) AND t3.value_2 = t1.value_2
ORDER BY
	1;

-- same query on different columns
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2, reference_table_test_third t3
WHERE
	t1.value_1 = date_part('day', t2.value_4) AND t3.value_2 = t1.value_1
ORDER BY
	1;

-- with the JOIN syntax
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 JOIN reference_table_test_second t2 USING (value_1)
							JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

-- and left/right joins
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 LEFT JOIN reference_table_test_second t2 USING (value_1)
							LEFT JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 RIGHT JOIN reference_table_test_second t2 USING (value_1)
							RIGHT JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

-- now, lets have some tests on UPSERTs and uniquness
CREATE TABLE reference_table_test_fourth (value_1 int, value_2 float PRIMARY KEY, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_fourth');

-- insert a row
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01');

-- now get the unique key violation
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01');

-- now get null constraint violation due to primary key
INSERT INTO reference_table_test_fourth (value_1, value_3, value_4) VALUES (1, '1.0', '2016-12-01');

-- lets run some upserts
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01') ON CONFLICT DO NOTHING RETURNING *;
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '10', '2016-12-01') ON CONFLICT (value_2) DO
	UPDATE SET value_3 = EXCLUDED.value_3, value_2 = EXCLUDED.value_2
	RETURNING *;
-- update all columns
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '10', '2016-12-01') ON CONFLICT (value_2) DO
	UPDATE SET value_3 = EXCLUDED.value_3 || '+10', value_2 = EXCLUDED.value_2 + 10, value_1 = EXCLUDED.value_1 + 10, value_4 = '2016-12-10'
	RETURNING *;

-- finally see that shard healths are OK
SELECT
	shardid, shardstate, nodename, nodeport
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'reference_table_test_fourth'::regclass);
