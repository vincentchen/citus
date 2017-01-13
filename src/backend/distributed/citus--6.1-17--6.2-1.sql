/* citus--6.1-17--6.2-1.sql */

SET search_path TO 'pg_catalog';

CREATE OR REPLACE FUNCTION citus_truncate_trigger()
    RETURNS trigger
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_truncate_trigger$$;
COMMENT ON FUNCTION citus_truncate_trigger()
    IS 'trigger function called when truncating the distributed table';

RESET search_path;
