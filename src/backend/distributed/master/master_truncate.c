/*-------------------------------------------------------------------------
 *
 * master_truncate.c
 *
 * Routine for truncating local data after a table has been distributed.
 *
 * Copyright (c) 2014-2017, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port.h"

#include <stddef.h>

#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_utility.h"
#include "distributed/pg_dist_partition.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_truncate_trigger);


/*
 * citus_truncate_trigger is called as a trigger when a distributed
 * table is truncated.
 */
Datum
citus_truncate_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Relation truncatedRelation = triggerData->tg_relation;

	Oid relationId = RelationGetRelid(truncatedRelation);
	char *relationName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char partitionMethod = PartitionMethod(relationId);

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	EnsureTablePermissions(relationId, ACL_TRUNCATE);

	if (!EnableDDLPropagation)
	{
		PG_RETURN_DATUM(PointerGetDatum(NULL));
	}

	if (partitionMethod == DISTRIBUTE_BY_APPEND)
	{
		DirectFunctionCall3(master_drop_all_shards,
							ObjectIdGetDatum(relationId),
							CStringGetTextDatum(relationName),
							CStringGetTextDatum(schemaName));
	}
	else
	{
		StringInfo truncateStatement = makeStringInfo();
		char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

		appendStringInfo(truncateStatement, "TRUNCATE TABLE %s CASCADE",
						 qualifiedTableName);

		DirectFunctionCall1(master_modify_multiple_shards,
							CStringGetTextDatum(truncateStatement->data));
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}
