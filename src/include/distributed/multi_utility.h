/*-------------------------------------------------------------------------
 *
 * multi_utility.h
 *	  Citus utility hook and related functionality.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_UTILITY_H
#define MULTI_UTILITY_H

#include "tcop/utility.h"

extern bool EnableDDLPropagation;

extern void multi_ProcessUtility(Node *parsetree, const char *queryString,
								 ProcessUtilityContext context, ParamListInfo params,
								 DestReceiver *dest, char *completionTag);
extern void ReplicateGrantStmt(Node *parsetree);

extern Datum master_drop_all_shards(PG_FUNCTION_ARGS);
extern Datum master_modify_multiple_shards(PG_FUNCTION_ARGS);


#endif /* MULTI_UTILITY_H */
