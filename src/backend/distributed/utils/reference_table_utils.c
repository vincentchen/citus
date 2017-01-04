/*-------------------------------------------------------------------------
 *
 * reference_table_utils.c
 *
 * Declarations for public utility functions related to reference tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* local function forward declarations */
static void ReplicateSingleShardTableToAllWorkers(Oid relationId);
static void ReplicateShardToAllWorkers(ShardInterval *shardInterval);
static void ConvertToReferenceTableMetadata(Oid relationId, uint64 shardId);
static List * ReferenceTableList(void);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(upgrade_to_reference_table);


/*
 * upgrade_to_reference_table accepts a broadcast table which has only one shard and
 * replicates it across all nodes to create a reference table. It also modifies related
 * metadata to mark the table as reference.
 */
Datum
upgrade_to_reference_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	List *shardIntervalList = NIL;
	ShardInterval *shardInterval = NULL;
	uint64 shardId = INVALID_SHARD_ID;

	EnsureSchemaNode();

	if (!IsDistributedTable(relationId))
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is not distributed.", relationName),
						errhint("Instead, you can use; "
								"create_reference_table('%s');", relationName)));
	}

	if (PartitionMethod(relationId) == DISTRIBUTE_BY_NONE)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is already a reference table",
								  relationName)));
	}

	shardIntervalList = LoadShardIntervalList(relationId);
	if (list_length(shardIntervalList) != 1)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" shard count is not one. Only "
								  "relations with one shard can be upgraded to "
								  "reference tables.", relationName)));
	}

	shardInterval = (ShardInterval *) linitial(shardIntervalList);
	shardId = shardInterval->shardId;

	LockShardDistributionMetadata(shardId, ExclusiveLock);
	LockShardResource(shardId, ExclusiveLock);

	ReplicateSingleShardTableToAllWorkers(relationId);

	PG_RETURN_VOID();
}


/*
 * ReplicateAllReferenceTablesToAllNodes function finds all reference tables and
 * replicates them to all worker nodes. It also modifies pg_dist_colocation table to
 * update the replication factor column. This function skips a worker node if that node
 * already has healthy placement of a particular reference table to prevent unnecessary
 * data transfer.
 */
void
ReplicateAllReferenceTablesToAllNodes()
{
	List *referenceTableList = ReferenceTableList();
	ListCell *referenceTableCell = NULL;

	/* we do not use pgDistNode, we only obtain a lock on it to prevent modifications */
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	List *workerNodeList = WorkerNodeList();
	int workerCount = list_length(workerNodeList);

	uint32 currentColocationId = INVALID_COLOCATION_ID;

	foreach(referenceTableCell, referenceTableList)
	{
		Oid referenceTableId = lfirst_oid(referenceTableCell);
		List *shardIntervalList = LoadShardIntervalList(referenceTableId);
		ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
		uint64 shardId = shardInterval->shardId;

		LockShardDistributionMetadata(shardId, ExclusiveLock);

		ReplicateShardToAllWorkers(shardInterval);

		/* we have this check to prevent accessing to cache multiple times */
		if (currentColocationId == INVALID_COLOCATION_ID)
		{
			/*
			 * We use TableColocationId function to find colocation id of reference tables
			 * instead of ColocationId function, because we do not know replication
			 * factor. It is not necessarily equal to worker count at this point.
			 */
			currentColocationId = TableColocationId(referenceTableId);
		}
	}

	/*
	 * After replicating reference tables, we will update replication factor column for
	 * colocation group of reference tables so that worker count will be equal to
	 * replication factor again.
	 */
	if (currentColocationId != INVALID_COLOCATION_ID)
	{
		UpdateColocationGroupReplicationFactor(currentColocationId, workerCount);
	}

	heap_close(pgDistNode, NoLock);
}


/*
 * ReplicateSingleShardTableToAllWorkers accepts a broadcast table and replicates it to
 * all worker nodes. It assumes that caller of this function ensures that given broadcast
 * table has only one shard.
 */
static void
ReplicateSingleShardTableToAllWorkers(Oid relationId)
{
	List *shardIntervalList = LoadShardIntervalList(relationId);
	ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
	uint64 shardId = shardInterval->shardId;

	List *foreignConstraintCommandList = CopyShardForeignConstraintCommandList(
		shardInterval);

	if (foreignConstraintCommandList != NIL || TableReferenced(relationId))
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is part of a foreign constraint. "
								  "Foreign key constraints are not allowed "
								  "from or to reference tables.", relationName)));
	}

	/*
	 * ReplicateShardToAllWorkers function opens separate transactions (i.e., not part
	 * of any coordinated transactions) to each worker and replicates given shard to all
	 * workers. If a worker already has a healthy replica of given shard, it skips that
	 * worker to prevent copying unnecessary data.
	 */
	ReplicateShardToAllWorkers(shardInterval);

	/*
	 * After copying the shards, we need to update metadata tables to mark this table as
	 * reference table. We modify pg_dist_partition, pg_dist_colocation and pg_dist_shard
	 * tables in ConvertToReferenceTableMetadata function.
	 */
	ConvertToReferenceTableMetadata(relationId, shardId);
}


/*
 * ReplicateShardToAllWorkers function replicates given shard to the given worker nodes
 * in a separate transactions. While replicating, it only replicates the shard to the
 * workers which does not have a healthy replica of the shard. This function also modifies
 * metadata by inserting/updating related rows in pg_dist_shard_placement. However, this
 * function does not obtain any lock on shard resource and shard metadata. It is caller's
 * responsibility to take those locks.
 */
static void
ReplicateShardToAllWorkers(ShardInterval *shardInterval)
{
	uint64 shardId = shardInterval->shardId;
	List *shardPlacementList = ShardPlacementList(shardId);
	bool missingOk = false;
	ShardPlacement *sourceShardPlacement = FinalizedShardPlacement(shardId, missingOk);
	char *srcNodeName = sourceShardPlacement->nodeName;
	uint32 srcNodePort = sourceShardPlacement->nodePort;
	char *tableOwner = TableOwner(shardInterval->relationId);
	List *ddlCommandList = CopyShardCommandList(shardInterval, srcNodeName, srcNodePort);

	/* we do not use pgDistNode, we only obtain a lock on it to prevent modifications */
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	List *workerNodeList = WorkerNodeList();
	ListCell *workerNodeCell = NULL;

	/*
	 * We will iterate over all worker nodes and if healthy placement is not exist at
	 * given node we will copy the shard to that node. Then we will also modify
	 * the metadata to reflect newly copied shard.
	 */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		bool missingWorkerOk = true;

		ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																	 nodeName, nodePort,
																	 missingWorkerOk);

		if (targetPlacement == NULL || targetPlacement->shardState != FILE_FINALIZED)
		{
			char *relationName = get_rel_name(shardInterval->relationId);
			ereport(NOTICE, (errmsg("Replicating reference table \"%s\" to worker "
									"%s:%d...", relationName, nodeName, nodePort)));

			SendCommandListToWorkerInSingleTransaction(nodeName, nodePort, tableOwner,
													   ddlCommandList);
			if (targetPlacement == NULL)
			{
				InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID, FILE_FINALIZED, 0,
										nodeName, nodePort);
			}
			else
			{
				UpdateShardPlacementState(targetPlacement->placementId, FILE_FINALIZED);
			}
		}
	}

	heap_close(pgDistNode, NoLock);
}


/*
 * ConvertToReferenceTableMetadata accepts a broadcast table and modifies its metadata to
 * reference table metadata. To do this, this function updates pg_dist_partition,
 * pg_dist_colocation and pg_dist_shard. This function assumes that caller ensures that
 * given broadcast table has only one shard.
 */
static void
ConvertToReferenceTableMetadata(Oid relationId, uint64 shardId)
{
	uint32 currentColocationId = TableColocationId(relationId);
	uint32 newColocationId = CreateReferenceTableColocationId();
	Var *distributionColumn = NULL;
	char shardStorageType = ShardStorageType(relationId);
	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;

	/* delete old metadata rows */
	DeletePartitionRow(relationId);
	DeleteColocationGroupIfNoTablesBelong(currentColocationId);
	DeleteShardRow(shardId);

	/* insert new metadata rows */
	InsertIntoPgDistPartition(relationId, DISTRIBUTE_BY_NONE, distributionColumn,
							  newColocationId, REPLICATION_MODEL_2PC);
	InsertShardRow(relationId, shardId, shardStorageType, shardMinValue, shardMaxValue);
}


/*
 * CreateReferenceTableColocationId creates a new co-location id for reference tables and
 * writes it into pg_dist_colocation, then returns the created co-location id. Since there
 * can be only one colocation group for all kinds of reference tables, if a co-location id
 * is already created for reference tables, this function returns it without creating
 * anything.
 */
uint32
CreateReferenceTableColocationId()
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	List *workerNodeList = WorkerNodeList();
	int shardCount = 1;
	int replicationFactor = list_length(workerNodeList);
	Oid distributionColumnType = InvalidOid;

	/* check for existing colocations */
	colocationId = ColocationId(shardCount, replicationFactor, distributionColumnType);
	if (colocationId == INVALID_COLOCATION_ID)
	{
		colocationId = CreateColocationGroup(shardCount, replicationFactor,
											 distributionColumnType);
	}

	return colocationId;
}


/*
 * ReferenceTableList function scans pg_dist_partition to create a list of all reference
 * tables. To create the list, it performs index scan.
 */
static List *
ReferenceTableList()
{
	List *referenceTableList = NIL;
	Relation pgDistPartition = NULL;
	TupleDesc tupleDescriptor = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	bool indexOK = true;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	char partitionMethod = DISTRIBUTE_BY_NONE;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_partmethod, BTEqualStrategyNumber,
				F_CHAREQ, CharGetDatum(partitionMethod));

	pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(pgDistPartition);
	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionPartitionMethodIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Oid referenceTableId = heap_getattr(heapTuple,
											Anum_pg_dist_partition_logicalrelid,
											tupleDescriptor, &isNull);

		referenceTableList = lappend_oid(referenceTableList, referenceTableId);
		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, AccessShareLock);

	return referenceTableList;
}