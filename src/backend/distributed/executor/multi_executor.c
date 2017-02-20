/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "executor/executor.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"


/*
 * FIXME: It'd probably be better to have different set of methods for:
 * - router readonly queries
 * - router modify
 * - router insert ... select
 * - real-time/task-tracker (no point in seperating those)
 *
 * I think it's better however to only have one type of CitusScanState, to
 * allow to easily share code between routines.
 */
static CustomExecMethods CitusCustomExecMethods = {
	"CitusScan",
	CitusBeginScan,
	CitusExecScan,
	CitusEndScan,
	CitusReScan,
#if (PG_VERSION_NUM >= 90600)
	NULL, /* NO EstimateDSMCustomScan callback */
	NULL, /* NO InitializeDSMCustomScan callback */
	NULL, /* NO InitializeWorkerCustomScan callback */
#endif
	NULL,
	NULL,
	CitusExplainScan
};


static CustomExecMethods RouterCustomExecMethods = {
	"CitusScan",
	RouterBeginScan,
	RouterExecScan,
	CitusEndScan,
	CitusReScan,
#if (PG_VERSION_NUM >= 90600)
	NULL, /* NO EstimateDSMCustomScan callback */
	NULL, /* NO InitializeDSMCustomScan callback */
	NULL, /* NO InitializeWorkerCustomScan callback */
#endif
	NULL,
	NULL,
	CitusExplainScan
};


Node *
CitusCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);
	scanState->executorType = JobExecutorType(scanState->multiPlan);

	if (scanState->executorType == MULTI_EXECUTOR_ROUTER)
	{
		scanState->customScanState.methods = &RouterCustomExecMethods;
	}
	else
	{
		scanState->customScanState.methods = &CitusCustomExecMethods;
	}

	return (Node *) scanState;
}


void
CitusBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	VerifyCitusScanState(node);
}


void
VerifyCitusScanState(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	MultiPlan *multiPlan = scanState->multiPlan;

	Assert(IsA(scanState, CustomScanState));

	/* ensure plan is executable */
	VerifyMultiPlanValidity(multiPlan);
}


TupleTableSlot *
CitusExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	MultiPlan *multiPlan = scanState->multiPlan;

	TupleTableSlot *resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;

	if (!scanState->finishedUnderlyingScan)
	{
		Job *workerJob = multiPlan->workerJob;
		StringInfo jobDirectoryName = NULL;
		EState *executorState = scanState->customScanState.ss.ps.state;
		List *workerTaskList = workerJob->taskList;
		ListCell *workerTaskCell = NULL;
		TupleDesc tupleDescriptor = NULL;
		Relation fakeRel = NULL;
		MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
		ExprContext *executorExpressionContext =
			GetPerTupleExprContext(executorState);
		uint32 columnCount = 0;
		Datum *columnValues = NULL;
		bool *columnNulls = NULL;

		/*
		 * We create a directory on the master node to keep task execution results.
		 * We also register this directory for automatic cleanup on portal delete.
		 */
		jobDirectoryName = MasterJobDirectoryName(workerJob->jobId);
		CreateDirectory(jobDirectoryName);

		ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
		ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);

		/* pick distributed executor to use */
		if (executorState->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY)
		{
			/* skip distributed query execution for EXPLAIN commands */
		}
		else if (scanState->executorType == MULTI_EXECUTOR_REAL_TIME)
		{
			MultiRealTimeExecute(workerJob);
		}
		else if (scanState->executorType == MULTI_EXECUTOR_TASK_TRACKER)
		{
			MultiTaskTrackerExecute(workerJob);
		}

		tupleDescriptor = node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

		/*
		 * Load data, collected by Multi*Execute() above, into a
		 * tuplestore. For that first create a tuplestore, and then copy
		 * the files one-by-one.
		 *
		 * FIXME: Should probably be in a separate routine.
		 *
		 * Long term it'd be a lot better if Multi*Execute() directly
		 * filled the tuplestores, but that's a fair bit of work.
		 */

		/*
		 * To be able to use copy.c, we need a Relation descriptor.  As
		 * there's no relation corresponding to the data loaded from
		 * workers, fake one.  We just need the bare minimal set of fields
		 * accessed by BeginCopyFrom().
		 *
		 * FIXME: should be abstracted into a separate function.
		 */
		fakeRel = palloc0(sizeof(RelationData));
		fakeRel->rd_att = tupleDescriptor;
		fakeRel->rd_rel = palloc0(sizeof(FormData_pg_class));
		fakeRel->rd_rel->relkind = RELKIND_RELATION;

		columnCount = tupleDescriptor->natts;
		columnValues = palloc0(columnCount * sizeof(Datum));
		columnNulls = palloc0(columnCount * sizeof(bool));

		Assert(scanState->tuplestorestate == NULL);
		scanState->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);

		foreach(workerTaskCell, workerTaskList)
		{
			Task *workerTask = (Task *) lfirst(workerTaskCell);
			StringInfo jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
			StringInfo taskFilename =
				TaskFilename(jobDirectoryName, workerTask->taskId);
			List *copyOptions = NIL;
			CopyState copyState = NULL;

			if (BinaryMasterCopyFormat)
			{
				DefElem *copyOption = makeDefElem("format",
												  (Node *) makeString("binary"));
				copyOptions = lappend(copyOptions, copyOption);
			}
			copyState = BeginCopyFrom(fakeRel, taskFilename->data, false, NULL,
									  copyOptions);

			while (true)
			{
				MemoryContext oldContext = NULL;
				bool nextRowFound = false;

				ResetPerTupleExprContext(executorState);
				oldContext = MemoryContextSwitchTo(executorTupleContext);

				nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
											columnValues, columnNulls, NULL);
				if (!nextRowFound)
				{
					MemoryContextSwitchTo(oldContext);
					break;
				}

				tuplestore_putvalues(scanState->tuplestorestate,
									 tupleDescriptor,
									 columnValues, columnNulls);
				MemoryContextSwitchTo(oldContext);
			}
		}

		scanState->finishedUnderlyingScan = true;
	}

	if (scanState->tuplestorestate != NULL)
	{
		Tuplestorestate *tupleStore = scanState->tuplestorestate;
		tuplestore_gettupleslot(tupleStore, true, false, resultSlot);

		return resultSlot;
	}

	return NULL;
}


void
CitusEndScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
}


void
CitusReScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	scanState->tuplestorestate = NULL;
	scanState->finishedUnderlyingScan = true;

	/*
	 * XXX: this probably already works, but if not should be easily
	 * supportable - probably hard to exercise right now though.
	 */
	elog(WARNING, "unsupported at this point");
}
