/*-------------------------------------------------------------------------
 *
 * merge_executor.c
 *
 * Executor logic for MERGE SQL statement.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/merge_executor.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_router_planner.h"
#include "distributed/repartition_executor.h"
#include "distributed/subplan_execution.h"

#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"


static int sourceResultPartitionColumnIndex(Query *mergeQuery,
											List *sourceTargetList,
											CitusTableCacheEntry *targetRelation);
static void ExecuteSourceAtWorkerAndRepartition(CitusScanState *scanState);
static void ExecuteSourceAtCoordAndRedistribution(CitusScanState *scanState);
static HTAB * ExecuteMergeSourcePlanIntoColocatedIntermediateResults(Oid targetRelationId,
																	 Query *mergeQuery,
																	 List *
																	 sourceTargetList,
																	 PlannedStmt *
																	 sourcePlan,
																	 EState *executorState,
																	 char *
																	 intermediateResultIdPrefix);


/*
 * NonPushableMergeCommandExecScan performs an MERGE INTO distributed_table
 * USING (source-query) ... command. This can be done either by aggregating
 * task results at the coordinator and repartitioning the results, or by
 * repartitioning task results and directly transferring data between nodes.
 */
TupleTableSlot *
NonPushableMergeCommandExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;

	if (!scanState->finishedRemoteScan)
	{
		switch (distributedPlan->modifyWithSelectMethod)
		{
			case MODIFY_WITH_SELECT_REPARTITION:
			{
				ExecuteSourceAtWorkerAndRepartition(scanState);
				break;
			}

			case MODIFY_WITH_SELECT_VIA_COORDINATOR:
			{
				ExecuteSourceAtCoordAndRedistribution(scanState);
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("Unexpected MERGE execution method(%d)",
									   distributedPlan->modifyWithSelectMethod)));
			}
		}

		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * ExecuteSourceAtWorkerAndRepartition Executes the Citus distributed plan, including any
 * sub-plans, and captures the results in intermediate files. Subsequently, redistributes
 * the result files to ensure colocation with the target, and directs the MERGE SQL
 * operation to the target shards on the worker nodes, utilizing the colocated
 * intermediate files as the data source.
 */
static void
ExecuteSourceAtWorkerAndRepartition(CitusScanState *scanState)
{
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Query *mergeQuery =
		copyObject(distributedPlan->modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *targetRte = ExtractResultRelationRTE(mergeQuery);
	RangeTblEntry *sourceRte = ExtractMergeSourceRangeTableEntry(mergeQuery);
	Oid targetRelationId = targetRte->relid;
	bool hasReturning = distributedPlan->expectResults;
	Query *sourceQuery = sourceRte->subquery;
	PlannedStmt *sourcePlan =
		copyObject(distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition);
	EState *executorState = ScanStateGetExecutorState(scanState);

	/*
	 * If we are dealing with partitioned table, we also need to lock its
	 * partitions. Here we only lock targetRelation, we acquire necessary
	 * locks on source tables during execution of those source queries.
	 */
	if (PartitionedTable(targetRelationId))
	{
		LockPartitionRelations(targetRelationId, RowExclusiveLock);
	}

	bool randomAccess = true;
	bool interTransactions = false;
	DistributedPlan *distSourcePlan =
		GetDistributedPlan((CustomScan *) sourcePlan->planTree);
	Job *distSourceJob = distSourcePlan->workerJob;
	List *distSourceTaskList = distSourceJob->taskList;
	bool binaryFormat =
		CanUseBinaryCopyFormatForTargetList(sourceQuery->targetList);

	ereport(DEBUG1, (errmsg("Executing subplans of the source query and "
							"storing the results at the respective node(s)")));

	ExecuteSubPlans(distSourcePlan);

	/*
	 * We have a separate directory for each transaction, so choosing
	 * the same result prefix won't cause filename conflicts. Results
	 * directory name also includes node id and database id, so we don't
	 * need to include them in the filename. We include job id here for
	 * the case "MERGE USING <source query>" is executed recursively.
	 */
	StringInfo distResultPrefixString = makeStringInfo();
	appendStringInfo(distResultPrefixString,
					 "repartitioned_results_" UINT64_FORMAT,
					 distSourceJob->jobId);
	char *distResultPrefix = distResultPrefixString->data;
	CitusTableCacheEntry *targetRelation = GetCitusTableCacheEntry(targetRelationId);

	ereport(DEBUG1, (errmsg("Redistributing source result rows across nodes")));

	/*
	 * partitionColumnIndex determines the column in the selectTaskList to
	 * use for (re)partitioning of the source result, which will colocate
	 * the result data with the target.
	 */
	int partitionColumnIndex =
		sourceResultPartitionColumnIndex(mergeQuery,
										 sourceQuery->targetList,
										 targetRelation);

	/*
	 * Below call partitions the results using shard ranges and partition method of
	 * targetRelation, and then colocates the result files with shards. These
	 * transfers are done by calls to fetch_intermediate_results() between nodes.
	 */
	List **redistributedResults =
		RedistributeTaskListResults(distResultPrefix,
									distSourceTaskList, partitionColumnIndex,
									targetRelation, binaryFormat);

	ereport(DEBUG1, (errmsg(
						 "Executing final MERGE on workers using intermediate results")));

	/*
	 * At this point source query has been executed on workers and results
	 * have been fetched in such a way that they are colocated with corresponding
	 * target shard(s). Create and execute a list of tasks of form
	 * MERGE  INTO ... USING SELECT * FROM read_intermediate_results(...);
	 */
	List *taskList =
		GenerateTaskListWithRedistributedResults(mergeQuery,
												 targetRelation,
												 redistributedResults,
												 binaryFormat);

	scanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	TupleDestination *tupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate,
								  tupleDescriptor);
	uint64 rowsMerged =
		ExecuteTaskListIntoTupleDest(ROW_MODIFY_COMMUTATIVE, taskList,
									 tupleDest,
									 hasReturning);
	executorState->es_processed = rowsMerged;
}


/*
 * ExecuteSourceAtCoordAndRedistribution Executes the plan that necessitates evaluation
 * at the coordinator and redistributes the resulting rows to intermediate files,
 * ensuring colocation with the target shards. Directs the MERGE SQL operation to the
 * target shards on the worker nodes, utilizing the colocated intermediate files as the
 * data source.
 */
void
ExecuteSourceAtCoordAndRedistribution(CitusScanState *scanState)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Query *mergeQuery =
		copyObject(distributedPlan->modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *targetRte = ExtractResultRelationRTE(mergeQuery);
	RangeTblEntry *sourceRte = ExtractMergeSourceRangeTableEntry(mergeQuery);
	Query *sourceQuery = sourceRte->subquery;
	Oid targetRelationId = targetRte->relid;
	PlannedStmt *sourcePlan =
		copyObject(distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition);
	char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
	bool hasReturning = distributedPlan->expectResults;

	/*
	 * If we are dealing with partitioned table, we also need to lock its
	 * partitions. Here we only lock targetRelation, we acquire necessary
	 * locks on source tables during execution of those source queries.
	 */
	if (PartitionedTable(targetRelationId))
	{
		LockPartitionRelations(targetRelationId, RowExclusiveLock);
	}

	ereport(DEBUG1, (errmsg("Collect source query results on coordinator")));

	List *prunedTaskList = NIL;
	HTAB *shardStateHash =
		ExecuteMergeSourcePlanIntoColocatedIntermediateResults(
			targetRelationId,
			mergeQuery,
			sourceQuery->targetList,     /*TODO */
			sourcePlan,
			executorState,
			intermediateResultIdPrefix);

	ereport(DEBUG1, (errmsg("Create a MERGE task list that needs to be routed")));

	/* generate tasks for the .. phase */
	List *taskList =
		GenerateTaskListWithColocatedIntermediateResults(targetRelationId, mergeQuery,
														 intermediateResultIdPrefix);

	/*
	 * We cannot actually execute MERGE INTO ... tasks that read from
	 * intermediate results that weren't created because no rows were
	 * written to them. Prune those tasks out by only including tasks
	 * on shards with connections.
	 */
	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		uint64 shardId = task->anchorShardId;
		bool shardModified = false;

		hash_search(shardStateHash, &shardId, HASH_FIND, &shardModified);
		if (shardModified)
		{
			prunedTaskList = lappend(prunedTaskList, task);
		}
	}

	if (prunedTaskList == NIL)
	{
		/* No task to execute */
		return;
	}

	ereport(DEBUG1, (errmsg("Execute MERGE task list")));
	bool randomAccess = true;
	bool interTransactions = false;
	Assert(scanState->tuplestorestate == NULL);
	scanState->tuplestorestate = tuplestore_begin_heap(randomAccess, interTransactions,
													   work_mem);
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	TupleDestination *tupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate, tupleDescriptor);
	uint64 rowsMerged =
		ExecuteTaskListIntoTupleDest(ROW_MODIFY_COMMUTATIVE, prunedTaskList,
									 tupleDest, hasReturning);
	executorState->es_processed = rowsMerged;
}


/*
 * sourceResultPartitionColumnIndex collects all Join conditions from the
 * ON clause and verifies if there is a join, either left or right, with
 * the distribution column of the given target. Once a match is found, it
 * returns the index of that match in the source's target list.
 */
static int
sourceResultPartitionColumnIndex(Query *mergeQuery, List *sourceTargetList,
								 CitusTableCacheEntry *targetRelation)
{
	/* Get all the equal(=) expressions from the ON clause */
	List *mergeJoinConditionList = QualifierList(mergeQuery->jointree);

	/*
	 * Now that we have gathered all the _equal_ expressions in the list, we proceed
	 * to match the one that joins with the distribution column of the target.
	 */
	Var *targetColumn = targetRelation->partitionColumn;
	List *sourceJoinColumns = NIL;
	Node *joinClause = NULL;
	foreach_ptr(joinClause, mergeJoinConditionList)
	{
		Node *leftOperand;
		Node *rightOperand;

		if (!BinaryOpExpression((Expr *) joinClause, &leftOperand, &rightOperand))
		{
			continue;
		}

		if (equal(leftOperand, targetColumn) && IsA(rightOperand, Var))
		{
			sourceJoinColumns = lappend(sourceJoinColumns, rightOperand);
		}
		else if (equal(rightOperand, targetColumn) && IsA(leftOperand, Var))
		{
			sourceJoinColumns = lappend(sourceJoinColumns, leftOperand);
		}
	}

	int partitionColumnIndex = -1;
	if (sourceJoinColumns)
	{
		/* Use the Var that's being used in INSERT clause */
		Var *sourceJoinColumn =
			ErrorIfInsertNotMatchTargetDistributionColumn(
				targetRelation->relationId, mergeQuery,
				sourceJoinColumns);

		/* If no INSERT action is present, we can use one of the Var */
		if (!sourceJoinColumn)
		{
			sourceJoinColumn = linitial(sourceJoinColumns);
		}

		/*
		 * Find the index in the source list that will be utilized
		 * for colocation-redistribution.
		 */
		partitionColumnIndex =
			DistributionColumnIndex(sourceTargetList, sourceJoinColumn);
	}

	if (partitionColumnIndex == -1)
	{
		ereport(ERROR, (errmsg("Missing required join condition between "
							   "source and target's distribution column"),
						errhint("Without a join condition on the target's "
								"distribution column, the source rows "
								"cannot be efficiently redistributed, and "
								"the NOT-MATCHED condition cannot be evaluated "
								"unambiguously. This can result in incorrect or "
								"unexpected results when attempting to merge "
								"tables in a distributed setting")));
	}
	else
	{
		ereport(DEBUG1, (errmsg("Using column - index:%d from the source list "
								"to redistribute", partitionColumnIndex)));
	}

	return partitionColumnIndex;
}


/*
 * ExecuteMergeSourcePlanIntoColocatedIntermediateResults Executes the given PlannedStmt
 * and inserts tuples into a set of intermediate results that are colocated with the
 * target table for further processing MERGE INTO. It also returns the hash of shard
 * states that were used to insert tuplesinto the target relation.
 */
static HTAB *
ExecuteMergeSourcePlanIntoColocatedIntermediateResults(Oid targetRelationId,
													   Query *mergeQuery,
													   List *sourceTargetList,
													   PlannedStmt *sourcePlan,
													   EState *executorState,
													   char *intermediateResultIdPrefix)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	CitusTableCacheEntry *targetRelation = GetCitusTableCacheEntry(targetRelationId);

	/* Get column name list and partition column index for the target table */
	List *columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
															 sourceTargetList);
	int partitionColumnIndex = sourceResultPartitionColumnIndex(mergeQuery,
																sourceTargetList,
																targetRelation);

	/* set up a DestReceiver that copies into the intermediate file */
	const bool publishableData = false;
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  intermediateResultIdPrefix,
																  publishableData);

	/* We can skip when wiring intermediate files */
	copyDest->skipCoercions = true;

	ExecutePlanIntoDestReceiver(sourcePlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;
	XactModificationLevel = XACT_MODIFICATION_DATA;

	return copyDest->shardStateHash;
}
