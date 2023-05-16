/*-------------------------------------------------------------------------
 *
 * merge_planner.c
 *
 * This file contains functions to help plan MERGE queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <stddef.h>

#include "postgres.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "distributed/citus_clauses.h"
#include "distributed/listutils.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_node_metadata.h"
#include "distributed/pg_version_constants.h"
#include "distributed/query_pushdown_planning.h"

#if PG_VERSION_NUM >= PG_VERSION_15

static DeferredErrorMessage * ErrorIfMergeHasUnsupportedTables(Oid targetRelationId,
															   Query *parse,
															   List *rangeTableList,
															   PlannerRestrictionContext *
															   restrictionContext);
static bool IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
											  skipOuterVars);
static DeferredErrorMessage * MergeQualAndTargetListFunctionsSupported(Oid
																	   resultRelationId,
																	   FromExpr *joinTree,
																	   Node *quals,
																	   List *targetList,
																	   CmdType commandType);


/*
 * ErrorIfMergeHasUnsupportedTables checks if all the tables(target, source or any CTE
 * present) in the MERGE command are local i.e. a combination of Citus local and Non-Citus
 * tables (regular Postgres tables), or distributed tables with some restrictions, please
 * see header of routine ErrorIfDistTablesNotColocated for details, raises an exception
 * for all other combinations.
 */
static DeferredErrorMessage *
ErrorIfMergeHasUnsupportedTables(Oid targetRelationId, Query *parse, List *rangeTableList,
								 PlannerRestrictionContext *restrictionContext)
{
	List *distTablesList = NIL;
	bool foundLocalTables = false;
	bool foundReferenceTables = false;

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		Oid relationId = rangeTableEntry->relid;

		switch (rangeTableEntry->rtekind)
		{
			case RTE_RELATION:
			{
				/* Check the relation type */
				break;
			}

			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_JOIN:
			case RTE_CTE:
			{
				/* Skip them as base table(s) will be checked */
				continue;
			}

			/*
			 * RTE_NAMEDTUPLESTORE is typically used in ephmeral named relations,
			 * such as, trigger data; until we find a genuine use case, raise an
			 * exception.
			 * RTE_RESULT is a node added by the planner and we shouldn't
			 * encounter it in the parse tree.
			 */
			case RTE_NAMEDTUPLESTORE:
			case RTE_RESULT:
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "MERGE command is not supported with "
									 "Tuplestores and results",
									 NULL, NULL);
			}

			default:
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "MERGE command: Unrecognized range table entry.",
									 NULL, NULL);
			}
		}

		/* RTE Relation can be of various types, check them now */
		switch (rangeTableEntry->relkind)
		{
			/* skip the regular views as they are replaced with subqueries */
			case RELKIND_VIEW:
			{
				continue;
			}

			case RELKIND_MATVIEW:
			case RELKIND_FOREIGN_TABLE:
			{
				/* These two cases as a target is not allowed */
				if (relationId == targetRelationId)
				{
					/* Usually we don't reach this exception as the Postgres parser catches it */
					StringInfo errorMessage = makeStringInfo();
					appendStringInfo(errorMessage, "MERGE command is not allowed on "
												   "relation type(relkind:%c)",
									 rangeTableEntry->relkind);
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL, NULL);
				}
				break;
			}

			case RELKIND_RELATION:
			case RELKIND_PARTITIONED_TABLE:
			{
				/* Check for citus/postgres table types */
				Assert(OidIsValid(relationId));
				break;
			}

			default:
			{
				StringInfo errorMessage = makeStringInfo();
				appendStringInfo(errorMessage, "Unexpected table type(relkind:%c) "
											   "in MERGE command",
								 rangeTableEntry->relkind);
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 errorMessage->data, NULL, NULL);
			}
		}

		/*
		 * For now, save all distributed tables, later (below) we will
		 * check for supported combination(s).
		 */
		if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			/* Append/Range distributed tables are not supported */
			if (IsCitusTableType(relationId, APPEND_DISTRIBUTED) ||
				IsCitusTableType(relationId, RANGE_DISTRIBUTED))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "For MERGE command, all the distributed tables "
									 "must be colocated, for append/range distribution, "
									 "colocation is not supported", NULL,
									 "Consider using hash distribution instead");
			}

			distTablesList = lappend(distTablesList, rangeTableEntry);
		}
		else if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			/* Reference table as a target is not allowed */
			if (relationId == targetRelationId)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Reference table as target "
									 "is not allowed in "
									 "MERGE command", NULL, NULL);
			}

			foundReferenceTables = true;
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			/* Citus local tables */
			foundLocalTables = true;
		}
		else if (!IsCitusTable(relationId))
		{
			/* Regular Postgres table */
			foundLocalTables = true;
		}

		/* Any other Citus table type missing ? */
	}

	/* Ensure all tables are indeed local (or a combination of reference and local) */
	if (list_length(distTablesList) == 0)
	{
		/*
		 * All the tables are local/reference, supported as long as
		 * coordinator is in the metadata.
		 */
		if (FindCoordinatorNodeId() == -1)
		{
			elog(ERROR, "Coordinator node is not in the metadata. TODO better meesage");
		}

		/* All the tables are local/reference, supported */
		return NULL;
	}

	if (foundLocalTables)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported with "
							 "combination of distributed/local tables yet",
							 NULL, NULL);
	}

	if (foundReferenceTables)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported with "
							 "combination of distributed/reference yet",
							 NULL,
							 "If target is distributed, source "
							 "must be distributed and co-located");
	}


	/* Ensure all distributed tables are indeed co-located */
	return ErrorIfDistTablesNotColocated(parse,
										 distTablesList,
										 restrictionContext);
}


/*
 * IsPartitionColumnInMerge returns true if the given column is a partition column.
 * The function uses FindReferencedTableColumn to find the original relation
 * id and column that the column expression refers to. It then checks whether
 * that column is a partition column of the relation.
 *
 * Also, the function returns always false for reference tables given that
 * reference tables do not have partition column.
 *
 * If skipOuterVars is true, then it doesn't process the outervars.
 */
bool
IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
								  skipOuterVars)
{
	bool isDistributionColumn = false;
	Var *column = NULL;
	RangeTblEntry *relationRTE = NULL;

	/* ParentQueryList is same as the original query for MERGE */
	FindReferencedTableColumn(columnExpression, list_make1(query), query, &column,
							  &relationRTE,
							  skipOuterVars);
	Oid relationId = relationRTE ? relationRTE->relid : InvalidOid;
	if (relationId != InvalidOid && column != NULL)
	{
		Var *distributionColumn = DistPartitionKey(relationId);

		/* not all distributed tables have partition column */
		if (distributionColumn != NULL && column->varattno ==
			distributionColumn->varattno)
		{
			isDistributionColumn = true;
		}
	}

	return isDistributionColumn;
}


/*
 * MergeQualAndTargetListFunctionsSupported Checks WHEN/ON clause actions to see what functions
 * are allowed, if we are updating distribution column, etc.
 */
static DeferredErrorMessage *
MergeQualAndTargetListFunctionsSupported(Oid resultRelationId, FromExpr *joinTree,
										 Node *quals,
										 List *targetList, CmdType commandType)
{
	uint32 rangeTableId = 1;
	Var *distributionColumn = NULL;
	if (IsCitusTable(resultRelationId) && HasDistributionKey(resultRelationId))
	{
		distributionColumn = PartitionColumn(resultRelationId, rangeTableId);
	}

	ListCell *targetEntryCell = NULL;
	bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
	bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		bool targetEntryDistributionColumn = false;
		AttrNumber targetColumnAttrNumber = InvalidAttrNumber;

		if (distributionColumn)
		{
			if (commandType == CMD_UPDATE)
			{
				/*
				 * Note that it is not possible to give an alias to
				 * UPDATE table SET ...
				 */
				if (targetEntry->resname)
				{
					targetColumnAttrNumber = get_attnum(resultRelationId,
														targetEntry->resname);
					if (targetColumnAttrNumber == distributionColumn->varattno)
					{
						targetEntryDistributionColumn = true;
					}
				}
			}
		}

		if (targetEntryDistributionColumn &&
			TargetEntryChangesValue(targetEntry, distributionColumn, joinTree))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "updating the distribution column is not "
								 "allowed in MERGE actions",
								 NULL, NULL);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  CitusIsVolatileFunction))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "functions used in MERGE actions on distributed "
								 "tables must not be VOLATILE",
								 NULL, NULL);
		}

		if (MasterIrreducibleExpression((Node *) targetEntry->expr,
										&hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  NodeIsFieldStore))
		{
			/* DELETE cannot do field indirection already */
			Assert(commandType == CMD_UPDATE || commandType == CMD_INSERT);
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "inserting or modifying composite type fields is not "
								 "supported", NULL,
								 "Use the column name to insert or update the composite "
								 "type as a single value");
		}
	}


	/*
	 * Check the condition, convert list of expressions into expression tree for further processing
	 */
	if (quals)
	{
		if (IsA(quals, List))
		{
			quals = (Node *) make_ands_explicit((List *) quals);
		}

		if (FindNodeMatchingCheckFunction((Node *) quals, CitusIsVolatileFunction))
		{
			StringInfo errorMessage = makeStringInfo();
			appendStringInfo(errorMessage, "functions used in the %s clause of MERGE "
										   "queries on distributed tables must not be VOLATILE",
							 (commandType == CMD_MERGE) ? "ON" : "WHEN");
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 errorMessage->data, NULL, NULL);
		}
		else if (MasterIrreducibleExpression(quals, &hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}
	}

	if (hasVarArgument)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "STABLE functions used in MERGE queries "
							 "cannot be called with column references",
							 NULL, NULL);
	}

	if (hasBadCoalesce)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "non-IMMUTABLE functions are not allowed in CASE or "
							 "COALESCE statements",
							 NULL, NULL);
	}

	if (quals != NULL && nodeTag(quals) == T_CurrentOfExpr)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot run MERGE actions with cursors",
							 NULL, NULL);
	}

	return NULL;
}


#endif


/*
 * CreateMergePlan attempts to create a plan for the given MERGE SQL
 * statement. If planning fails ->planningError is set to a description
 * of the failure.
 */
DistributedPlan *
CreateMergePlan(Query *originalQuery, Query *query,
				PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	bool multiShardQuery = false;
	Oid targetRelationId = ModifyQueryResultRelationId(originalQuery);

	Assert(originalQuery->commandType == CMD_MERGE);
	Assert(OidIsValid(targetRelationId));

	distributedPlan->targetRelationId = targetRelationId;
	distributedPlan->modLevel = RowModifyLevelForQuery(query);
	distributedPlan->planningError = MergeQuerySupported(targetRelationId,
														 originalQuery,
														 multiShardQuery,
														 plannerRestrictionContext);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	Job *job = RouterJob(originalQuery, plannerRestrictionContext,
						 &distributedPlan->planningError);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	ereport(DEBUG1, (errmsg("Creating MERGE router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->combineQuery = NULL;

	/* MERGE doesn't support RETURNING clause */
	distributedPlan->expectResults = false;
	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	return distributedPlan;
}


/*
 * IsLocalTableModification returns true if the table modified is a Postgres table.
 * We do not support recursive planning for MERGE yet, so we could have a join
 * between local and Citus tables. Only allow local tables when it is the target table.
 */
bool
IsLocalTableModification(Oid targetRelationId, Query *query, uint64 shardId,
						 RTEListProperties *rteProperties)
{
	/* No-op for SELECT command */
	if (!IsModifyCommand(query))
	{
		return false;
	}

	/* For MERGE, we have to check only the target relation */
	if (IsMergeQuery(query) && !IsCitusTable(targetRelationId))
	{
		/* Postgres table */
		return true;
	}

	if (shardId == INVALID_SHARD_ID && ContainsOnlyLocalTables(rteProperties))
	{
		return true;
	}

	return false;
}
