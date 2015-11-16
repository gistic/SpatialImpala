// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// This file contains all structs, enums, etc., that together make up
// a plan tree. All information recorded in struct TPlan and below is independent
// of the execution parameters of any one of the backends on which it is running
// (those are recorded in TPlanFragmentExecParams).

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "CatalogObjects.thrift"
include "ExecStats.thrift"
include "Exprs.thrift"
include "Types.thrift"
include "ExternalDataSource.thrift"

enum TPlanNodeType {
  HDFS_SCAN_NODE,
  HBASE_SCAN_NODE,
  HASH_JOIN_NODE,
  AGGREGATION_NODE,
  SORT_NODE,
  EMPTY_SET_NODE,
  EXCHANGE_NODE,
  UNION_NODE,
  SELECT_NODE,
  NESTED_LOOP_JOIN_NODE,
  DATA_SOURCE_NODE,
  ANALYTIC_EVAL_NODE,
  SINGULAR_ROW_SRC_NODE,
  UNNEST_NODE,
  SUBPLAN_NODE,
}

// phases of an execution node
enum TExecNodePhase {
  PREPARE,
  OPEN,
  GETNEXT,
  CLOSE,
  INVALID
}

// what to do when hitting a debug point (TImpalaQueryOptions.DEBUG_ACTION)
enum TDebugAction {
  WAIT,
  FAIL
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

// Specification of subsection of a single hdfs file.
struct THdfsFileSplit {
  // File name (not the full path).  The path is assumed to be the
  // 'location' of the THdfsPartition referenced by partition_id.
  1: required string file_name

  // starting offset
  2: required i64 offset

  // length of split
  3: required i64 length

  // ID of partition within the THdfsTable associated with this scan node.
  4: required i64 partition_id

  // total size of the hdfs file
  5: required i64 file_length

  // compression type of the hdfs file
  6: required CatalogObjects.THdfsCompression file_compression

  // last modified time of the file
  7: required i64 mtime
}

// key range for single THBaseScanNode
// TODO: does 'binary' have an advantage over string? strings can
// already store binary data
struct THBaseKeyRange {
  // inclusive
  1: optional string startKey

  // exclusive
  2: optional string stopKey
}

// Specification of an individual data range which is held in its entirety
// by a storage server
struct TScanRange {
  // one of these must be set for every TScanRange2
  1: optional THdfsFileSplit hdfs_file_split
  2: optional THBaseKeyRange hbase_key_range
}

struct THdfsScanNode {
  1: required Types.TTupleId tuple_id

  // Conjuncts that can be evaluated while materializing the items (tuples) of
  // collection-typed slots. Maps from item tuple id to the list of conjuncts
  // to be evaluated.
  2: optional map<Types.TTupleId, list<Exprs.TExpr>> collection_conjuncts
}

struct TDataSourceScanNode {
  1: required Types.TTupleId tuple_id

  // The external data source to scan
  2: required CatalogObjects.TDataSource data_source

  // Init string for the table passed to the data source. May be an empty string.
  3: required string init_string

  // Scan predicates in conjunctive normal form that were accepted by the data source.
  4: required list<list<ExternalDataSource.TBinaryPredicate>> accepted_predicates
}

struct THBaseFilter {
  1: required string family
  2: required string qualifier
  // Ordinal number into enum HBase CompareFilter.CompareOp.
  // We don't use TExprOperator because the op is interpreted by an HBase Filter, and
  // not the c++ expr eval.
  3: required i32 op_ordinal
  4: required string filter_constant
}

struct THBaseScanNode {
  1: required Types.TTupleId tuple_id

  // TODO: remove this, we already have THBaseTable.tableName
  2: required string table_name

  3: optional list<THBaseFilter> filters

  // Suggested max value for "hbase.client.scan.setCaching"
  4: optional i32 suggested_max_caching
}

struct TEqJoinCondition {
  // left-hand side of "<a> = <b>"
  1: required Exprs.TExpr left;
  // right-hand side of "<a> = <b>"
  2: required Exprs.TExpr right;
}

enum TJoinOp {
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  LEFT_ANTI_JOIN,

  // Similar to LEFT_ANTI_JOIN with special handling for NULLs for the join conjuncts
  // on the build side. Those NULLs are considered candidate matches, and therefore could
  // be rejected (ANTI-join), based on the other join conjuncts. This is in contrast
  // to LEFT_ANTI_JOIN where NULLs are not matches and therefore always returned.
  NULL_AWARE_LEFT_ANTI_JOIN,

  RIGHT_OUTER_JOIN,
  RIGHT_SEMI_JOIN,
  RIGHT_ANTI_JOIN,
  FULL_OUTER_JOIN,
  CROSS_JOIN
}

struct THashJoinNode {
  1: required TJoinOp join_op

  // equi-join predicates
  2: required list<TEqJoinCondition> eq_join_conjuncts

  // non equi-join predicates
  3: optional list<Exprs.TExpr> other_join_conjuncts

  // If true, this join node can (but may choose not to) generate slot filters
  // after constructing the build side that can be applied to the probe side.
  4: optional bool add_probe_filters
}

struct TNestedLoopJoinNode {
  1: required TJoinOp join_op

  // Join conjuncts (both equi-join and non equi-join). All other conjuncts that are
  // evaluated at the join node are stored in TPlanNode.conjuncts.
  2: optional list<Exprs.TExpr> join_conjuncts
}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  // aggregate exprs. The root of each expr is the aggregate function. The
  // other exprs are the inputs to the aggregate function.
  2: required list<Exprs.TExpr> aggregate_functions

  // Tuple id used for intermediate aggregations (with slots of agg intermediate types)
  3: required Types.TTupleId intermediate_tuple_id

  // Tupld id used for the aggregation output (with slots of agg output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // aggregate functions.
  4: required Types.TTupleId output_tuple_id

  // Set to true if this aggregation node needs to run the finalization step.
  5: required bool need_finalize
}

struct TSortInfo {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  3: required list<bool> nulls_first
  // Expressions evaluated over the input row that materialize the tuple to be sorted.
  // Contains one expr per slot in the materialized tuple.
  4: optional list<Exprs.TExpr> sort_tuple_slot_exprs
}

struct TSortNode {
  1: required TSortInfo sort_info
  // Indicates whether the backend service should use topn vs. sorting
  2: required bool use_top_n;
  // This is the number of rows to skip before returning results
  3: optional i64 offset
}

enum TAnalyticWindowType {
  // Specifies the window as a logical offset
  RANGE,

  // Specifies the window in physical units
  ROWS
}

enum TAnalyticWindowBoundaryType {
  // The window starts/ends at the current row.
  CURRENT_ROW,

  // The window starts/ends at an offset preceding current row.
  PRECEDING,

  // The window starts/ends at an offset following current row.
  FOLLOWING
}

struct TAnalyticWindowBoundary {
  1: required TAnalyticWindowBoundaryType type

  // Predicate that checks: child tuple '<=' buffered tuple + offset for the orderby expr
  2: optional Exprs.TExpr range_offset_predicate

  // Offset from the current row for ROWS windows.
  3: optional i64 rows_offset_value
}

struct TAnalyticWindow {
  // Specifies the window type for the start and end bounds.
  1: required TAnalyticWindowType type

  // Absence indicates window start is UNBOUNDED PRECEDING.
  2: optional TAnalyticWindowBoundary window_start

  // Absence indicates window end is UNBOUNDED FOLLOWING.
  3: optional TAnalyticWindowBoundary window_end
}

// Defines a group of one or more analytic functions that share the same window,
// partitioning expressions and order-by expressions and are evaluated by a single
// ExecNode.
struct TAnalyticNode {
  // Exprs on which the analytic function input is partitioned. Input is already sorted
  // on partitions and order by clauses, partition_exprs is used to identify partition
  // boundaries. Empty if no partition clause is specified.
  1: required list<Exprs.TExpr> partition_exprs

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries. Empty if no order-by clause is specified or for windows
  // specifying ROWS.
  2: required list<Exprs.TExpr> order_by_exprs

  // Functions evaluated over the window for each input row. The root of each expr is
  // the aggregate function. Child exprs are the inputs to the function.
  3: required list<Exprs.TExpr> analytic_functions

  // Window specification
  4: optional TAnalyticWindow window

  // Tuple used for intermediate results of analytic function evaluations
  // (with slots of analytic intermediate types)
  5: required Types.TTupleId intermediate_tuple_id

  // Tupld used for the analytic function output (with slots of analytic output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // analytic functions.
  6: required Types.TTupleId output_tuple_id

  // id of the buffered tuple (identical to the input tuple, which is assumed
  // to come from a single SortNode); not set if both partition_exprs and
  // order_by_exprs are empty
  7: optional Types.TTupleId buffered_tuple_id

  // predicate that checks: child tuple is in the same partition as the buffered tuple,
  // i.e. each partition expr is equal or both are not null. Only set if
  // buffered_tuple_id is set; should be evaluated over a row that is composed of the
  // child tuple and the buffered tuple
  8: optional Exprs.TExpr partition_by_eq

  // predicate that checks: the order_by_exprs are equal or both NULL when evaluated
  // over the child tuple and the buffered tuple. only set if buffered_tuple_id is set;
  // should be evaluated over a row that is composed of the child tuple and the buffered
  // tuple
  9: optional Exprs.TExpr order_by_eq
}

struct TUnionNode {
  // A UnionNode materializes all const/result exprs into this tuple.
  1: required Types.TTupleId tuple_id
  // List or expr lists materialized by this node.
  // There is one list of exprs per query stmt feeding into this union node.
  2: required list<list<Exprs.TExpr>> result_expr_lists
  // Separate list of expr lists coming from a constant select stmts.
  3: required list<list<Exprs.TExpr>> const_expr_lists
}

struct TExchangeNode {
  // The ExchangeNode's input rows form a prefix of the output rows it produces;
  // this describes the composition of that prefix
  1: required list<Types.TTupleId> input_row_tuples
  // For a merging exchange, the sort information.
  2: optional TSortInfo sort_info
  // This is the number of rows to skip before returning results
  3: optional i64 offset
}

struct TUnnestNode {
  // Expr that returns the in-memory collection to be scanned.
  // Currently always a SlotRef into an array-typed slot.
  1: required Exprs.TExpr collection_expr
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required Types.TPlanNodeId node_id
  2: required TPlanNodeType node_type
  3: required i32 num_children
  4: required i64 limit
  // Tuples in row produced by node. Must be non-empty.
  5: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  6: required list<bool> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts

  // one field per PlanNode subclass
  8: optional THdfsScanNode hdfs_scan_node
  9: optional THBaseScanNode hbase_scan_node
  10: optional TDataSourceScanNode data_source_node
  11: optional THashJoinNode hash_join_node
  12: optional TNestedLoopJoinNode nested_loop_join_node
  13: optional TAggregationNode agg_node
  14: optional TSortNode sort_node
  15: optional TUnionNode union_node
  16: optional TExchangeNode exchange_node
  17: optional TAnalyticNode analytic_node
  21: optional TUnnestNode unnest_node

  // Label that should be used to print this node to the user.
  18: optional string label

  // Additional details that should be printed to the user. This is node specific
  // e.g. table name, join strategy, etc.
  19: optional string label_detail

  // Estimated execution stats generated by the planner.
  20: optional ExecStats.TExecStats estimated_stats
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
