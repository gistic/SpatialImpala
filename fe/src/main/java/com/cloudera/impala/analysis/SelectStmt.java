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

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ColumnAliasGenerator;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TableAliasGenerator;
import com.cloudera.impala.common.TreeNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  protected SelectList selectList_;
  protected final ArrayList<String> colLabels_; // lower case column labels
  protected final List<TableRef> tableRefs_;
  protected Expr whereClause_;
  protected ArrayList<Expr> groupingExprs_;
  protected final Expr havingClause_;  // original having clause

  // havingClause with aliases and agg output resolved
  private Expr havingPred_;

  // set if we have any kind of aggregation operation, include SELECT DISTINCT
  private AggregateInfo aggInfo_;

  // set if we have AnalyticExprs in the select list/order by clause
  private AnalyticInfo analyticInfo_;

  // SQL string of this SelectStmt before inline-view expression substitution.
  // Set in analyze().
  protected String sqlString_;

  // substitutes all exprs in this select block to reference base tables
  // directly
  private ExprSubstitutionMap baseTblSmap_ = new ExprSubstitutionMap();

  public SelectStmt(SelectList selectList,
             List<TableRef> tableRefList,
             Expr wherePredicate, ArrayList<Expr> groupingExprs,
             Expr havingPredicate, ArrayList<OrderByElement> orderByElements,
             LimitElement limitElement) {
    super(orderByElements, limitElement);
    this.selectList_ = selectList;
    if (tableRefList == null) {
      this.tableRefs_ = Lists.newArrayList();
    } else {
      this.tableRefs_ = tableRefList;
    }
    this.whereClause_ = wherePredicate;
    this.groupingExprs_ = groupingExprs;
    this.havingClause_ = havingPredicate;
    this.colLabels_ = Lists.newArrayList();
    this.havingPred_ = null;
    this.aggInfo_ = null;
    this.sortInfo_ = null;
    // Set left table refs to ensure correct toSql() before analysis.
    for (int i = 1; i < tableRefs_.size(); ++i) {
      tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
    }
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() { return selectList_; }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() { return havingPred_; }

  public List<TableRef> getTableRefs() { return tableRefs_; }
  public boolean hasWhereClause() { return whereClause_ != null; }
  public boolean hasGroupByClause() { return groupingExprs_ != null; }
  public Expr getWhereClause() { return whereClause_; }
  public void setWhereClause(Expr whereClause) { whereClause_ = whereClause; }
  public AggregateInfo getAggInfo() { return aggInfo_; }
  public boolean hasAggInfo() { return aggInfo_ != null; }
  public AnalyticInfo getAnalyticInfo() { return analyticInfo_; }
  public boolean hasAnalyticInfo() { return analyticInfo_ != null; }
  @Override
  public ArrayList<String> getColLabels() { return colLabels_; }
  public ExprSubstitutionMap getBaseTblSmap() { return baseTblSmap_; }

  // Column alias generator used during query rewriting.
  private ColumnAliasGenerator columnAliasGenerator_ = null;
  public ColumnAliasGenerator getColumnAliasGenerator() {
    if (columnAliasGenerator_ == null) {
      columnAliasGenerator_ = new ColumnAliasGenerator(colLabels_, null);
    }
    return columnAliasGenerator_;
  }

  // Table alias generator used during query rewriting.
  private TableAliasGenerator tableAliasGenerator_ = null;
  public TableAliasGenerator getTableAliasGenerator() {
    if (tableAliasGenerator_ == null) {
      tableAliasGenerator_ = new TableAliasGenerator(analyzer_, null);
    }
    return tableAliasGenerator_;
  }

  /**
   * Creates resultExprs and baseTblResultExprs.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    // Start out with table refs to establish aliases.
    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (int i = 0; i < tableRefs_.size(); ++i) {
      // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
      TableRef tblRef = tableRefs_.get(i);
      tblRef = analyzer.resolveTableRef(tblRef);
      Preconditions.checkNotNull(tblRef);
      tableRefs_.set(i, tblRef);
      tblRef.setLeftTblRef(leftTblRef);
      try {
        tblRef.analyze(analyzer);
      } catch (AnalysisException e) {
        // Only re-throw the exception if no tables are missing.
        if (analyzer.getMissingTbls().isEmpty()) throw e;
      }
      leftTblRef = tblRef;
    }

    // All tableRefs have been analyzed, but at least one table was found missing.
    // There is no reason to proceed with analysis past this point.
    if (!analyzer.getMissingTbls().isEmpty()) {
      throw new AnalysisException("Found missing tables. Aborting analysis.");
    }

    // analyze plan hints from select list
    selectList_.analyzePlanHints(analyzer);

    // populate resultExprs_, aliasSmap_, and colLabels_
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      SelectListItem item = selectList_.getItems().get(i);
      if (item.isStar()) {
        TableName tblName = item.getTblName();
        if (tblName == null) {
          expandStar(analyzer);
        } else {
          expandStar(analyzer, tblName);
        }
      } else {
        // Analyze the resultExpr before generating a label to ensure enforcement
        // of expr child and depth limits (toColumn() label may call toSql()).
        item.getExpr().analyze(analyzer);
        if (item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
          throw new AnalysisException(
              "Subqueries are not supported in the select list.");
        }
        resultExprs_.add(item.getExpr());
        String label = item.toColumnLabel(i, analyzer.useHiveColLabels());
        SlotRef aliasRef = new SlotRef(null, label);
        Expr existingAliasExpr = aliasSmap_.get(aliasRef);
        if (existingAliasExpr != null && !existingAliasExpr.equals(item.getExpr())) {
          // If we have already seen this alias, it refers to more than one column and
          // therefore is ambiguous.
          ambiguousAliasList_.add(aliasRef);
        }
        aliasSmap_.put(aliasRef, item.getExpr().clone());
        colLabels_.add(label);
      }
    }
    // The root stmt may not return a complex-typed value directly because we'd need to
    // serialize it in a meaningful way. We allow complex types in the select list for
    // non-root stmts to support views.
    for (Expr expr: resultExprs_) {
      if (expr.getType().isComplexType() && analyzer.isRootAnalyzer()) {
        throw new AnalysisException(String.format(
            "Expr '%s' in select list of root statement returns a complex type '%s'.\n" +
            "Only scalar types are allowed in the select list of the root statement.",
            expr.toSql(), expr.getType().toSql()));
      }
    }

    if (TreeNode.contains(resultExprs_, AnalyticExpr.class)) {
      if (tableRefs_.isEmpty()) {
        throw new AnalysisException("Analytic expressions require FROM clause.");
      }

      // do this here, not after analyzeAggregation(), otherwise the AnalyticExprs
      // will get substituted away
      if (selectList_.isDistinct()) {
        throw new AnalysisException(
            "cannot combine SELECT DISTINCT with analytic functions");
      }
    }

    if (whereClause_ != null) {
      whereClause_.analyze(analyzer);
      if (whereClause_.contains(Expr.isAggregatePredicate())) {
        throw new AnalysisException(
            "aggregate function not allowed in WHERE clause");
      }
      whereClause_.checkReturnsBool("WHERE clause", false);
      Expr e = whereClause_.findFirstOf(AnalyticExpr.class);
      if (e != null) {
        throw new AnalysisException(
            "WHERE clause must not contain analytic expressions: " + e.toSql());
      }
      analyzer.registerConjuncts(whereClause_, false);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);
    analyzeAnalytics(analyzer);
    if (evaluateOrderBy_) createSortTupleInfo(analyzer);

    // Remember the SQL string before inline-view expression substitution.
    sqlString_ = toSql();
    resolveInlineViewRefs(analyzer);

    // If this block's select-project-join portion returns an empty result set and the
    // block has no aggregation, then mark this block as returning an empty result set.
    if (analyzer.hasEmptySpjResultSet() && aggInfo_ == null) {
      analyzer.setHasEmptyResultSet();
    }

    if (aggInfo_ != null) LOG.debug("post-analysis " + aggInfo_.debugString());
  }

  /**
   * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
    // Mark unassigned join predicates. Some predicates that must be evaluated by a join
    // can also be safely evaluated below the join (picked up by getBoundPredicates()).
    // Such predicates will be marked twice and that is ok.
    List<Expr> unassigned =
        analyzer.getUnassignedConjuncts(getTableRefIds(), true);
    List<Expr> unassignedJoinConjuncts = Lists.newArrayList();
    for (Expr e: unassigned) {
      if (analyzer.evalByJoin(e)) unassignedJoinConjuncts.add(e);
    }
    List<Expr> baseTblJoinConjuncts =
        Expr.substituteList(unassignedJoinConjuncts, baseTblSmap_, analyzer);
    materializeSlots(analyzer, baseTblJoinConjuncts);

    if (evaluateOrderBy_) {
      // mark ordering exprs before marking agg/analytic exprs because they could contain
      // agg/analytic exprs that are not referenced anywhere but the ORDER BY clause
      sortInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (hasAnalyticInfo()) {
      // Mark analytic exprs before marking agg exprs because they could contain agg
      // exprs that are not referenced anywhere but the analytic expr.
      // Gather unassigned predicates and mark their slots. It is not desirable
      // to account for propagated predicates because if an analytic expr is only
      // referenced by a propagated predicate, then it's better to not materialize the
      // analytic expr at all.
      ArrayList<TupleId> tids = Lists.newArrayList();
      getMaterializedTupleIds(tids); // includes the analytic tuple
      List<Expr> conjuncts = analyzer.getUnassignedConjuncts(tids, false);
      materializeSlots(analyzer, conjuncts);
      analyticInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (aggInfo_ != null) {
      // mark all agg exprs needed for HAVING pred and binding predicates as materialized
      // before calling AggregateInfo.materializeRequiredSlots(), otherwise they won't
      // show up in AggregateInfo.getMaterializedAggregateExprs()
      ArrayList<Expr> havingConjuncts = Lists.newArrayList();
      if (havingPred_ != null) havingConjuncts.add(havingPred_);
      // Ignore predicates bound to a group-by slot because those
      // are already evaluated below this agg node (e.g., in a scan).
      Set<SlotId> groupBySlots = Sets.newHashSet();
      for (int i = 0; i < aggInfo_.getGroupingExprs().size(); ++i) {
        groupBySlots.add(aggInfo_.getOutputTupleDesc().getSlots().get(i).getId());
      }
      // Binding predicates are assigned to the final output tuple of the aggregation,
      // which is the tuple of the 2nd phase agg for distinct aggs.
      ArrayList<Expr> bindingPredicates =
          analyzer.getBoundPredicates(aggInfo_.getResultTupleId(), groupBySlots, false);
      havingConjuncts.addAll(bindingPredicates);
      havingConjuncts.addAll(
          analyzer.getUnassignedConjuncts(aggInfo_.getResultTupleId().asList(), false));
      materializeSlots(analyzer, havingConjuncts);
      aggInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }
  }

  /**
    * Populates baseTblSmap_ with our combined inline view smap and creates
    * baseTblResultExprs.
    */
  protected void resolveInlineViewRefs(Analyzer analyzer)
      throws AnalysisException {
    // Gather the inline view substitution maps from the enclosed inline views
    for (TableRef tblRef: tableRefs_) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        baseTblSmap_ =
            ExprSubstitutionMap.combine(baseTblSmap_, inlineViewRef.getBaseTblSmap());
      }
    }
    baseTblResultExprs_ = Expr.trySubstituteList(resultExprs_, baseTblSmap_, analyzer);
    LOG.trace("baseTblSmap_: " + baseTblSmap_.debugString());
    LOG.trace("resultExprs: " + Expr.debugString(resultExprs_));
    LOG.trace("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
  }

  public List<TupleId> getTableRefIds() {
    List<TupleId> result = Lists.newArrayList();
    for (TableRef ref: tableRefs_) {
      result.add(ref.getId());
    }
    return result;
  }

  /**
   * Expand "*" select list item, ignoring semi-joined tables.
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException {
    if (tableRefs_.isEmpty()) {
      throw new AnalysisException("'*' expression in select list requires FROM clause.");
    }
    // expand in From clause order
    for (TableRef tableRef: tableRefs_) {
      if (analyzer.isSemiJoined(tableRef.getId())) continue;
      expandStar(analyzer, tableRef.getAliasAsName(), tableRef.getDesc());
    }
  }

  /**
   * Expand "<tbl>.*" select list item.
   */
  private void expandStar(Analyzer analyzer, TableName tblName)
      throws AnalysisException {
    TupleDescriptor tupleDesc = analyzer.getDescriptor(tblName);
    if (tupleDesc == null) {
      throw new AnalysisException("unknown table alias '" + tblName.toString() + "'");
    }
    if (analyzer.isSemiJoined(tupleDesc.getId())) {
      throw new AnalysisException(String.format(
          "'*' expression cannot reference semi-/anti-joined table '%s'",
          tblName.toString()));
    }
    expandStar(analyzer, tblName, tupleDesc);
  }

  /**
   * Expand "*" for a particular tuple descriptor by appending analyzed slot refs for
   * each column to selectListExprs.
   */
  private void expandStar(Analyzer analyzer, TableName tblName, TupleDescriptor desc)
      throws AnalysisException {
    Preconditions.checkState(!analyzer.isSemiJoined(desc.getId()));
    for (Column col: desc.getTable().getColumnsInHiveOrder()) {
      SlotRef slotRef = new SlotRef(tblName, col.getName());
      slotRef.analyze(analyzer);
      resultExprs_.add(slotRef);
      colLabels_.add(col.getName().toLowerCase());
    }
  }

  /**
   * Analyze aggregation-relevant components of the select block (Group By clause,
   * select list, Order By clause), substitute AVG with SUM/COUNT, create the
   * AggregationInfo, including the agg output tuple, and transform all post-agg exprs
   * given AggregationInfo's smap.
   */
  private void analyzeAggregation(Analyzer analyzer)
      throws AnalysisException {
    if (groupingExprs_ == null && !selectList_.isDistinct()
        && !TreeNode.contains(resultExprs_, Expr.isAggregatePredicate())
        && (sortInfo_ == null
            || !TreeNode.contains(sortInfo_.getOrderingExprs(),
                                  Expr.isAggregatePredicate()))) {
      // we're not computing aggregates
      return;
    }

    // If we're computing an aggregate, we must have a FROM clause.
    if (tableRefs_.size() == 0) {
      throw new AnalysisException(
          "aggregation without a FROM clause is not allowed");
    }

    if ((groupingExprs_ != null ||
        TreeNode.contains(resultExprs_, Expr.isAggregatePredicate()))
        && selectList_.isDistinct()) {
      throw new AnalysisException(
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    }

    // disallow '*' and explicit GROUP BY (we can't group by '*', and if you need to
    // name all star-expanded cols in the group by clause you might as well do it
    // in the select list)
    if (groupingExprs_ != null) {
      for (SelectListItem item : selectList_.getItems()) {
        if (item.isStar()) {
          throw new AnalysisException(
              "cannot combine '*' in select list with GROUP BY: " + item.toSql());
        }
      }
    }

    // disallow subqueries in the GROUP BY clause
    if (groupingExprs_ != null) {
      for (Expr expr: groupingExprs_) {
        if (expr.contains(Predicates.instanceOf(Subquery.class))) {
          throw new AnalysisException(
              "Subqueries are not supported in the GROUP BY clause.");
        }
      }
    }

    // analyze grouping exprs
    ArrayList<Expr> groupingExprsCopy = Lists.newArrayList();
    if (groupingExprs_ != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them later)
      groupingExprsCopy = Expr.cloneList(groupingExprs_);
      substituteOrdinals(groupingExprsCopy, "GROUP BY", analyzer);
      Expr ambiguousAlias = getFirstAmbiguousAlias(groupingExprsCopy);
      if (ambiguousAlias != null) {
        throw new AnalysisException("Column '" + ambiguousAlias.toSql() +
            "' in GROUP BY clause is ambiguous");
      }
      groupingExprsCopy =
          Expr.trySubstituteList(groupingExprsCopy, aliasSmap_, analyzer);
      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).contains(Expr.isAggregatePredicate())) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain aggregate functions: "
                  + groupingExprs_.get(i).toSql());
        }
        if (groupingExprsCopy.get(i).contains(AnalyticExpr.class)) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain analytic expressions: "
                  + groupingExprsCopy.get(i).toSql());
        }
      }
    }

    // analyze having clause
    if (havingClause_ != null) {
      if (havingClause_.contains(Predicates.instanceOf(Subquery.class))) {
        throw new AnalysisException("Subqueries are not supported in the HAVING clause.");
      }
      // substitute aliases in place (ordinals not allowed in having clause)
      havingPred_ = havingClause_.substitute(aliasSmap_, analyzer);
      havingPred_.checkReturnsBool("HAVING clause", true);
      // can't contain analytic exprs
      Expr analyticExpr = havingPred_.findFirstOf(AnalyticExpr.class);
      if (analyticExpr != null) {
        throw new AnalysisException(
            "HAVING clause must not contain analytic expressions: "
               + analyticExpr.toSql());
      }
    }

    // Collect the aggregate expressions from the SELECT, HAVING and ORDER BY clauses
    // of this statement.
    ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
    TreeNode.collect(resultExprs_, Expr.isAggregatePredicate(), aggExprs);
    if (havingPred_ != null) {
      havingPred_.collect(Expr.isAggregatePredicate(), aggExprs);
    }
    if (sortInfo_ != null) {
      // TODO: Avoid evaluating aggs in ignored order-bys
      TreeNode.collect(sortInfo_.getOrderingExprs(), Expr.isAggregatePredicate(),
          aggExprs);
    }

    // Optionally rewrite all count(distinct <expr>) into equivalent NDV() calls.
    ExprSubstitutionMap ndvSmap = null;
    if (analyzer.getQueryCtx().getRequest().query_options.appx_count_distinct) {
      ndvSmap = new ExprSubstitutionMap();
      for (FunctionCallExpr aggExpr: aggExprs) {
        if (!aggExpr.isDistinct()
            || !aggExpr.getFnName().getFunction().equals("count")
            || aggExpr.getParams().size() != 1) {
          continue;
        }
        FunctionCallExpr ndvFnCall =
            new FunctionCallExpr("ndv", aggExpr.getParams().exprs());
        ndvFnCall.analyzeNoThrow(analyzer);
        Preconditions.checkState(ndvFnCall.getType().equals(aggExpr.getType()));
        ndvSmap.put(aggExpr, ndvFnCall);
      }
      // Replace all count(distinct <expr>) with NDV(<expr>).
      List<Expr> substAggExprs = Expr.substituteList(aggExprs, ndvSmap, analyzer);
      aggExprs.clear();
      for (Expr aggExpr: substAggExprs) {
        Preconditions.checkState(aggExpr instanceof FunctionCallExpr);
        aggExprs.add((FunctionCallExpr) aggExpr);
      }
    }

    // When DISTINCT aggregates are present, non-distinct (i.e. ALL) aggregates are
    // evaluated in two phases (see AggregateInfo for more details). In particular,
    // COUNT(c) in "SELECT COUNT(c), AGG(DISTINCT d) from R" is transformed to
    // "SELECT SUM(cnt) FROM (SELECT COUNT(c) as cnt from R group by d ) S".
    // Since a group-by expression is added to the inner query it returns no rows if
    // R is empty, in which case the SUM of COUNTs will return NULL.
    // However the original COUNT(c) should have returned 0 instead of NULL in this case.
    // Therefore, COUNT([ALL]) is transformed into zeroifnull(COUNT([ALL]) if
    // i) There is no GROUP-BY clause, and
    // ii) Other DISTINCT aggregates are present.
    ExprSubstitutionMap countAllMap = createCountAllMap(aggExprs, analyzer);
    countAllMap = ExprSubstitutionMap.compose(ndvSmap, countAllMap, analyzer);
    List<Expr> substitutedAggs = Expr.substituteList(aggExprs, countAllMap, analyzer);
    aggExprs.clear();
    TreeNode.collect(substitutedAggs, Expr.isAggregatePredicate(), aggExprs);
    try {
      createAggInfo(groupingExprsCopy, aggExprs, analyzer);
    } catch (InternalException e) {
      // should never happen
      Preconditions.checkArgument(false);
    }

    // combine avg smap with the one that produces the final agg output
    AggregateInfo finalAggInfo =
        aggInfo_.getSecondPhaseDistinctAggInfo() != null
          ? aggInfo_.getSecondPhaseDistinctAggInfo()
          : aggInfo_;

    ExprSubstitutionMap combinedSmap =
        ExprSubstitutionMap.compose(countAllMap, finalAggInfo.getOutputSmap(), analyzer);
    LOG.trace("combined smap: " + combinedSmap.debugString());

    // change select list, having and ordering exprs to point to agg output. We need
    // to reanalyze the exprs at this point.
    LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
    LOG.trace("resultexprs: " + Expr.debugString(resultExprs_));
    resultExprs_ = Expr.substituteList(resultExprs_, combinedSmap, analyzer);
    LOG.trace("post-agg selectListExprs: " + Expr.debugString(resultExprs_));
    if (havingPred_ != null) {
      // Make sure the predicate in the HAVING clause does not contain a
      // subquery.
      Preconditions.checkState(!havingPred_.contains(
          Predicates.instanceOf(Subquery.class)));
      havingPred_ = havingPred_.substitute(combinedSmap, analyzer);
      analyzer.registerConjuncts(havingPred_, true);
      LOG.debug("post-agg havingPred: " + havingPred_.debugString());
    }
    if (sortInfo_ != null) {
      sortInfo_.substituteOrderingExprs(combinedSmap, analyzer);
      LOG.debug("post-agg orderingExprs: " +
          Expr.debugString(sortInfo_.getOrderingExprs()));
    }

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      if (!resultExprs_.get(i).isBound(finalAggInfo.getOutputTupleId())) {
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectList_.getItems().get(i).getExpr().toSql());
      }
    }
    if (orderByElements_ != null) {
      for (int i = 0; i < orderByElements_.size(); ++i) {
        if (!sortInfo_.getOrderingExprs().get(i).isBound(
            finalAggInfo.getOutputTupleId())) {
          throw new AnalysisException(
              "ORDER BY expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + orderByElements_.get(i).getExpr().toSql());
        }
      }
    }
    if (havingPred_ != null) {
      if (!havingPred_.isBound(finalAggInfo.getOutputTupleId())) {
        throw new AnalysisException(
            "HAVING clause not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + havingClause_.toSql());
      }
    }
  }

  /**
   * Create a map from COUNT([ALL]) -> zeroifnull(COUNT([ALL])) if
   * i) There is no GROUP-BY, and
   * ii) There are other distinct aggregates to be evaluated.
   * This transformation is necessary for COUNT to correctly return 0 for empty
   * input relations.
   */
  private ExprSubstitutionMap createCountAllMap(
      List<FunctionCallExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException {
    ExprSubstitutionMap scalarCountAllMap = new ExprSubstitutionMap();

    if (groupingExprs_ != null && !groupingExprs_.isEmpty()) {
      // There are grouping expressions, so no substitution needs to be done.
      return scalarCountAllMap;
    }

    com.google.common.base.Predicate<FunctionCallExpr> isNotDistinctPred =
        new com.google.common.base.Predicate<FunctionCallExpr>() {
          public boolean apply(FunctionCallExpr expr) {
            return !expr.isDistinct();
          }
        };
    if (Iterables.all(aggExprs, isNotDistinctPred)) {
      // Only [ALL] aggs, so no substitution needs to be done.
      return scalarCountAllMap;
    }

    com.google.common.base.Predicate<FunctionCallExpr> isCountPred =
        new com.google.common.base.Predicate<FunctionCallExpr>() {
          public boolean apply(FunctionCallExpr expr) {
            return expr.getFnName().getFunction().equals("count");
          }
        };

    Iterable<FunctionCallExpr> countAllAggs =
        Iterables.filter(aggExprs, Predicates.and(isCountPred, isNotDistinctPred));
    for (FunctionCallExpr countAllAgg: countAllAggs) {
      // Replace COUNT(ALL) with zeroifnull(COUNT(ALL))
      ArrayList<Expr> zeroIfNullParam = Lists.newArrayList(countAllAgg.clone());
      FunctionCallExpr zeroIfNull =
          new FunctionCallExpr("zeroifnull", zeroIfNullParam);
      zeroIfNull.analyze(analyzer);
      scalarCountAllMap.put(countAllAgg, zeroIfNull);
    }

    return scalarCountAllMap;
  }

  /**
   * Create aggInfo for the given grouping and agg exprs.
   */
  private void createAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs, Analyzer analyzer)
          throws AnalysisException, InternalException {
    if (selectList_.isDistinct()) {
       // Create aggInfo for SELECT DISTINCT ... stmt:
       // - all select list items turn into grouping exprs
       // - there are no aggregate exprs
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      ArrayList<Expr> distinctGroupingExprs = Expr.cloneList(resultExprs_);
      aggInfo_ =
          AggregateInfo.create(distinctGroupingExprs, null, null, analyzer);
    } else {
      aggInfo_ = AggregateInfo.create(groupingExprs, aggExprs, null, analyzer);
    }
  }

  /**
   * If the select list contains AnalyticExprs, create AnalyticInfo and substitute
   * AnalyticExprs using the AnalyticInfo's smap.
   */
  private void analyzeAnalytics(Analyzer analyzer)
      throws AnalysisException {
    // collect AnalyticExprs from the SELECT and ORDER BY clauses
    ArrayList<Expr> analyticExprs = Lists.newArrayList();
    TreeNode.collect(resultExprs_, AnalyticExpr.class, analyticExprs);
    if (sortInfo_ != null) {
      TreeNode.collect(sortInfo_.getOrderingExprs(), AnalyticExpr.class,
          analyticExprs);
    }
    if (analyticExprs.isEmpty()) return;
    analyticInfo_ = AnalyticInfo.create(analyticExprs, analyzer);

    // change select list and ordering exprs to point to analytic output. We need
    // to reanalyze the exprs at this point.
    resultExprs_ = Expr.substituteList(resultExprs_, analyticInfo_.getSmap(), analyzer);
    LOG.trace("post-analytic selectListExprs: " + Expr.debugString(resultExprs_));
    if (sortInfo_ != null) {
      sortInfo_.substituteOrderingExprs(analyticInfo_.getSmap(), analyzer);
      LOG.trace("post-analytic orderingExprs: " +
          Expr.debugString(sortInfo_.getOrderingExprs()));
    }
  }


  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   * expressions from select list
   */
  @Override
  protected void substituteOrdinals(List<Expr> exprs, String errorPrefix,
      Analyzer analyzer) throws AnalysisException {
    // substitute ordinals
    ListIterator<Expr> i = exprs.listIterator();
    while (i.hasNext()) {
      Expr expr = i.next();
      if (!(expr instanceof NumericLiteral)) continue;
      expr.analyze(analyzer);
      if (!expr.getType().isIntegerType()) continue;
      long pos = ((NumericLiteral) expr).getLongValue();
      if (pos < 1) {
        throw new AnalysisException(
            errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
      }
      if (pos > selectList_.getItems().size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      if (selectList_.getItems().get((int) pos - 1).isStar()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal refers to '*' in select list: "
            + expr.toSql());
      }
      // create copy to protect against accidentally shared state
      i.set(selectList_.getItems().get((int)pos - 1).getExpr().clone());
    }
  }

  /**
   * Returns the SQL string corresponding to this SelectStmt.
   */
  @Override
  public String toSql() {
    // Return the SQL string before inline-view expression substitution.
    if (sqlString_ != null) return sqlString_;

    StringBuilder strBuilder = new StringBuilder();
    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql());
      strBuilder.append(" ");
    }

    // Select list
    strBuilder.append("SELECT ");
    if (selectList_.isDistinct()) {
      strBuilder.append("DISTINCT ");
    }
    if (selectList_.hasPlanHints()) {
      strBuilder.append(ToSqlUtils.getPlanHintsSql(selectList_.getPlanHints()) + " ");
    }
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      strBuilder.append(selectList_.getItems().get(i).toSql());
      strBuilder.append((i+1 != selectList_.getItems().size()) ? ", " : "");
    }
    // From clause
    if (!tableRefs_.isEmpty()) {
      strBuilder.append(" FROM ");
      for (int i = 0; i < tableRefs_.size(); ++i) {
        strBuilder.append(tableRefs_.get(i).toSql());
      }
    }
    // Where clause
    if (whereClause_ != null) {
      strBuilder.append(" WHERE ");
      strBuilder.append(whereClause_.toSql());
    }
    // Group By clause
    if (groupingExprs_ != null) {
      strBuilder.append(" GROUP BY ");
      for (int i = 0; i < groupingExprs_.size(); ++i) {
        strBuilder.append(groupingExprs_.get(i).toSql());
        strBuilder.append((i+1 != groupingExprs_.size()) ? ", " : "");
      }
    }
    // Having clause
    if (havingClause_ != null) {
      strBuilder.append(" HAVING ");
      strBuilder.append(havingClause_.toSql());
    }
    // Order By clause
    if (orderByElements_ != null) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql());
        strBuilder.append((i+1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql());
    return strBuilder.toString();
  }

  /**
   * If the select statement has a sort/top that is evaluated, then the sort tuple
   * is materialized. Else, if there is aggregation then the aggregate tuple id is
   * materialized. Otherwise, all referenced tables are materialized.
   * If there are analytics and no sort, then the returned tuple ids also include
   * the logical analytic output tuple.
   */
  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    if (evaluateOrderBy_) {
      tupleIdList.add(sortInfo_.getSortTupleDescriptor().getId());
    } else if (aggInfo_ != null) {
      // Return the tuple id produced in the final aggregation step.
      tupleIdList.add(aggInfo_.getResultTupleId());
    } else {
      for (TableRef tblRef: tableRefs_) {
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
    // We materialize the agg tuple or the table refs together with the analytic tuple.
    if (hasAnalyticInfo() && !evaluateOrderBy_) {
      tupleIdList.add(analyticInfo_.getOutputTupleId());
    }
  }

  private ArrayList<TableRef> cloneTableRefs() {
    ArrayList<TableRef> clone = Lists.newArrayList();
    for (TableRef tblRef: tableRefs_) {
      clone.add(tblRef.clone());
    }
    return clone;
  }

  @Override
  public QueryStmt clone() {
    SelectStmt selectClone = new SelectStmt(selectList_.clone(), cloneTableRefs(),
        (whereClause_ != null) ? whereClause_.clone().reset() : null,
        (groupingExprs_ != null) ? Expr.resetList(Expr.cloneList(groupingExprs_)) : null,
        (havingClause_ != null) ? havingClause_.clone().reset() : null,
        cloneOrderByElements(),
        (limitElement_ != null) ? limitElement_.clone() : null);
    selectClone.setWithClause(cloneWithClause());
    return selectClone;
  }

  /**
   * Check if the stmt returns a single row. This can happen
   * in the following cases:
   * 1. select stmt with a 'limit 1' clause
   * 2. select stmt with an aggregate function and no group by.
   * 3. select stmt with no from clause.
   *
   * This function may produce false negatives because the cardinality of the
   * result set also depends on the data a stmt is processing.
   */
  public boolean returnsSingleRow() {
    // limit 1 clause
    if (limitElement_ != null && limitElement_.getLimit() == 1) return true;
    // No from clause (base tables or inline views)
    if (tableRefs_.isEmpty()) return true;
    // Aggregation with no group by and no DISTINCT
    if (hasAggInfo() && !hasGroupByClause() && !selectList_.isDistinct()) return true;
    // In all other cases, return false.
    return false;
  }
}
