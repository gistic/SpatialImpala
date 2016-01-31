// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Path;
import com.cloudera.impala.analysis.StatementBase;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SelectListItem;
import com.cloudera.impala.analysis.SelectList;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.InlineViewRef;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.ListIterator;
import java.util.HashMap;

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialPointInclusionStmt extends QueryStmt {
  private final static Logger LOG =
      LoggerFactory.getLogger(SpatialPointInclusionStmt.class);
  private static final String TABLE_NOT_SPATIAL_ERROR_MSG =
      "Table is not a spatial table.";
  private static final String TAG = "tag";
  private static final String X = "x";
  private static final String Y = "y";

  private TableName tableName_;

  protected SelectList selectList_;
  protected List<TableRef> tableRefs_;

  protected ArrayList<String> colLabels_;

  private ExprSubstitutionMap baseTblSmap_ = new ExprSubstitutionMap();

  //The rectangle provided in the query
  private final Rectangle rect_;

  public SpatialPointInclusionStmt (TableName tableName, Rectangle rect) {
    super(null, null);
    TableRef tableRef = new TableRef(tableName.toPath(), null);
    tableRefs_ = new ArrayList(1);
    tableRefs_.add(tableRef);
    this.tableName_ = tableName;
    this.rect_ = rect;

    colLabels_ = new ArrayList();

    List<SelectListItem> items = new ArrayList<SelectListItem>();
    items.add(new SelectListItem(new SlotRef(
        Path.createRawPath(tableName_.toString(), X)), null));
    items.add(new SelectListItem(new SlotRef(
        Path.createRawPath(tableName_.toString(), Y)), null));

    selectList_ = new SelectList(items);
    GIsIntersect = new ArrayList<GlobalIndexRecord>();
    GIsFullyContained = new ArrayList<GlobalIndexRecord>();
  }

  public List<TableRef> getTableRefs() {
    return tableRefs_;
  }

  public TupleId getTupleId() {
    return this.tupleId_;
  }

  public Rectangle getRectangle() {
    return rect_;
  }

  public SlotRef getXSlotRef() {
    return (SlotRef)selectList_.getItems().get(0).getExpr();
  }
	
  public SlotRef getYSlotRef() {
    return (SlotRef)selectList_.getItems().get(1).getExpr();
  }

  // Global indexes of partitions which only intersect with the rectangle.
  private List<GlobalIndexRecord> GIsIntersect;

  // Global indexes of partitions which fully contained in the rectangle.
  private List<GlobalIndexRecord> GIsFullyContained;

  // Single tuple materialized by the spatial union. Set in analyze().
  protected TupleId tupleId_;

  // Set prior to unnesting
  protected String toSqlString_ = null;

  public List<GlobalIndexRecord> getIntersectedGIs() {
    return GIsIntersect;
  }

  public List<GlobalIndexRecord> getFullyContainedGIs() {
    return GIsFullyContained;
  }

  protected void resolveInlineViewRefs(Analyzer analyzer) throws AnalysisException {
    // Gather the inline view substitution maps from the enclosed inline views.
    for (TableRef tblRef: tableRefs_) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        baseTblSmap_ = ExprSubstitutionMap.combine(baseTblSmap_,
            inlineViewRef.getBaseTblSmap());
      }
    }

    baseTblResultExprs_ = Expr.trySubstituteList(resultExprs_, baseTblSmap_,
        analyzer, false);

    LOG.trace("baseTblSmap_: " + baseTblSmap_.debugString());
    LOG.trace("resultExprs: " + Expr.debugString(resultExprs_));
    LOG.trace("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    // Getting table and checking for existence.
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

    tupleId_ = leftTblRef.getDesc().getId();

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
      // Analyze the resultExpr before generating a label to ensure enforcement
      // of expr child and depth limits (toColumn() label may call toSql()).
      item.getExpr().analyze(analyzer);

      resultExprs_.add(item.getExpr());
      String label = item.toColumnLabel(i, analyzer.useHiveColLabels());
      SlotRef aliasRef = new SlotRef(Lists.newArrayList(label));
      Expr existingAliasExpr = aliasSmap_.get(aliasRef);
      if (existingAliasExpr != null && !existingAliasExpr.equals(item.getExpr())) {
        // If we have already seen this alias, it refers to more than one column and
        // therefore is ambiguous.
        ambiguousAliasList_.add(aliasRef);
      }
      aliasSmap_.put(aliasRef, item.getExpr().clone());
      colLabels_.add(label);
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

    resolveInlineViewRefs(analyzer);
    Table table;

    if (!tableName_.isFullyQualified()) {
      tableName_ = new TableName(analyzer.getDefaultDb(), tableName_.getTbl());
    }

    table = analyzer.getTable(tableName_, Privilege.SELECT);

    // Table should be an instance of a Spatial table.
    if (!(table instanceof SpatialHdfsTable))
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG);

    SpatialHdfsTable spatialTable = (SpatialHdfsTable) table;

    // Global index shouldn't be null.
    GlobalIndex globalIndex = spatialTable.getGlobalIndexIfAny();

    if (globalIndex == null)
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
          + " : Table doesn't have global indexes.");

    List<String> columnNames = spatialTable.getColumnNames();
    if (!(columnNames.contains(TAG) && columnNames.contains(X) && columnNames.contains(Y)))
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG 
          + " : Table doesn't have the required columns.");

    // Now fill the GIsIntersect and GIsFullyContained vectors 
    HashMap<String, GlobalIndexRecord> globalIndexMap = globalIndex.getGlobalIndexMap();

    for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
      if (rect_.contains(gIRecord.getMBR())) {
        GIsFullyContained.add(gIRecord);
        LOG.info("GI is Fully Contained: " + gIRecord.getTag());
      } else if (rect_.intersects(gIRecord.getMBR())) {
        GIsIntersect.add(gIRecord);
        LOG.info("GI is Intersected: " + gIRecord.getTag());
      }
    }
  }

  @Override
  public String toSql() {
    return "load points from table " + tableName_.getTbl()
        + " overlaps rectangle" + rect_.toString() + ";";
  }

  public SelectList getSelectList() { return selectList_; }
	  
  @Override
  public QueryStmt clone() {
    SpatialPointInclusionStmt stmt = new SpatialPointInclusionStmt(tableName_, rect_);
    return stmt;
  }

  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
  }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    for (TableRef tblRef: tableRefs_) {
      tupleIdList.addAll(tblRef.getMaterializedTupleIds());
    }
  }

  /**
   * Returns all physical (non-inline-view) TableRefs of this statement and the nested
   * statements of inline views. The returned TableRefs are in depth-first order.
   */
  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.addAll(tableRefs_);
  }

  @Override
  public ArrayList<String> getColLabels() { return colLabels_; }

}
