// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.TableId;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.thrift.TTable;

import java.util.Map;

public class SpatialHdfsTable extends HdfsTable {
	protected GlobalIndex global_index_;

	public SpatialHdfsTable(TableId id,
			org.apache.hadoop.hive.metastore.api.Table msTbl, Db db,
			String name, String owner) {

		super(id, msTbl, db, name, owner);
		global_index_ = initializeGlobalIndex(this.getName(), msTbl);
	}

	public GlobalIndex getGlobalIndexIfAny() {
		return global_index_;
	}

	private GlobalIndex initializeGlobalIndex(String tableName,
			org.apache.hadoop.hive.metastore.api.Table msTbl) {

		if (msTbl == null)
			return null;

		Map<String, String> params = msTbl.getParameters();
		if (params == null)
			return null;

		String globalIndexPath = params.get(GlobalIndex.GLOBAL_INDEX_TABLE_PARAM);
		String indexedColumns = params.get(GlobalIndex.INDEXED_ON_KEYWORD);
		
		if (globalIndexPath == null || indexedColumns == null)
			return null;

		return GlobalIndex.loadAndCreateGlobalIndex(tableName, globalIndexPath, indexedColumns);
	}

	@Override
	public TTable toThrift() {
		// Send all metadata between the catalog service and the FE.
		TTable table = super.toThrift();
		if (global_index_ != null)
			table.setGlobalIndex(global_index_.toThrift());
		return table;
	}

	@Override
	protected void loadFromThrift(TTable thriftTable)
			throws TableLoadingException {

		super.loadFromThrift(thriftTable);
		if (thriftTable.getGlobalIndex() != null)
			global_index_ = GlobalIndex
					.fromThrift(thriftTable.getGlobalIndex());
	}

	public static boolean isSpatial(
			org.apache.hadoop.hive.metastore.api.Table msTbl) {

		return msTbl != null
				&& msTbl.getParameters() != null
				&& msTbl.getParameters().get(
						GlobalIndex.GLOBAL_INDEX_TABLE_PARAM) != null;
	}
}