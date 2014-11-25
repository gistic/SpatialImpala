// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/*
 * Global Index's catalog class responsible for holding the global indexes'
 * records of a specific Spatial Table.
 */
public class GlobalIndex implements CatalogObject {
	private static String GLOBAL_INDEX_SUFFIX = "_global_index";
	private HashMap<String, GlobalIndexRecord> globalIndexMap = new HashMap<String, GlobalIndexRecord>();
	private final String tableName_;
	private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

	private GlobalIndex(String tableName,
			HashMap<String, GlobalIndexRecord> globalIndexMap) {
		this.tableName_ = tableName;
		this.globalIndexMap = globalIndexMap;
	}

	public List<GlobalIndexRecord> getGIsforPoint(int x, int y) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (gIRecord.getMBR().includesPoint(x, y))
				globalIndexes.add(gIRecord);
		}
		return globalIndexes;
	}

	public GlobalIndexRecord getGIRecordforTag(String tag) {
		return globalIndexMap.get(tag);
	}

	public TCatalogObjectType getCatalogObjectType() {
		return TCatalogObjectType.GLOBAL_INDEX;
	}

	public String getName() {
		return tableName_ + GLOBAL_INDEX_SUFFIX;
	}

	public long getCatalogVersion() {
		return catalogVersion_;
	}

	public void setCatalogVersion(long newVersion) {
		catalogVersion_ = newVersion;
	}

	public boolean isLoaded() {
		return true;
	}

	// TODO: Add methods fromThrift and toThrift after creating TGlobalIndex.

	public static GlobalIndex loadAndCreateGlobalIndex(String tableName) {
		// TODO: Add file path as a param. and load the indexes into an object.
		return new GlobalIndex(tableName, null);
	}
}