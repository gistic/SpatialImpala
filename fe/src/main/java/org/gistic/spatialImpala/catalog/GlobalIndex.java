// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TGlobalIndex;
import com.cloudera.impala.thrift.TGlobalIndexRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map.Entry;

/*
 * Global Index's catalog class responsible for holding the global indexes'
 * records of a specific Spatial Table.
 */
public class GlobalIndex implements CatalogObject {
	private static String GLOBAL_INDEX_SUFFIX = "_global_index";
	public static String GLOBAL_INDEX_TABLE_PARAM = "globalIndex";
	private HashMap<String, GlobalIndexRecord> globalIndexMap;
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
	
	public TGlobalIndex toThrift() {
		HashMap<String, TGlobalIndexRecord> tGlobalIndexMap = new HashMap<String, TGlobalIndexRecord>();
		for (Entry<String, GlobalIndexRecord> gIRecord : globalIndexMap.entrySet()) {
			tGlobalIndexMap.put(gIRecord.getKey(), gIRecord.getValue().toThrift());
		}
		return new TGlobalIndex(tableName_, tGlobalIndexMap);
	}

	// TODO: Add methods fromThrift and toThrift after creating TGlobalIndex.

	public static GlobalIndex loadAndCreateGlobalIndex(String tableName) {
		// TODO: Add file path as a param. and load the indexes into an object.
		return new GlobalIndex(tableName, null);
	}
	
	public static GlobalIndex fromThrift(TGlobalIndex tGlobalIndex) {
		HashMap<String, GlobalIndexRecord> gIMap = new HashMap<String, GlobalIndexRecord>();
		for (Entry<String, TGlobalIndexRecord> gIRecord : tGlobalIndex.getGlobalIndexMap().entrySet()) {
			gIMap.put(gIRecord.getKey(), GlobalIndexRecord.fromThrift(gIRecord.getValue()));
		}
		return new GlobalIndex(tGlobalIndex.getTbl_name(), gIMap);
	}
}