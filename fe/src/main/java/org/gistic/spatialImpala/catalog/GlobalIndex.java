// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogObject;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TGlobalIndex;
import com.cloudera.impala.thrift.TGlobalIndexRecord;

import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map.Entry;

/*
 * Global Index's catalog class responsible for holding the global indexes'
 * records of a specific Spatial Table.
 */
public class GlobalIndex implements CatalogObject {
	private static final Logger LOG = Logger.getLogger(GlobalIndex.class);

	private static String GLOBAL_INDEX_READ_EXCEPTION_MSG = "Couldn't parse Global Index file: ";
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
	
	public List<GlobalIndexRecord> getGIsforRectangle(Rectangle rect) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (gIRecord.getMBR().overlaps(rect)) {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ " overlaps with: " + rect);
				globalIndexes.add(gIRecord);
			}
			else {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ " does not overlap with: " + rect);
			}
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

	private static HashMap<String, GlobalIndexRecord> loadGlobalIndex(String globalIndexPath) {
		LOG.info("Loading global index file.");
		Path gIPath = new Path(globalIndexPath.trim());
		gIPath = FileSystemUtil.createFullyQualifiedPath(gIPath);
		String data = null;
		try {
			data = FileSystemUtil.readFile(gIPath);
		} catch (IOException e) {
			LOG.error(GLOBAL_INDEX_READ_EXCEPTION_MSG + globalIndexPath);
			return null;
		}
		
		if (data == null || data.length() == 0) {
			LOG.error(GLOBAL_INDEX_READ_EXCEPTION_MSG + globalIndexPath);
			return null;
		}
		
		HashMap<String, GlobalIndexRecord> gIMap = new HashMap<String, GlobalIndexRecord>();
		Scanner scanner = new Scanner(data);
		while (scanner.hasNext()) {
			String[] separatedRecord = scanner.next().split(",");
			if (separatedRecord.length != 6) {
				LOG.error(GLOBAL_INDEX_READ_EXCEPTION_MSG + globalIndexPath);
				scanner.close();
				return null;
			}
			
			int id = 0;
			double x1 = 0;
			double y1 = 0;
			double x2 = 0;
			double y2 = 0;
			
			try {
				id = Integer.parseInt(separatedRecord[0]);
				x1 = Double.parseDouble(separatedRecord[1]);
				y1 = Double.parseDouble(separatedRecord[2]);
				x2 = Double.parseDouble(separatedRecord[3]);
				y2 = Double.parseDouble(separatedRecord[4]);
			} catch (Exception e) {
				LOG.error(GLOBAL_INDEX_READ_EXCEPTION_MSG + globalIndexPath);
				scanner.close();
				return null;
			}
			LOG.info("Reading Record: [" + id + ", " + separatedRecord[5] + ", " + x1 + ", " + y1 + ", " + x2 + ", " + y2 + "]");
			GlobalIndexRecord gIRecord = new GlobalIndexRecord(id, separatedRecord[5], new Rectangle(x1, y1, x2, y2));
			gIMap.put(separatedRecord[5], gIRecord);
		}
		scanner.close();
		return gIMap;
	}
	
	public static GlobalIndex loadAndCreateGlobalIndex(String tableName, String globalIndexPath) {
		HashMap<String, GlobalIndexRecord> gIMap = loadGlobalIndex(globalIndexPath);
		return gIMap != null ? new GlobalIndex(tableName, gIMap) : null;
	}
	
	public static GlobalIndex fromThrift(TGlobalIndex tGlobalIndex) {
		HashMap<String, GlobalIndexRecord> gIMap = new HashMap<String, GlobalIndexRecord>();
		for (Entry<String, TGlobalIndexRecord> gIRecord : tGlobalIndex.getGlobalIndexMap().entrySet()) {
			gIMap.put(gIRecord.getKey(), GlobalIndexRecord.fromThrift(gIRecord.getValue()));
		}
		return new GlobalIndex(tGlobalIndex.getTbl_name(), gIMap);
	}
}