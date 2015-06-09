// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogObject;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TGlobalIndex;
import com.cloudera.impala.thrift.TGlobalIndexRecord;

import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
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
	public static String INDEXED_ON_KEYWORD = "index";
	private HashMap<String, GlobalIndexRecord> globalIndexMap;
	private final String tableName_;
	private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;
	private List<SlotRef> indexedOnCols;
	private String indexes;

	private GlobalIndex(String tableName,
			HashMap<String, GlobalIndexRecord> globalIndexMap, String indexes) {
		this.tableName_ = tableName;
		this.globalIndexMap = globalIndexMap;
		indexedOnCols = new ArrayList<SlotRef>();
		this.indexes = indexes;
		fillIndexedOnCols (indexes);
	}
	
	public HashMap<String, GlobalIndexRecord> getGlobalIndexMap() {
		return globalIndexMap;
	}

	public List<GlobalIndexRecord> getGIsforPoint(int x, int y) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (gIRecord.getMBR().includesPoint(x, y))
				globalIndexes.add(gIRecord);
		}
		return globalIndexes;
	}

	public List<GlobalIndexRecord> getGIsforKnn(Point pt) {
		double maxdist = 0;
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (gIRecord.getMBR().includesPoint(pt.getX(), pt.getY()))
				maxdist = Math.max(maxdist, gIRecord.getMBR().getMaxDist(pt.getX(), pt.getY()));
		}
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
    	for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
    		if (gIRecord.getMBR().getMinDist(pt.getX(), pt.getY()) < maxdist )
    			globalIndexes.add(gIRecord);
    		}
    	return globalIndexes;
  	}

	public List<GlobalIndexRecord> getGIsIntersectRectangle(Rectangle rect) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (gIRecord.getMBR().intersects(rect)) {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ " intersect with: " + rect);
				globalIndexes.add(gIRecord);
			}
			else {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ " does not intersect with: " + rect);
			}
		}
		return globalIndexes;
	}
	
	public List<GlobalIndexRecord> getGIsContainedInRectangle(Rectangle rect) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (rect.contains(gIRecord.getMBR())) {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ "is fully contaied in: " + rect);
				globalIndexes.add(gIRecord);
			}
			else {
				LOG.info("GI record: " + gIRecord.getMBR()
						+ " is not fully contaied in: " + rect);
			}
		}
		return globalIndexes;
	}

	/*public HashMap<String, List<GlobalIndexRecord>> getGIsforJoin(GlobalIndex secondIndex) {
		HashMap<String, List<GlobalIndexRecord>> ret = new HashMap<String, List<GlobalIndexRecord>>();
		for (String key: globalIndexMap.keySet()) {
			ret.put(key,secondIndex.getGIsforRectangle(globalIndexMap.get(key).getMBR()));
		}
		return ret;
	}*/
	
	private void fillIndexedOnCols (String indexes) {
		List<String> columnNames = Arrays.asList(indexes.split("\\s*,\\s*"));
		for (String name : columnNames) {
			indexedOnCols.add(new SlotRef(null, name));
		}
			
	}
	
	public boolean isOneOfIndexedCol(SlotRef col) {
		for (SlotRef indexedCol : indexedOnCols) {
			if (indexedCol.getColumnName().equals(col.getColumnName())) {
				return true;
			}
		}
		
		return false;
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
		return new TGlobalIndex(tableName_, tGlobalIndexMap, indexes);
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
			String line = scanner.next();
                        String[] separatedRecord = line.split(",");
			if (separatedRecord.length != 8) {
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
			GlobalIndexRecord gIRecord = new GlobalIndexRecord(id, separatedRecord[7], new Rectangle(x1, y1, x2, y2));
			gIMap.put(separatedRecord[7], gIRecord);
		}
		scanner.close();
		return gIMap;
	}
	
	public static GlobalIndex loadAndCreateGlobalIndex(String tableName, String globalIndexPath, String indexes) {
		HashMap<String, GlobalIndexRecord> gIMap = loadGlobalIndex(globalIndexPath);
		return gIMap != null ? new GlobalIndex(tableName, gIMap, indexes) : null;
	}
	
	public static GlobalIndex fromThrift(TGlobalIndex tGlobalIndex) {
		HashMap<String, GlobalIndexRecord> gIMap = new HashMap<String, GlobalIndexRecord>();
		for (Entry<String, TGlobalIndexRecord> gIRecord : tGlobalIndex.getGlobalIndexMap().entrySet()) {
			gIMap.put(gIRecord.getKey(), GlobalIndexRecord.fromThrift(gIRecord.getValue()));
		}
		return new GlobalIndex(tGlobalIndex.getTbl_name(), gIMap, tGlobalIndex.getIndex());
	}
}
