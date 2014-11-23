// Copyright 2014 GISTIC

package org.gistic.spatialImpala.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/*
 * This class is a dummy class includes a hardcoded
 * Global Indexes for testing purpose.
 * Hardcoded data included belongs to the test data located at $IMPALA_HOME/testdata/dummy_catalog_test_data/ 
 */
public class GlobalIndex {
	private static HashMap<String, GlobalIndexRecord> globalIndexMap = new HashMap<String, GlobalIndexRecord>();
	
	static {
		globalIndexMap.put("data_00001_4", new GlobalIndexRecord(1, "data_00001_4", new Rectangle(0.0,0.0,500000.0,333333.3333333333)));
		globalIndexMap.put("data_00002_5", new GlobalIndexRecord(2, "data_00002_5", new Rectangle(500000.0,0.0,1000000.0,333333.3333333333)));
		globalIndexMap.put("data_00003_3", new GlobalIndexRecord(3, "data_00003_3", new Rectangle(0.0,333333.3333333333,500000.0,666666.6666666666)));
		globalIndexMap.put("data_00004_2", new GlobalIndexRecord(4, "data_00004_2", new Rectangle(500000.0,333333.3333333333,1000000.0,666666.6666666666)));
		globalIndexMap.put("data_00005_1", new GlobalIndexRecord(5, "data_00005_1", new Rectangle(0.0,666666.6666666666,500000.0,1000000.0)));
		globalIndexMap.put("data_00006", new GlobalIndexRecord(6, "data_00006", new Rectangle(500000.0,666666.6666666666,1000000.0,1000000.0)));
	}
	
	public List<GlobalIndexRecord> getGIsforPoint(int x, int y) {
		List<GlobalIndexRecord> globalIndexes = new ArrayList<GlobalIndexRecord>();
		for (GlobalIndexRecord giRecord : globalIndexMap.values()) {
			if (giRecord.mbr.includesPoint(x, y))
				globalIndexes.add(giRecord);
		}
		return globalIndexes;
	}
	
	public GlobalIndexRecord getGIRecordforTag(String tag) {
		return globalIndexMap.get(tag);
	}
	
	public static class GlobalIndexRecord {
		public int id;
		public String tag;
		public Rectangle mbr;
		
		public GlobalIndexRecord(int id, String tag, Rectangle mbr) {
			this.id = id;
			this.tag = tag;
			this.mbr = mbr;
		}
	}
	
	public static class Rectangle {
		public double x1;
		public double y1;
		public double x2;
		public double y2;
		
		public Rectangle(double x1, double y1, double x2, double y2) {
			this.x1 = x1;
			this.y1 = y1;
			this.x2 = x2;
			this.y2 = y2;
		}
		
		public boolean includesPoint(int x, int y) {
			return (x >= x1) && (x <= x2) && (y >= y1) && (y <= y2);
		}
	}
}