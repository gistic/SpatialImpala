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

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/*
 * This class is a dummy class includes a hardcoded
 * Global Indexes for testing purpose.
 * Hardcoded data included belongs to the test data located at $IMPALA_HOME/testdata/dummy_catalog_test_data/ 
 */
public class DummyGI {
	private static HashMap<String, DummyGIRecord> globalIndexMap = new HashMap<String, DummyGIRecord>();
	
	static {
		globalIndexMap.put("data_00001_4", new DummyGIRecord(1, "data_00001_4", new DummyRectangle(0.0,0.0,500000.0,333333.3333333333)));
		globalIndexMap.put("data_00002_5", new DummyGIRecord(2, "data_00002_5", new DummyRectangle(500000.0,0.0,1000000.0,333333.3333333333)));
		globalIndexMap.put("data_00003_3", new DummyGIRecord(3, "data_00003_3", new DummyRectangle(0.0,333333.3333333333,500000.0,666666.6666666666)));
		globalIndexMap.put("data_00004_2", new DummyGIRecord(4, "data_00004_2", new DummyRectangle(500000.0,333333.3333333333,1000000.0,666666.6666666666)));
		globalIndexMap.put("data_00005_1", new DummyGIRecord(5, "data_00005_1", new DummyRectangle(0.0,666666.6666666666,500000.0,1000000.0)));
		globalIndexMap.put("data_00006", new DummyGIRecord(6, "data_00006", new DummyRectangle(500000.0,666666.6666666666,1000000.0,1000000.0)));
	}
	
	public List<DummyGIRecord> getGIsforPoint(int x, int y) {
		List<DummyGIRecord> globalIndexes = new ArrayList<DummyGIRecord>();
		for (DummyGIRecord giRecord : globalIndexMap.values()) {
			if (giRecord.mbr.includesPoint(x, y))
				globalIndexes.add(giRecord);
		}
		return globalIndexes;
	}
	
	public DummyGIRecord getGIRecordforTag(String tag) {
		return globalIndexMap.get(tag);
	}
	
	public static class DummyGIRecord {
		public int id;
		public String tag;
		public DummyRectangle mbr;
		
		public DummyGIRecord(int id, String tag, DummyRectangle mbr) {
			this.id = id;
			this.tag = tag;
			this.mbr = mbr;
		}
	}
	
	public static class DummyRectangle {
		public double x1;
		public double y1;
		public double x2;
		public double y2;
		
		public DummyRectangle(double x1, double y1, double x2, double y2) {
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