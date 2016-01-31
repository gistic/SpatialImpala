// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.thrift.TGlobalIndexRecord;
import com.cloudera.impala.thrift.TRectangle;

/*
 * Global Index Record class responsible for holding a single
 * global index partition.
 */
public class GlobalIndexRecord {
  private int id;
  private String tag;
  private Rectangle mbr;

  public GlobalIndexRecord(int id, String tag, Rectangle mbr) {
    this.id = id;
    this.tag = tag;
    this.mbr = mbr;
  }

  public int getId() {
    return id;
  }

  public String getTag() {
    return tag;
  }

  public Rectangle getMBR() {
    return mbr;
  }

  public TGlobalIndexRecord toThrift() {
    return new TGlobalIndexRecord(this.id, this.tag, this.mbr.toThrift());
  }

  public static GlobalIndexRecord fromThrift(TGlobalIndexRecord gIRecord) {
    TRectangle rect = gIRecord.getMbr();
    return new GlobalIndexRecord(gIRecord.getId(), gIRecord.getTag(),
        Rectangle.fromThrift(rect));
  }
}
