// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

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
}