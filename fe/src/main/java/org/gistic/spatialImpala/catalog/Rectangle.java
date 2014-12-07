// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.thrift.TRectangle;

public class Rectangle {
	private double x1;
	private double y1;
	private double x2;
	private double y2;

	public Rectangle(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
	}

	public boolean includesPoint(int x, int y) {
		return (x >= x1) && (x <= x2) && (y >= y1) && (y <= y2);
	}
	
	public boolean overlaps(Rectangle rect) {
		if (this.x1 > rect.x2 || this.x2 < rect.x1)
			return false;
		
		if (this.y1 < rect.y2 || this.y2 > rect.y1)
			return false;
		
		return true;
	}
	
	public TRectangle toThrift() {
		return new TRectangle(this.x1, this.y1, this.x2, this.y2);
	}
	
	public static Rectangle fromThrift(TRectangle rect) {
		return new Rectangle(rect.getX1(), rect.getY1(), rect.getX2(), rect.getY2());
	}
	
	public double getX1() {
		return x1;
	}
	
	public double getY1() {
		return y1;
	}
	
	public double getX2() {
		return x2;
	}
	
	public double getY2() {
		return y2;
	}
	
	@Override
	public String toString() {
		return "(" + x1 + ", " + y1 + ", "
				+ x2 + ", " + y2 + ")";
	}
}