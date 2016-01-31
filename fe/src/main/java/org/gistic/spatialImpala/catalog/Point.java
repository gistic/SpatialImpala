// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.catalog;

import com.cloudera.impala.thrift.TPoint;

public class Point {
  private double x;
  private double y;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public TPoint toThrift() {
    return new TPoint(this.x, this.y);
  }

  public static Point fromThrift(TPoint p) {
    return new Point(p.getX(), p.getY());
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  @Override
  public String toString() {
    return "(" + x + ", " + y + ")";
  }
}
