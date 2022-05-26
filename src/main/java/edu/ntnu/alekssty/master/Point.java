package edu.ntnu.alekssty.master;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;

public class Point {

    public DenseVector vector;
    public String domain;
    public int accDistCalc;

    @Override
    public String toString() {
        return "Point{" +
                "vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }

    public Point(DenseVector vector, String domain) {
        this.vector = vector;
        this.domain = domain;
        accDistCalc = 0;
    }

    public double distance(DenseVector vector) {
        accDistCalc++;
        return DistanceMeasure.getInstance("euclidean").distance(vector, this.vector);
    }

    public int giveDistCalcAccAndReset() {
        int t = accDistCalc;
        accDistCalc = 0;
        return t;
    }
}
