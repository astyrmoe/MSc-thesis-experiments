package edu.ntnu.alekssty.master;

import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;

public class AbstractPoint {

    public DenseVector vector;
    public final String domain;

    @Override
    public String toString() {
        return "Point{" +
                "vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }

    public AbstractPoint(DenseVector vector, String domain) {
        this.vector = vector;
        this.domain = domain;
    }

    public double distance(DenseVector vector) {
        return DistanceMeasure.getInstance("euclidean").distance(vector, this.vector);
    }
}
