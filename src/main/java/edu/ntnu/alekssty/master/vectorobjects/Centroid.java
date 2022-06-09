package edu.ntnu.alekssty.master.vectorobjects;

import org.apache.flink.ml.linalg.DenseVector;

public interface Centroid {

    int move(DenseVector vector, int cardinality);
    int move(DenseVector vector);
    int update(Centroid[] centroids);
    String getDomain();
    boolean isFinished();
    int getCardinality();
    double getMovement();
    DenseVector getVector();
    int getID();
    double distance(DenseVector vector);
}
