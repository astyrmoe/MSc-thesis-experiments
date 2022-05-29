package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public interface Centroid {

    void move(DenseVector vector);
    void update(Centroid[] centroids);
    double getMovement();
    DenseVector getVector();
    int getID();
    double distance(DenseVector vector);
}
