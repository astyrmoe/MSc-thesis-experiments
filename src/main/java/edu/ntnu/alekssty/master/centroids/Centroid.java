package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public interface Centroid {

    int move(DenseVector vector);
    int update(Centroid[] centroids);
    String getDomain();
    boolean isFinished();
    double getMovement();
    DenseVector getVector();
    int getID();
    void setFinished();
    double distance(DenseVector vector);
}
