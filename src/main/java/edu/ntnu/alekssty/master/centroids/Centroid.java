package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public interface Centroid {

    void move(DenseVector vector);
    void update(Centroid[] centroids);
    String getDomain();
    boolean isFinished();
    double getMovement();
    DenseVector getVector();
    int getID();
    void setFinished();
    double distance(DenseVector vector);
}
