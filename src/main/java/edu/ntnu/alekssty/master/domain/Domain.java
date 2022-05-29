package edu.ntnu.alekssty.master.domain;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface Domain {

    String getDomain();
    boolean isFinished();
    void setFinished();
    void update(DenseVector[] vectors);
    Centroid getCentroid(int i);

}
