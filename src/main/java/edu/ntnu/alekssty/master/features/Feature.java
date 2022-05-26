package edu.ntnu.alekssty.master.features;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface Feature {

    void update(Centroid[] centroids);
    DenseVector getVector();
    boolean isFinished();
    void setFinished();
    String getDomain();
    int getAssignedClusterID();
    String getLabel();

}
