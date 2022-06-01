package edu.ntnu.alekssty.master.points;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface Point {

    int update(Centroid[] centroids);
    DenseVector getVector();
    boolean isFinished();
    void setFinished();
    String getDomain();
    int getAssignedClusterID();
    String getLabel();

}
