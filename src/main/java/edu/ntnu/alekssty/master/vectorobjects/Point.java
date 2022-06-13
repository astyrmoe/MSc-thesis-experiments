package edu.ntnu.alekssty.master.vectorobjects;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface Point {

    int update(Centroid[] centroids);
    DenseVector getVector();
    String getDomain();
    int getAssignedClusterID();
    String getLabel();

}
