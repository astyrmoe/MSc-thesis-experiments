package edu.ntnu.alekssty.master.vectorobjects;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface Centroid {

    DenseVector getVector();
    int getID();
    double distance(DenseVector vector);
    int move(DenseVector vector);
    int update(Centroid[] centroids);
    String getDomain();

}
