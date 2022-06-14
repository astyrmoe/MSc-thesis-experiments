package edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface OfflineCentroid extends Centroid {

    int getCardinality();
    double getMovement();
    int move(DenseVector vector, int cardinality);

}
