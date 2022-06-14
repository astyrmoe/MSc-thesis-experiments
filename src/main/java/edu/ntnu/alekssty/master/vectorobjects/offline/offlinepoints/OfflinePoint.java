package edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints;

import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public interface OfflinePoint extends Point {

    boolean isFinished();
    void setFinished();

}
