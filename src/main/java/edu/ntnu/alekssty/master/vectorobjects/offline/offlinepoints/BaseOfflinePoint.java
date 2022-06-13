package edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints;

import edu.ntnu.alekssty.master.vectorobjects.BasePoint;
import edu.ntnu.alekssty.master.vectorobjects.Vector;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseOfflinePoint extends BasePoint {

    public boolean finished;

    public BaseOfflinePoint(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
        finished = false;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished() {
        finished = true;

    }
}
