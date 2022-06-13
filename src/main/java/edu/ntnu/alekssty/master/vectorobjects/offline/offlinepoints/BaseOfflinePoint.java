package edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints;

import edu.ntnu.alekssty.master.vectorobjects.Vector;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseOfflinePoint extends Vector {

    public int assignedClusterID;
    public boolean finished;
    public String label;

    public BaseOfflinePoint(DenseVector vector, String domain, String label) {
        super(vector, domain);
        assignedClusterID = -1;
        finished = false;
        this.label = label;
    }

    public DenseVector getVector() {
        return vector;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished() {
        finished = true;

    }

    public String getLabel() {
        return label;
    }

    public String getDomain() {
        return domain;
    }

    public int getAssignedClusterID() {
        return assignedClusterID;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "assignedClusterID=" + assignedClusterID +
                ", finished=" + finished +
                ", vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }
}
