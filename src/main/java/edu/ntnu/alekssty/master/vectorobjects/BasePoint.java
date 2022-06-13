package edu.ntnu.alekssty.master.vectorobjects;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class BasePoint extends Vector {

    public int assignedClusterID;
    public String label;

    public BasePoint(DenseVector vector, String domain, String label) {
        super(vector, domain);
        assignedClusterID = -1;
        this.label = label;
    }

    public DenseVector getVector() {
        return vector;
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
                ", vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }
}
