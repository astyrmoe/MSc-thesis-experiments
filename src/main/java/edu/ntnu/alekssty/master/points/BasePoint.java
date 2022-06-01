package edu.ntnu.alekssty.master.points;

import edu.ntnu.alekssty.master.AbstractPoint;
import org.apache.flink.ml.linalg.DenseVector;

public class BasePoint extends AbstractPoint {

    public int assignedClusterID;
    public boolean finished;

    public BasePoint(DenseVector vector, String domain) {
        super(vector, domain);
        assignedClusterID = -1;
        finished = false;
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
