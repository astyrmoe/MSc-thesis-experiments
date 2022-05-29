package edu.ntnu.alekssty.master.centroids;

import edu.ntnu.alekssty.master.AbstractPoint;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseCentroid extends AbstractPoint {

    boolean finished;
    double movement;
    final int ID;

    public BaseCentroid(DenseVector vector, int ID, String domain) {
        super(vector, domain);
        this.ID = ID;
        this.movement = Double.MAX_VALUE;
        this.finished = false;
    }

    public void move(DenseVector vector) {
        this.movement = distance(vector);
        this.vector = vector;
    }

    public double getMovement() {
        return movement;
    }

    public DenseVector getVector() {
        return vector;
    }

    public int getID() {
        return ID;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "finished=" + finished +
                ", movement=" + movement +
                ", ID=" + ID +
                ", vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }
}
