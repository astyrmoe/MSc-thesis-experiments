package edu.ntnu.alekssty.master.centroids;

import edu.ntnu.alekssty.master.Point;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseCentroid extends Point {

    boolean finished;
    double movement;
    int ID;

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

    public String getDomain() {
        return domain;
    }

    public boolean isFinished() {
        return finished;
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

    public void setFinished() {
        finished = true;
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
