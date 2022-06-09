package edu.ntnu.alekssty.master.vectorobjects.offlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Vector;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseCentroid extends Vector {

    boolean finished;
    public double movement;
    final int ID;
    public int cardinality;

    public BaseCentroid(DenseVector vector, int ID, String domain) {
        super(vector, domain);
        this.ID = ID;
        this.movement = Double.MAX_VALUE;
        this.finished = false;
    }

    public int move(DenseVector vector, int cardinality) {
        this.movement = distance(vector);
        this.vector = vector;
        this.cardinality = cardinality;
        return giveDistCalcAccAndReset();
    }

    public int move(DenseVector vector) {
        this.movement = distance(vector);
        this.vector = vector;
        return giveDistCalcAccAndReset();
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

    public int getCardinality() {
        return cardinality;
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
