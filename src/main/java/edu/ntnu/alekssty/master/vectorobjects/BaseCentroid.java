package edu.ntnu.alekssty.master.vectorobjects;

import org.apache.flink.ml.linalg.DenseVector;

public class BaseCentroid extends Vector {

    public final int ID;

    public BaseCentroid(DenseVector vector, int ID, String domain) {
        super(vector, domain);
        this.ID = ID;
    }

    public int move(DenseVector vector, int cardinality) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    public int move(DenseVector vector) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    public String getDomain() {
        return domain;
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
                ", ID=" + ID +
                ", vector=" + vector +
                ", domain='" + domain + '\'' +
                '}';
    }

}
