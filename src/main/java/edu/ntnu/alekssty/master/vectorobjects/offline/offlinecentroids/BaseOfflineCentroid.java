package edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.BaseCentroid;
import edu.ntnu.alekssty.master.vectorobjects.Vector;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseOfflineCentroid extends BaseCentroid {

    boolean finished;
    public double movement;
    public int cardinality;

    public BaseOfflineCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
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

    public double getMovement() {
        return movement;
    }

    public int getCardinality() {
        return cardinality;
    }
}
