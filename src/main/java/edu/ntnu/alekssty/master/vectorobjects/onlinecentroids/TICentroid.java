package edu.ntnu.alekssty.master.vectorobjects.onlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offlinecentroids.PhilipsCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class TICentroid extends PhilipsCentroid implements Centroid {

    public TICentroid(DenseVector vector, int ID, String domain, int k) {
        super(vector, ID, domain, k);
        this.movement = -1;
    }

    @Override
    public int move(DenseVector vector, int cardinality) {
        this.vector = vector;
        this.cardinality = cardinality;
        return giveDistCalcAccAndReset();
    }

    @Override
    public int move(DenseVector vector) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

}
