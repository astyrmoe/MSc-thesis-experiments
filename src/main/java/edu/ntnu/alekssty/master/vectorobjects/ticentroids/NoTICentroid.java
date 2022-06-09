package edu.ntnu.alekssty.master.vectorobjects.ticentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.centroids.NaiveCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NoTICentroid extends NaiveCentroid implements Centroid {
    public NoTICentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int move(DenseVector vector, int cardinality) {
        this.cardinality = cardinality;
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    @Override
    public int move(DenseVector vector) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }
}
