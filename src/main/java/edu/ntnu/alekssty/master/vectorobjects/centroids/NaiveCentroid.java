package edu.ntnu.alekssty.master.vectorobjects.centroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NaiveCentroid extends BaseCentroid implements Centroid {

    public NaiveCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        return giveDistCalcAccAndReset();
    }
}
