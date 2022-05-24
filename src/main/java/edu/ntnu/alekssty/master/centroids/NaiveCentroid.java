package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class NaiveCentroid extends BaseCentroid implements Centroid {

    public NaiveCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public void update(Centroid[] centroids) {
    }
}
