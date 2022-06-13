package edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NaiveCentroid extends BaseOfflineCentroid implements OfflineCentroid {

    public NaiveCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        return giveDistCalcAccAndReset();
    }
}
