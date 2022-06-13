package edu.ntnu.alekssty.master.vectorobjects.online.onlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.BaseCentroid;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.NaiveCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NoTICentroid extends BaseCentroid implements Centroid {

    public NoTICentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int move(DenseVector vector, int cardinality) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    @Override
    public int update(Centroid[] centroids) {
        return giveDistCalcAccAndReset();
    }

    @Override
    public int move(DenseVector vector) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }
}
