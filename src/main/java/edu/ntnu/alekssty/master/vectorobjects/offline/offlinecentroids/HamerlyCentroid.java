package edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class HamerlyCentroid extends BaseOfflineCentroid implements OfflineCentroid {

    double halfDistToSecClosest;

    public HamerlyCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        double minDist = Double.MAX_VALUE;
        for (Centroid c : centroids) {
            if (distance(c.getVector()) < minDist && c.getID() != this.ID) {
                minDist = distance(c.getVector());
            }
        }
        halfDistToSecClosest = minDist / 2;
        return giveDistCalcAccAndReset();
    }

    public double getHalfDistToSecondClosest() {
        return halfDistToSecClosest;
    }
}
