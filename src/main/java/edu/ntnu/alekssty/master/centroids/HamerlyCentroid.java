package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class HamerlyCentroid extends BaseCentroid implements Centroid {

    double halfDistToSecClosest;

    public HamerlyCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public void update(Centroid[] centroids) {
        double minDist = Double.MAX_VALUE;
        for (Centroid c : centroids) {
            if (distance(c.getVector()) < minDist && c.getID() != this.ID) {
                minDist = distance(c.getVector());
            }
        }
        halfDistToSecClosest = minDist / 2;
    }

    public double getHalfDistToSecondClosest() {
        return halfDistToSecClosest;
    }
}
