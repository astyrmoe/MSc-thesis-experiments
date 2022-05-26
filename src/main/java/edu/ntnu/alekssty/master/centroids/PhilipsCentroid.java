package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class PhilipsCentroid extends BaseCentroid implements Centroid {

    DenseVector distToOthers;

    public double getDistanceTo(Centroid c) {
        if (distToOthers == null) {
            return distance(c.getVector());
        }
        return distToOthers.get(c.getID());
    }

    public PhilipsCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        distToOthers = new DenseVector(centroids.length);
        for (int i = 0; i < centroids.length; i ++) {
            if (i == this.ID) {
                distToOthers.values[i] = 0;
                continue;
            }
            distToOthers.values[i] = distance(centroids[i].getVector());
        }
        return giveDistCalcAccAndReset();
    }
}
