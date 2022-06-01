package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class PhilipsCentroid extends BaseCentroid implements Centroid {

    public DenseVector distToOthers;

    public double getDistanceTo(Centroid c) {
        if (distToOthers.get(c.getID()) == 0) {
            double dist = distance(c.getVector());
            distToOthers.values[c.getID()] = dist;
            ((PhilipsCentroid)c).distToOthers.values[this.ID] = dist;
            return dist;
        }
        return distToOthers.get(c.getID());
    }

    public PhilipsCentroid(DenseVector vector, int ID, String domain, int k) {
        super(vector, ID, domain);
        distToOthers = new DenseVector(k);
    }

    @Override
    public int update(Centroid[] centroids) {
        for (Centroid c : centroids) {
            if (c.getMovement() == 0 && this.movement == 0) {continue;}
            if (c.getID() <= this.ID) {continue;}
            double dist = distance(c.getVector());
            distToOthers.values[c.getID()] = dist;
            ((PhilipsCentroid)c).distToOthers.values[this.ID] = dist;
        }
        return giveDistCalcAccAndReset();
    }
}
