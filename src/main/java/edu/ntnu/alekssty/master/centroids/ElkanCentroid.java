package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class ElkanCentroid extends BaseCentroid implements Centroid {

    // TODO Is this one used by anything else than find 2nd closest?
    public DenseVector distanceToOtherCentroids;

    public ElkanCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
    }

    @Override
    public void update(Centroid[] centroids) {
        if (distanceToOtherCentroids == null) {
            distanceToOtherCentroids = new DenseVector(centroids.length);
            distanceToOtherCentroids.values[this.ID] = 0;
        }
        for (Centroid c : centroids) {
            if (c.getMovement() == 0 && this.movement == 0) {continue;}
            if (c.getID() <= this.ID) {continue;}
            double dist = distance(c.getVector());
            distanceToOtherCentroids.values[c.getID()] = dist;
            ((ElkanCentroid)c).updateDistanceToMe(this.ID, dist, centroids.length);
        }
    }

    private void updateDistanceToMe(int id, double dist, int k) {
        if (distanceToOtherCentroids == null) {
            distanceToOtherCentroids = new DenseVector(k);
            distanceToOtherCentroids.values[this.ID] = 0;
        }
        distanceToOtherCentroids.values[id] = dist;
    }

    public int findOtherCloseCentroidID() {
        double minValue = Double.MAX_VALUE;
        int minID = 0;
        for (int i = 0; i < this.distanceToOtherCentroids.values.length; i ++) {
            if (i != this.ID && minValue > this.distanceToOtherCentroids.values[i]) {
                minValue = this.distanceToOtherCentroids.values[i];
                minID = i;
            }
        }
        return minID;
    }
}
