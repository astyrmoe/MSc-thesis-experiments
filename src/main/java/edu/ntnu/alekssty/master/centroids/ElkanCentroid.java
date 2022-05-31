package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

import java.util.Arrays;

public class ElkanCentroid extends BaseCentroid implements Centroid {

    // TODO Is this one used by anything else than find 2nd closest?
    public DenseVector distanceToOtherCentroids;
    public double halfDistToClosestCentroid;

    public ElkanCentroid(DenseVector vector, int ID, String domain, int k) {
        super(vector, ID, domain);
        distanceToOtherCentroids = new DenseVector(k);
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
        halfDistToClosestCentroid = Arrays.stream(distanceToOtherCentroids.values).min().getAsDouble() / 2;
    }

    private void updateDistanceToMe(int id, double dist, int k) {
        if (distanceToOtherCentroids == null) {
            distanceToOtherCentroids = new DenseVector(k);
            distanceToOtherCentroids.values[this.ID] = 0;
        }
        distanceToOtherCentroids.values[id] = dist;
    }

    public double findOtherCloseCentroidID() {
        return halfDistToClosestCentroid;
    }
}
