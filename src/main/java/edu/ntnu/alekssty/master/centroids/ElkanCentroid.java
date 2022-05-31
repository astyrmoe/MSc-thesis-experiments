package edu.ntnu.alekssty.master.centroids;

import org.apache.flink.ml.linalg.DenseVector;

import java.util.Arrays;

public class ElkanCentroid extends BaseCentroid implements Centroid {

    @Override
    public String toString() {
        return "ElkanCentroid{" +
                "vector=" + vector +
                ", finished=" + finished +
                ", movement=" + movement +
                ", ID=" + ID +
                ", distanceToOtherCentroids=" + distanceToOtherCentroids +
                ", halfDistToClosestCentroid=" + halfDistToClosestCentroid +
                '}';
    }

    // TODO Is this one used by anything else than find 2nd closest?
    public DenseVector distanceToOtherCentroids;
    public double halfDistToClosestCentroid;

    public ElkanCentroid(DenseVector vector, int ID, String domain, int k) {
        super(vector, ID, domain);
        distanceToOtherCentroids = new DenseVector(k);
    }

    @Override
    public void update(Centroid[] centroids) {
        double closestDist = Double.MAX_VALUE;
        for (Centroid c : centroids) {
            if (c.getMovement() == 0 && this.movement == 0) {continue;}
            if (c.getID() <= this.ID) {continue;}
            double dist = distance(c.getVector());
            distanceToOtherCentroids.values[c.getID()] = dist;
            ((ElkanCentroid)c).updateDistanceToMe(this.ID, dist, centroids.length);
        }
        for (int i = 0; i < distanceToOtherCentroids.size(); i++) {
            if (distanceToOtherCentroids.get(i)<closestDist && i!=this.ID) {
                closestDist = distanceToOtherCentroids.get(i);
            }
        }
        halfDistToClosestCentroid = closestDist / 2;
    }

    private void updateDistanceToMe(int id, double dist, int k) {
        distanceToOtherCentroids.values[id] = dist;
    }

    public double findOtherCloseCentroidID() {
        return halfDistToClosestCentroid;
    }
}
