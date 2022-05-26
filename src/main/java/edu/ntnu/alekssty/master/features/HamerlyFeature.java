package edu.ntnu.alekssty.master.features;

import edu.ntnu.alekssty.master.centroids.Centroid;
import edu.ntnu.alekssty.master.centroids.HamerlyCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class HamerlyFeature extends BaseFeature implements Feature{

    double lowerBound;
    double upperBound;

    public HamerlyFeature(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
        lowerBound = Double.MAX_VALUE;
        upperBound = Double.MAX_VALUE;
    }

    @Override
    public void update(Centroid[] centroids) {
        if (assignedClusterID == -1) {
            assignedClusterID = 0;
        }
        upperBound = upperBound + centroids[assignedClusterID].getMovement();
        double biggestMovement = 0;
        for (Centroid c : centroids) {
            if (c.getMovement() > biggestMovement) {
                biggestMovement = c.getMovement();
            }
        }
        lowerBound = lowerBound - biggestMovement;
        double z = Double.max(lowerBound, ((HamerlyCentroid)centroids[assignedClusterID]).getHalfDistToSecondClosest());
        if (upperBound <= z) {
            return;
        }
        upperBound = distance(centroids[assignedClusterID].getVector());
        if (upperBound <= z) {
            return;
        }
        double bestDist = Double.MAX_VALUE;
        double secBestDist = Double.MAX_VALUE;
        int bestC = 0;
        int secBestC = 0;
        for (int i = 0; i < centroids.length; i++) {
            double candidateDist = distance(centroids[i].getVector());
            if (candidateDist < bestDist) {
                secBestDist = bestDist;
                secBestC = bestC;
                bestDist = candidateDist;
                bestC = i;
                continue;
            }
            if (candidateDist < secBestDist) {
                secBestDist = candidateDist;
                secBestC = i;
            }
        }
        if (bestC != assignedClusterID) {
            assignedClusterID = bestC;
            upperBound = distance(centroids[assignedClusterID].getVector());
        }
        lowerBound = distance(centroids[secBestC].getVector());
    }
}
