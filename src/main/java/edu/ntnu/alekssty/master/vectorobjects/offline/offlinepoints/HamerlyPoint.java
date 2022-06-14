package edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.HamerlyCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class HamerlyPoint extends BaseOfflinePoint implements OfflinePoint {

    double lowerBound;
    double upperBound;

    public HamerlyPoint(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
        lowerBound = Double.MAX_VALUE;
        upperBound = Double.MAX_VALUE;
    }

    @Override
    public int update(Centroid[] centroids) {
        if (assignedClusterID == -1) {
            assignedClusterID = 0;
        }
        upperBound = upperBound + ((OfflineCentroid)centroids[assignedClusterID]).getMovement();
        double biggestMovement = 0;
        for (Centroid c : centroids) {
            if (((OfflineCentroid)c).getMovement() > biggestMovement) {
                biggestMovement = ((OfflineCentroid)c).getMovement();
            }
        }
        lowerBound = lowerBound - biggestMovement;
        double z = Double.max(lowerBound, ((HamerlyCentroid)centroids[assignedClusterID]).getHalfDistToSecondClosest());
        if (upperBound <= z) {
            return giveDistCalcAccAndReset();
        }
        upperBound = distance(centroids[assignedClusterID].getVector());
        if (upperBound <= z) {
            return giveDistCalcAccAndReset();
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
        return giveDistCalcAccAndReset();
    }
}
