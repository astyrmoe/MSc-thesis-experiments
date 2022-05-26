package edu.ntnu.alekssty.master.features;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NaiveFeature extends BaseFeature implements Feature {

    public NaiveFeature(DenseVector vector, String domain) {
        super(vector, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        boolean allCentroidsFinished = true;
        for (Centroid naiveCentroid : centroids) {
            if (naiveCentroid.getMovement() != 0) {
                allCentroidsFinished = false;
                break;
            }
        }
        if (allCentroidsFinished) {
            this.finished = true;
            return giveDistCalcAccAndReset();
        }
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        for (Centroid naiveCentroid : centroids) {
            double distance = distance(naiveCentroid.getVector());
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = naiveCentroid.getID();
            }
        }
        assignedClusterID = closestCentroidId;
        return giveDistCalcAccAndReset();
    }
}
