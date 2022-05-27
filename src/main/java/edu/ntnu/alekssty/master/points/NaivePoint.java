package edu.ntnu.alekssty.master.points;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class NaivePoint extends BasePoint implements Point {

    public NaivePoint(DenseVector vector, String domain) {
        super(vector, domain);
    }

    @Override
    public void update(Centroid[] centroids) {
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
    }
}
