package edu.ntnu.alekssty.master.points;

import edu.ntnu.alekssty.master.centroids.Centroid;
import edu.ntnu.alekssty.master.centroids.PhilipsCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class PhilipsPoint extends BasePoint implements Point {

    public PhilipsPoint(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
    }

    @Override
    public int update(Centroid[] centroids) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        for (int i = 0; i < centroids.length; i++) {
            Centroid centroid = centroids[i];
            if (assignedClusterID != -1) {
                if (2*distance(centroids[assignedClusterID].getVector()) <= ((PhilipsCentroid)centroids[assignedClusterID]).getDistanceTo(centroids[i])) {
                    continue;
                }
            }
            double distance = distance(centroid.getVector());
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = i;
                assignedClusterID = closestCentroidId;
            }
        }
        return giveDistCalcAccAndReset();
    }
}
