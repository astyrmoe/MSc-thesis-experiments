package edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.PhilipsCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class PhilipsPoint extends BaseOfflinePoint implements OfflinePoint {

    public PhilipsPoint(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
    }

    @Override
    public int update(Centroid[] centroids) {
        if (assignedClusterID == -1) {
            assignedClusterID = 0;
        }
        double distToAssigned = distance(centroids[assignedClusterID].getVector());
        for (int i = 0; i < centroids.length; i++) {
            if (i == assignedClusterID) {
                continue;
            }
            Centroid centroid = centroids[i];
            if (2 * distToAssigned <= ((PhilipsCentroid) centroids[assignedClusterID]).getDistanceTo((OfflineCentroid) centroid)) {
                continue;
            }
            double newDist = distance(centroid.getVector());
            if (newDist < distToAssigned) {
                distToAssigned = newDist;
                assignedClusterID = i;
            }
        }
        return giveDistCalcAccAndReset();
    }
}
