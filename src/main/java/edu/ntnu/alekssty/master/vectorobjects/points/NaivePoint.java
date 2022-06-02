package edu.ntnu.alekssty.master.vectorobjects.points;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import org.apache.flink.ml.linalg.DenseVector;

public class NaivePoint extends BasePoint implements Point {

    public NaivePoint(DenseVector vector, String domain, String label) {
        super(vector, domain, label);
    }

    @Override
    public int update(Centroid[] centroids) {
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
