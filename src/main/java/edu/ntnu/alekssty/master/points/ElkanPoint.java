package edu.ntnu.alekssty.master.points;

import edu.ntnu.alekssty.master.centroids.Centroid;
import edu.ntnu.alekssty.master.centroids.ElkanCentroid;
import org.apache.flink.ml.linalg.DenseVector;

import static java.lang.Double.max;

public class ElkanPoint extends BasePoint implements Point {

    public Double upperBound;
    public DenseVector lowerBounds;
    public boolean updateUb;

    public ElkanPoint(DenseVector vector, String domain) {
        super(vector, domain);
        updateUb = true;
    }

    @Override
    public void update(Centroid[] centroids) {
        int k = centroids.length;
        if (assignedClusterID == -1) {
            this.lowerBounds = new DenseVector(k);
            boolean[] skipStatus = new boolean[k];
            for (int i =0;i<k;i++) {
                this.lowerBounds.values[i] = 0;
                skipStatus[i] = false;
            }
            double minDistance = Double.MAX_VALUE;
            for (int j = 0; j < k; j++) {
                if (skipStatus[j]) {continue;}
                double distance = distance(centroids[j].getVector());
                this.lowerBounds.values[j] = distance;
                if (distance < minDistance) {
                    minDistance = distance;
                    this.upperBound = minDistance;
                    this.assignedClusterID = j;
                    for (int z = j + 1; z < k; z++) {
                        ElkanCentroid jC = (ElkanCentroid) centroids[j];
                        double distToZCentroid;
                        if (jC.distanceToOtherCentroids.get(z) != 0) {
                            distToZCentroid = jC.distanceToOtherCentroids.get(z);
                        } else {
                            distToZCentroid = jC.distance(centroids[z].getVector());
                            jC.distanceToOtherCentroids.values[z] = distToZCentroid;
                            ((ElkanCentroid)centroids[z]).distanceToOtherCentroids.values[j] = distToZCentroid;
                        }
                        if (distToZCentroid >= 2 * distance) {
                            skipStatus[z] = true;
                        }
                    }
                }
            }
            return;
        }
        for (int j = 0; j < k; j++) {
            this.lowerBounds.values[j] = max(this.lowerBounds.values[j] - centroids[j].getMovement(), 0);
        }
        this.upperBound = this.upperBound + centroids[this.assignedClusterID].getMovement();
        updateUb = true;
        if (this.upperBound <= ((ElkanCentroid) centroids[this.assignedClusterID]).halfDistToClosestCentroid) {
            return;
        }
        double d1, d2 = 0;
        for (int j = 0; j < k; j++) {
            if (j != this.assignedClusterID &&
                    this.upperBound > this.lowerBounds.values[j] &&
                    this.upperBound > 0.5 * ((ElkanCentroid)centroids[this.assignedClusterID]).distanceToOtherCentroids.get(j)) {
                if (updateUb) {
                    d1 = distance(centroids[this.assignedClusterID].getVector());
                    this.upperBound = d1;
                    this.lowerBounds.values[this.assignedClusterID] = d1;
                    updateUb = false;
                }
                d1 = this.upperBound;
                if (d1 > this.lowerBounds.values[j] ||
                        d1 > 0.5 * ((ElkanCentroid)centroids[this.assignedClusterID]).distanceToOtherCentroids.get(j)) {
                    d2 = distance(centroids[j].getVector());
                    this.lowerBounds.values[j] = d2;
                    if (d2 < d1) {
                        this.assignedClusterID = j;
                        this.upperBound = d2;
                        updateUb = false;
                    }
                }
            }
        }
    }
}
