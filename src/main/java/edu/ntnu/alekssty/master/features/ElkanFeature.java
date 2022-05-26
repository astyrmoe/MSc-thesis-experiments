package edu.ntnu.alekssty.master.features;

import edu.ntnu.alekssty.master.centroids.Centroid;
import edu.ntnu.alekssty.master.centroids.ElkanCentroid;
import org.apache.flink.ml.linalg.DenseVector;

import static java.lang.Double.max;

public class ElkanFeature extends BaseFeature implements Feature {

    public Double upperBound;
    public DenseVector lowerBounds;

    public ElkanFeature(DenseVector vector, String domain) {
        super(vector, domain);
    }

    @Override
    public int update(Centroid[] centroids) {
        int k = centroids.length;
        if (assignedClusterID == -1) {
            this.lowerBounds = new DenseVector(k);
            double minDistance = Double.MAX_VALUE;
            boolean[] skipStatus = new boolean[k];
            for (int j = 0; j < k; j++) {
                skipStatus[j] = false;
            }
            for (int j = 0; j < k; j++) {
                if (skipStatus[j]) {
                    continue;
                }
                double distance = distance(centroids[j].getVector());
                this.lowerBounds.values[j] = distance;
                if (distance < minDistance) {
                    minDistance = distance;
                    this.upperBound = minDistance;
                    this.assignedClusterID = j;
                    for (int z = j + 1; z < k; z++) {
                        if (centroids[j].distance(centroids[z].getVector()) >= 2 * distance) {
                            skipStatus[z] = true;
                        }
                    }
                }
            }
            return giveDistCalcAccAndReset();
        }
        boolean allCentroidsFinished = true;
        for (Centroid centorid : centroids) {
            if (centorid.getMovement() != 0) {
                allCentroidsFinished = false;
                break;
            }
        }
        if (allCentroidsFinished) {
            this.finished = true;
            return giveDistCalcAccAndReset();
        }
        for (int j = 0; j < k; j++) {
            this.lowerBounds.values[j] = max(this.lowerBounds.values[j] - centroids[j].getMovement(), 0);
        }
        this.upperBound = this.upperBound + centroids[this.assignedClusterID].getMovement();
        boolean updateUb = true;
        double d1, d2 = 0;
        if (this.upperBound <= centroids[this.assignedClusterID].distance(centroids[((ElkanCentroid) centroids[this.assignedClusterID]).findOtherCloseCentroidID()].getVector())) {
            return giveDistCalcAccAndReset();
        }
        for (int j = 0; j < k; j++) {
            if (j != this.assignedClusterID && this.upperBound > this.lowerBounds.values[j] && this.upperBound > 0.5 * centroids[this.assignedClusterID].distance(centroids[j].getVector())) {
                if (updateUb) {
                    d1 = distance(centroids[this.assignedClusterID].getVector());
                    this.upperBound = d1;
                    this.lowerBounds.values[this.assignedClusterID] = d1;
                    updateUb = false;
                }
                d1 = this.upperBound;
                if (d1 > this.lowerBounds.values[j] || d1 > centroids[this.assignedClusterID].distance(centroids[j].getVector())) {
                    d2 = distance(centroids[this.assignedClusterID].getVector());
                    this.lowerBounds.values[j] = d2;
                    if (d2 < d1) {
                        this.assignedClusterID = j;
                        this.upperBound = d2;
                        updateUb = false;
                    }
                }
            }
        }
        return giveDistCalcAccAndReset();
    }
}
