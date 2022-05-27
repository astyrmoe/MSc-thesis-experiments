/*package edu.ntnu.alekssty.features;

import edu.ntnu.alekssty.centroids.Centroid;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.linalg.DenseVector;

public class DrakeFeature extends BaseFeature implements Feature {

    double upperBound;
    Tuple2<Integer, Double>[] lowerBounds;
    int nextB;

    public DrakeFeature(DenseVector vector, Tuple3<String, String, String> domain) {
        super(vector, domain);
    }

    @Override
    public void update(Centroid[] centroids) {
        int b;
        if (assignedClusterID == -1) {
            b = findStartingB(centroids.length);
            upperBound = Double.MAX_VALUE;
            for (int j = 0; j < b; j++) {
                lowerBounds[j] = Tuple2.of(-1,0D);
            }
        } else {
            b = nextB;
            upperBound += centroids[assignedClusterID].getMovement();
            lowerBounds[b].f1 -= getBiggestMovement(centroids).f1;
            for (int j = b-1; j>0; j--) {
                lowerBounds[j] = Double.min(lowerBounds[j].f1-getMovementOfJthClosestToPoint(centroids, j).f1, lowerBounds[j+1].f1);
            }
        }
        int m = b;
        int j = argmax();
        if (j < b) {
            // TODO
        }
        else if (j == b || lowerBounds[b].f1 < upperBound){
            // TODO
        }
        m = Integer.max(m, j);
        nextB = Integer.max(b, m);
    }

    private int argmax() {
        int b = lowerBounds.length;
        for (int i = b; i > 0; i--) {
            if (upperBound <= lowerBounds[i - 1].f1) {
                return lowerBounds[i - 1].f0;
            }
        }
        return b;
    }

    private Tuple2<Integer, Double> getMovementOfJthClosestToPoint(Centroid[] centroids, int j) {
        return null;
    }

    private Tuple2<Integer, Double> getBiggestMovement(Centroid[] centroids) {
        double bM = 0D;
        for (Centroid centroid : centroids) {
            if (centroid.getMovement() > bM) {
                bM = centroid.getMovement();
            }
        }
        return Tuple2.of(-1, bM);
    }

    private int findStartingB(int k) {
        if (k > 8) {
            return (int) Math.floor(k/8);
        }
        return (int) Math.ceil(k/2);
    }
}
*/