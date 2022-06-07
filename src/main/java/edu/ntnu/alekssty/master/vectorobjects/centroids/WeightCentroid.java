package edu.ntnu.alekssty.master.vectorobjects.centroids;

import org.apache.flink.ml.linalg.DenseVector;

public class WeightCentroid extends NaiveCentroid {

    int weight;

    public WeightCentroid(DenseVector vector, int ID, String domain) {
        super(vector, ID, domain);
        weight = 1; // TODO
    }

    @Override
    public int move(DenseVector newPoint) {
        DenseVector newMean = new DenseVector(newPoint.size());
        for (int i = 0; i < newMean.size(); i++) {
            newMean.values[i] = (this.vector.get(i)*weight+newPoint.get(i)) / (weight+1);
        }
        weight++;
        return super.move(newMean);
    }
}
