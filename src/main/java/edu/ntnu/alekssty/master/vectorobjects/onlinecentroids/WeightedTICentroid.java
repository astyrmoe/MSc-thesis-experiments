package edu.ntnu.alekssty.master.vectorobjects.onlinecentroids;

import org.apache.flink.ml.linalg.DenseVector;

public class WeightedTICentroid extends TICentroid implements WeightedCentroid {

    int weight;

    public WeightedTICentroid(DenseVector vector, int ID, String domain, int k, Integer f2) {
        super(vector, ID, domain, k);
        //weight = f2;
        weight = 1000;
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

    @Override
    public int getWeight() {
        return weight;
    }
}
