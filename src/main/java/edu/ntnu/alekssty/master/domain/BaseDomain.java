package edu.ntnu.alekssty.master.domain;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.ml.linalg.DenseVector;

public class BaseDomain implements Domain {

    boolean finished;
    String domain;
    Centroid[] centroids;

    public BaseDomain(String domain, Centroid[] centroids) {
        this.domain = domain;
        this.centroids = centroids;
        finished = false;
    }

    @Override
    public String getDomain() {
        return domain;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void setFinished() {
        finished = true;
    }

    @Override
    public void update(DenseVector[] vectors) {
        for (Centroid centroid : centroids) {
            centroid.move(vectors[centroid.getID()]);
        }
        for (Centroid centroid : centroids) {
            centroid.update(centroids);
        }
    }

    @Override
    public Centroid getCentroid(int i) {
        return centroids[i];
    }
}
