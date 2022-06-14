package edu.ntnu.alekssty.master.vectorobjects.online.onlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.BaseCentroid;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.PhilipsCentroid;
import org.apache.flink.ml.linalg.DenseVector;

public class TICentroid extends BaseCentroid implements Centroid {

    public DenseVector distToOthers;

    public TICentroid(DenseVector vector, int ID, String domain, int k) {
        super(vector, ID, domain);
        distToOthers = new DenseVector(k);
    }

    public double getDistanceTo(Centroid c) {
        if (distToOthers.get(c.getID()) == 0) {
            double dist = distance(c.getVector());
            distToOthers.values[c.getID()] = dist;
            ((TICentroid)c).distToOthers.values[this.ID] = dist;
            return dist;
        }
        return distToOthers.get(c.getID());
    }


    @Override
    public int move(DenseVector vector, int cardinality) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    @Override
    public int move(DenseVector vector) {
        this.vector = vector;
        return giveDistCalcAccAndReset();
    }

    @Override
    public int update(Centroid[] centroids) {
        for (Centroid c : centroids) {
            if (c.getID() <= this.ID) {continue;}
            double dist = distance(c.getVector());
            distToOthers.values[c.getID()] = dist;
            ((TICentroid)c).distToOthers.values[this.ID] = dist;
        }
        return giveDistCalcAccAndReset();
    }
}
