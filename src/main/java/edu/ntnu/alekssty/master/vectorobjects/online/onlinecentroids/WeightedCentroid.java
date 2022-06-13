package edu.ntnu.alekssty.master.vectorobjects.online.onlinecentroids;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;

public interface WeightedCentroid extends Centroid {
    public int getWeight();
}
