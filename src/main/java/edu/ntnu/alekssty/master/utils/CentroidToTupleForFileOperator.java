package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CentroidToTupleForFileOperator implements FlatMapFunction<OfflineCentroid[], Tuple3<String, String, Integer>> {
    @Override
    public void flatMap(OfflineCentroid[] centroids, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
        for (OfflineCentroid centroid : centroids) {
            String featureString = "";
            for (double d : centroid.getVector().values) {
                featureString += d + "#";
            }
            featureString = featureString.substring(0, featureString.length() - 1);
            collector.collect(Tuple3.of(centroid.getDomain(), featureString, centroid.getCardinality()));
        }
    }
}
