package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CentroidToTupleForFileOperator implements FlatMapFunction<Centroid[], Tuple2<String, String>> {
    @Override
    public void flatMap(Centroid[] centroids, Collector<Tuple2<String, String>> collector) throws Exception {
        for (Centroid centroid : centroids) {
            String featureString = "";
            for (double d : centroid.getVector().values) {
                featureString += d + "#";
            }
            featureString = featureString.substring(0, featureString.length() - 1);
            collector.collect(Tuple2.of(centroid.getDomain(), featureString));
        }
    }
}
