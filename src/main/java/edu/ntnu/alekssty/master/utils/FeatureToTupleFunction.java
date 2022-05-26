package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.features.Feature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FeatureToTupleFunction implements MapFunction<Feature, Tuple2<Integer, String>> {
    @Override
    public Tuple2<Integer, String> map(Feature feature) throws Exception {
        return Tuple2.of(feature.getAssignedClusterID(), ComputeId.compute(feature.getVector(), feature.getDomain()));
    }
}
