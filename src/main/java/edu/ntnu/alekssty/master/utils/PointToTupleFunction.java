package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.points.Point;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PointToTupleFunction implements MapFunction<Point, Tuple2<Integer, String>> {
    @Override
    public Tuple2<Integer, String> map(Point point) throws Exception {
        return Tuple2.of(point.getAssignedClusterID(), ComputeId.compute(point.getVector(), point.getDomain()));
    }
}
