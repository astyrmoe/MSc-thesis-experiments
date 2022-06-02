package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.vectorobjects.Point;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class PointsToTupleForFileOperator implements MapFunction<Point, Tuple3<String, Integer, String>> {
    @Override
    public Tuple3<String, Integer, String> map(Point row) throws Exception {
        return Tuple3.of(row.getDomain(), row.getAssignedClusterID(), row.getLabel());
    }
}
