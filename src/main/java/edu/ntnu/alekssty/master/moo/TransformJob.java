package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.NewIteration;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import edu.ntnu.alekssty.master.vectorobjects.points.NaivePoint;
import edu.ntnu.alekssty.master.vectorobjects.points.PhilipsPoint;
import edu.ntnu.alekssty.master.vectorobjects.onlinecentroids.NoTICentroid;
import edu.ntnu.alekssty.master.vectorobjects.onlinecentroids.TICentroid;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class TransformJob {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String job = "transform";
        String inputCentroidPath = parameter.get("input-centroid-path", "/tmp/experiment-results/NAIVE-offline-centroids.csv");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");
        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Tuple3<String, DenseVector, String>> testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();
        DataStream<Tuple3<String, DenseVector, Integer>> centroidSource = new StreamCentroidConnector(inputCentroidPath, env).connect().getCentroids();

        DataStream<Centroid[]> centroids = centroidSource
                .keyBy(t->t.f0)
                .process(new MakeCentroids(method, k))
                .keyBy(Centroid::getDomain)
                .window(EndOfStreamWindows.get())
                .apply(new ToList())
                .map(new UpdateCentroids());

        MapStateDescriptor<String, Centroid[]> centroidStateDesc = new MapStateDescriptor<String, Centroid[]>("centroid-state", String.class, Centroid[].class);
        testSource
                .map(new MakePoints(method))
                .keyBy(Point::getDomain)
                .connect(centroids.broadcast(centroidStateDesc))
                .process(new UpdatePoints(centroidStateDesc))
                .map(new PointsToTupleForFileOperator())
                .writeAsCsv(outputsPath + method + "-" + job + "-points.csv", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult jobRes = env.execute();
        System.out.println(jobRes.getNetRuntime());
        System.out.println(jobRes.getJobExecutionResult());
    }

    private static class MakeCentroids extends KeyedProcessFunction<String, Tuple3<String, DenseVector, Integer>, Centroid> {
        private final Methods method;
        private final int k;
        ValueState<Integer> nextID;

        public MakeCentroids(Methods method, int k) {
            this.method = method;
            this.k = k;
        }

        @Override
        public void processElement(Tuple3<String, DenseVector, Integer> centroid, KeyedProcessFunction<String, Tuple3<String, DenseVector, Integer>, Centroid>.Context context, Collector<Centroid> collector) throws Exception {
            if(nextID.value() == null) {
                nextID.update(0);
            }
            Centroid out;
            switch (method) {
                case PHILIPS:
                    out = new TICentroid(centroid.f1, nextID.value(), centroid.f0, k);
                    break;
                default:
                    out = new NoTICentroid(centroid.f1, nextID.value(), centroid.f0);
            }
            collector.collect(out);
            nextID.update(nextID.value()+1);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            nextID = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("nextID", Integer.class));
        }
    }

    private static class ToList implements WindowFunction<Centroid, Centroid[], String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Centroid> iterable, Collector<Centroid[]> collector) throws Exception {
            Centroid[] out = new Centroid[0];
            for (Centroid i : iterable) {
                if (out.length == 0) {
                    out = new Centroid[1];
                    out[0] = i;
                    continue;
                }
                out = Arrays.copyOf(out, out.length+1);
                out[out.length-1] = i;
            }
            collector.collect(out);
        }
    }

    private static class UpdateCentroids extends RichMapFunction<Centroid[], Centroid[]> {
        IntCounter distCalcAcc = new IntCounter();

        @Override
        public Centroid[] map(Centroid[] centroids) throws Exception {
            for (Centroid c : centroids) {
                int distCalcs = c.update(centroids);
                distCalcAcc.add(distCalcs);
            }
            return centroids;
        }

        @Override
        public void open(Configuration s) {
            getRuntimeContext().addAccumulator("distance-calculations-c"+ NewIteration.getInstance().getCentroid(), distCalcAcc);
        }
    }

    private static class MakePoints implements MapFunction<Tuple3<String, DenseVector, String>, Point> {
        private final Methods method;

        public MakePoints(Methods method) {
            this.method = method;
        }

        @Override
        public Point map(Tuple3<String, DenseVector, String> in) throws Exception {
            Point out;
            switch (method) {
                case NAIVE:
                    out = new NaivePoint(in.f1, in.f0, in.f2);
                    break;
                case PHILIPS:
                    out = new PhilipsPoint(in.f1, in.f0, in.f2);
                    break;
                default:
                    out = new NaivePoint(in.f1, in.f0, in.f2);
            }
            return out;
        }
    }

    private static class UpdatePoints extends KeyedBroadcastProcessFunction<String, Point, Centroid[], Point> {

        final Map<String, List<Point>> buffer;
        private final MapStateDescriptor<String, Centroid[]> centroidStateDesc;
        IntCounter distCalcAcc;

        public UpdatePoints(MapStateDescriptor<String, Centroid[]> centroidStateDesc) {
            this.centroidStateDesc = centroidStateDesc;
            buffer = new HashMap<>();
            distCalcAcc = new IntCounter();
        }

        @Override
        public void open(Configuration c) {
            getRuntimeContext().addAccumulator("distance-calculations-p"+ NewIteration.getInstance().getPoint(), distCalcAcc);
        }

        @Override
        public void processElement(Point in, KeyedBroadcastProcessFunction<String, Point, Centroid[], Point>.ReadOnlyContext readOnlyContext, Collector<Point> collector) throws Exception {
            String domain = in.getDomain();
            if (!readOnlyContext.getBroadcastState(centroidStateDesc).contains(domain)) {
                if (!buffer.containsKey(domain)) {
                    buffer.put(domain, new ArrayList<>());
                }
                buffer.get(domain).add(in);
                return;
            }
            collector.collect(assignClosestCentorid(in, readOnlyContext.getBroadcastState(centroidStateDesc).get(domain)));
        }

        private Point assignClosestCentorid(Point in, Centroid[] centroids) {
            int distAcc = in.update(centroids);
            distCalcAcc.add(distAcc);
            return in;
        }

        @Override
        public void processBroadcastElement(Centroid[] centroids, KeyedBroadcastProcessFunction<String, Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
            String domain = centroids[0].getDomain();
            context.getBroadcastState(centroidStateDesc).put(domain, centroids);
            if (buffer.containsKey(domain)) {
                for (Point point : buffer.get(domain)) {
                    collector.collect(assignClosestCentorid(point, centroids));
                }
                buffer.remove(domain);
            }
        }

    }
}
