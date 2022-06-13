package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.debugging.DebugCentorids;
import edu.ntnu.alekssty.master.debugging.DebugPoints;
import edu.ntnu.alekssty.master.debugging.DebugWeightedCentorids;
import edu.ntnu.alekssty.master.utils.NewIteration;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.onlinecentroids.WeightedCentroid;
import edu.ntnu.alekssty.master.vectorobjects.onlinecentroids.WeightedNoTICentroid;
import edu.ntnu.alekssty.master.vectorobjects.onlinecentroids.WeightedTICentroid;
import edu.ntnu.alekssty.master.vectorobjects.points.NaivePoint;
import edu.ntnu.alekssty.master.vectorobjects.points.PhilipsPoint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class SequentialJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String job = "seq";
        String inputCentroidPath = parameter.get("input-centroid-path", "/tmp/experiment-results/NAIVE-offline-centroids.csv");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");
        int seedForRnd = parameter.getInt("seed-rnd-input", 0);

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, DenseVector, String>> testSource;
        if (seedForRnd==0) {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();//.filter(t->t.f0.equals("tcpefsS0"));
        } else {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getRandomPoints(seedForRnd);
        }

        DataStream<Tuple3<String, DenseVector, Integer>> centroidSource = new StreamCentroidConnector(inputCentroidPath, env).connect().getCentroids();

        DataStream<WeightedCentroid[]> centroids = centroidSource
                .keyBy(t->t.f0)
                .process(new MakeCentroids(method, k))
                .keyBy(Centroid::getDomain)
                .window(EndOfStreamWindows.get())
                .apply(new ToList())
                .map(new InitCentroids());

        DataStream<Point> points = testSource.map(new MakePoints(method));

        IterationBody body = new SeqIterationBody();
        DataStreamList iterationResult = Iterations.iterateUnboundedStreams(
                DataStreamList.of(centroids),
                DataStreamList.of(points),
                body
        );

        DataStream<Point> resultedPoints = iterationResult.get(0);
        resultedPoints.map(new PointsToTupleForFileOperator())
                .writeAsCsv(outputsPath + method + "-" + job + "-points.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        JobExecutionResult jobResult = env.execute(job);
        System.out.println(jobResult.getNetRuntime());
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
    }

    private static class SeqIterationBody implements IterationBody {

        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {
            DataStream<WeightedCentroid[]> centroids = dataStreamList.get(0);
            DataStream<Point> points = dataStreamList1.get(0);
            //centroids.process(new DebugWeightedCentorids("i c", true, true, "tcpefsS0"));
            //points.process(new DebugPoints("i p", true, true, "tcpefsS0"));

            SingleOutputStreamOperator<Point> finishedPoint = points.connect(centroids.broadcast())
                    .process(new UpdatePoints());
            //finishedPoint.process(new DebugPoints("f p", true, true, "tcpefsS0"));

            DataStream<WeightedCentroid[]> newCentroids = finishedPoint.connect(centroids.broadcast())
                    .process(new CentroidUpdater());
            //newCentroids.process(new DebugWeightedCentorids("n c", true, true, "tcpefsS0"));

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids),
                    DataStreamList.of(finishedPoint)
            );
        }

        private static class UpdatePoints extends CoProcessFunction<Point, WeightedCentroid[], Point> {

            Map<String, WeightedCentroid[]> centroidStorage = new HashMap<>();
            Map<String, List<Point>> buffer = new HashMap<>();
            IntCounter distCalcAcc = new IntCounter();

            @Override
            public void processElement1(
                    Point point,
                    CoProcessFunction<Point, WeightedCentroid[], Point>.Context context,
                    Collector<Point> collector
            ) throws Exception {
                if (!centroidStorage.containsKey(point.getDomain())) {
                    buffer.computeIfAbsent(point.getDomain(), k -> new ArrayList<>());
                    buffer.get(point.getDomain()).add(point);
                    return;
                }
                updatePoint(point, collector);
                centroidStorage.remove(point.getDomain());
            }

            private void updatePoint(Point point, Collector<Point> collector) {
                int distCalc = point.update(centroidStorage.get(point.getDomain()));
                distCalcAcc.add(distCalc);
                collector.collect(point);
            }

            @Override
            public void processElement2(
                    WeightedCentroid[] centroids,
                    CoProcessFunction<Point, WeightedCentroid[], Point>.Context context,
                    Collector<Point> collector
            ) throws Exception {
                String domain = centroids[0].getDomain();
                centroidStorage.put(domain, centroids);
                if (buffer.containsKey(domain)) {
                    if (buffer.get(domain).isEmpty()) {
                        return;
                    }
                    Point p = buffer.get(domain).remove(0);
                    updatePoint(p, collector);
                    centroidStorage.remove(domain);
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("dist-calcs-up", distCalcAcc);
            }
        }

        private static class CentroidUpdater extends CoProcessFunction<Point, WeightedCentroid[], WeightedCentroid[]> {

            IntCounter distCalcAcc = new IntCounter();
            Map<String, WeightedCentroid[]> centroidStore = new HashMap<>();
            Map<String, Point> buffer = new HashMap<>();

            @Override
            public void processElement1(Point point, CoProcessFunction<Point, WeightedCentroid[], WeightedCentroid[]>.Context context, Collector<WeightedCentroid[]> collector) throws Exception {
                String domain = point.getDomain();
                if (!centroidStore.containsKey(domain)) {
                    buffer.put(domain, point);
                    return;
                }
                collector.collect(updateCentroid(point, centroidStore.get(domain)));
                centroidStore.remove(domain);
            }

            private WeightedCentroid[] updateCentroid(Point point, WeightedCentroid[] centroids) {
                int distCalcs = centroids[point.getAssignedClusterID()].move(point.getVector());
                for (WeightedCentroid c : centroids) {
                    distCalcs += c.update(centroids);
                }
                distCalcAcc.add(distCalcs);
                return centroids;
            }

            @Override
            public void processElement2(WeightedCentroid[] weightedCentroids, CoProcessFunction<Point, WeightedCentroid[], WeightedCentroid[]>.Context context, Collector<WeightedCentroid[]> collector) throws Exception {
                String domain = weightedCentroids[0].getDomain();
                if (buffer.containsKey(domain)) {
                    Point point = buffer.get(domain);
                    collector.collect(updateCentroid(point, weightedCentroids));
                    buffer.remove(domain);
                    return;
                }
                centroidStore.put(domain, weightedCentroids);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                int iterationt = NewIteration.getInstance().getCentroid();
                getRuntimeContext().addAccumulator("distance-calculations-c"+iterationt, distCalcAcc);
            }
        }
    }

    private static class ToList implements WindowFunction<WeightedCentroid, WeightedCentroid[], String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<WeightedCentroid> iterable, Collector<WeightedCentroid[]> collector) throws Exception {
            WeightedCentroid[] out = new WeightedCentroid[0];
            for (WeightedCentroid i : iterable) {
                if (out.length == 0) {
                    out = new WeightedCentroid[1];
                    out[0] = i;
                    continue;
                }
                out = Arrays.copyOf(out, out.length+1);
                out[out.length-1] = i;
            }
            collector.collect(out);
        }
    }

    private static class InitCentroids extends RichMapFunction<WeightedCentroid[], WeightedCentroid[]> {
        IntCounter distCalcAcc = new IntCounter();

        @Override
        public WeightedCentroid[] map(WeightedCentroid[] centroids) throws Exception {
            for (WeightedCentroid c : centroids) {
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

    private static class MakeCentroids extends KeyedProcessFunction<String, Tuple3<String, DenseVector, Integer>, WeightedCentroid> {
        private final Methods method;
        private final int k;
        ValueState<Integer> nextID;

        public MakeCentroids(Methods method, int k) {
            this.method = method;
            this.k = k;
        }

        @Override
        public void processElement(
                Tuple3<String, DenseVector, Integer> centroid,
                KeyedProcessFunction<String, Tuple3<String, DenseVector, Integer>, WeightedCentroid>.Context context,
                Collector<WeightedCentroid> collector
        ) throws Exception {
            if(nextID.value() == null) {
                nextID.update(0);
            }
            WeightedCentroid out;
            switch (method) {
                case PHILIPS:
                    out = new WeightedTICentroid(centroid.f1, nextID.value(), centroid.f0, k, centroid.f2);
                    break;
                default:
                    out = new WeightedNoTICentroid(centroid.f1, nextID.value(), centroid.f0, centroid.f2);
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

    private static class MakePoints implements MapFunction<Tuple3<String, DenseVector, String>, Point> {
        private final Methods method;

        public MakePoints(Methods method) {
            this.method = method;
        }

        @Override
        public Point map(Tuple3<String, DenseVector, String> in) throws Exception {
            Point out;
            switch (method) {
                case PHILIPS:
                    out = new PhilipsPoint(in.f1, in.f0, in.f2);
                    break;
                default:
                    out = new NaivePoint(in.f1, in.f0, in.f2);
            }
            return out;
        }
    }
}
