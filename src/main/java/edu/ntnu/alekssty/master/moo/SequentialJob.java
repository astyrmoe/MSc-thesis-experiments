package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.NewIteration;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.ticentroids.WeightedNoTICentroid;
import edu.ntnu.alekssty.master.vectorobjects.ticentroids.WeightedTICentroid;
import edu.ntnu.alekssty.master.vectorobjects.points.NaivePoint;
import edu.ntnu.alekssty.master.vectorobjects.points.PhilipsPoint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Tuple3<String, DenseVector, String>> testSource;
        if (seedForRnd==0) {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();
        } else {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getRandomPoints(seedForRnd);
        }

        DataStream<Tuple3<String, DenseVector, Integer>> centroidSource = new StreamCentroidConnector(inputCentroidPath, env).connect().getCentroids();

        DataStream<Centroid[]> centroids = centroidSource
                .keyBy(t->t.f0)
                .process(new MakeCentroids(method, k))
                .keyBy(Centroid::getDomain)
                .window(EndOfStreamWindows.get())
                .apply(new ToList())
                .map(new UpdateCentroidsInitial());

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
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
    }

    private static class SeqIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {
            DataStream<Centroid[]> centroids = dataStreamList.get(0);
            DataStream<Point> points = dataStreamList1.get(0);

            OutputTag<Centroid[]> o = new OutputTag<Centroid[]>("newCentroids"){};

            SingleOutputStreamOperator<Point> finishedPoint = points.connect(centroids.broadcast()).process(new UpdatePoints());

            return new IterationBodyResult(
                    DataStreamList.of(finishedPoint.getSideOutput(o)),
                    DataStreamList.of(finishedPoint)
            );
        }

        private static class UpdatePoints extends CoProcessFunction<Point, Centroid[], Point> {

            Map<String, Centroid[]> centroidStorage = new HashMap<>();
            Map<String, List<Point>> buffer = new HashMap<>();
            IntCounter distCalcAcc = new IntCounter();

            @Override
            public void processElement1(Point point, CoProcessFunction<Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
                if (!centroidStorage.containsKey(point.getDomain())) {
                    buffer.computeIfAbsent(point.getDomain(), k -> new ArrayList<>());
                    buffer.get(point.getDomain()).add(point);
                    return;
                }
                int distCalc = point.update(centroidStorage.get(point.getDomain()));
                distCalcAcc.add(distCalc);
                centroidStorage.get(point.getDomain())[point.getAssignedClusterID()].move(point.getVector());
                for (Centroid c : centroidStorage.get(point.getDomain())) {
                    int distCalcs = c.update(centroidStorage.get(point.getDomain()));
                    distCalcAcc.add(distCalcs);
                }
                collector.collect(point);
            }

            @Override
            public void processElement2(Centroid[] centroids, CoProcessFunction<Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
                centroidStorage.put(centroids[0].getDomain(), centroids);
                if (buffer.containsKey(centroids[0].getDomain())) {
                    for (Point p : buffer.get(centroids[0].getDomain())) {
                        processElement1(p, context, collector);
                    }
                    buffer.remove(centroids[0].getDomain());
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("dist-calcs-up", distCalcAcc);
            }
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

    private static class UpdateCentroidsInitial extends RichMapFunction<Centroid[], Centroid[]> {
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
