package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.NewIteration;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.centroids.NaiveCentroid;
import edu.ntnu.alekssty.master.vectorobjects.centroids.PhilipsCentroid;
import edu.ntnu.alekssty.master.vectorobjects.points.BasePoint;
import edu.ntnu.alekssty.master.vectorobjects.points.NaivePoint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class SequentialJob {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String job = "sequential";
        String inputCentroidPath = parameter.get("input-centroid-path", "/tmp/experiment-results/NAIVE-offline-centroids.csv");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Tuple3<String, DenseVector, String>> testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();
        DataStream<Tuple2<String, DenseVector>> centroidSource = new StreamCentroidConnector(inputCentroidPath, env).connect().getCentroids();

        DataStream<Centroid[]> centroids = centroidSource.keyBy(t->t.f0).process(new KeyedProcessFunction<String, Tuple2<String, DenseVector>, Centroid>() {
            ValueState<Integer> nextID;
            @Override
            public void processElement(Tuple2<String, DenseVector> centroid, KeyedProcessFunction<String, Tuple2<String, DenseVector>, Centroid>.Context context, Collector<Centroid> collector) throws Exception {
                if(nextID.value() == null) {
                    nextID.update(0);
                }
                Centroid out;
                switch (method) {
                    case NAIVE:
                        out = new NaiveCentroid(centroid.f1, nextID.value(), centroid.f0);
                        break;
                    case PHILIPS:
                        out = new PhilipsCentroid(centroid.f1, nextID.value(), centroid.f0, k);
                        break;
                    default:
                        out = new NaiveCentroid(centroid.f1, nextID.value(), centroid.f0);
                }
                collector.collect(out);
                nextID.update(nextID.value()+1);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                nextID = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("nextID", Integer.class));
            }
        }).keyBy(Centroid::getDomain).window(EndOfStreamWindows.get()).apply(new WindowFunction<Centroid, Centroid[], String, TimeWindow>() {
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
        }).map(new RichMapFunction<Centroid[], Centroid[]>() {
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
        });

        DataStream<Point> points = testSource.map(t -> new NaivePoint(t.f1, t.f0, t.f2));

        IterationBody body = new SeqIterationBody();
        DataStreamList iterationResult = Iterations.iterateUnboundedStreams(
                DataStreamList.of(centroids),
                DataStreamList.of(points),
                body
        );

        DataStream<Point> resultedPoints = iterationResult.get(0);
        resultedPoints.map(new PointsToTupleForFileOperator()).writeAsCsv(outputsPath + method + "-" + job + "-points.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        JobExecutionResult jobResult = env.execute(job);
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
    }

    private static class SeqIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {
            DataStream<Centroid[]> centroids = dataStreamList.get(0);
            DataStream<Point> points = dataStreamList1.get(0);

            OutputTag<Centroid[]> o = new OutputTag<Centroid[]>("newCentroids"){};

            SingleOutputStreamOperator<Point> finishedPoint = points.connect(centroids.broadcast()).keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point point) throws Exception {
                    return point.getDomain();
                }
            }, new KeySelector<Centroid[], String>() {
                @Override
                public String getKey(Centroid[] centroids) throws Exception {
                    return centroids[0].getDomain();
                }
            }).process(new KeyedCoProcessFunction<String, Point, Centroid[], Point>() {

                Map<String, Centroid[]> centroidStorage = new HashMap<>();
                Map<String, List<Point>> buffer = new HashMap<>();

                @Override
                public void processElement1(Point point, KeyedCoProcessFunction<String, Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
                    if (!centroidStorage.containsKey(point.getDomain())) {
                        buffer.computeIfAbsent(point.getDomain(), k -> new ArrayList<>());
                        buffer.get(point.getDomain()).add(point);
                        return;
                    }
                    point.update(centroidStorage.get(point.getDomain()));
                    centroidStorage.get(point.getDomain())[point.getAssignedClusterID()].move(point.getVector());
                    for (Centroid c : centroidStorage.get(point.getDomain())) {
                        c.update(centroidStorage.get(point.getDomain()));
                    }
                    collector.collect(point);
                }

                @Override
                public void processElement2(Centroid[] centroids, KeyedCoProcessFunction<String, Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
                    centroidStorage.put(centroids[0].getDomain(), centroids);
                    if (buffer.containsKey(centroids[0].getDomain())) {
                        for (Point p : buffer.get(centroids[0].getDomain())) {
                            processElement1(p, context, collector);
                        }
                        buffer.remove(centroids[0].getDomain());
                    }
                }
            }).returns(Point.class);

            return new IterationBodyResult(
                    DataStreamList.of(finishedPoint.getSideOutput(o)),
                    DataStreamList.of(finishedPoint)
            );
        }
    }
}
