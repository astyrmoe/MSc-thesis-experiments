package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.KMeansOfflineImprovements;
import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.NewIteration;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.centroids.NaiveCentroid;
import edu.ntnu.alekssty.master.vectorobjects.centroids.PhilipsCentroid;
import edu.ntnu.alekssty.master.vectorobjects.points.NaivePoint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.*;

public class OnlineBatchNew {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String job = "onlinebatch";
        String inputCentroidPath = parameter.get("input-centroid-path", "/tmp/experiment-results/NAIVE-offline-centroids.csv");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Tuple3<String, DenseVector, String>> testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();
        DataStream<Tuple3<String, DenseVector, Integer>> centroidSource = new StreamCentroidConnector(inputCentroidPath, env).connect().getCentroids();

        DataStream<Centroid[]> centroids = centroidSource
                .keyBy(t->t.f0)
                .process(new MakeCentroids(method, k))// TODO MAKE CENTROIDS
                .keyBy(Centroid::getDomain)
                .window(EndOfStreamWindows.get())
                .apply(new ToList())
                .map(new UpdateCentroidsInitial());

        DataStream<Point> points = testSource.map(t -> new NaivePoint(t.f1, t.f0, t.f2)); // TODO MAKE POINTS

        IterationBody body = new OnlineBatchIterationBody();
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
    }

    private static class OnlineBatchIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {

            DataStream<Centroid[]> iterationCentroids = dataStreamList.get(0);
            DataStream<Point> iterationPoints = dataStreamList.get(1);
            DataStream<Point> inputPoints = dataStreamList1.get(0);

            DataStream<Point> newIterationPoints = iterationPoints.connect(iterationCentroids.broadcast()).process(new PointUpdater2());
            DataStream<Point> pointsStillWithUs = newIterationPoints.filter(new FinishedPoints(false));
            DataStream<Centroid[]> finishedCentroids = iterationCentroids.filter(new FinishedCentoids(true));
            DataStream<Centroid[]> centroidsStillWithUs = iterationCentroids.filter(new FinishedCentoids(false));

            DataStream<Point> outputPoints = inputPoints.connect(finishedCentroids.broadcast()).process(new AssignClosestCentroid());
            DataStream<Point> inputPointsIntoIteration = outputPoints.keyBy(Point::getDomain).countWindow(100).apply(new EmitPintsIntoIteration());
            DataStream<Point> joinedPoints = pointsStillWithUs.union(inputPointsIntoIteration);

            DataStream<Tuple4<String, Integer, DenseVector, Integer>> newCentroidValues = joinedPoints.keyBy(Point::getDomain).flatMap(new NewCentoridValuesOpeator());
            DataStream<Centroid[]> newCentroids = newCentroidValues.connect(centroidsStillWithUs.broadcast()).process(new CentroidUpdater());

/*                    .
            process(new ProcessWindowFunction<Point, Tuple3<String, Integer, DenseVector>[], String, Window>() {
                Map<Integer, DenseVector> sum;
                Map<Integer, Integer> number;

                @Override
                public void process(String s, ProcessWindowFunction<Point, Tuple3<String, Integer, DenseVector>[], String, Window>.Context context, Iterable<Point> iterable, Collector<Tuple3<String, Integer, DenseVector>[]> collector) throws Exception {
                    for (Point p : iterable) {
                        if (!sum.containsKey(p.getAssignedClusterID())) {
                            sum.put(p.getAssignedClusterID(), p.getVector());
                            number.put(p.getAssignedClusterID(), 1);
                            return;
                        }
                        DenseVector newVector = sum.get(p.getAssignedClusterID());
                        for (int i = 0; i < newVector.size(); i++) {
                            newVector.values[i] += p.getVector().get(i);
                        }
                        sum.put(p.getAssignedClusterID(), newVector);
                        number.put(p.getAssignedClusterID(), number.get(p.getAssignedClusterID()) + 1);
                    }
                    Tuple3<String, Integer, DenseVector>[] out = new Tuple3[0];
                    for (Integer i : sum.keySet()) {
                        out = Arrays.copyOf(out, out.length + 1);
                        DenseVector vector = sum.get(i);
                        for (int j = 0; j < vector.size(); j++) {
                            vector.values[j] /= number.get(i);
                        }
                        out[out.length - 1] = Tuple3.of(s, i, vector);
                    }
                    collector.collect(out);
                }
            });*/

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids, pointsStillWithUs),
                    DataStreamList.of(outputPoints)
            );
        }

        private static class PointUpdater2 extends CoProcessFunction<Point, Centroid[], Point> {
            @Override
            public void processElement1(Point point, CoProcessFunction<Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {

            }

            @Override
            public void processElement2(Centroid[] centroids, CoProcessFunction<Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {

            }
        }
    }

    private static class CentroidUpdater extends BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]> implements IterationListener<Centroid[]> {

        IntCounter distCalcAcc = new IntCounter();
        Map<String, Centroid[]> centroidStore = new HashMap<>();
        Map<String, Tuple3<String, Integer, DenseVector>[]> buffer = new HashMap<>();
        List<Centroid[]> out = new ArrayList<>();

        @Override
        public void processElement(Tuple3<String, Integer, DenseVector>[] tuple3s, BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]>.ReadOnlyContext readOnlyContext, Collector<Centroid[]> collector) throws Exception {
            String domain = tuple3s[0].f0;
            if (!centroidStore.containsKey(domain)) {
                buffer.put(domain,tuple3s);
                return;
            }
            Centroid[] Centroids = centroidStore.get(domain);
            updateCentroid(tuple3s, Centroids);
            centroidStore.remove(domain);
        }

        private void updateCentroid(Tuple3<String, Integer, DenseVector>[] tuple3s, Centroid[] centroids) {
            for (Centroid Centroid : centroids) {
                for (Tuple3<String, Integer, DenseVector> tuple3 : tuple3s) {
                    if (tuple3.f1 == Centroid.getID()) {
                        int distCalcs = Centroid.move(tuple3.f2);
                        distCalcAcc.add(distCalcs);
                        break;
                    }
                }
            }
            for (Centroid Centroid : centroids) {
                int distCalcs = Centroid.update(centroids);
                distCalcAcc.add(distCalcs);
            }
            out.add(centroids);
        }

        @Override
        public void processBroadcastElement(Centroid[] Centroids, BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]>.Context context, Collector<Centroid[]> collector) throws Exception {
            String domain = Centroids[0].getDomain();
            if (buffer.containsKey(domain)) {
                Tuple3<String, Integer, DenseVector>[] tuple3s = buffer.get(domain);
                updateCentroid(tuple3s, Centroids);
                buffer.remove(domain);
                return;
            }
            centroidStore.put(domain, Centroids);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int iterationt = NewIteration.getInstance().getCentroid();
            getRuntimeContext().addAccumulator("distance-calculations-c"+iterationt, distCalcAcc);
        }

        @Override
        public void onEpochWatermarkIncremented(int i, IterationListener.Context context, Collector<Centroid[]> collector) throws Exception {
            assert buffer.isEmpty();
            assert centroidStore.isEmpty();
            for (Centroid[] c : out) {
                collector.collect(c);
            }
            out.clear();
        }

        @Override
        public void onIterationTerminated(IterationListener.Context context, Collector<Centroid[]> collector) throws Exception {
        }
    }
}
