package edu.ntnu.alekssty.master.batch;

import edu.ntnu.alekssty.master.debugging.DebugCentorids;
import edu.ntnu.alekssty.master.debugging.DebugPoints;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import edu.ntnu.alekssty.master.vectorobjects.centroids.*;
import edu.ntnu.alekssty.master.vectorobjects.points.*;
import edu.ntnu.alekssty.master.utils.NewIteration;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class KMeansOfflineImprovements implements KMeansParams<KMeansOfflineImprovements> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeansOfflineImprovements() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }


    public DataStreamList fit(DataStream<Tuple3<String, DenseVector, String>> input, Methods method) {

        System.out.println("Method used: " + method);

        // TODO Make operator its own class
        DataStream<Point> points = input.map(t -> (pointMaker(method,t.f1,t.f0,t.f2))).name("FeatureMaker");

        //DataStream<Centroid[]> initCentroids = points.keyBy(Point::getDomain).process(new MakeFirstFeaturesCentroids(method, getK()));
        DataStream<Centroid[]> initCentroids = selectRandomCentroids(points, getK(), getSeed(), method);
        initCentroids.process(new DebugCentorids("C Init centroids", true, true, "tcprjeS0"));

        IterationConfig config = IterationConfig.newBuilder()
                .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                .build();

        IterationBody body = new KMeansIterationBody(getK());

        DataStreamList iterationResult = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(initCentroids, points),
                ReplayableDataStreamList.notReplay(),
                config,
                body
        );

        DataStream<Centroid[]> iterationResultsCentroids = iterationResult.get(0);
        //iterationResultsCentroids.process(new DebugCentorids("C Iteration result", true, true));
        DataStream<Point> iterationsResultFeatures = iterationResult.get(1);
        //iterationsResultFeatures.process(new DebugPoints("F Iteration result", true, true));

        return DataStreamList.of(
                iterationResultsCentroids,
                iterationsResultFeatures
        );
    }

    private static class KMeansIterationBody implements IterationBody {

        final int k;
        final MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
                "centroids",
                String.class,
                Centroid[].class);

        public KMeansIterationBody(int k) {
            this.k = k;
            centroidStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        }

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Centroid[]> centroids = variableStreams.get(0);
            //centroids.process(new DebugCentorids("C Into iteration", true, true));
            DataStream<Point> points = variableStreams.get(1);
            //points.process(new DebugPoints("F Into iteration", true, true));

            DataStream<Integer> terminationCriteria = centroids.flatMap(new FlatMapFunction<Centroid[], Integer>() {
                @Override
                public void flatMap(Centroid[] centroids, Collector<Integer> collector) throws Exception {
                    for (Centroid centroid : centroids) {
                        if (!centroid.isFinished()) {
                            collector.collect(0);
                            break;
                        }
                    }
                }
            }).name("Termination criteria");

            DataStream<Point> newPoints = points.connect(centroids.broadcast()).transform("up", TypeInformation.of(Point.class), new UpdatePointsTransform());
            //newPoints.process(new DebugPoints("F New point", true, true));

            DataStream<Centroid[]> finalCentroids = centroids.filter(new CentroidFilterFunction(true)).name("Filter finished centroids");
            DataStream<Point> finalPoints = newPoints.filter(new PointFilterFunction(true)).name("Filter finished points");
            DataStream<Centroid[]> centroidsNotFinished = centroids.filter(new CentroidFilterFunction(false)).name("Filter not finished centroids");
            DataStream<Point> pointsNotFinished = newPoints.filter(new PointFilterFunction(false)).name("Filter not finished points");

            DataStream<Tuple4<String, Integer, DenseVector, Integer>[]> newCentroidValues = pointsNotFinished.keyBy(Point::getDomain).flatMap(new NewCentroidValuesOperator());

            DataStream<Centroid[]> newCentroids = newCentroidValues
                    .connect(centroidsNotFinished.broadcast(centroidStateDescriptor))
                    .process(new CentroidUpdater()).name("Centroid updater");
            //newCentroids.process(new DebugCentorids("C New centroids", true, true));

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids, pointsNotFinished),
                    DataStreamList.of(finalCentroids,finalPoints),
                    terminationCriteria
            );
        }
    }

    private static Point pointMaker(Methods type, DenseVector features, String domain, String label) throws Exception {
        Point out;
        switch (type) {
            case ELKAN:
                out = new ElkanPoint(features, domain, label);
                break;
            case PHILIPS:
                out = new PhilipsPoint(features, domain, label);
                break;
            case NAIVE:
                out = new NaivePoint(features, domain, label);
                break;
            case HAMERLY:
                out = new HamerlyPoint(features, domain, label);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        return out;
    }

    private static class MakeFirstFeaturesCentroids extends KeyedProcessFunction<String, Point, Centroid[]> {
        private final Methods method;
        ValueState<Centroid[]> centroid;
        private final int k;

        public MakeFirstFeaturesCentroids(Methods method, int k) {
            this.method = method;
            this.k = k;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            centroid = getRuntimeContext().getState(new ValueStateDescriptor<Centroid[]>("d", Centroid[].class));
        }

        @Override
        public void processElement(Point point, KeyedProcessFunction<String, Point, Centroid[]>.Context context, Collector<Centroid[]> collector) throws Exception {
            if (centroid.value() == null) {
                centroid.update(new Centroid[0]);
            }
            int centroidsSoFar = centroid.value().length;
            if (centroidsSoFar >= k) {
                return;
            }
            Centroid[] newArray = Arrays.copyOf(centroid.value(), centroidsSoFar+1);
            newArray[centroidsSoFar] = makeCentroid(point, method, centroidsSoFar);
            if (centroidsSoFar + 1 == k) {
                collector.collect(newArray);
            }
            centroid.update(newArray);
        }

        private Centroid makeCentroid(Point point, Methods method, int i) {
            Centroid out;
            switch (method) {
                case ELKAN:
                    out = new ElkanCentroid(point.getVector(), i, point.getDomain(), k);
                    break;
                case PHILIPS:
                    out = new PhilipsCentroid(point.getVector(), i, point.getDomain(), k);
                    break;
                case HAMERLY:
                    out = new HamerlyCentroid(point.getVector(), i, point.getDomain());
                    break;
                default:
                    out = new NaiveCentroid(point.getVector(), i, point.getDomain());
            }
            return out;
        }
    }

    private static DataStream<Centroid[]> selectRandomCentroids(DataStream<Point> points, int k, long seed, Methods type) {
        DataStream<Centroid[]> resultStream =
                DataStreamUtils.mapPartition(
                        points,
                        new MapPartitionFunction<Point, Centroid[]>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Point> iterable, Collector<Centroid[]> out) throws Exception {
                                Dictionary<String, List<Point>> d = new Hashtable<>();
                                for (Point vector : iterable) {
                                    if (d.get(vector.getDomain()) == null) {
                                        d.put(vector.getDomain(), new ArrayList<>());
                                    }
                                    d.get(vector.getDomain()).add(vector);
                                }
                                List<Point> vectors;
                                for (Enumeration<List<Point>> e = d.elements(); e.hasMoreElements();) {
                                    vectors = e.nextElement();
                                    if (vectors.size() < k) {
                                        continue;
                                    }
                                    Collections.shuffle(vectors, new Random(seed));
                                    Point[] centroids = vectors.subList(0, k).toArray(new Point[0]);
                                    Centroid[] outArray = new Centroid[k];
                                    for (int i = 0; i < k ; i++) {
                                        switch (type) {
                                            case ELKAN:
                                                outArray[i] = new ElkanCentroid(centroids[i].getVector(), i, centroids[i].getDomain(), k);
                                                break;
                                            case PHILIPS:
                                                outArray[i] = new PhilipsCentroid(centroids[i].getVector(), i, centroids[i].getDomain(), k);
                                                break;
                                            case HAMERLY:
                                                outArray[i] = new HamerlyCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                                                break;
                                            default:
                                                outArray[i] = new NaiveCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                                        }
                                    }
                                    out.collect(outArray);
                                }
                            }
                        });
        resultStream.getTransformation().setParallelism(1);
        return resultStream;
    }

    private static class UpdatePointsTransform extends AbstractStreamOperator<Point> implements TwoInputStreamOperator<Point, Centroid[], Point>, IterationListener<Point> {
        Map<String, Centroid[]> centroids = new HashMap<>();
        Map<String, List<Point>> buffer = new HashMap<>();
        ListState<Point> out;
        IntCounter distCalcAcc = new IntCounter();

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<Point> collector) throws Exception {
            for (Point point : out.get()) {
                collector.collect(point);
            }
            out.clear();
            assert buffer.isEmpty();
            centroids.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Point> collector) throws Exception {
        }

        @Override
        public void processElement1(StreamRecord<Point> streamRecord) throws Exception {
            Point point = streamRecord.getValue();
            String domain = point.getDomain();
            if (!centroids.containsKey(domain)) {
                if(!buffer.containsKey(domain)) {
                    buffer.put(domain, new ArrayList<>());
                }
                buffer.get(domain).add(point);
                return;
            }
            out.add(updatePoint(point, centroids.get(domain)));
        }

        @Override
        public void processElement2(StreamRecord<Centroid[]> streamRecord) throws Exception {
            Centroid[] centorid = streamRecord.getValue();
            String domain = centorid[0].getDomain();
            centroids.put(domain, centorid);
            if (buffer.containsKey(domain)) {
                for (Point point : buffer.get(domain)) {
                    Point returnedPoint = updatePoint(point, centorid);
                    out.add(returnedPoint);
                }
                buffer.remove(domain);
            }
        }

        private Point updatePoint(Point point, Centroid[] centroids) {
            if (finishedCentroids(centroids)) {
                point.setFinished();
                return point;
            }
            int distCalcs = point.update(centroids);
            distCalcAcc.add(distCalcs);
            return point;
        }

        private boolean finishedCentroids(Centroid[] centroids) {
            boolean allCentroidsFinished = true;
            for (Centroid centroid : centroids) {
                if (centroid.getMovement() != 0) {
                    allCentroidsFinished = false;
                    break;
                }
            }
            return allCentroidsFinished;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            out = context.getOperatorStateStore().getListState(new ListStateDescriptor<Point>("out-up", Point.class));
        }
    }

    private static class NewCentroidValuesOperator extends RichFlatMapFunction<Point, Tuple4<String, Integer, DenseVector, Integer>[]> implements IterationListener<Tuple4<String, Integer, DenseVector, Integer>[]>{
        Map<String, Map<Integer, Tuple2<DenseVector, Integer>>> storage = new HashMap<>();

        @Override
        public void flatMap(Point point, Collector<Tuple4<String, Integer, DenseVector, Integer>[]> collector) throws Exception {
            if (!storage.containsKey(point.getDomain())) {
                storage.put(point.getDomain(), new HashMap<>());
            }
            Map<Integer, Tuple2<DenseVector, Integer>> domainStorage = storage.get(point.getDomain());
            if (!domainStorage.containsKey(point.getAssignedClusterID())) {
                domainStorage.put(point.getAssignedClusterID(), Tuple2.of(point.getVector(), 1));
                storage.put(point.getDomain(), domainStorage);
                return;
            }
            Tuple2<DenseVector, Integer> clusterstorage = domainStorage.get(point.getAssignedClusterID());
            for (int i = 0; i < clusterstorage.f0.size(); i++) {
                clusterstorage.f0.values[i] += point.getVector().get(i);
            }
            domainStorage.put(point.getAssignedClusterID(), Tuple2.of(clusterstorage.f0, clusterstorage.f1+1));
            storage.put(point.getDomain(), domainStorage);
        }

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<Tuple4<String, Integer, DenseVector, Integer>[]> collector) throws Exception {
            if (storage.isEmpty()) {
                return;
            }
            for (String domain : storage.keySet()) {
                Map<Integer, Tuple2<DenseVector, Integer>> domainMap = storage.get(domain);
                Tuple4<String, Integer, DenseVector, Integer>[] out = new Tuple4[0];
                for (Integer id : domainMap.keySet()) {
                    out = Arrays.copyOf(out, out.length+1);
                    DenseVector newVector = domainMap.get(id).f0;
                    for (int j = 0; j < newVector.size(); j++) {
                        newVector.values[j] /= domainMap.get(id).f1;
                    }
                    out[out.length-1] = Tuple4.of(domain, id, newVector, domainMap.get(id).f1);
                }
                collector.collect(out);
            }
            storage.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Tuple4<String, Integer, DenseVector, Integer>[]> collector) throws Exception {

        }
    }

    private static class CentroidUpdater extends BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], Centroid[], Centroid[]> implements IterationListener<Centroid[]> {

        IntCounter distCalcAcc = new IntCounter();
        Map<String, Centroid[]> centroidStore = new HashMap<>();
        Map<String, Tuple4<String, Integer, DenseVector, Integer>[]> buffer = new HashMap<>();
        List<Centroid[]> out = new ArrayList<>();

        @Override
        public void processElement(Tuple4<String, Integer, DenseVector, Integer>[] tuple3s, BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], Centroid[], Centroid[]>.ReadOnlyContext readOnlyContext, Collector<Centroid[]> collector) throws Exception {
            String domain = tuple3s[0].f0;
            if (!centroidStore.containsKey(domain)) {
                buffer.put(domain,tuple3s);
                return;
            }
            Centroid[] Centroids = centroidStore.get(domain);
            updateCentroid(tuple3s, Centroids);
            centroidStore.remove(domain);
        }

        private void updateCentroid(Tuple4<String, Integer, DenseVector, Integer>[] tuple3s, Centroid[] centroids) {
            for (Centroid Centroid : centroids) {
                for (Tuple4<String, Integer, DenseVector, Integer> tuple3 : tuple3s) {
                    if (tuple3.f1 == Centroid.getID()) {
                        int distCalcs = Centroid.move(tuple3.f2, tuple3.f3);
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
        public void processBroadcastElement(Centroid[] Centroids, BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], Centroid[], Centroid[]>.Context context, Collector<Centroid[]> collector) throws Exception {
            String domain = Centroids[0].getDomain();
            if (buffer.containsKey(domain)) {
                Tuple4<String, Integer, DenseVector, Integer>[] tuple3s = buffer.get(domain);
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

    private static class CentroidFilterFunction implements FilterFunction<Centroid[]> {

        final boolean giveFinished;

        public CentroidFilterFunction(boolean giveFinished) {
            this.giveFinished = giveFinished;
        }

        @Override
        public boolean filter(Centroid[] Centroids) throws Exception {
            boolean allCentroidsFinished = true;
            for (Centroid Centroid : Centroids) {
                if (Centroid.getMovement()!=0) {
                    allCentroidsFinished = false;
                    break;
                }
            }
            if (giveFinished) {
                return allCentroidsFinished;
            }
            return !allCentroidsFinished;
        }
    }

    private static class PointFilterFunction implements FilterFunction<Point> {

        final boolean giveFinished;

        public PointFilterFunction(boolean giveFinished) {
            this.giveFinished = giveFinished;
        }

        @Override
        public boolean filter(Point point) throws Exception {
            if (giveFinished) {
                return point.isFinished();
            }
            return !point.isFinished();
        }
    }

}
