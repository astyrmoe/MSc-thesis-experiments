package edu.ntnu.alekssty.master.offline;

import edu.ntnu.alekssty.master.Methods;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.OfflinePoint;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.*;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.ElkanPoint;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.HamerlyPoint;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.NaivePoint;
import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.PhilipsPoint;
import edu.ntnu.alekssty.master.utils.NewIteration;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

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

        // TODO Make operator its own class
        DataStream<OfflinePoint> points = input.map(t -> (pointMaker(method,t.f1,t.f0,t.f2))).name("FeatureMaker");

        //DataStream<Centroid[]> initCentroids = points.keyBy(Point::getDomain).process(new MakeFirstFeaturesCentroids(method, getK()));
        DataStream<OfflineCentroid[]> initCentroids = selectRandomCentroids(points, getK(), getSeed(), method);
        //initCentroids.process(new DebugCentorids("C Init centroids", true, true));

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

        DataStream<OfflineCentroid[]> iterationResultsCentroids = iterationResult.get(0);
        //iterationResultsCentroids.process(new DebugCentorids("C Iteration result", true, true));
        DataStream<OfflinePoint> iterationsResultFeatures = iterationResult.get(1);
        //iterationsResultFeatures.process(new DebugPoints("F Iteration result", true, true));

        return DataStreamList.of(
                iterationResultsCentroids,
                iterationsResultFeatures
        );
    }

    private static class KMeansIterationBody implements IterationBody {

        final int k;
        final MapStateDescriptor<String, OfflineCentroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
                "centroids",
                String.class,
                OfflineCentroid[].class);

        public KMeansIterationBody(int k) {
            this.k = k;
            centroidStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        }

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<OfflineCentroid[]> centroids = variableStreams.get(0);
            //centroids.process(new DebugCentorids("C Into iteration", true, true));
            DataStream<OfflinePoint> points = variableStreams.get(1);
            //points.process(new DebugPoints("F Into iteration", true, true));

            DataStream<Integer> terminationCriteria = centroids.flatMap(new FlatMapFunction<OfflineCentroid[], Integer>() {
                @Override
                public void flatMap(OfflineCentroid[] centroids, Collector<Integer> collector) throws Exception {
                    for (OfflineCentroid centroid : centroids) {
                        if (!centroid.isFinished()) {
                            collector.collect(0);
                            break;
                        }
                    }
                }
            }).name("Termination criteria");

            DataStream<OfflinePoint> newPoints = points.connect(centroids.broadcast()).transform("up", TypeInformation.of(OfflinePoint.class), new UpdatePointsTransform());
            //newPoints.process(new DebugPoints("F New point", true, true));

            DataStream<OfflineCentroid[]> finalCentroids = centroids.filter(new CentroidFilterFunction(true)).name("Filter finished centroids");
            DataStream<OfflinePoint> finalPoints = newPoints.filter(new PointFilterFunction(true)).name("Filter finished points");
            DataStream<OfflineCentroid[]> centroidsNotFinished = centroids.filter(new CentroidFilterFunction(false)).name("Filter not finished centroids");
            DataStream<OfflinePoint> pointsNotFinished = newPoints.filter(new PointFilterFunction(false)).name("Filter not finished points");

            DataStream<Tuple4<String, Integer, DenseVector, Integer>[]> newCentroidValues = pointsNotFinished.keyBy(OfflinePoint::getDomain).flatMap(new NewCentroidValuesOperator());

            DataStream<OfflineCentroid[]> newCentroids = newCentroidValues
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

    private static OfflinePoint pointMaker(Methods type, DenseVector features, String domain, String label) throws Exception {
        OfflinePoint out;
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

    private static class MakeFirstFeaturesCentroids extends KeyedProcessFunction<String, OfflinePoint, OfflineCentroid[]> {
        private final Methods method;
        ValueState<OfflineCentroid[]> centroid;
        private final int k;

        public MakeFirstFeaturesCentroids(Methods method, int k) {
            this.method = method;
            this.k = k;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            centroid = getRuntimeContext().getState(new ValueStateDescriptor<OfflineCentroid[]>("d", OfflineCentroid[].class));
        }

        @Override
        public void processElement(OfflinePoint point, KeyedProcessFunction<String, OfflinePoint, OfflineCentroid[]>.Context context, Collector<OfflineCentroid[]> collector) throws Exception {
            if (centroid.value() == null) {
                centroid.update(new OfflineCentroid[0]);
            }
            int centroidsSoFar = centroid.value().length;
            if (centroidsSoFar >= k) {
                return;
            }
            OfflineCentroid[] newArray = Arrays.copyOf(centroid.value(), centroidsSoFar+1);
            newArray[centroidsSoFar] = makeCentroid(point, method, centroidsSoFar);
            if (centroidsSoFar + 1 == k) {
                collector.collect(newArray);
            }
            centroid.update(newArray);
        }

        private OfflineCentroid makeCentroid(OfflinePoint point, Methods method, int i) {
            OfflineCentroid out;
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

    private static DataStream<OfflineCentroid[]> selectRandomCentroids(DataStream<OfflinePoint> points, int k, long seed, Methods type) {
        DataStream<OfflineCentroid[]> resultStream =
                DataStreamUtils.mapPartition(
                        points,
                        new MapPartitionFunction<OfflinePoint, OfflineCentroid[]>() {
                            @Override
                            public void mapPartition(
                                    Iterable<OfflinePoint> iterable, Collector<OfflineCentroid[]> out) throws Exception {
                                Dictionary<String, List<OfflinePoint>> d = new Hashtable<>();
                                for (OfflinePoint vector : iterable) {
                                    if (d.get(vector.getDomain()) == null) {
                                        d.put(vector.getDomain(), new ArrayList<>());
                                    }
                                    d.get(vector.getDomain()).add(vector);
                                }
                                List<OfflinePoint> vectors;
                                for (Enumeration<List<OfflinePoint>> e = d.elements(); e.hasMoreElements();) {
                                    vectors = e.nextElement();
                                    if (vectors.size() < k) {
                                        continue;
                                    }
                                    Collections.shuffle(vectors, new Random(seed));
                                    OfflinePoint[] centroids = vectors.subList(0, k).toArray(new OfflinePoint[0]);
                                    OfflineCentroid[] outArray = new OfflineCentroid[k];
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

    private static class UpdatePointsTransform extends AbstractStreamOperator<OfflinePoint> implements TwoInputStreamOperator<OfflinePoint, OfflineCentroid[], OfflinePoint>, IterationListener<OfflinePoint> {
        Map<String, OfflineCentroid[]> centroids = new HashMap<>();
        Map<String, List<OfflinePoint>> buffer = new HashMap<>();
        ListState<OfflinePoint> out;
        IntCounter distCalcAcc = new IntCounter();

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<OfflinePoint> collector) throws Exception {
            for (OfflinePoint point : out.get()) {
                collector.collect(point);
            }
            out.clear();
            assert buffer.isEmpty();
            centroids.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OfflinePoint> collector) throws Exception {
        }

        @Override
        public void processElement1(StreamRecord<OfflinePoint> streamRecord) throws Exception {
            OfflinePoint point = streamRecord.getValue();
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
        public void processElement2(StreamRecord<OfflineCentroid[]> streamRecord) throws Exception {
            OfflineCentroid[] centorid = streamRecord.getValue();
            String domain = centorid[0].getDomain();
            centroids.put(domain, centorid);
            if (buffer.containsKey(domain)) {
                for (OfflinePoint point : buffer.get(domain)) {
                    OfflinePoint returnedPoint = updatePoint(point, centorid);
                    out.add(returnedPoint);
                }
                buffer.remove(domain);
            }
        }

        private OfflinePoint updatePoint(OfflinePoint point, OfflineCentroid[] centroids) {
            if (finishedCentroids(centroids)) {
                point.setFinished();
                return point;
            }
            int distCalcs = point.update(centroids);
            distCalcAcc.add(distCalcs);
            return point;
        }

        private boolean finishedCentroids(OfflineCentroid[] centroids) {
            boolean allCentroidsFinished = true;
            for (OfflineCentroid centroid : centroids) {
                if (centroid.getMovement() != 0) {
                    allCentroidsFinished = false;
                    break;
                }
            }
            return allCentroidsFinished;
        }

        @Override
        public void open() throws Exception {
            super.open();
            getRuntimeContext().addAccumulator("dist-calc-up-"+NewIteration.getInstance().getPoint(), distCalcAcc);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            out = context.getOperatorStateStore().getListState(new ListStateDescriptor<OfflinePoint>("out-up", OfflinePoint.class));
        }
    }

    private static class NewCentroidValuesOperator extends RichFlatMapFunction<OfflinePoint, Tuple4<String, Integer, DenseVector, Integer>[]> implements IterationListener<Tuple4<String, Integer, DenseVector, Integer>[]>{
        Map<String, Map<Integer, Tuple2<DenseVector, Integer>>> storage = new HashMap<>();

        @Override
        public void flatMap(OfflinePoint point, Collector<Tuple4<String, Integer, DenseVector, Integer>[]> collector) throws Exception {
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

    private static class CentroidUpdater extends BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], OfflineCentroid[], OfflineCentroid[]> implements IterationListener<OfflineCentroid[]> {

        IntCounter distCalcAcc = new IntCounter();
        Map<String, OfflineCentroid[]> centroidStore = new HashMap<>();
        Map<String, Tuple4<String, Integer, DenseVector, Integer>[]> buffer = new HashMap<>();
        List<OfflineCentroid[]> out = new ArrayList<>();

        @Override
        public void processElement(Tuple4<String, Integer, DenseVector, Integer>[] tuple3s, BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], OfflineCentroid[], OfflineCentroid[]>.ReadOnlyContext readOnlyContext, Collector<OfflineCentroid[]> collector) throws Exception {
            String domain = tuple3s[0].f0;
            if (!centroidStore.containsKey(domain)) {
                buffer.put(domain,tuple3s);
                return;
            }
            OfflineCentroid[] Centroids = centroidStore.get(domain);
            updateCentroid(tuple3s, Centroids);
            centroidStore.remove(domain);
        }

        private void updateCentroid(Tuple4<String, Integer, DenseVector, Integer>[] tuple3s, OfflineCentroid[] centroids) {
            for (OfflineCentroid Centroid : centroids) {
                for (Tuple4<String, Integer, DenseVector, Integer> tuple3 : tuple3s) {
                    if (tuple3.f1 == Centroid.getID()) {
                        int distCalcs = Centroid.move(tuple3.f2, tuple3.f3);
                        distCalcAcc.add(distCalcs);
                        break;
                    }
                }
            }
            for (OfflineCentroid Centroid : centroids) {
                int distCalcs = Centroid.update(centroids);
                distCalcAcc.add(distCalcs);
            }
            out.add(centroids);
        }

        @Override
        public void processBroadcastElement(OfflineCentroid[] Centroids, BroadcastProcessFunction<Tuple4<String, Integer, DenseVector, Integer>[], OfflineCentroid[], OfflineCentroid[]>.Context context, Collector<OfflineCentroid[]> collector) throws Exception {
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
        public void onEpochWatermarkIncremented(int i, IterationListener.Context context, Collector<OfflineCentroid[]> collector) throws Exception {
            assert buffer.isEmpty();
            assert centroidStore.isEmpty();
            for (OfflineCentroid[] c : out) {
                collector.collect(c);
            }
            out.clear();
        }

        @Override
        public void onIterationTerminated(IterationListener.Context context, Collector<OfflineCentroid[]> collector) throws Exception {
        }
    }

    private static class CentroidFilterFunction implements FilterFunction<OfflineCentroid[]> {

        final boolean giveFinished;

        public CentroidFilterFunction(boolean giveFinished) {
            this.giveFinished = giveFinished;
        }

        @Override
        public boolean filter(OfflineCentroid[] Centroids) throws Exception {
            boolean allCentroidsFinished = true;
            for (OfflineCentroid Centroid : Centroids) {
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

    private static class PointFilterFunction implements FilterFunction<OfflinePoint> {

        final boolean giveFinished;

        public PointFilterFunction(boolean giveFinished) {
            this.giveFinished = giveFinished;
        }

        @Override
        public boolean filter(OfflinePoint point) throws Exception {
            if (giveFinished) {
                return point.isFinished();
            }
            return !point.isFinished();
        }
    }

}
