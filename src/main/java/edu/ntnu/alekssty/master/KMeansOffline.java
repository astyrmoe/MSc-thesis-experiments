package edu.ntnu.alekssty.master;

import edu.ntnu.alekssty.master.centroids.*;
import edu.ntnu.alekssty.master.points.*;
import edu.ntnu.alekssty.master.points.Point;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class KMeansOffline implements KMeansParams<KMeansOffline> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeansOffline() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    static final OutputTag<String> smallDomains = new OutputTag<String>("small-domains"){};
    static final OutputTag<String> smallDomainsPoints = new OutputTag<String>("small-domains-points"){};
    static final OutputTag<Point> largeDomainsPoints = new OutputTag<Point>("large-domains-points"){};

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    public DataStreamList fit(Table input, String type) {

        Methods method = Methods.valueOf(type.toUpperCase());
        System.out.println("Method used: " + method);

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) input).getTableEnvironment();

        // TODO
        DataStream<Point> points = tEnv.toDataStream(input)
                .map(row -> (pointMaker(
                        method,
                        (DenseVector) row.getField("features"),
                        (String) row.getField("domain")
                ))).name("FeatureMaker");

        DataStream<Centroid[]> initCentroids = selectRandomCentroids(points, getK(), getSeed(), method);
        //initCentroids.process(new DebugCentorids("C Init centroids", true, false));

        IterationConfig config = IterationConfig.newBuilder()
                .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.PER_ROUND)
                .build();

        IterationBody body = new KMeansIterationBody(getK());

        DataStreamList iterationResult = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(initCentroids, points),
                ReplayableDataStreamList.notReplay(),
                config,
                body
        );

        DataStream<Centroid[]> iterationResultsCentroids = iterationResult.get(0);
        //iterationResultsCentroids.process(new DebugCentorids("C Iteration result", true, false));
        DataStream<Point> iterationsResultFeatures = iterationResult.get(1);
        //iterationsResultFeatures.process(new DebugFeatures("F Iteration result", true, false));

        return DataStreamList.of(
                iterationResultsCentroids,
                iterationsResultFeatures
        );
    }

    private static Point pointMaker(Methods type, DenseVector features, String domain) throws Exception {
        Point out;
        switch (type) {
            case ELKAN:
                out = new ElkanPoint(features, domain);
                break;
            case PHILIPS:
                out = new PhilipsPoint(features, domain);
                break;
            case NAIVE:
                out = new NaivePoint(features, domain);
                break;
            case HAMERLY:
                out = new HamerlyPoint(features, domain);
                break;
/*
            case DRAKE:
                out = new DrakeFeature(features, domain);
                break;
*/
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        return out;
    }

    // TODO WHAT DIS?
    private static SingleOutputStreamOperator<Centroid[]> temp(DataStream<Point> points, int k, long seed, Methods type) {
        return points.keyBy(Point::getDomain).window(EndOfStreamWindows.get()).process(new ProcessWindowFunction<Point, Centroid[], String, TimeWindow>() {
            final IntCounter accNumPointsToCentroid = new IntCounter();
            final IntCounter accNumDomainsToCentroid = new IntCounter();
            final IntCounter accNumberOfSmallDomains = new IntCounter();
            final IntCounter accNumberOfLargeDomains = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("points-into-select-centroid", accNumPointsToCentroid);
                getRuntimeContext().addAccumulator("domains-into-select-centroid", accNumDomainsToCentroid);
                getRuntimeContext().addAccumulator("small-domains", accNumberOfSmallDomains);
                getRuntimeContext().addAccumulator("large-domains", accNumberOfLargeDomains);
            }

            @Override
            public void process(String s, ProcessWindowFunction<Point, Centroid[], String, TimeWindow>.Context context, Iterable<Point> iterable, Collector<Centroid[]> collector) throws Exception {
                accNumDomainsToCentroid.add(1);
                List<Point> vectors = new ArrayList<>();
                for (Point point : iterable) {
                    accNumPointsToCentroid.add(1);
                    vectors.add(point);
                }
                if (vectors.size()<k) {
                    context.output(smallDomains, s);
                    accNumberOfSmallDomains.add(1);
                    return;
                }
                accNumberOfLargeDomains.add(1);
                Collections.shuffle(vectors, new Random(seed));
                int i = 0;
                Centroid[] outArray = new Centroid[k];
                for (Point vector : vectors) {
                    if (i<k) {
                        switch (type) {
                            case ELKAN:
                                outArray[i] = new ElkanCentroid(vector.getVector(), i, vector.getDomain(), k);
                                break;
                            case PHILIPS:
                                outArray[i] = new PhilipsCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            case HAMERLY:
                                outArray[i] = new HamerlyCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            default:
                                outArray[i] = new NaiveCentroid(vector.getVector(), i, vector.getDomain());
                        }
                    }
                    context.output(largeDomainsPoints, vector);
                    i++;
                }
/*
                Feature[] centroids = vectors.subList(0, k).toArray(new Feature[0]);
                Centroid[] outArray = new Centroid[k];
                for (int i = 0; i < k ; i++) {
                    switch (type) {
                        case ELKAN:
                            outArray[i] = new ElkanCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                            break;
                        case PHILIPS:
                            outArray[i] = new PhilipsCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                            break;
                        case HAMERLY:
                            outArray[i] = new HamerlyCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                            break;
                        case DRAKE:
                            if (k < 3) {
                                throw new Exception("k<3 is not allowed");
                            }
                            outArray[i] = new DrakeCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                            break;
                        default:
                            outArray[i] = new NaiveCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
                    }
                }
*/
                collector.collect(outArray);
            }
        });
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
                                                outArray[i] = new PhilipsCentroid(centroids[i].getVector(), i, centroids[i].getDomain());
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

    private static class UpdatePoints extends BroadcastProcessFunction<Point, Centroid[], Point> {

        Map<String, List<Point>> buffer;
        final MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
                "centroids",
                String.class,
                Centroid[].class);

        @Override
        public void processElement(Point point, BroadcastProcessFunction<Point, Centroid[], Point>.ReadOnlyContext readOnlyContext, Collector<Point> collector) throws Exception {
            ReadOnlyBroadcastState<String, Centroid[]> state = readOnlyContext.getBroadcastState(centroidStateDescriptor);
            String domain = point.getDomain();
            if (!state.contains(domain)) {
                if (!buffer.containsKey(domain)) {
                    buffer.put(domain, new ArrayList<>());
                }
                buffer.get(domain).add(point);
                return;
            }
            updatePoint(point, state.get(domain), collector);
        }

        private void updatePoint(Point point, Centroid[] centroids, Collector<Point> collector) {
            if (finishedCentroids(centroids)) {
                point.setFinished();
                collector.collect(point);
                return;
            }
            point.update(centroids);
            collector.collect(point);
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
        public void processBroadcastElement(Centroid[] centroids, BroadcastProcessFunction<Point, Centroid[], Point>.Context context, Collector<Point> collector) throws Exception {
            BroadcastState<String, Centroid[]> state = context.getBroadcastState(centroidStateDescriptor);
            String domain = centroids[0].getDomain();
            state.put(domain, centroids);
            if (buffer.containsKey(domain)) {
                for (Point point : buffer.get(domain)) {
                    updatePoint(point, centroids, collector);
                }
                buffer.remove(domain);
            }
        }

        @Override
        public void close() throws Exception {
            buffer = null;
            super.close();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            buffer = new HashMap<>();
        }
    }

    private static class CentroidValueFormatter implements MapFunction<Point, Tuple4<String, Integer, DenseVector, Long>> {
        @Override
        public Tuple4<String, Integer, DenseVector, Long> map(Point value) {
            return Tuple4.of(value.getDomain(), value.getAssignedClusterID(), value.getVector(), 1L);
        }
    }

    private static class KeyCentroidValues implements KeySelector<Tuple4<String, Integer, DenseVector, Long>, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> getKey(Tuple4<String, Integer, DenseVector, Long> domainIntegerDenseVectorLongTuple4) throws Exception {
            return Tuple2.of(domainIntegerDenseVectorLongTuple4.f0, domainIntegerDenseVectorLongTuple4.f1);
        }
    }

    private static class CentroidValueAccumulator implements ReduceFunction<Tuple4<String, Integer, DenseVector, Long>> {
        @Override
        public Tuple4<String, Integer, DenseVector, Long> reduce(
                Tuple4<String, Integer, DenseVector, Long> v1, Tuple4<String, Integer, DenseVector, Long> v2) {
            for (int i = 0; i < v1.f2.size(); i++) {
                v1.f2.values[i] += v2.f2.values[i];
            }
            return new Tuple4<>(v1.f0, v1.f1, v1.f2, v1.f3 + v2.f3);
        }
    }

    private static class CentroidValueAverager implements MapFunction<Tuple4<String, Integer, DenseVector, Long>, Tuple3<String, Integer, DenseVector>> {
        @Override
        public Tuple3<String, Integer, DenseVector> map(Tuple4<String, Integer, DenseVector, Long> value) {
            for (int i = 0; i < value.f2.size(); i++) {
                value.f2.values[i] /= value.f3;
            }
            return Tuple3.of(value.f0, value.f1, value.f2);
        }
    }

    private static class ToList implements AllWindowFunction<Tuple3<String, Integer, DenseVector>, Tuple3<String, Integer, DenseVector>[], TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<Tuple3<String, Integer, DenseVector>> iterable, Collector<Tuple3<String, Integer, DenseVector>[]> collector) throws Exception {
            Dictionary<String, Tuple3<String, Integer, DenseVector>[]> d = new Hashtable<>();
            for (Tuple3<String, Integer, DenseVector> i : iterable) {
                if (d.get(i.f0) == null) {
                    d.put(i.f0, new Tuple3[0]);
                }
                Tuple3<String, Integer, DenseVector>[] newArray = Arrays.copyOf(d.get(i.f0), d.get(i.f0).length + 1);
                newArray[d.get(i.f0).length] = i;
                d.put(i.f0,newArray);
            }
            for (Enumeration<Tuple3<String, Integer, DenseVector>[]> e = d.elements(); e.hasMoreElements();) {
                collector.collect(e.nextElement());
            }
        }
    }

    private static class CentroidUpdater extends BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]> {

        Map<String, Tuple3<String, Integer, DenseVector>[]> buffer;
        final MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
                "centroids",
                String.class,
                Centroid[].class);

        @Override
        public void processElement(Tuple3<String, Integer, DenseVector>[] tuple3s, BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]>.ReadOnlyContext readOnlyContext, Collector<Centroid[]> collector) throws Exception {
            ReadOnlyBroadcastState<String, Centroid[]> state = readOnlyContext.getBroadcastState(centroidStateDescriptor);
            String domain = tuple3s[0].f0;
            if (!state.contains(domain)) {
                buffer.put(domain,tuple3s);
                return;
            }
            Centroid[] Centroids = state.get(domain);
            updateCentroid(tuple3s, Centroids, collector);
        }

        private void updateCentroid(Tuple3<String, Integer, DenseVector>[] tuple3s, Centroid[] centroids, Collector<Centroid[]> collector) {
            for (Centroid Centroid : centroids) {
                for (Tuple3<String, Integer, DenseVector> tuple3 : tuple3s) {
                    if (tuple3.f1 == Centroid.getID()) {
                        Centroid.move(tuple3.f2);
                        break;
                    }
                }
            }
            for (Centroid Centroid : centroids) {
                Centroid.update(centroids);
            }
            collector.collect(centroids);
        }

        @Override
        public void processBroadcastElement(Centroid[] Centroids, BroadcastProcessFunction<Tuple3<String, Integer, DenseVector>[], Centroid[], Centroid[]>.Context context, Collector<Centroid[]> collector) throws Exception {
            String domain = Centroids[0].getDomain();
            if (buffer.containsKey(domain)) {
                Tuple3<String, Integer, DenseVector>[] tuple3s = buffer.get(domain);
                updateCentroid(tuple3s, Centroids, collector);
                return;
            }
            context.getBroadcastState(centroidStateDescriptor).put(domain, Centroids);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            buffer = new HashMap<>();
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
                if (!Centroid.isFinished()) {
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
            //centroids.process(new DebugCentorids("C Into iteration", false, false));
            DataStream<Point> points = variableStreams.get(1);
            //points.process(new DebugFeatures("F Into iteration", false, false));

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

            DataStream<Point> newPoints = points
                    .connect(centroids.broadcast(centroidStateDescriptor))
                    .process(new UpdatePoints()).name("Update Points");
            //newPoints.process(new DebugFeatures("F New point", false, false));

            DataStream<Centroid[]> finalCentroids = centroids.filter(new CentroidFilterFunction(true)).name("Filter finished centroids");
            DataStream<Point> finalPoints = newPoints.filter(new PointFilterFunction(true)).name("Filter finished points");
            //finalPoints.process(new DebugFeatures("F Final point filter", false, false));

            DataStream<Centroid[]> centroidsNotFinished = centroids.filter(new CentroidFilterFunction(false)).name("Filter not finished centroids");
            DataStream<Point> pointsNotFinished = newPoints.filter(new PointFilterFunction(false)).name("Filter not finished points");
            //pointsNotFinished.process(new DebugFeatures("F Points still with us filter", false, false));

            DataStreamList perRoundResults = IterationBody.forEachRound(
                    DataStreamList.of(pointsNotFinished),
                    dataStreamList -> {
                        DataStream<Point> newPoints1 = dataStreamList.get(0);

                        DataStream<Tuple3<String, Integer, DenseVector>[]> newCentroidValues = newPoints1
                                .map(new CentroidValueFormatter()).name("Centroid value formatter")
                                .keyBy(new KeyCentroidValues())
                                .window(EndOfStreamWindows.get())
                                .reduce(new CentroidValueAccumulator()).name("Centroid value accumulator")
                                .map(new CentroidValueAverager()).name("Centroid value averager")
                                .windowAll(EndOfStreamWindows.get())
                                .apply(new ToList()).name("Centroid value to list");

                        return DataStreamList.of(
                                newCentroidValues
                        );
                    }
            );
            DataStream<Tuple3<String, Integer, DenseVector>[]> newCentroidValues = perRoundResults.get(0);

            DataStream<Centroid[]> newCentroids = newCentroidValues
                    .connect(centroidsNotFinished.broadcast(centroidStateDescriptor))
                    .process(new CentroidUpdater()).name("Centroid updater");

            // TODO Move to beginning of next iteration
            DataStream<Centroid[]> cf = newCentroids.map(new MapFunction<Centroid[], Centroid[]>() {
                @Override
                public Centroid[] map(Centroid[] centroids) throws Exception {
                    boolean allCentroidsFinished = true;
                    for (Centroid centroid : centroids) {
                        if (centroid.getMovement() != 0) {
                            allCentroidsFinished = false;
                            break;
                        }
                    }
                    if (allCentroidsFinished) {
                        for (Centroid centroid : centroids) {
                            centroid.setFinished();
                        }
                    }
                    return centroids;
                }
            });

            return new IterationBodyResult(
                    DataStreamList.of(
                            cf,
                            pointsNotFinished
                    ),
                    DataStreamList.of(
                            finalCentroids,
                            finalPoints
                    ),
                    terminationCriteria
            );
        }
    }
}
