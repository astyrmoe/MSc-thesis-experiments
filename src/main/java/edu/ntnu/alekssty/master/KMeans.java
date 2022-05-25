package edu.ntnu.alekssty.master;

import edu.ntnu.alekssty.master.centroids.*;
import edu.ntnu.alekssty.master.features.*;
import edu.ntnu.alekssty.master.utils.DebugCentorids;
import edu.ntnu.alekssty.master.utils.DebugFeatures;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

public class KMeans implements KMeansParams<KMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeans() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    static final OutputTag<String> smallDomains = new OutputTag<String>("small-domains"){};
    static final OutputTag<String> smallDomainsFeatures = new OutputTag<String>("small-domains-features"){};
    static final OutputTag<Feature> largeDomainsFeatures = new OutputTag<Feature>("large-domains-features"){};

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    public DataStreamList fit(Table input, String type) {

        Methods method = Methods.valueOf(type.toUpperCase());
        System.out.println("Method used: " + method);

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) input).getTableEnvironment();

        // TODO
        DataStream<Feature> points = tEnv.toDataStream(input)
                .map(row -> (featureMaker(
                        method,
                        (DenseVector) row.getField("features"),
                        (String) row.getField("domain")
                )));

        DataStream<Centroid[]> initCentroids = selectRandomCentroids(points, getK(), getSeed(), method);
        //SingleOutputStreamOperator<Centroid[]> initCentroids = temp(points, getK(), getSeed(), method);
        initCentroids.process(new DebugCentorids("C Init centroids", true, false));

        //DataStream<Feature> filteredPoints = initCentroids.getSideOutput(largeDomainsFeatures);

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

        DataStream<Centroid[]> iRCentroids = iterationResult.get(0);
        iRCentroids.process(new DebugCentorids("C Iteration result", true, false));
        DataStream<Feature> as = iterationResult.get(1);
        as.process(new DebugFeatures("F Iteration result", true, false));
        as.process(new ProcessFunction<Feature, Feature>() {
            IntCounter accFeatureOut;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                accFeatureOut = new IntCounter();
                getRuntimeContext().addAccumulator("feature-out", accFeatureOut);
            }

            @Override
            public void processElement(Feature feature, ProcessFunction<Feature, Feature>.Context context, Collector<Feature> collector) throws Exception {
                accFeatureOut.add(1);
            }
        });

        return DataStreamList.of(
                iRCentroids,
                iterationResult.get(1)
                //initCentroids.getSideOutput(smallDomains)
        );
    }

    private static Feature featureMaker(Methods type, DenseVector features, String domain) throws Exception {
        Feature out;
        switch (type) {
            case ELKAN:
                out = new ElkanFeature(features, domain);
                break;
            case PHILIPS:
                out = new PhilipsPoint(features, domain);
                break;
            case NAIVE:
                out = new NaiveFeature(features, domain);
                break;
            case HAMERLY:
                out = new HamerlyFeature(features, domain);
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

    private static SingleOutputStreamOperator<Centroid[]> temp(DataStream<Feature> points, int k, long seed, Methods type) {
        return points.keyBy(Feature::getDomain).window(EndOfStreamWindows.get()).process(new ProcessWindowFunction<Feature, Centroid[], String, TimeWindow>() {
            IntCounter accNumFeaturesToCentroid = new IntCounter();
            IntCounter accNumDomainsToCentoird = new IntCounter();
            IntCounter accNumberOfSmallDomains = new IntCounter();
            IntCounter accNumberOfLargeDomains = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("features-into-select-centroid", accNumFeaturesToCentroid);
                getRuntimeContext().addAccumulator("domains-into-select-centroid", accNumDomainsToCentoird);
                getRuntimeContext().addAccumulator("small-domains", accNumberOfSmallDomains);
                getRuntimeContext().addAccumulator("large-domains", accNumberOfLargeDomains);
            }

            @Override
            public void process(String s, ProcessWindowFunction<Feature, Centroid[], String, TimeWindow>.Context context, Iterable<Feature> iterable, Collector<Centroid[]> collector) throws Exception {
                accNumDomainsToCentoird.add(1);
                List<Feature> vectors = new ArrayList<>();
                for (Feature feature : iterable) {
                    accNumFeaturesToCentroid.add(1);
                    vectors.add(feature);
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
                for (Feature vector : vectors) {
                    if (i<k) {
                        switch (type) {
                            case ELKAN:
                                outArray[i] = new ElkanCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            case PHILIPS:
                                outArray[i] = new PhilipsCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            case HAMERLY:
                                outArray[i] = new HamerlyCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            case DRAKE:
                                if (k < 3) {
                                    throw new Exception("k<3 is not allowed");
                                }
                                outArray[i] = new DrakeCentroid(vector.getVector(), i, vector.getDomain());
                                break;
                            default:
                                outArray[i] = new NaiveCentroid(vector.getVector(), i, vector.getDomain());
                        }
                    }
                    context.output(largeDomainsFeatures, vector);
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

    private static DataStream<Centroid[]> selectRandomCentroids(DataStream<Feature> points, int k, long seed, Methods type) {
        DataStream<Centroid[]> resultStream =
                DataStreamUtils.mapPartition(
                        points,
                        new MapPartitionFunction<Feature, Centroid[]>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Feature> iterable, Collector<Centroid[]> out) throws Exception {
                                Dictionary<String, List<Feature>> d = new Hashtable<>();
                                for (Feature vector : iterable) {
                                    if (d.get(vector.getDomain()) == null) {
                                        d.put(vector.getDomain(), new ArrayList<>());
                                    }
                                    d.get(vector.getDomain()).add(vector);
                                }
                                List<Feature> vectors;
                                for (Enumeration<List<Feature>> e = d.elements(); e.hasMoreElements();) {
                                    vectors = e.nextElement();
                                    if (vectors.size() < k) {
                                        continue;
                                    }
                                    Collections.shuffle(vectors, new Random(seed));
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
                                    out.collect(outArray);
                                }
                            }
                        });
        resultStream.getTransformation().setParallelism(1);
        return resultStream;
    }

    private static class UpdatePoints extends BroadcastProcessFunction<Feature, Centroid[], Feature> {

        Map<String, List<Feature>> buffer;
        MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
                "centroids",
                String.class,
                Centroid[].class);

        @Override
        public void processElement(Feature feature, BroadcastProcessFunction<Feature, Centroid[], Feature>.ReadOnlyContext readOnlyContext, Collector<Feature> collector) throws Exception {
            ReadOnlyBroadcastState<String, Centroid[]> state = readOnlyContext.getBroadcastState(centroidStateDescriptor);
            String domain = feature.getDomain();
            if (!state.contains(domain)) {
                if (!buffer.containsKey(domain)) {
                    buffer.put(domain, new ArrayList<>());
                }
                buffer.get(domain).add(feature);
                return;
            }
            if (finishedCentroids(state.get(domain))) {
                feature.setFinished();
                collector.collect(feature);
                return;
            }
            feature.update(state.get(domain));
            collector.collect(feature);
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
        public void processBroadcastElement(Centroid[] centroids, BroadcastProcessFunction<Feature, Centroid[], Feature>.Context context, Collector<Feature> collector) throws Exception {
            BroadcastState<String, Centroid[]> state = context.getBroadcastState(centroidStateDescriptor);
            String domain = centroids[0].getDomain();
            state.put(domain, centroids);
            if (buffer.containsKey(domain)) {
                for (Feature feature : buffer.get(domain)) {
                    if (finishedCentroids(centroids)) {
                        feature.setFinished();
                        collector.collect(feature);
                        continue;
                    }
                    feature.update(centroids);
                    collector.collect(feature);
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
            //getRuntimeContext().addAccumulator("number-of-points-in-iteration", numFeaturesInUpdatePoints);
            buffer = new HashMap<>();
        }
    }

    private static class CentroidValueFormatter implements MapFunction<Feature, Tuple4<String, Integer, DenseVector, Long>> {
        @Override
        public Tuple4<String, Integer, DenseVector, Long> map(Feature value) {
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
        MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
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

        boolean giveFinished;

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

    private static class FeatureFilterFunction implements FilterFunction<Feature> {

        boolean giveFinished;

        public FeatureFilterFunction(boolean giveFinished) {
            this.giveFinished = giveFinished;
        }

        @Override
        public boolean filter(Feature feature) throws Exception {
            if (giveFinished) {
                return feature.isFinished();
            }
            return !feature.isFinished();
        }
    }

    private static class KMeansIterationBody implements IterationBody {

        int k;

        MapStateDescriptor<String, Centroid[]> centroidStateDescriptor = new MapStateDescriptor<>(
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
            centroids.process(new DebugCentorids("C Into iteration", false, false));
            DataStream<Feature> points = variableStreams.get(1);
            points.process(new DebugFeatures("F Into iteration", false, false));

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
            });

            DataStream<Feature> newPoints = points.connect(centroids.broadcast(centroidStateDescriptor))
                    .process(new UpdatePoints());
            newPoints.process(new DebugFeatures("F New point", false, false));

            DataStream<Centroid[]> finalCentroids = centroids.filter(new CentroidFilterFunction(true));
            DataStream<Feature> finalPoints = newPoints.filter(new FeatureFilterFunction(true));
            finalPoints.process(new DebugFeatures("F Final point filter", false, false));

            DataStream<Centroid[]> centroidsStillWithUs = centroids.filter(new CentroidFilterFunction(false));
            DataStream<Feature> pointsStillWithUs = newPoints.filter(new FeatureFilterFunction(false));
            pointsStillWithUs.process(new DebugFeatures("F Points still with us filter", false, false));

            DataStreamList perRoundResults = IterationBody.forEachRound(
                    DataStreamList.of(pointsStillWithUs),
                    dataStreamList -> {
                        DataStream<Feature> newPoints1 = dataStreamList.get(0);

                        DataStream<Tuple3<String, Integer, DenseVector>[]> newCentroidValues = newPoints1
                                .map(new CentroidValueFormatter())
                                .keyBy(new KeyCentroidValues())
                                .window(EndOfStreamWindows.get())
                                .reduce(new CentroidValueAccumulator())
                                .map(new CentroidValueAverager())
                                .windowAll(EndOfStreamWindows.get())
                                .apply(new ToList());

                        return DataStreamList.of(
                                newCentroidValues
                        );
                    }
            );
            DataStream<Tuple3<String, Integer, DenseVector>[]> newCentroidValues = perRoundResults.get(0);

            DataStream<Centroid[]> newCentroids = newCentroidValues
                    .connect(centroidsStillWithUs.broadcast(centroidStateDescriptor))
                    .process(new CentroidUpdater());

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
                            pointsStillWithUs
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
