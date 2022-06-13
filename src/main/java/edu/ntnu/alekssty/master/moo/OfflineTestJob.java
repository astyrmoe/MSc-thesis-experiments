package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.KMeansOfflineImprovements;
import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.CentroidToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.Counter;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OfflineTestJob {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());
        String job = "offlineB";
        int seedForRnd = parameter.getInt("seed-rnd-input", 0);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamNSLKDDConnector source = new StreamNSLKDDConnector(inputPointPath, env);
        source.connect();

        KMeansOfflineImprovements engine = new KMeansOfflineImprovements()
                .setK(k)
                .setMaxIter(20);

        DataStream<Tuple3<String, DenseVector, String>> testSource;
        if (seedForRnd==0) {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getPoints();
        } else {
            testSource = new StreamNSLKDDConnector(inputPointPath, env).connect().getRandomPoints(seedForRnd);
        }

        DataStream<Tuple2<Integer, Tuple3<String, DenseVector, String>>> taggedInput = testSource.countWindowAll(1000).process(new ProcessAllWindowFunction<Tuple3<String, DenseVector, String>, Tuple2<Integer, Tuple3<String, DenseVector, String>>, GlobalWindow>() {
            int i = 0;
            @Override
            public void process(ProcessAllWindowFunction<Tuple3<String, DenseVector, String>, Tuple2<Integer, Tuple3<String, DenseVector, String>>, GlobalWindow>.Context context, Iterable<Tuple3<String, DenseVector, String>> iterable, Collector<Tuple2<Integer, Tuple3<String, DenseVector, String>>> collector) throws Exception {
                for (Tuple3<String, DenseVector, String> stringDenseVectorStringTuple3 : iterable) {
                    collector.collect(Tuple2.of(i,stringDenseVectorStringTuple3));
                }
                i++;
            }
        }).setParallelism(1);

        Map<Integer, DataStreamList> res = new HashMap<>();

        for (int i = 0; i < 23 ; i++) {
            int finalI = i;
            DataStream<Tuple2<Integer, Tuple3<String, DenseVector, String>>> temp2 = taggedInput.filter(e -> e.f0 == finalI);
            DataStream<Tuple3<String, DenseVector, String>> temp = temp2.map(t -> t.f1).returns(new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(DenseVector.class), TypeInformation.of(String.class)));
            DataStreamList returned = engine.fit(temp, method);
            res.put(i, returned);
        }

        for (Integer i : res.keySet()) {
            DataStream<Point> resP = res.get(i).get(1);
            resP.map(new PointsToTupleForFileOperator())
/*
                    .windowAll(EndOfStreamWindows.get())
                    .process(new ProcessAllWindowFunction<Tuple3<String, Integer, String>, Tuple5<String, Integer, Integer, Integer, Integer>, TimeWindow>() {
                        @Override
                        public void process(ProcessAllWindowFunction<Tuple3<String, Integer, String>, Tuple5<String, Integer, Integer, Integer, Integer>, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, String>> iterable, Collector<Tuple5<String, Integer, Integer, Integer, Integer>> collector) throws Exception {
                            int tp = 0;
                            int fp = 0;
                            int tn = 0;
                            int fn = 0;
                            Map<String, Tuple4<Integer, Integer, Integer, Integer>> domainMap = new HashMap<>();
                            for (Tuple3<String, Integer, String> point : iterable) {
                                String domain = point.f0;
                                Tuple4<Integer, Integer, Integer, Integer> tuple = domainMap.get(domain);
                                if (point.f1 == 0) {
                                    tuple.f0 ++;
                                    if (point.f2.equals("normal")) {
                                        tuple.f1 ++;
                                    }
                                }
                                if (point.f1 == 1) {
                                    tuple.f2 ++;
                                    if (point.f2.equals("normal")) {
                                        tuple.f3 ++;
                                    }
                                }
                                domainMap.put(domain, tuple);
                            }
                            for (String domain : domainMap.keySet()) {
                                Tuple4<Integer, Integer, Integer, Integer> tuple = domainMap.get(domain);
                                boolean aNormal;
                                if (tuple.f1 != tuple.f3) {
                                    aNormal = tuple.f1>tuple.f3;
                                } else {
                                    aNormal = tuple.f0 > tuple.f2;
                                }
                                if (aNormal) {
                                    tp += tuple.f2 - tuple.f3;
                                    fp += tuple.f3;
                                    tn += tuple.f1;
                                    fn += tuple.f0 - tuple.f1;
                                } else {
                                    tp += tuple.f0 - tuple.f1;
                                    fp += tuple.f1;
                                    tn += tuple.f3;
                                    fn += tuple.f2 - tuple.f3;
                                }
                            }
                            double br = (tp+fn)/(tp+fp+tn+fn);
                            double tpr = tp/(tp+fn);
                            double fpr = fp/(fp+tn);
                            double ppv = br*tpr/(br*tpr+(1-br)*fpr);
                            System.out.println("TPR = " + tpr);
                            System.out.println("FPR = " + fpr);
                            System.out.println("PPV = " + ppv);
                            collector.collect(Tuple5.of(String.valueOf(i), tp, fp, tn, fn));
                        }
                    })
*/
                    .writeAsCsv(outputsPath + "batch/" + method + "-" + job + "-points" + i + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        JobExecutionResult jobResult = env.execute("Experimental work");
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
    }

}
