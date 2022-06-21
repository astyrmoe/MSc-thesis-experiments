package edu.ntnu.alekssty.master.moo;

import edu.ntnu.alekssty.master.batch.KMeansOfflineImprovements;
import edu.ntnu.alekssty.master.batch.Methods;
import edu.ntnu.alekssty.master.utils.PointsToTupleForFileOperator;
import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class BatchJob {
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTest+.txt");
        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());
        String job = "batch";
        int seedForRnd = parameter.getInt("seed-rnd-input", 0);

        int batchSize = parameter.getInt("batch-size", 1000);
        int buckets = parameter.getInt("buckets", 23);

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

        //DataStream<Tuple3<String, DenseVector, String>> testSource2 = testSource.filter(t -> Newt.get().contains(t.f0));

        DataStream<Tuple2<Integer, Tuple3<String, DenseVector, String>>> taggedInput = testSource.countWindowAll(batchSize).process(new ProcessAllWindowFunction<Tuple3<String, DenseVector, String>, Tuple2<Integer, Tuple3<String, DenseVector, String>>, GlobalWindow>() {
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

        for (int i = 0; i < buckets ; i++) {
            int finalI = i;
            DataStream<Tuple2<Integer, Tuple3<String, DenseVector, String>>> temp2 = taggedInput.filter(e -> e.f0 == finalI);
            DataStream<Tuple3<String, DenseVector, String>> temp = temp2.map(t -> t.f1).returns(new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(DenseVector.class), TypeInformation.of(String.class)));
            DataStreamList returned = engine.fit(temp, method);
            res.put(i, returned);
        }

        for (Integer i : res.keySet()) {
            DataStream<Point> resP = res.get(i).get(1);
            resP.map(new PointsToTupleForFileOperator())
                    .writeAsCsv(outputsPath + "batch/" + method + "-" + job + "-points" + i + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        JobExecutionResult jobResult = env.execute("Experimental work");
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
    }
}
