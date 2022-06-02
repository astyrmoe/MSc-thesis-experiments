package edu.ntnu.alekssty.master.dataoperations;

import edu.ntnu.alekssty.master.utils.StreamNSLKDDConnector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MakeFasitJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String outputsPath = parameters.get("outputs-path", "/tmp/experiment-results/");
        String inputPath = parameters.get("input-path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        StreamNSLKDDConnector connector = new StreamNSLKDDConnector(inputPath, env);
        connector.connect();

        DataStream<Tuple3<String, Integer, Integer>> results = connector.getPoints()
                .keyBy(t -> t.f0)
                .window(EndOfStreamWindows.get())
                .aggregate(new AggregateFunction<Tuple3<String, DenseVector, String>, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                        @Override
                        public Tuple3<String, Integer, Integer> createAccumulator() {
                            return Tuple3.of(null, 0, 0);
                        }

                        @Override
                        public Tuple3<String, Integer, Integer> add(Tuple3<String, DenseVector, String> row, Tuple3<String, Integer, Integer> acc) {
                            String domain = row.f0;
                            String cluster = row.f2;
                            if (cluster.equals("normal")) {
                                return Tuple3.of(domain, acc.f1 + 1, acc.f2);
                            }
                            return Tuple3.of(domain, acc.f1, acc.f2 + 1);
                        }

                        @Override
                        public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) {
                            return stringIntegerIntegerTuple3;
                        }

                        @Override
                        public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> acc0, Tuple3<String, Integer, Integer> acc1) {
                            return Tuple3.of(acc0.f0, acc0.f1 + acc1.f1, acc0.f2 + acc1.f2);
                        }
                });
        results.writeAsCsv(outputsPath + "LF.csv", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}
