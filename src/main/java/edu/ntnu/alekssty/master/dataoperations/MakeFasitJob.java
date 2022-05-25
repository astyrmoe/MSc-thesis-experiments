package edu.ntnu.alekssty.master.dataoperations;

import edu.ntnu.alekssty.master.utils.NSLKDDConnector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class MakeFasitJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String rootPath = parameters.get("root", "/tmp/experiment-results/");
        String path = parameters.get("path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        NSLKDDConnector connector = new NSLKDDConnector(path, tEnv);
        connector.connect();

        Table data = connector.getDataTable().select($("domain"), $("cluster"));

        DataStream<Tuple3<String, Integer, Integer>> results = tEnv.toDataStream(data)
                .keyBy(t -> t.getField("domain"))
                .window(EndOfStreamWindows.get())
                .aggregate(new AggregateFunction<Row, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                        @Override
                        public Tuple3<String, Integer, Integer> createAccumulator() {
                            return Tuple3.of(null, 0, 0);
                        }

                        @Override
                        public Tuple3<String, Integer, Integer> add(Row row, Tuple3<String, Integer, Integer> acc) {
                            String domain = (String) row.getField("domain");
                            String cluster = (String) row.getField("cluster");
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
        results.writeAsCsv(rootPath + "LF.csv", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}
