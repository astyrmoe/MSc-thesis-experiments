package edu.ntnu.alekssty.master.dataoperations;

import edu.ntnu.alekssty.master.utils.NSLKDDConnector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TestDuplicates {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        //String rootPath = parameters.get("root", "/tmp/experiment-results/");
        String path = parameters.get("path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        NSLKDDConnector connector = new NSLKDDConnector(path, tEnv);
        connector.connect();

        Table data = connector.getDataTable();

        tEnv.toDataStream(data).keyBy(r->(String)r.getField("id")).process(new KeyedProcessFunction<String, Row, Row>() {
            private transient ValueState<Row> already;
            @Override
            public void processElement(Row row, KeyedProcessFunction<String, Row, Row>.Context context, Collector<Row> collector) throws Exception {
                if (already.value()!= null) {
                    collector.collect(row);
                    collector.collect(already.value());
                }
                already.update(row);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Row> desc = new ValueStateDescriptor<>(
                        "already",
                        TypeInformation.of(Row.class),
                        null
                );
                already = getRuntimeContext().getState(desc);
                super.open(parameters);
            }
        }).map(new MapFunction<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Row row) throws Exception {
                return Tuple2.of((String)row.getField("domain"), (String) row.getField("class"));
            }
        }).print("Duplicate");

        env.execute();
    }
}
