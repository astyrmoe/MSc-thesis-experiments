package edu.ntnu.alekssty.master.dataoperations;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class ResultJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        int k = parameter.getInt("k", 2);
        String method = parameter.get("method", "naive");
        String rootPath = parameter.get("root", "/tmp/experiment-results/");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setRuntimeMode(RuntimeExecutionMode.BATCH)
                .setParallelism(1);

        CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
        FileSource<SomePojo> source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(rootPath + method+"-points.csv"))).build();

        DataStream<SomePojo> points = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source");

        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> accumulated = points.keyBy(t -> t.domain).window(EndOfStreamWindows.get()).aggregate(new AggregateFunction<SomePojo, Tuple5<String, Integer, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> createAccumulator() {
                return Tuple5.of(null, 0, 0, 0, 0);
            }

            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> add(SomePojo somePojo, Tuple5<String, Integer, Integer, Integer, Integer> acc) {
                int cA = 0;
                int cB = 0;
                int cAn = 0;
                int cBn = 0;
                if (somePojo.assigned == 0) {
                    cA = 1;
                    if (somePojo.label.equals("normal")) {
                        cAn = 1;
                    }
                }
                if (somePojo.assigned == 1) {
                    cB = 1;
                    if (somePojo.label.equals("normal")) {
                        cBn = 1;
                    }
                }
                return Tuple5.of(somePojo.domain, acc.f1 + cA, acc.f2 + cAn, acc.f3 + cB, acc.f4 + cBn);
            }

            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> getResult(Tuple5<String, Integer, Integer, Integer, Integer> stringIntegerIntegerIntegerIntegerTuple5) {
                return stringIntegerIntegerIntegerIntegerTuple5;
            }

            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> merge(Tuple5<String, Integer, Integer, Integer, Integer> acc0, Tuple5<String, Integer, Integer, Integer, Integer> acc1) {
                return Tuple5.of(acc0.f0, acc0.f1 + acc1.f1, acc0.f2 + acc1.f2, acc0.f3 + acc1.f3, acc0.f4 + acc1.f4);
            }
        });

        accumulated.writeAsCsv(rootPath + method + "-accumulated.csv", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    @JsonPropertyOrder({"domain", "assigned", "label"})
    private static class SomePojo {
        public String domain;
        public int assigned;
        public String label;
    }
}
