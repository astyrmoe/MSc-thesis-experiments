package edu.ntnu.alekssty.master.dataoperations;

import edu.ntnu.alekssty.master.batch.Methods;
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

public class ResultJobBatch {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());


        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");
        String job = parameter.get("job", "batch");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setRuntimeMode(RuntimeExecutionMode.BATCH)
                .setParallelism(1);
        CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);

        int i = 0;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r0 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r1 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r2 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r3 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r4 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r5 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r6 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r7 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r8 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r9 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r10 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r11 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r12 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r13 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r14 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r15 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r16 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r17 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r18 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r19 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r20 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> r21 = extracted(outputsPath, method, job, i, csvFormat, env);
        i++;

        r0.union(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21)
                        .writeAsCsv(outputsPath +"batch/"+ method + "-" + job + "-accumulated"+ "EVRYTHING" +".csv", FileSystem.WriteMode.OVERWRITE);


        System.out.println(i);

        env.execute();
    }

    private static DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> extracted(String outputsPath, Methods method, String job, int i, CsvReaderFormat<SomePojo> csvFormat, StreamExecutionEnvironment env) {
        String inputPointPath = outputsPath + "batch/" + method + "-" + job + "-points"+ i +".csv";
        FileSource<SomePojo> source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(inputPointPath))).build();
        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> aggregated = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .keyBy(t -> t.domain)
                .window(EndOfStreamWindows.get())
                .aggregate(new Aggregate());
        aggregated.writeAsCsv(outputsPath +"batch/"+ method + "-" + job + "-accumulated"+ i +".csv", FileSystem.WriteMode.OVERWRITE);
        return aggregated;
    }

    @JsonPropertyOrder({"domain", "assigned", "label"})
    private static class SomePojo {
        public String domain;
        public int assigned;
        public String label;
    }

    private static class Aggregate implements AggregateFunction<SomePojo, Tuple5<String, Integer, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Integer>> {
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
    }
}
