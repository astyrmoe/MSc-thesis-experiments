package edu.ntnu.alekssty.master.dataoperations;

import edu.ntnu.alekssty.master.batch.Methods;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;

public class ResultJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        int k = parameter.getInt("k", 2);
        Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());

        String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");
        String job = parameter.get("job", "offline");
        String inputPointPath = parameter.get("input-point-path", outputsPath + method + "-" + job + "-points.csv");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setRuntimeMode(RuntimeExecutionMode.BATCH)
                .setParallelism(1);

        CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
        FileSource<SomePojo> source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(inputPointPath))).build();

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

        accumulated.writeAsCsv(outputsPath + method + "-" + job + "-accumulated.csv", FileSystem.WriteMode.OVERWRITE);

        accumulated.map(new MapFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> map(Tuple5<String, Integer, Integer, Integer, Integer> in) throws Exception {
                int tp = 0;
                int fp = 0;
                int tn = 0;
                int fn = 0;
                Tuple4<Integer, Integer, Integer, Integer> tuple = Tuple4.of(in.f1, in.f2, in.f3, in.f4);
                boolean aNormal;
                if (tuple.f1 != tuple.f3) {
                    aNormal = tuple.f1 > tuple.f3;
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
                return Tuple5.of(in.f0, tp, fp, tn, fn);
            }
        }).filter(t-> t.f2+t.f3!=0&&t.f1+t.f4!=0).windowAll(EndOfStreamWindows.get()).process(new ProcessAllWindowFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple3<Double, Double, Double>, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple3<Double, Double, Double>, TimeWindow>.Context context, Iterable<Tuple5<String, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple3<Double, Double, Double>> collector) throws Exception {
                int tp = 0;
                int fp = 0;
                int tn = 0;
                int fn = 0;
                for (Tuple5<String, Integer, Integer, Integer, Integer> i : iterable) {
                    //System.out.println(i);
                    tp += i.f1;
                    fp += i.f2;
                    tn += i.f3;
                    fn += i.f4;
                }
                double br = ((double) tp + (double) fn) / ((double) tp + (double) fp + (double) tn + (double) fn);
                double tpr = (double) tp / ((double) tp + (double) fn);
                double fpr = (double) fp / ((double) fp + (double) tn);
                double ppv = br * tpr / (br * tpr + (1 - br) * fpr);
                System.out.println("TPR = " + tpr);
                System.out.println("FPR = " + fpr);
                System.out.println("PPV = " + ppv);
                collector.collect(Tuple3.of(tpr, fpr, ppv));
            }
        });
        env.execute();
    }

    @JsonPropertyOrder({"domain", "assigned", "label"})
    private static class SomePojo {
        public String domain;
        public int assigned;
        public String label;
    }
}
