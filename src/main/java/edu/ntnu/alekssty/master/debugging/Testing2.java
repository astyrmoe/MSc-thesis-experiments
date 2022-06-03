package edu.ntnu.alekssty.master.debugging;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.*;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.*;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.iteration.IterationConfig.OperatorLifeCycle.ALL_ROUND;
import static org.apache.flink.iteration.IterationConfig.OperatorLifeCycle.PER_ROUND;

// FUNKER IKKE
public class Testing2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Integer> initParameters = env.fromElements(0,0,0);
        DataStream<String> dataset = env.fromElements("a", "b", "c");

        DataStreamList resultStreams = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(initParameters),
                ReplayableDataStreamList.notReplay(dataset),
                IterationConfig.newBuilder().setOperatorLifeCycle(ALL_ROUND).build(),
                new MyIterationBody());

        //resultStreams.get(0).print("1");

        env.execute();
    }

    private static class IntegerStringIntegerBroadcastProcessFunction extends BroadcastProcessFunction<Integer, String, Integer> {
        Map<Integer, String> c = new HashMap<>();
        Integer buffer;

        @Override
        public void processElement(Integer integer, BroadcastProcessFunction<Integer, String, Integer>.ReadOnlyContext readOnlyContext, Collector<Integer> collector) throws Exception {
            collector.collect(integer + 1);
            c.get(1);
            buffer = integer;
        }

        @Override
        public void processBroadcastElement(String s, BroadcastProcessFunction<Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
            c.put(1, s);
            buffer = 0;
        }
    }

    private static class MyIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {
            DataStream<Integer> modelUpdate = dataStreamList.get(0);
            DataStream<String> dataset2 = dataStreamList1.get(0);

            DataStream<Integer> newModelUpdate = modelUpdate.connect(dataset2.broadcast()).flatMap(new IntegerStringIntegerRichCoFlatMapFunction());
            return new IterationBodyResult(
                    DataStreamList.of(newModelUpdate),
                    DataStreamList.of(newModelUpdate.filter(t->t>10)));
        }

        private static class MyCoProcessFunction extends CoProcessFunction<Integer, String, Integer> {
            Map<Integer, String> s = new HashMap<>();
            Integer buffer;

            @Override
            public void processElement1(Integer integer, CoProcessFunction<Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
                buffer = integer;
                s.get(1);
                collector.collect(integer+1);
            }

            @Override
            public void processElement2(String sd, CoProcessFunction<Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
                buffer = 0;
                s.put(1, sd);
            }
        }

        private static class StringIntegerStringIntegerKeyedCoProcessFunction extends KeyedCoProcessFunction<String, Integer, String, Integer> {
            Map<Integer, String> s = new HashMap<>();
            Integer buffer;

            @Override
            public void processElement1(Integer integer, KeyedCoProcessFunction<String, Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
                buffer = integer;
                s.get(1);
                collector.collect(integer+1);
            }

            @Override
            public void processElement2(String sd, KeyedCoProcessFunction<String, Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
                buffer = 0;
                s.put(1, sd);
            }
        }

        private static class IntegerStringIntegerRichCoFlatMapFunction extends RichCoFlatMapFunction<Integer, String, Integer> implements IterationListener<Integer> {
            Map<Integer, String> s = new HashMap<>();
            Integer buffer;

            @Override
            public void flatMap1(Integer integer, Collector<Integer> collector) throws Exception {
                System.out.println(integer);
                buffer = integer;
                s.get(1);
                collector.collect(integer+1);
            }

            @Override
            public void flatMap2(String sd, Collector<Integer> collector) throws Exception {
                buffer = 0;
                s.put(1, sd);
            }

            @Override
            public void onEpochWatermarkIncremented(int i, Context context, Collector<Integer> collector) throws Exception {
                System.out.println("incr"+i);

            }

            @Override
            public void onIterationTerminated(Context context, Collector<Integer> collector) throws Exception {
                System.out.println("ferdi");

            }
        }

        private static class IntegerStringIntegerCoFlatMapFunction implements CoFlatMapFunction<Integer, String, Integer> {
            Map<Integer, String> s = new HashMap<>();
            Integer buffer;

            @Override
            public void flatMap1(Integer integer, Collector<Integer> collector) throws Exception {
                buffer = integer;
                if (s.containsKey(1)) {
                    s.get(1);
                }
                collector.collect(integer+1);

            }

            @Override
            public void flatMap2(String sd, Collector<Integer> collector) throws Exception {
                buffer = 0;
                s.put(1, sd);

            }
        }

        private static class IntegerStringIntegerTwoInputStreamOperator extends AbstractStreamOperator<Integer> implements TwoInputStreamOperator<Integer, String, Integer> {
            Map<Integer, String> s = new HashMap<>();
            Integer buffer;

            @Override
            public void processElement1(StreamRecord<Integer> streamRecord) throws Exception {
                buffer = streamRecord.getValue();
                s.get(1);
                output.collect(new StreamRecord<>(buffer+1));

            }

            @Override
            public void processElement2(StreamRecord<String> streamRecord) throws Exception {
                buffer = 0;
                s.put(1, streamRecord.getValue());

            }

            @Override
            public void processWatermark1(Watermark watermark) throws Exception {

            }

            @Override
            public void processWatermark2(Watermark watermark) throws Exception {

            }

            @Override
            public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {

            }

            @Override
            public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {

            }

            @Override
            public void open() throws Exception {

            }

            @Override
            public void finish() throws Exception {

            }

            @Override
            public void close() throws Exception {

            }

            @Override
            public void prepareSnapshotPreBarrier(long l) throws Exception {

            }

            @Override
            public OperatorMetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public OperatorID getOperatorID() {
                return null;
            }

            @Override
            public void notifyCheckpointComplete(long l) throws Exception {

            }

            @Override
            public void setCurrentKey(Object o) {

            }

            @Override
            public Object getCurrentKey() {
                return null;
            }
        }
    }
}
