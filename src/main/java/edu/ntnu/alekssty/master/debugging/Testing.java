package edu.ntnu.alekssty.master.debugging;

import edu.ntnu.alekssty.master.utils.StreamCentroidConnector;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.iteration.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.iteration.IterationConfig.OperatorLifeCycle.ALL_ROUND;

// FUNKER
public class Testing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStream<Integer> initParameters = env.fromElements(1,2,3);
        DataStream<String> dataset = env.fromElements("a", "b", "c");

        DataStreamList resultStreams = Iterations.iterateUnboundedStreams(
                DataStreamList.of(initParameters),
                DataStreamList.of(dataset),
                new MyIterationBody());

        resultStreams.get(0).print("1");

        env.execute();
    }

    private static class IntegerStringIntegerBroadcastProcessFunction extends BroadcastProcessFunction<Integer, String, Integer> {
        private final MapStateDescriptor<Integer, String> desc;
        Integer buffer;

        public IntegerStringIntegerBroadcastProcessFunction(MapStateDescriptor<Integer, String> desc) {
            this.desc = desc;
        }

        @Override
        public void processElement(Integer integer, BroadcastProcessFunction<Integer, String, Integer>.ReadOnlyContext readOnlyContext, Collector<Integer> collector) throws Exception {
            collector.collect(integer + 1);
            readOnlyContext.getBroadcastState(desc).get(1);
            buffer = integer;
        }

        @Override
        public void processBroadcastElement(String s, BroadcastProcessFunction<Integer, String, Integer>.Context context, Collector<Integer> collector) throws Exception {
            context.getBroadcastState(desc).put(1, s);
            buffer = 0;
        }
    }

    private static class MyIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(DataStreamList dataStreamList, DataStreamList dataStreamList1) {
            DataStream<Integer> modelUpdate = dataStreamList.get(0);
            DataStream<String> dataset2 = dataStreamList1.get(0);

            MapStateDescriptor<Integer, String> desc = new MapStateDescriptor<Integer, String>("t", Integer.class, String.class);
            DataStream<Integer> newModelUpdate = modelUpdate.connect(dataset2.broadcast(desc)).process(new IntegerStringIntegerBroadcastProcessFunction(desc));

            return new IterationBodyResult(
                    DataStreamList.of(newModelUpdate),
                    DataStreamList.of(newModelUpdate.filter(t->t>10)));
        }
    }
}
