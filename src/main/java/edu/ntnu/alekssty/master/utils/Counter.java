package edu.ntnu.alekssty.master.utils;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Counter extends ProcessFunction<Integer, Integer> {
    final String title;
    final IntCounter counter = new IntCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(title, counter);
    }

    public Counter(String title) {
        this.title = title;
    }

    @Override
    public void processElement(Integer integer, ProcessFunction<Integer, Integer>.Context context, Collector<Integer> collector) throws Exception {
        counter.add(1);
    }
}
