package edu.ntnu.alekssty.master.utils;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

public class CalculateRatesFunction extends RichMapFunction<Row, Row> {
    //Map<String, IntCounter> intCounters;

    MapStateDescriptor<String, IntCounter> stateDesc = new MapStateDescriptor<String, IntCounter>(
            "state",
            String.class,
            IntCounter.class
    );
    MapState<String, IntCounter> state;
    IntCounter clusterA = new IntCounter();
    IntCounter clusterB = new IntCounter();
    IntCounter clusterANormal = new IntCounter();
    IntCounter clusterBNormal = new IntCounter();
    IntCounter domains = new IntCounter();
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getMapState(stateDesc);
        //intCounters = new HashMap<>();
        getRuntimeContext().addAccumulator("cA", clusterA);
        getRuntimeContext().addAccumulator("cB", clusterB);
        getRuntimeContext().addAccumulator("cAn", clusterANormal);
        getRuntimeContext().addAccumulator("cBn", clusterBNormal);
        getRuntimeContext().addAccumulator("number-of-domains", domains);
    }
    @Override
    public Row map(Row row) throws Exception {
        String domain = (String) row.getField("domain");
        if (!state.contains(domain+"cA")) {
            domains.add(1);
            state.put(domain+"cA", new IntCounter());
            getRuntimeContext().addAccumulator(domain+"cA", state.get(domain+"cA"));
            state.put(domain+"cB", new IntCounter());
            getRuntimeContext().addAccumulator(domain+"cB", state.get(domain+"cB"));
            state.put(domain+"cAn", new IntCounter());
            getRuntimeContext().addAccumulator(domain+"cAn", state.get(domain+"cAn"));
            state.put(domain+"cBn", new IntCounter());
            getRuntimeContext().addAccumulator(domain+"cBn", state.get(domain+"cBn"));
        }
        /*if (!intCounters.containsKey(row.getField("domain"))) {
            intCounters.put(domain+"cA", new IntCounter());
            getRuntimeContext().addAccumulator(domain, intCounters.get(domain));
        }*/
        String cluster = (String) row.getField("cluster");
        Integer assigned = (Integer) row.getField("assigned");
        if (assigned == 0) {
            clusterA.add(1);
            state.get(domain+"cA").add(1);
            if (cluster.equals("normal")) {
                clusterANormal.add(1);
                state.get(domain+"cAn").add(1);
            }
        }
        if (assigned == 1) {
            clusterB.add(1);
            state.get(domain+"cB").add(1);
            if (cluster.equals("normal")) {
                clusterBNormal.add(1);
                state.get(domain+"cBn").add(1);
            }
        }
        return row;
    }

    @Override
    public void close() throws Exception {
        //System.out.println(state);
        super.close();
    }
}
