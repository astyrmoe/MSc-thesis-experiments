package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.features.Feature;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DebugFeatures extends ProcessFunction<Feature, Feature> {

    IntCounter accFeatures =new IntCounter();

    String domainFilter;
    String title;
    boolean acc;
    boolean print;

    public DebugFeatures(String title, boolean acc, boolean print, String domainFilter) {
        this.domainFilter = domainFilter;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }

    public DebugFeatures(String title, boolean acc, boolean print) {
        this.domainFilter = null;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (acc) {
            getRuntimeContext().addAccumulator(title + "-features", accFeatures);
        }
    }

    @Override
    public void processElement(Feature feature, ProcessFunction<Feature, Feature>.Context context, Collector<Feature> collector) throws Exception {
        if (domainFilter != null && !feature.getDomain().equals(domainFilter)) {return;}
        if (acc) {accFeatures.add(1);}
        if (print) {System.out.println(title + " - " + feature);}
    }
}
