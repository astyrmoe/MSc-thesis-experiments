package edu.ntnu.alekssty.master.debugging;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.OfflinePoint;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DebugPoints extends ProcessFunction<OfflinePoint, OfflinePoint> {

    final IntCounter accPoints =new IntCounter();

    final String domainFilter;
    final String title;
    final boolean acc;
    final boolean print;

    public DebugPoints(String title, boolean acc, boolean print, String domainFilter) {
        this.domainFilter = domainFilter;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }

    public DebugPoints(String title, boolean acc, boolean print) {
        this.domainFilter = null;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (acc) {
            getRuntimeContext().addAccumulator(title + "-features", accPoints);
        }
    }

    @Override
    public void processElement(OfflinePoint point, ProcessFunction<OfflinePoint, OfflinePoint>.Context context, Collector<OfflinePoint> collector) throws Exception {
        if (domainFilter != null && !point.getDomain().equals(domainFilter)) {return;}
        if (acc) {
            accPoints.add(1);}
        if (print) {System.out.println(title + " - " + point);}
    }
}
