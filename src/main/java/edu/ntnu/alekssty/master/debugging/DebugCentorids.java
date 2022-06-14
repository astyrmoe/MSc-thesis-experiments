package edu.ntnu.alekssty.master.debugging;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinecentroids.OfflineCentroid;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DebugCentorids extends ProcessFunction<OfflineCentroid[], OfflineCentroid[]> {

    final IntCounter accCentroids=new IntCounter();
    final IntCounter accDomains = new IntCounter();

    final String domainFilter;
    final String title;
    final boolean acc;
    boolean print;

    public DebugCentorids(String title, boolean acc, boolean print, String domainFilter) {
        this.domainFilter = domainFilter;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }

    public DebugCentorids(String title, boolean acc, boolean print) {
        this.domainFilter = null;
        this.title = title;
        this.acc = acc;
        this.print = print;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (acc) {
            getRuntimeContext().addAccumulator(title + "-centroids", accCentroids);
            getRuntimeContext().addAccumulator(title + "-domains", accDomains);
        }
    }

    @Override
    public void processElement(OfflineCentroid[] centroids, ProcessFunction<OfflineCentroid[], OfflineCentroid[]>.Context context, Collector<OfflineCentroid[]> collector) throws Exception {
        if (domainFilter != null && !centroids[0].getDomain().equals(domainFilter)) {
            return;
        }
        if (acc) {
            accDomains.add(1);
            accCentroids.add(centroids.length);
        }
        if (print) {
            System.out.println(title + " - Domain: " + centroids[0].getDomain());
            for (OfflineCentroid centroid : centroids) {
                if (acc) {accCentroids.add(1);}
                System.out.println(title + " - -> " + centroid);
            }
        }
    }
}
