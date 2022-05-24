package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.centroids.Centroid;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PrintCentorids extends ProcessFunction<Centroid[], Centroid[]> {
    @Override
    public void processElement(Centroid[] centroids, ProcessFunction<Centroid[], Centroid[]>.Context context, Collector<Centroid[]> collector) throws Exception {
        System.out.println("Domain: " + centroids[0].getDomain());
        for (Centroid centroid : centroids) {
            System.out.println("-> " + centroid);
        }
    }
}
