package edu.ntnu.alekssty.master;

import edu.ntnu.alekssty.master.utils.NSLKDDConnector;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KMeansFlinkOfflineJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        String path = parameter.get("path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");
        int k = parameter.getInt("k", 2);
        String rootPath = parameter.get("root", "/tmp/experiment-results/");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);//.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//, EnvironmentSettings.inBatchMode());

        NSLKDDConnector source = new NSLKDDConnector(path, tEnv);
        source.connect();

        KMeans engine = new KMeans()
                .setK(k)
                .setMaxIter(20);

        Table data = source.getDataTable();//.where($("domain").isEqual("tcpauthSH"));

        KMeansModel result = engine.fit(data);

        // TODO Not able to compare times!

        //System.out.println("PARAMETES:\n" + parameter);
        //System.out.println("EXEC PLAN:\n" + env.getExecutionPlan());
        JobExecutionResult jobResult = env.execute("Experimental work");
        System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());

    }

}
