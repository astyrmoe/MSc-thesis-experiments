package edu.ntnu.alekssty.master;

import edu.ntnu.alekssty.master.offline.KMeansOfflineImprovementsJob;
import edu.ntnu.alekssty.master.debugging.Testing;
import edu.ntnu.alekssty.master.debugging.Testing2;
import edu.ntnu.alekssty.master.moo.BatchJob;
import edu.ntnu.alekssty.master.moo.SequentialJob;
import edu.ntnu.alekssty.master.moo.TransformJob;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;

public class Job {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        System.out.println("CONFIG:\n"+ Arrays.toString(args));

        String job = parameter.get("job", "offline");

        switch (job) {
            case "offline":
                KMeansOfflineImprovementsJob.main(args);
                break;
            case "transform":
                TransformJob.main(args);
                break;
            case "test":
                Testing.main(args);
                break;
            case "test2":
                Testing2.main(args);
                break;
            case "seq":
                SequentialJob.main(args);
                break;
            case "batch":
                BatchJob.main(args);
                break;
        }
    }
}
