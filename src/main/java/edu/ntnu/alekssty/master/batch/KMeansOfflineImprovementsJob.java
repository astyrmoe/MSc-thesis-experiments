/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ntnu.alekssty.master.batch;

import edu.ntnu.alekssty.master.debugging.DebugPoints;
import edu.ntnu.alekssty.master.utils.*;
import edu.ntnu.alekssty.master.vectorobjects.Centroid;
import edu.ntnu.alekssty.master.vectorobjects.Point;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KMeansOfflineImprovementsJob {

	public static void main(String[] args) throws Exception {

		ParameterTool parameter = ParameterTool.fromArgs(args);

		String inputPointPath = parameter.get("input-point-path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");
		String outputsPath = parameter.get("outputs-path", "/tmp/experiment-results/");

		int k = parameter.getInt("k", 2);
		Methods method = Methods.valueOf(parameter.get("method", "naive").toUpperCase());
		String job = "offline";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setParallelism(1);//.setRuntimeMode(RuntimeExecutionMode.BATCH);

		StreamNSLKDDConnector source = new StreamNSLKDDConnector(inputPointPath, env);
		source.connect();

		KMeansOfflineImprovements engine = new KMeansOfflineImprovements()
				.setK(k)
				.setMaxIter(20);

		DataStream<Tuple3<String, DenseVector, String>> input = source.getPoints();//.filter(r -> r.f0.equals("tcprjeS0"));

		DataStreamList result = engine.fit(input, method);

		DataStream<Centroid[]> resultedCentroids = result.get(0);
		resultedCentroids
				.flatMap(new CentroidToTupleForFileOperator()).name("Make centroids csv-ready")
				.writeAsCsv(outputsPath + method+ "-" + job + "-centroids.csv", FileSystem.WriteMode.OVERWRITE);

		DataStream<Point> resultedPoints = result.get(1);
		//resultedPoints.process(new DebugPoints("F Resulted feature", true, true));
		DataStream<Tuple3<String, Integer, String>> readyForCSV = resultedPoints.map(new PointsToTupleForFileOperator());
		readyForCSV.map(t->1).process(new Counter("Ready for CSV"));
		readyForCSV.writeAsCsv(outputsPath + method + "-" + job + "-points.csv", FileSystem.WriteMode.OVERWRITE);

		JobExecutionResult jobResult = env.execute("Experimental work");
		System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
	}
}
