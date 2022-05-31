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

package edu.ntnu.alekssty.master;

import edu.ntnu.alekssty.master.centroids.Centroid;
import edu.ntnu.alekssty.master.points.Point;
import edu.ntnu.alekssty.master.utils.DebugPoints;
import edu.ntnu.alekssty.master.utils.PointToTupleFunction;
import edu.ntnu.alekssty.master.utils.NSLKDDConnector;
import edu.ntnu.alekssty.master.utils.Counter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class ExperimentJob {

	public static void main(String[] args) throws Exception {

		ParameterTool parameter = ParameterTool.fromArgs(args);

		String method = parameter.get("method", "naive");
		String path = parameter.get("path", "/home/aleks/dev/master/NSL-KDD/KDDTrain+.txt");
		int k = parameter.getInt("k", 2);
		String rootPath = parameter.get("root", "/tmp/experiment-results/");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setParallelism(1);//.setRuntimeMode(RuntimeExecutionMode.BATCH);
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//, EnvironmentSettings.inBatchMode());

		NSLKDDConnector source = new NSLKDDConnector(path, tEnv);
		source.connect();

		KMeansOffline engine = new KMeansOffline()
				.setK(k)
				.setMaxIter(20);

		Table data = source.getDataTable();//.where($("domain").isEqual("tcptimeSH"));

		DataStreamList result = engine.fit(data, method);

		//result.get(2).writeAsText(rootPath + method + "-smalldomains.csv", FileSystem.WriteMode.OVERWRITE);

		DataStream<Centroid[]> resultedCentroids = result.get(0);
		resultedCentroids
				.flatMap(new CentroidToTupleForFileOperator()).name("Make centroids csv-ready")
				.writeAsCsv(rootPath + method+"-centroids.csv", FileSystem.WriteMode.OVERWRITE);

		DataStream<Point> resultedFeatures = result.get(1);
		//resultedFeatures.process(new DebugPoints("F Resulted feature", true, true));
		DataStream<Tuple2<Integer, String>> pointsToResultTable = resultedFeatures.map(new PointToTupleFunction());
		pointsToResultTable.map(t->1).process(new Counter("Points to result table"));
		tEnv.toDataStream(data).map(t->1).process(new Counter("Original data"));
		Table workingTable = tEnv.fromDataStream(pointsToResultTable).as("assigned", "id2")
				.join(data).where($("id").isEqual($("id2")));
		tEnv.toDataStream(workingTable).map(t->1).process(new Counter("Working table"));
		DataStream<Tuple3<String, Integer, String>> readyForCSV = tEnv.toDataStream(workingTable.select($("domain"), $("assigned"), $("cluster")))
				.map(new PointsToTupleForFileOperator());
		readyForCSV.map(t->1).process(new Counter("Ready for CSV"));
		readyForCSV.writeAsCsv(rootPath + method + "-points.csv", FileSystem.WriteMode.OVERWRITE);

		//System.out.println("PARAMETES:\n" + parameter);
		//System.out.println("EXEC PLAN:\n" + env.getExecutionPlan());
		JobExecutionResult jobResult = env.execute("Experimental work");
		System.out.println("JOB RESULTS:\n" + jobResult.getJobExecutionResult());
	}

	private static class CentroidToTupleForFileOperator implements FlatMapFunction<Centroid[], Tuple2<String, DenseVector>> {
		@Override
		public void flatMap(Centroid[] centroids, Collector<Tuple2<String, DenseVector>> collector) throws Exception {
			for (Centroid centroid : centroids) {
				collector.collect(Tuple2.of(centroid.getDomain(), centroid.getVector()));
			}
		}
	}

	private static class PointsToTupleForFileOperator implements MapFunction<Row, Tuple3<String, Integer, String>> {
		@Override
		public Tuple3<String, Integer, String> map(Row row) throws Exception {
			return Tuple3.of((String)row.getField("domain"), (Integer)row.getField("assigned"), (String)row.getField("cluster"));
		}
	}
}
