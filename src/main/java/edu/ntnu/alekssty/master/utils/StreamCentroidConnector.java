package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.vectorobjects.centroids.BaseCentroid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class StreamCentroidConnector {

    final String path;
    final StreamExecutionEnvironment env;
    FileSource<InputObject> source;

    public StreamCentroidConnector(String path, StreamExecutionEnvironment env) {
        this.path = path;
        this.env = env;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamCentroidConnector c = new StreamCentroidConnector("/tmp/experiment-results/naive-centroids.csv", env);
        c.connect();
        env.fromSource(c.source, WatermarkStrategy.noWatermarks(), "test").map(o -> new BaseCentroid(o.getDenseVectors(), 1, o.domain)).print();
        env.execute();
    }


    public StreamCentroidConnector connect() {
        CsvReaderFormat<InputObject> csvFormat =
                CsvReaderFormat.forSchema(
                        new CsvMapper(),
                        CsvSchema.builder()
                                .addColumn(
                                        new CsvSchema.Column(0, "domain", CsvSchema.ColumnType.STRING))
                                .addColumn(
                                        new CsvSchema.Column(1, "features", CsvSchema.ColumnType.ARRAY)
                                                .withArrayElementSeparator("#"))
                                .addColumn(new CsvSchema.Column(2, "cardinality", CsvSchema.ColumnType.NUMBER))
                                .build(),
                        TypeInformation.of(InputObject.class));
        source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(path))).build();
        return this;
    }

    public DataStream<Tuple3<String, DenseVector, Integer>> getCentroids() {
        DataStreamSource<InputObject> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "centroids");
        DataStream<Tuple3<String, DenseVector, Integer>> tupeledStream = stream.map(new MapFunction<InputObject, Tuple3<String, DenseVector, Integer>>() {
            @Override
            public Tuple3<String, DenseVector, Integer> map(InputObject inputObject) throws Exception {
                return Tuple3.of(inputObject.domain, inputObject.getDenseVectors(), inputObject.cardinality);
            }
        });
        return tupeledStream;
    }

    @JsonPropertyOrder({
            "domain",
            "features"
    })
    private static class InputObject {
        public String domain;
        public double[] features;
        public int cardinality;
        public DenseVector getDenseVectors() {
            return Vectors.dense(features);
        }
    }
}
