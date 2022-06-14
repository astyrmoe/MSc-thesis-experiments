package edu.ntnu.alekssty.master.utils;

import edu.ntnu.alekssty.master.vectorobjects.offline.offlinepoints.BaseOfflinePoint;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.*;

public class StreamNSLKDDConnector {

    final String path;
    final StreamExecutionEnvironment env;
    FileSource<InputObject> source;

    public StreamNSLKDDConnector(String path, StreamExecutionEnvironment env) {
        this.path = path;
        this.env = env;
    }

    public DataStream<Tuple3<String, DenseVector, String>> getRandomPoints(long seed) {
        return this.getPoints().windowAll(EndOfStreamWindows.get()).process(new ProcessAllWindowFunction<Tuple3<String, DenseVector, String>, Tuple3<String, DenseVector, String>, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple3<String, DenseVector, String>, Tuple3<String, DenseVector, String>, TimeWindow>.Context context, Iterable<Tuple3<String, DenseVector, String>> iterable, Collector<Tuple3<String, DenseVector, String>> collector) throws Exception {
                List<Tuple3<String, DenseVector, String>> out = new ArrayList<>();
                for (Tuple3<String, DenseVector, String> stringDenseVectorStringTuple3 : iterable) {
                    out.add(stringDenseVectorStringTuple3);
                }
                Collections.shuffle(out,new Random(seed));
                for (Tuple3<String, DenseVector, String> o : out) {
                    collector.collect(o);
                }
            }
        });
    }

    public DataStream<Tuple3<String, DenseVector, String>> getPoints() {
        DataStreamSource<InputObject> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "NSLKDD");
        DataStream<Tuple3<String, DenseVector, String>> tupeledStream = stream.map(new MapFunction<InputObject, Tuple3<String, DenseVector, String>>() {
            @Override
            public Tuple3<String, DenseVector, String> map(InputObject inputObject) throws Exception {
                return Tuple3.of(inputObject.getDomain(), inputObject.getFeatures(), inputObject.getNormalOrAnormal());
            }
        });
        return tupeledStream;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamNSLKDDConnector c = new StreamNSLKDDConnector("/home/aleks/dev/master/NSL-KDD/KDDTrain+_20Percent.txt", env);
        c.connect();
        env.fromSource(c.source, WatermarkStrategy.noWatermarks(), "test").map(o -> new BaseOfflinePoint(o.getFeatures(), o.getDomain(), o.getNormalOrAnormal())).print();
        env.execute();
    }

    public StreamNSLKDDConnector connect() {
        CsvReaderFormat<InputObject> csvFormat = CsvReaderFormat.forPojo(InputObject.class);
        source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(path))).build();
        return this;
    }

    @JsonPropertyOrder({
            "duration",
            "protocol_type",
            "service",
            "flag",
            "src_bytes",
            "dst_bytes",
            "land",
            "wrong_fragment",
            "urgent",
            "hot",
            "num_failed_logins",
            "logged_in",
            "num_compromised",
            "root_shell",
            "su_attempted",
            "num_root",
            "num_file_creations",
            "num_shells",
            "num_access_files",
            "num_outbound_cmds",
            "is_hot_login",
            "is_guest_login",
            "count",
            "srv_count",
            "serror_rate",
            "srv_serror_rate",
            "rerror_rate",
            "srv_rerror_rate",
            "same_srv_rate",
            "diff_srv_rate",
            "srv_diff_host_rate",
            "dst_host_count",
            "dst_host_srv_count",
            "dst_host_same_srv_rate",
            "dst_host_diff_srv_rate",
            "dst_host_same_src_port_rate",
            "dst_host_srv_diff_host_rate",
            "dst_host_serror_rate",
            "dst_host_srv_serror_rate",
            "dst_host_rerror_rate",
            "dst_host_srv_rerror_rate",
            "label",
            "difficulty"
    })
    private static class InputObject {
        public double duration;
        public String protocol_type;
        public String service;
        public String flag;
        public double src_bytes;
        public double dst_bytes;
        public double land;
        public double wrong_fragment;
        public double urgent;
        public double hot;
        public double num_failed_logins;
        public double logged_in;
        public double num_compromised;
        public double root_shell;
        public double su_attempted;
        public double num_root;
        public double num_file_creations;
        public double num_shells;
        public double num_access_files;
        public double num_outbound_cmds;
        public double is_hot_login;
        public double is_guest_login;
        public double count;
        public double srv_count;
        public double serror_rate;
        public double srv_serror_rate;
        public double rerror_rate;
        public double srv_rerror_rate;
        public double same_srv_rate;
        public double diff_srv_rate;
        public double srv_diff_host_rate;
        public double dst_host_count;
        public double dst_host_srv_count;
        public double dst_host_same_srv_rate;
        public double dst_host_diff_srv_rate;
        public double dst_host_same_src_port_rate;
        public double dst_host_srv_diff_host_rate;
        public double dst_host_serror_rate;
        public double dst_host_srv_serror_rate;
        public double dst_host_rerror_rate;
        public double dst_host_srv_rerror_rate;
        public String  label;
        public int difficulty;

        public String getDomain() {
            return protocol_type + service + flag;
        }

        public DenseVector getFeatures() {
            return Vectors.dense(
                    duration,
                    src_bytes,
                    dst_bytes,
                    land,
                    wrong_fragment,
                    urgent,
                    hot,
                    num_failed_logins,
                    logged_in,
                    num_compromised,
                    root_shell,
                    su_attempted,
                    num_root,
                    num_file_creations,
                    num_shells,
                    num_access_files,
                    num_outbound_cmds,
                    is_hot_login,
                    is_guest_login,
                    count,
                    srv_count,
                    serror_rate,
                    srv_serror_rate,
                    rerror_rate,
                    srv_rerror_rate,
                    same_srv_rate,
                    diff_srv_rate,
                    srv_diff_host_rate,
                    dst_host_count,
                    dst_host_srv_count,
                    dst_host_same_srv_rate,
                    dst_host_diff_srv_rate,
                    dst_host_same_src_port_rate,
                    dst_host_srv_diff_host_rate,
                    dst_host_serror_rate,
                    dst_host_srv_serror_rate,
                    dst_host_rerror_rate,
                    dst_host_srv_rerror_rate
            );
        }

        public int getDifficultyLevel() {
            return difficulty;
        }

        public String getNormalOrAnormal() {
            if (label.equals("normal")) {
                return "normal";
            }
            return "anormal";
        }
    }
}
