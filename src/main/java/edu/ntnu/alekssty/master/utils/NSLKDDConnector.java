package edu.ntnu.alekssty.master.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class NSLKDDConnector {

    final String path;
    final StreamTableEnvironment tEnv;

    public NSLKDDConnector(String path, StreamTableEnvironment tEnv) {
        this.path = path;
        this.tEnv = tEnv;
    }

    public static void main(String[] args) {
        test(args[0]);
    }

    public void connect() {
        this.tEnv.createTemporaryTable("SourceTableDataTypes", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("duration", DataTypes.FLOAT())
                        .column("protocol_type", DataTypes.STRING())
                        .column("service", DataTypes.STRING())
                        .column("flag", DataTypes.STRING())
                        .column("src_bytes", DataTypes.BIGINT())
                        .column("dst_bytes", DataTypes.BIGINT())
                        .column("land", DataTypes.BOOLEAN())
                        .column("wrong_fragment", DataTypes.BIGINT())
                        .column("urgent", DataTypes.BIGINT())
                        .column("hot", DataTypes.BIGINT())
                        .column("num_failed_logins", DataTypes.BIGINT())
                        .column("logged_in", DataTypes.BOOLEAN())
                        .column("num_compromised", DataTypes.BIGINT())
                        .column("root_shell", DataTypes.BOOLEAN())
                        .column("su_attempted", DataTypes.BIGINT())
                        .column("num_root", DataTypes.BIGINT())
                        .column("num_file_creations", DataTypes.BIGINT())
                        .column("num_shells", DataTypes.BIGINT())
                        .column("num_access_files", DataTypes.BIGINT())
                        .column("num_outbound_cmds", DataTypes.BIGINT())
                        .column("is_hot_login", DataTypes.BOOLEAN())
                        .column("is_guest_login", DataTypes.BOOLEAN())
                        .column("count", DataTypes.BIGINT())
                        .column("srv_count", DataTypes.BIGINT())
                        .column("serror_rate", DataTypes.FLOAT())
                        .column("srv_serror_rate", DataTypes.FLOAT())
                        .column("rerror_rate", DataTypes.FLOAT())
                        .column("srv_rerror_rate", DataTypes.FLOAT())
                        .column("same_srv_rate", DataTypes.FLOAT())
                        .column("diff_srv_rate", DataTypes.FLOAT())
                        .column("srv_diff_host_rate", DataTypes.FLOAT())
                        .column("dst_host_count", DataTypes.BIGINT())
                        .column("dst_host_srv_count", DataTypes.BIGINT())
                        .column("dst_host_same_srv_rate", DataTypes.FLOAT())
                        .column("dst_host_diff_srv_rate", DataTypes.FLOAT())
                        .column("dst_host_same_src_port_rate", DataTypes.FLOAT())
                        .column("dst_host_srv_diff_host_rate", DataTypes.FLOAT())
                        .column("dst_host_serror_rate", DataTypes.FLOAT())
                        .column("dst_host_srv_serror_rate", DataTypes.FLOAT())
                        .column("dst_host_rerror_rate", DataTypes.FLOAT())
                        .column("dst_host_srv_rerror_rate", DataTypes.FLOAT())
                        .column("class", DataTypes.STRING())
                        .column("difficulty", DataTypes.DOUBLE())
                        .build())
                .option("path", this.path)
                .format(FormatDescriptor.forFormat("csv").build())
                .build()
        );
        this.tEnv.createTemporaryTable("SourceTableNumbers", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("duration", DataTypes.DOUBLE())
                        .column("protocol_type", DataTypes.STRING())
                        .column("service", DataTypes.STRING())
                        .column("flag", DataTypes.STRING())
                        .column("src_bytes", DataTypes.DOUBLE())
                        .column("dst_bytes", DataTypes.DOUBLE())
                        .column("land", DataTypes.DOUBLE())
                        .column("wrong_fragment", DataTypes.DOUBLE())
                        .column("urgent", DataTypes.DOUBLE())
                        .column("hot", DataTypes.DOUBLE())
                        .column("num_failed_logins", DataTypes.DOUBLE())
                        .column("logged_in", DataTypes.DOUBLE())
                        .column("num_compromised", DataTypes.DOUBLE())
                        .column("root_shell", DataTypes.DOUBLE())
                        .column("su_attempted", DataTypes.DOUBLE())
                        .column("num_root", DataTypes.DOUBLE())
                        .column("num_file_creations", DataTypes.DOUBLE())
                        .column("num_shells", DataTypes.DOUBLE())
                        .column("num_access_files", DataTypes.DOUBLE())
                        .column("num_outbound_cmds", DataTypes.DOUBLE())
                        .column("is_hot_login", DataTypes.DOUBLE())
                        .column("is_guest_login", DataTypes.DOUBLE())
                        .column("count", DataTypes.DOUBLE())
                        .column("srv_count", DataTypes.DOUBLE())
                        .column("serror_rate", DataTypes.DOUBLE())
                        .column("srv_serror_rate", DataTypes.DOUBLE())
                        .column("rerror_rate", DataTypes.DOUBLE())
                        .column("srv_rerror_rate", DataTypes.DOUBLE())
                        .column("same_srv_rate", DataTypes.DOUBLE())
                        .column("diff_srv_rate", DataTypes.DOUBLE())
                        .column("srv_diff_host_rate", DataTypes.DOUBLE())
                        .column("dst_host_count", DataTypes.DOUBLE())
                        .column("dst_host_srv_count", DataTypes.DOUBLE())
                        .column("dst_host_same_srv_rate", DataTypes.DOUBLE())
                        .column("dst_host_diff_srv_rate", DataTypes.DOUBLE())
                        .column("dst_host_same_src_port_rate", DataTypes.DOUBLE())
                        .column("dst_host_srv_diff_host_rate", DataTypes.DOUBLE())
                        .column("dst_host_serror_rate", DataTypes.DOUBLE())
                        .column("dst_host_srv_serror_rate", DataTypes.DOUBLE())
                        .column("dst_host_rerror_rate", DataTypes.DOUBLE())
                        .column("dst_host_srv_rerror_rate", DataTypes.DOUBLE())
                        .column("class", DataTypes.STRING())
                        .column("difficulty", DataTypes.DOUBLE())
                .build())
                .option("path", this.path)
                .format(FormatDescriptor.forFormat("csv").build())
                .build()
        );
    }

    public Table getSourceTableNumbers() {
        return tEnv.from("SourceTableNumbers");
    }

    public Table getDataTable() {
        Table fourStringsAndFeatureArray =  getSourceTableNumbers().leftOuterJoinLateral(call(FloatsToFeatureArrayMaker.class,
                        $("class"),
                        //$("protocol_type"),
                        //$("service"),
                        //$("flag"),
                        $("duration"),
                        $("src_bytes"),
                        $("dst_bytes"),
                        $("land"),
                        $("wrong_fragment"),
                        $("urgent"),
                        $("hot"),
                        $("num_failed_logins"),
                        $("logged_in"),
                        $("num_compromised"),
                        $("root_shell"),
                        $("su_attempted"),
                        $("num_root"),
                        $("num_file_creations"),
                        $("num_shells"),
                        $("num_access_files"),
                        $("num_outbound_cmds"),
                        $("is_hot_login"),
                        $("is_guest_login"),
                        $("count"),
                        $("srv_count"),
                        $("serror_rate"),
                        $("srv_serror_rate"),
                        $("rerror_rate"),
                        $("srv_rerror_rate"),
                        $("same_srv_rate"),
                        $("diff_srv_rate"),
                        $("srv_diff_host_rate"),
                        $("dst_host_count"),
                        $("dst_host_srv_count"),
                        $("dst_host_same_srv_rate"),
                        $("dst_host_diff_srv_rate"),
                        $("dst_host_same_src_port_rate"),
                        $("dst_host_srv_diff_host_rate"),
                        $("dst_host_serror_rate"),
                        $("dst_host_srv_serror_rate"),
                        $("dst_host_rerror_rate"),
                        $("dst_host_srv_rerror_rate")
                ).as("cluster", "features"))
                .select($("class"), $("cluster"), $("protocol_type"), $("service"), $("flag"), $("features"));
        DataStream<R> formattedRecords = tEnv.toDataStream(fourStringsAndFeatureArray).map(new FormatRowFunction()).name("Make final row");
        return tEnv.fromDataStream(formattedRecords).as("class", "cluster", "domain", "features", "id");
    }

    public static void test(String path) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        NSLKDDConnector nslkddConnector = new NSLKDDConnector(path, tEnv);
        nslkddConnector.connect();
        Table t = nslkddConnector.getDataTable();
        t.printSchema();
        t.limit(4).execute().print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<s STRING, floats ARRAY<DOUBLE>>"))
    public static class FloatsToFeatureArrayMaker extends TableFunction<Row> {
        public void eval(String s1, Double... floats) {
            Double[] d = new Double[floats.length];
            int i = 0;
            for (double f : floats) {
                d[i] = f;
                i++;
            }
            if (!s1.equals("normal")) {
                s1 = "abnormal";
            }
            collect(Row.of(s1, d));
        }
    }

    public static class FormatRowFunction implements MapFunction<Row, R> {
        @Override
        public R map(Row row) throws Exception {
            R object = new R();
            object.domain = (String) row.getField("protocol_type") + (String) row.getField("service") + (String) row.getField("flag");
            Double[] features = (Double[]) row.getField("features");
            double[] f = new double[features.length];
            int i = 0;
            for (double d : features) {
                f[i] = d;
                i++;
            }
            object.attackClass = (String) row.getField("class");
            object.cluster = (String) row.getField("cluster");
            object.features = Vectors.dense(f);
            object.id = ComputeId.compute(object.features, object.domain);
            //object.difficulty = (double) row.getField("difficulty");
            return object;
        }
    }

    public static class R {
        public String id;
        public String domain;
        public String attackClass;
        public String cluster;
        public DenseVector features;
        //public double difficulty;

        public R() {
        }
    }
}
