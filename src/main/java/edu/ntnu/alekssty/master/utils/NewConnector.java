package edu.ntnu.alekssty.master.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

public class NewConnector {

    String path;
    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;
    FileSource<InputObject> source;

    public NewConnector(String path, StreamExecutionEnvironment env) {
        this.path = path;
        this.env = env;
        this.tEnv = StreamTableEnvironment.create(env);
    }

    public static void main(String[] args) {
        test();
    }

    private static void test() {
    }

    public void connect() {
        CsvReaderFormat<InputObject> csvFormat = CsvReaderFormat.forPojo(InputObject.class);
        source = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(path))).build();
    }

    private class InputObject {



    }
}
