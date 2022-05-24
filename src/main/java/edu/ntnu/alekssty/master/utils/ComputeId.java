package edu.ntnu.alekssty.master.utils;

import org.apache.flink.ml.linalg.DenseVector;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public abstract class ComputeId {

    public static String compute(DenseVector vector) throws UnsupportedEncodingException {
        return Base64.getEncoder().encodeToString(vector.toString().getBytes(StandardCharsets.UTF_8));
    }
}
