package edu.ntnu.alekssty.master.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Stack;

public class NewIteration {

    static NewIteration instance = null;
    int iterationP;
    int iterationC;

    private NewIteration() {
    }

    public static NewIteration getInstance() {
        if (instance == null) {
            instance = new NewIteration();
            instance.iterationC = 0;
            instance.iterationP = 0;
        }
        return instance;
    }

    public int getPoint() {
        instance.iterationP ++;
        return instance.iterationP-1;
    }

    public int getCentroid() {
        instance.iterationC++;
        return instance.iterationC-1;
    }
}
