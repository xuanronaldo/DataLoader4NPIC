package com.timecho.npic.single;

import com.timecho.npic.common.Utils;

import java.io.IOException;

public class SingleDataGenerator {
    public static void main(String[] args) throws IOException {
        Utils.generateSingleCSV("dump0.csv", 1690000000000l, 200000);
    }
}
