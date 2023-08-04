package com.timecho.npic.multi;

import com.timecho.npic.common.Utils;

import java.io.IOException;

public class MultiDataGenerator {
    public static void main(String[] args) throws IOException {
        final long timestamp = 1690000000000l;
        final String baseFilename = "dump%d.csv";
        final int rowSize = 200000;

        for (int i = 0; i < 50; i++) {
            Utils.generateSingleCSV(String.format(baseFilename, i), timestamp + i * rowSize, rowSize);
        }
    }
}
