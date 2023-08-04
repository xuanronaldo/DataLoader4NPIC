package com.timecho.npic.common;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Utils {
    public static List<MeasurementSchema> getSchemas(List<String> headerNames) {
        ArrayList<MeasurementSchema> schemas = new ArrayList<>();
        headerNames.stream().forEach(headerName -> {
            if (!"Time".equals(headerName)) {
                schemas.add(new MeasurementSchema(headerName, TSDataType.DOUBLE));
            }
        });
        return schemas;
    }

    public static DataPoint getDataPoint(TSDataType dataType, String measurementId, Object value) {
        switch (dataType) {
            case TEXT: return new StringDataPoint(measurementId, Binary.valueOf((String) value));
            case BOOLEAN: return new BooleanDataPoint(measurementId, (boolean) value);
            case INT32: return new IntDataPoint(measurementId, (int) value);
            case INT64: return new LongDataPoint(measurementId, (long) value);
            case FLOAT: return new FloatDataPoint(measurementId, (float) value);
            default: return new DoubleDataPoint(measurementId, (double) value);
        }
    }

    public static void generateSingleCSV(String path, long startTimestamp, int rowSize) throws IOException {
        PrintWriter writer = new PrintWriter(path);
        CSVPrinter printer = CSVFormat.DEFAULT.builder()
                .setDelimiter(',')
                .build()
                .print(writer);

//        ArrayList<String> headers = new ArrayList<>();
//        headers.add("Time");
//        for (int i = 0; i < 50; i++) {
//            headers.add(String.format("s_%d", i));
//        }
//        printer.printRecord(headers);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Random random = new Random();
        for (int i = 0; i < rowSize; i++) {
            ArrayList<Object> values = new ArrayList<>();
            values.add(format.format(new Date(startTimestamp + i)));
            for (int j = 0; j < 440; j++) {
                values.add(Double.valueOf(random.nextInt(1000)) / 1000);
            }
            printer.printRecord(values);
        }
        printer.close(true);
    }
}
