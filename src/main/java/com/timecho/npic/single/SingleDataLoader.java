package com.timecho.npic.single;

import com.timecho.npic.common.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class SingleDataLoader {
    final static String DEVICE = "root.test.d_0";
    final static String CURRENT_PATH = System.getProperty("user.dir");

    public static void main(String[] args) throws IOException, InterruptedException {
        String filePath = args[0];
        int poolSize = Integer.valueOf(args[1]);
        FileReader in = new FileReader(filePath);

        long startReadingTimestamp = System.currentTimeMillis();

        ArrayList<String> headers = new ArrayList<String>() {{
            add("Time");
            for (int i = 0; i < 440; i++) {
                add("s_" + i);
            }
        }};
        CSVParser parser = CSVFormat.DEFAULT
                .builder()
                .setHeader(headers.toArray(new String[headers.size()]))
                .setDelimiter(',')
                .build()
                .parse(in);
        List<CSVRecord> records = parser.getRecords();
        long stopReadingTimestamp = System.currentTimeMillis();

        List<String> headerNames = parser.getHeaderNames();
        List<MeasurementSchema> schemas = Utils.getSchemas(headerNames);

        CountDownLatch latch = new CountDownLatch(poolSize);

        ThreadPoolExecutor parsePool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        ThreadPoolExecutor loadPool = new ThreadPoolExecutor(
                1,
                poolSize,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        for (int i = 0; i < poolSize; i++) {
            String fileName = String.format("%d.tsfile", i);
            File tsfile = new File(fileName);
            if (tsfile.exists()) {
                tsfile.delete();
            }

            int startIndex = i * records.size() / poolSize;
            int stopIndex = (i + 1) * records.size() / poolSize;

            parsePool.submit(() -> {
                TsFileWriter tsFileWriter = null;
                try {
                    tsFileWriter = new TsFileWriter(tsfile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                tsFileWriter.registerTimeseries(new Path(DEVICE), schemas);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                for (int index = startIndex; index < stopIndex; index++) {
                    CSVRecord record = records.get(index);
                    Long time = null;
                    try {
                        time = dateFormat.parse(record.get("Time")).getTime();
                        TSRecord tsRecord = new TSRecord(time, DEVICE);
                        headerNames.stream().forEach(headerName -> {
                            if (!"Time".equals(headerName)) {
                                tsRecord.addTuple(Utils.getDataPoint(TSDataType.DOUBLE, headerName, Double.valueOf(record.get(headerName))));
                            }
                        });
                        tsFileWriter.write(tsRecord);

                        if (index % 100 == 0) {
                            tsFileWriter.flushAllChunkGroups();
                        }
                    } catch (ParseException | IOException | WriteProcessException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    tsFileWriter.flushAllChunkGroups();
                    tsFileWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                loadPool.submit(() -> {
                    try {
                        Session session = new Session.Builder().build();
                        session.open();
                        session.executeNonQueryStatement(String.format("load '%s/%s'", CURRENT_PATH, fileName));
                        session.close();
                    } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                });
            });
        }

        latch.await();

        long stopWritingTimestamp = System.currentTimeMillis();
        System.out.println("读数据用时: " + (stopReadingTimestamp - startReadingTimestamp) + " ms");
        System.out.println("写数据用时: " + (stopWritingTimestamp - stopReadingTimestamp) + " ms");
        System.out.println("总共用时: " + (stopWritingTimestamp - startReadingTimestamp) + " ms");

        parsePool.shutdown();
        loadPool.shutdown();
    }
}
