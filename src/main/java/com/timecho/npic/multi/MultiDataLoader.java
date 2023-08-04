package com.timecho.npic.multi;

import com.timecho.npic.common.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiDataLoader {
    final static String DEVICE = "root.test.d_0";
    final static String CURRENT_PATH = System.getProperty("user.dir");

    final static ArrayList<String> HEADERS = new ArrayList<String>() {{
        add("Time");
        for (int i = 0; i < 440; i++) {
            add("s_" + i);
        }
    }};

    public static void main(String[] args) throws InterruptedException {
        String path = args[0];
        Integer poolSize = Integer.valueOf(args[1]);

        File dir = new File(path);
        File[] files = dir.listFiles();

        CountDownLatch latch = new CountDownLatch(files.length);

        ThreadPoolExecutor parsePool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        ThreadPoolExecutor writePool = new ThreadPoolExecutor(
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

        long startTimestamp = System.currentTimeMillis();

        for (File file : files) {
            if (!file.getName().endsWith(".csv")) {
                latch.countDown();
                continue;
            }
            parsePool.submit(() -> {
                try {
                    CSVParser parser = CSVFormat.DEFAULT
                            .builder()
                            .setHeader(HEADERS.toArray(new String[HEADERS.size()]))
                            .setDelimiter(',')
                            .build()
                            .parse(new FileReader(file));
                    List<String> headerNames = parser.getHeaderNames();
                    List<MeasurementSchema> schemas = Utils.getSchemas(headerNames);

                    File tsfile = new File(String.format("%s.tsfile", file.getName().replace(".csv", "")));
                    if (tsfile.exists()) {
                        tsfile.delete();
                    }
                    TsFileWriter tsFileWriter = new TsFileWriter(tsfile);
                    tsFileWriter.registerTimeseries(new Path(DEVICE), schemas);

                    writePool.submit(() -> {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        AtomicInteger index = new AtomicInteger(0);
                        parser.stream().forEach(record -> {
                            long time = 0;
                            try {
                                time = dateFormat.parse(record.get("Time")).getTime();
                                TSRecord tsRecord = new TSRecord(time, DEVICE);
                                headerNames.stream().forEach(headerName -> {
                                    if (!"Time".equals(headerName)) {
                                        tsRecord.addTuple(Utils.getDataPoint(TSDataType.DOUBLE, headerName, Double.valueOf(record.get(headerName))));
                                    }
                                });
                                tsFileWriter.write(tsRecord);

                                if (index.getAndIncrement() % 100 == 0) {
                                    tsFileWriter.flushAllChunkGroups();
                                }
                            } catch (ParseException | IOException | WriteProcessException e) {
                                throw new RuntimeException(e);
                            }
                        });
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
                                session.executeNonQueryStatement(String.format("load '%s/%s'", CURRENT_PATH, tsfile.getName()));
                                session.close();
                            } catch (IoTDBConnectionException | StatementExecutionException e) {
                                throw new RuntimeException(e);
                            }
                            latch.countDown();
                        });
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        latch.await();

        long stopTimestamp = System.currentTimeMillis();
        System.out.println("总共用时：" + (stopTimestamp - startTimestamp) + " ms");

        parsePool.shutdown();
        writePool.shutdown();
        loadPool.shutdown();
    }
}
