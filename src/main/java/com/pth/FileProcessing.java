package com.pth;

import com.google.common.base.Stopwatch;
import com.pth.domain.ReportTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileProcessing {

    // Since the csv file quite large, so we need to increase the JVM memory, eg: -Xms5G -Xmx5G -XX:NewSize=1024M
    public static void main(String[] args) throws Exception {
        log.info ("Initialize free memory {}", Runtime.getRuntime().freeMemory());
        final String fileName = "star2002-full.csv";
        final String filePath = "C:\\Users\\mypc\\Downloads\\import\\" + fileName;
        final String outputFilePath = "C:\\Users\\mypc\\Downloads\\import\\out\\" + fileName;

        // Test read file
        readWriteFile(filePath, outputFilePath);

        // Test read and export to pdf with jasper report
//        exportPDF(outputFilePath);

//        emptyDir("D:\\Work\\IntelliJ\\mypc\\src\\main\\resources\\reports\\out");
    }

    private static void emptyDir(String dirPath) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Trying to clear folder {}",  dirPath);
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            log.info("Directory {} doest not exist", dirPath);
            return;
        }

        log.info("Total files: {}", dir.list().length);

        for (File file : dir.listFiles()) {
            file.deleteOnExit();
        }
        log.info("Completed! Execution time {}(s)", sw.elapsed(TimeUnit.SECONDS));
    }

    private static void exportPDF(String filePath) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Exporting pdf...");

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        reader.lines().parallel().forEach(s -> {
            String[] rowData = s.split(DEFAULT_SEPARATE);

            ReportTesting reportData = new ReportTesting(rowData[0], rowData[1], rowData[2], rowData[3], rowData[4], rowData[5], rowData[6]);
            JasperReportProcessing.exportPdf(reportData, UUID.randomUUID().toString());
        });

        log.info("Done export! Execution time {}(s)", sw.elapsed(TimeUnit.SECONDS));
    }

    private static void readWriteFile(String filePath, String outputFilePath) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        //        readFileWithCsvParser(filePath);
        log.info("Reading data...");
        List<String> allRows = readFileWithJava8BufferedReader(filePath);// 15857625 rows, 7s
//        readFileWithMappedByteBuffer(outputFilePath); // 15857625 rows, 12s
//        readFileWithScanner(outputFilePath); // 15857625 rows, 19s
//        readFileWithJava8Stream(filePath); // 15857625 rows, 8s
        log.info("Done reading! Execution time {}(s)", sw.elapsed(TimeUnit.SECONDS));
        log.info("After read free memory: {}", Runtime.getRuntime().freeMemory());

        sw.reset();
        sw.start();

        processDataAndWrite(allRows, outputFilePath);

        log.info("Done processing! Execution time {}(s)", sw.elapsed(TimeUnit.SECONDS));
        log.info("After processing free memory: {}", Runtime.getRuntime().freeMemory());

        // read data after processing
        long beforeMem = Runtime.getRuntime().freeMemory();
        allRows.clear();
        System.gc();
        long afterMem = Runtime.getRuntime().freeMemory();
        log.info(beforeMem + " --- " + afterMem);

        // Read the processed file.
        readFileWithJava8BufferedReader(outputFilePath);
    }

    private static List<String> readFileWithJava8BufferedReader(final String filePath) throws IOException {
        List<String> allRows = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        allRows.addAll(reader.lines().collect(Collectors.toList()));
        log.info("Finished reading file by Java 8 Buffer Reader. Number of rows: {}", allRows.size());
        return allRows;
    }

    private static void readFileWithMappedByteBuffer(final String filePath) throws FileNotFoundException, IOException {
        List<String> allRows = new ArrayList<>();
        RandomAccessFile aFile = new RandomAccessFile(filePath, "rw");
        FileChannel inChannel = aFile.getChannel();
        MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_WRITE, 0, inChannel.size());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buffer.limit(); i++) {
            byte read = buffer.get();
            char c = (char) read;
            if (c == '\n') {
                allRows.add(sb.toString());
                sb = new StringBuilder();
            }
            sb.append(c);

        }
        aFile.close();

        log.info("Finished reading file by mapped byte buffer. Number of rows: {}", allRows.size());
    }

    private static final String DEFAULT_SEPARATE = ",";
    private static final String DOUBLE_QUOTE = "\"";
    private static void processDataAndWrite(List<String> allRows, String outputFilePath) throws IOException {
        log.info("Processing data...");
        PrintWriter pw = null;
        int rowCount = 0;
        try {
            pw = new PrintWriter(outputFilePath);

            for (String rowData : allRows) {
                if (rowCount++ == 100000) break;

                String[] data = rowData.split(DEFAULT_SEPARATE);
                StringBuilder sb = new StringBuilder();
                int i = 0;
                for (String s : data) {
                    if (s == null) {
                        s = "\"\"";
                    }
                    sb.append(DOUBLE_QUOTE);
                    sb.append(StringUtils.strip(s));
                    sb.append(DOUBLE_QUOTE);

                    if (++i != data.length)
                        sb.append(DEFAULT_SEPARATE);
                }

                sb.append("\n");
                pw.write(sb.toString());
            }
            pw.flush();
        } finally {
            if (pw != null) pw.close();
        }
    }

    private static void readFileWithScanner(final String filePath) throws IOException {
        List<String> allRows = new ArrayList<>();
        FileInputStream inputStream = null;
        Scanner sc = null;
        try {
            inputStream = new FileInputStream(filePath);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                allRows.add(sc.nextLine());
                // Do something with the line
            }

        } catch (IOException ex) {
            System.out.println(ex);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }

        log.info("Finished reading file by scanner. Number of rows: {}", allRows.size());
    }

    private static void readFileWithJava8Stream(final String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Stream<String> linesStream = Files.lines(path);
        List<String> allRows = linesStream.collect(Collectors.toList());
        log.info("Finished reading file by java 8 streaming. Number of rows: {}", allRows.size());
    }
}