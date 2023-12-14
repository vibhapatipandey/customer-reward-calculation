package com.java.file.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ParallelZipFileProcessor {

	public static void main(String[] args) {
        String zipFilePath = "C:\\testdata\\TEST.N057.BOD.UNLD93.DAILY.ZIP_01302023"; // Replace with your input zip file path
        String outputDirectory = "C:\\testdata\\parallel-output\\"; // Replace with your output directory path
        String logFilePath = "C:\\testdata\\parallel-output.txt"; // Log file path
        int numThreads = 4; // Number of threads for parallel processing (adjust as needed)

        try {
            Files.createDirectories(Paths.get(outputDirectory));

            ZipFile zipFile = new ZipFile(zipFilePath, StandardCharsets.UTF_8);
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            List<ZipEntry> entryList = Collections.list((Enumeration<ZipEntry>)entries);

            Map<String, BufferedWriter> writers = new ConcurrentHashMap<>();
            Map<String, Integer> recordCountMap = new ConcurrentHashMap<>();

            // Create an ExecutorService with a fixed number of threads
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

            List<Callable<Void>> tasks = new ArrayList<>();

            for (ZipEntry entry : entryList) {
                tasks.add(() -> {
                    try {
                        String entryData = readZipEntryData(zipFile, entry);
                        processEntryData(entryData, writers, outputDirectory, recordCountMap);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.err.println("Error processing zip entry: " + e.getMessage());
                    }
                    return null;
                });
            }

            // Invoke all tasks and wait for them to complete
            executorService.invokeAll(tasks);

            // Shutdown the ExecutorService
            executorService.shutdown();

            closeWriters(writers);
            writeLog(logFilePath, recordCountMap);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }catch (OutOfMemoryError e) {
			System.err.println("Error is: "+e.getMessage());
		}
    } 

    private static String readZipEntryData(ZipFile zipFile, ZipEntry entry) throws IOException {
        try (InputStream entryInputStream = zipFile.getInputStream(entry)) {
            ByteArrayOutputStream entryOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = entryInputStream.read(buffer)) != -1) {
                entryOutputStream.write(buffer, 0, bytesRead);
            }
            return entryOutputStream.toString("UTF-8");
        }
    }

    private static void processEntryData(
            String entryData,
            Map<String, BufferedWriter> writers,
            String outputDirectory,
            Map<String, Integer> recordCountMap) {
        try (BufferedReader entryReader = new BufferedReader(new StringReader(entryData))) {
            String line;
            while ((line = entryReader.readLine()) != null) {
                if (line.length() < 7) {
                    continue;
                }

                String recordType = line.substring(7, 10);
                BufferedWriter writer = writers.computeIfAbsent(recordType, key -> {
                    try {
                        String outputFile = outputDirectory + "/output_" + key + ".txt";
                        return new BufferedWriter(new FileWriter(outputFile));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                writer.write(line + System.lineSeparator());

                // Update the record count for the current record type
                recordCountMap.merge(recordType, 1, Integer::sum);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void closeWriters(Map<String, BufferedWriter> writers) {
        writers.values().forEach(writer -> {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void writeLog(String logFilePath, Map<String, Integer> recordCountMap) {
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFilePath))) {
            for (Map.Entry<String, Integer> entry : recordCountMap.entrySet()) {
                logWriter.write("Record Type: " + entry.getKey() + ", Rows Processed: " + entry.getValue());
                logWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	

}
