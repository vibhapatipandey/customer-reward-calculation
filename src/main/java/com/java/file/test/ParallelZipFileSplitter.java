package com.java.file.test;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.*;

public class ParallelZipFileSplitter {
	public static void main(String[] args) {
		String zipFilePath = "C:\\testdata\\TEST.N057.BOD.UNLD93.DAILY.ZIP_01302023"; // Replace with your input zip
																						// file path
		String outputDirectory = "C:\\testdata\\chunk-files\\"; // Replace with your output directory path
		String logFilePath = "C:\\testdata\\chunk-process-output.txt"; // Log file path
		int bufferSize = 1024 * 1024; // 1 MB buffer size (adjust as needed)
		int numThreads = 4; // Number of threads for parallel processing (adjust as needed)

		try {
			// Create the output directory if it doesn't exist
			Files.createDirectories(Paths.get(outputDirectory));
			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath));
			// ZipEntry entry;

			// Create a map to associate record types with output files
			Map<String, BufferedWriter> writers = new HashMap<>();
			Map<String, Integer> recordCountMap = new ConcurrentHashMap<>();

			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

			byte[] buffer = new byte[bufferSize];
			int bytesRead;

			while ((zipInputStream.getNextEntry()) != null) {
				// Create a temporary file to store the entry data
				File tempFile = File.createTempFile("temp", ".txt");
				FileOutputStream tempFileOutputStream = new FileOutputStream(tempFile);

				while ((bytesRead = zipInputStream.read(buffer)) != -1) {
					tempFileOutputStream.write(buffer, 0, bytesRead);
				}

				tempFileOutputStream.close();

				// Submit processing tasks to the thread pool
				executorService.submit(() -> {
					processFile(tempFile, outputDirectory, writers, recordCountMap);
					tempFile.delete(); // Delete the temporary file after processing
				});
			}

			// Shutdown the thread pool and wait for all tasks to complete
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

			// Close all writers
			for (BufferedWriter writer : writers.values()) {
				writer.close();
			}
			writeLog(logFilePath, recordCountMap);
			zipInputStream.close();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Zip file processed in chunk.");
	}

	private static void processFile(File tempFile, String outputDirectory, Map<String, BufferedWriter> writers,
			Map<String, Integer> recordCountMap) {
		// Process the temporary file's content line by line
		try (BufferedReader tempFileReader = new BufferedReader(new FileReader(tempFile))) {
			String line;
			while ((line = tempFileReader.readLine()) != null) {
				// Skip lines that are too short
				if (line.trim().length() > 1) {
					// Assuming the record type is in a specific position in the line
					String recordType = line.substring(7, 10); // Extract the record type

					// Check if a writer exists for this record type
					if (!writers.containsKey(recordType)) {
						String outputFile = outputDirectory + "/output_" + recordType + ".txt";
						BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
						writers.put(recordType, writer);
					}

					// Write the line to the corresponding output file
					BufferedWriter writer = writers.get(recordType);
					writer.write(line + System.lineSeparator());
					// writer.newLine();
					// Update the record count for the current record type
					recordCountMap.merge(recordType, 1, Integer::sum);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * private static void writeLog(String logFilePath, Map<String, Integer>
	 * recordCountMap) { try (BufferedWriter logWriter = new BufferedWriter(new
	 * FileWriter(logFilePath))) { for (Map.Entry<String, Integer> entry :
	 * recordCountMap.entrySet()) { logWriter.write("Record Type: " + entry.getKey()
	 * + ", Rows Processed: " + entry.getValue()); logWriter.newLine(); } } catch
	 * (IOException e) { e.printStackTrace(); } }
	 */

	private static void writeLog(String logFilePath, Map<String, Integer> recordCountMap) {
		try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFilePath))) {
			List<String> sortedKeys = new ArrayList<>(recordCountMap.keySet());
			Collections.sort(sortedKeys);
			// Access values in sorted order
			for (String key : sortedKeys) {
				Integer value = recordCountMap.get(key);
				logWriter.write("Record Type: " + key + ", Rows Processed: " + value);
				logWriter.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
