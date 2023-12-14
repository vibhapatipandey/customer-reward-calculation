package com.java.file.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipFileSplitter {

	public static void main(String[] args) {
		String zipFilePath = "D:\\testdata\\TEST.N057.BOD.UNLD93.DAILY.ZIP_01302023"; // Replace with your input zip
																						// file path
		String outputDirectory = "D:\\testdata\\output\\"; // Replace with your output directory path
		int bufferSize = 1024 * 1024;
		try {
			// Create the output directory if it doesn't exist
			Files.createDirectories(Paths.get(outputDirectory));

			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath));
			ZipEntry entry;

			// Create a map to associate record types with output files
			Map<String, BufferedWriter> writers = new HashMap<>();

			byte[] buffer = new byte[bufferSize];
			int bytesRead;

			while ((entry = zipInputStream.getNextEntry()) != null) {
				// Create a temporary file to store the entry data
				File tempFile = File.createTempFile("temp", ".txt");
				FileOutputStream tempFileOutputStream = new FileOutputStream(tempFile);

				while ((bytesRead = zipInputStream.read(buffer)) != -1) {
					tempFileOutputStream.write(buffer, 0, bytesRead);
				}

				tempFileOutputStream.close();

				// Process the temporary file's content line by line
				try (BufferedReader tempFileReader = new BufferedReader(new FileReader(tempFile))) {
					String line;
					while ((line = tempFileReader.readLine()) != null) {

						// Ensure the line is long enough to extract the substring
						if (line.length() < 7) {
							continue;// Adjust the length based on your file format
						}
						String recordType = line.substring(7,10); // Extract the record type
						// Check if a writer exists for this record type
						if (!writers.containsKey(recordType)) {
							String outputFile = outputDirectory + "/output_" + recordType + ".txt";
							BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
							writers.put(recordType, writer);
						}

						// Write the line to the corresponding output file
						BufferedWriter writer = writers.get(recordType);
						writer.write(line);
						writer.newLine();
						/*
						 * else {
						 * 
						 * continue; //System.out.println("Line is ::"+ line.length()); }
						 */
					}
				}

				// Delete the temporary file
				tempFile.delete();
			}

			// Close all writers
			for (BufferedWriter writer : writers.values()) {
				writer.close();
			}

			zipInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
