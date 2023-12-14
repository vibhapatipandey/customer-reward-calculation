package com.java.file.test;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Example1 {
	
	 private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	public static void main(String[] args) {
		
		String zipFilePath = "C:\\testData\\TEST.N057.BOD.UNLD93.DAILY.ZIP_01302023";
		//String outputFilePath = "D:\\software\\testData\\";
		
		//String zipFilePath = "large_input.zip"; // Replace with your input zip file path
        String outputDirectory = "C:\\testData\\files"; // Replace with your output directory path

        try {
            // Create the output directory if it doesn't exist
            Files.createDirectories(Paths.get(outputDirectory));

            ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath));
            ZipEntry entry;

            // Create a map to associate record types with output files
            Map<String, BufferedWriter> writers = new HashMap<>();

            while ((entry = zipInputStream.getNextEntry()) != null) {
                // Read the current entry's data
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
               
                byte[] buffer = new byte[MAX_ARRAY_SIZE];
                int bytesRead;
                String entryData = null;
                while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                    //byteArrayOutputStream.write(buffer, 0, bytesRead);
                	entryData = new String(buffer, 0, bytesRead);
                }

                //String entryData = byteArrayOutputStream.toString("UTF-8");
                byteArrayOutputStream.close();

                // Assuming the record type is in a specific position in the entry data
                String recordType = entryData.substring(7,10); // Adjust this based on your file format

                // Check if a writer exists for this record type
                if (!writers.containsKey(recordType)) {
                    String outputFile = outputDirectory + "/output_" + recordType + ".txt";
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
                    writers.put(recordType, writer);
                }

                // Write the entry data to the corresponding output file
                BufferedWriter writer = writers.get(recordType);
                writer.write(entryData);
                writer.newLine();
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



