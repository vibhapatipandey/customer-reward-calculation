package com.java.file.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileSplitter {
    public static void main(String[] args) {
        String inputFile = "C:\\testData\\output.txt"; // Replace with your input file path
        String outputDirectory = "C:\\testData\\parallel-output\\"; // Replace with your output directory path
        String logFilePath = "C:\\testdata\\parallel-output.txt"; // Log file path

        try {
            // Create a map to associate record types with output files
            Map<String, BufferedWriter> writers = new HashMap<>();
            Map<String, Integer> recordCountMap = new ConcurrentHashMap<>();
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line;
            int count =0;
            while ((line = reader.readLine()) != null) {
            	if (line.trim().length() > 1) {
            		
            		 // Assuming the record type is in a specific position in the line
                    String recordType = line.substring(7,10); // Adjust this based on your file format

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
                    recordCountMap.merge(recordType, 1, Integer::sum);
            	}else {
            		System.out.println("lines length in file is : "+line);
            		continue;
            	}
            	count++;
              }
            System.out.println("lines length in file is : "+count);
            // Close all writers
            for (BufferedWriter writer : writers.values()) {
                writer.close();
            }
            writeLog(logFilePath, recordCountMap);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
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
        	
			/*
			 * for (Map.Entry<String, Integer> entry : recordCountMap.entrySet()) {
			 * logWriter.write("Record Type: " + entry.getKey() + ", Rows Processed: " +
			 * entry.getValue()); logWriter.newLine(); }
			 */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
