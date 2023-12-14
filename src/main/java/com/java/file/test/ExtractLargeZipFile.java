package com.java.file.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ExtractLargeZipFile {

	public static void main(String[] args) {
		String zipFilePath = "C:\\testData\\TEST.N057.BOD.UNLD93.DAILY.ZIP_01302023";
		String outputFilePath = "C:\\testData\\output.txt";
		int bufferSize = 1024; // Adjust the buffer size according to your needs

		try (FileInputStream fileInputStream = new FileInputStream(zipFilePath);
				ZipInputStream zipInputStream = new ZipInputStream(fileInputStream);
				FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath);
				Writer writer = new OutputStreamWriter(fileOutputStream)) {

			byte[] buffer = new byte[bufferSize];
			int bytesRead;

			ZipEntry zipEntry;
			while ((zipEntry = zipInputStream.getNextEntry()) != null) {
				System.out.println("Extracting: " + zipEntry.getName());

				while ((bytesRead = zipInputStream.read(buffer)) != -1) {
					// Process the chunk of data in the 'buffer'
					String chunkData = new String(buffer, 0, bytesRead);
					//String recordType = chunkData.substring(8,10);
					
					//System.out.println("Chunck data :::"+recordType);
//US010010702023
					// Write the chunk data to the output file
					writer.write(chunkData);
				}

				zipInputStream.closeEntry();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
