package com.marcelherd.bdea.uebung1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Util {
	
	public static void cleanUp(String path) {
		File directory = new File(path);
		
		if (!directory.exists() || !directory.isDirectory()) {
			return;
		}
		
		for (File file : directory.listFiles()) {
			if (!file.isDirectory()) {
				file.delete();
			}
		}
		
		directory.delete();
	}
	
	public static void printFileOutput(String path) throws IOException {
		File outputFile = new File(path);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
		
		System.out.println("\nOutput:\n");
		
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			System.out.println(line);
		}
		
		bufferedReader.close();
	}

}
