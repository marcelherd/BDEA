package com.marcelherd.bdea.hyperloglog;

public class Application {
	
	private static String sampleText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Lorem ipsum dolor sit amet.";
	
	private static String[] strings = sampleText.replaceAll("[^A-Za-z0-9 ]", "").split(" ");
	
	private static final int UNIQUE_WORDS = 19;
	
	public static void main(String[] args) {
		HyperLogLog hll = new HyperLogLog();
		
		for (String s : strings) {
			hll.onEventReceived(s);
		}
		
		System.out.println("Counted unique words: " + hll.getCounter());
		System.out.println("Actual unique words: " + UNIQUE_WORDS);
	}
	
}
