package com.marcelherd.bdea.hyperloglog;

public class Application {
	
	private static String random() {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < 16; i++) {
			sb.append((Math.random() < 0.5 ? "0" : "1"));
		}
		
		return sb.toString();
	}
	
	public static void main(String[] args) {
		HyperLogLog hll = new HyperLogLog();
		
		// Probably 100 different strings
		for (int i = 0; i < 100; i++) {
			hll.onEventReceived(random());
		}
		
		System.out.println("(1) Estimated unique words: " + hll.getCounter());
		
		// Two different strings, 100 times
		hll = new HyperLogLog();
		
		String s1 = random();
		String s2 = random();
		
		for (int i = 0; i < 100; i++) {
			hll.onEventReceived((Math.random() < 0.5 ? s1 : s2));
		}
		
		System.out.println("(2) Estimated unique words: " + hll.getCounter());
	}
	
}
