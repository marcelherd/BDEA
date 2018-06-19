package com.marcelherd.bdea.hyperloglog;

public class HyperLogLog {
	
	private static final int NUM_BUCKETS = 64;
		
	private int[] buckets;
	
	public HyperLogLog() {
		buckets = new int[NUM_BUCKETS];
	}
	
	public void onEventReceived(String data) {
		String s_targetBucket = data.substring(0, 6);
		int targetBucket = Integer.parseInt(s_targetBucket, 2);
		
		String s_key = data.substring(6);
		int leadingZeros = 0;
		for (char c : s_key.toCharArray()) {
			if (Integer.parseInt(String.valueOf(c)) == 0) {
				leadingZeros++;
			} else {
				break;
			}
		}
		
		buckets[targetBucket] = (leadingZeros > buckets[targetBucket] ? leadingZeros : buckets[targetBucket]);
	}
	
	public double getCounter() {
		final int m = NUM_BUCKETS;
		final double am = 0.783d;
		
		double zaehler = am * m * m;
		double nenner = 0.d;
		
		for (int j = 0; j < m; j++) {
			nenner += Math.pow(2, buckets[j] * -1);
		}
		
		return zaehler / nenner;
	}

}
