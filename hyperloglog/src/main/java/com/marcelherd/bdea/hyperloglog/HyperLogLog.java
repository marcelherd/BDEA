package com.marcelherd.bdea.hyperloglog;

public class HyperLogLog {
	
	private int counter = 0;
	
	public void onEventReceived(String data) {
		// increment counter if the received data is unique, using the hyperloglog algorithm
	}
	
	public int getCounter() { return counter; }

}
