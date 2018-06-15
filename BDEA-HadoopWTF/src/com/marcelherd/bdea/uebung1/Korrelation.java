package com.marcelherd.bdea.uebung1;

import java.util.Arrays;

public class Korrelation {
	
	public static class Mathe {
		public static double calcKorrelation(int[] x, int[] y) {
			assert x.length == y.length;
			
			final int N = x.length;
			
			int sum = 0;
			for (int i = 0; i < N; i++) {
				sum += x[i] * y[i];
			}
			
			//int product = N * calcMittel(x) * calcMittel(y);
			
			return 0.d;
		}
		
		public static double calcMittel(int[] x) {
			return Arrays.stream(x).sum() / x.length;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
