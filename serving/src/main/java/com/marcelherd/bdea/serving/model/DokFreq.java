package com.marcelherd.bdea.serving.model;

public class DokFreq {
	
	public String term;
	public int df;
	
	public DokFreq(String term, int df) {
		this.term = term;
		this.df = df;
	}
	
	@Override
	public String toString() {
		return term + "," + df;
	}
	
}
