package com.marcelherd.bdea.serving.model;

public class TF {
	
	public String fileHash;
	public String word;
	public int count;
	
	public TF(String fileHash, String word, int count) {
		this.fileHash = fileHash;
		this.word = word;
		this.count = count;
	}
	
	@Override
	public String toString() {
		return fileHash + "," + word + "," + count;
	}
	
}
