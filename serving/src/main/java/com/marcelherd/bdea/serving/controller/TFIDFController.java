package com.marcelherd.bdea.serving.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.marcelherd.bdea.serving.model.DokFreq;
import com.marcelherd.bdea.serving.model.TF;
import com.marcelherd.bdea.serving.service.CassandraService;
import com.marcelherd.bdea.serving.service.CassandraServiceTF;
import com.marcelherd.bdea.serving.util.MapUtil;

@RestController
public class TFIDFController {
	
	@Autowired
	private CassandraServiceTF cassandra;
	
	@Autowired
	private CassandraService cassandraDf;
	
	@PostMapping("/tfidf")
	public String tfidf(@RequestParam("filename") String filename) {
		List<TF> frequenzen = cassandra.findByFilename(filename);
		
		Map<String, Double> tfidf = new HashMap<String, Double>();
		
		final int N = cassandra.countFiles();
		
		for (TF tf : frequenzen) {
			DokFreq dokFreqFuerTerm = cassandraDf.findByTerm(tf.word);
			if (dokFreqFuerTerm == null) continue;
			int df = dokFreqFuerTerm.df;
			double currTFIDF = (double) tf.count * Math.log((double) N / (double) df);
			tfidf.put(tf.word, currTFIDF);
		}
		
		tfidf = MapUtil.sortByValue(tfidf);
		
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Double> entry : tfidf.entrySet()) {
			sb.append(entry.getKey() + "," + entry.getValue() + "<br />");
		}
		
		return sb.toString();
	}

}
