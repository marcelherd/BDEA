package com.marcelherd.bdea.serving.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.marcelherd.bdea.serving.service.CassandraServiceTF;

@RestController
public class TFController {
	
	@Autowired
	private CassandraServiceTF cassandra;
	
	@GetMapping("/tf")
	public String tf() {
		StringBuilder sb = new StringBuilder();
		cassandra.findAll().stream()
			.map(tf -> tf.toString() + "<br />")
			.forEach(tf -> sb.append(tf));
		return sb.toString();
	}

}
