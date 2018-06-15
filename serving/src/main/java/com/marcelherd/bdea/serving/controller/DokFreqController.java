package com.marcelherd.bdea.serving.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.marcelherd.bdea.serving.service.CassandraService;

@RestController
public class DokFreqController {
	
	@Autowired
	private CassandraService cassandra;
	
	@GetMapping("/df")
	public String df() {
		StringBuilder sb = new StringBuilder();
		cassandra.findAll().stream()
			.map(df -> df.toString() + "<br />")
			.forEach(df -> sb.append(df));
		return sb.toString();
	}

}
