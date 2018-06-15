package com.marcelherd.bdea.serving.service;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.marcelherd.bdea.serving.model.TF;

@Service
public class CassandraServiceTF {
	
	public static final String DEFAULT_NODE = "hoppy.informatik.hs-mannheim.de";
	public static final int DEFAULT_PORT = 9042;
	public static final String DEFAULT_KEYSPACE = "MH_BDEA3";	
	public static final String TABLE_NAME = "TermFrequenz";
	
	private Cluster cluster;
	
	private Session session;
	
	@PostConstruct
	public void init() {
		Builder builder = Cluster.builder().addContactPoint(DEFAULT_NODE).withPort(DEFAULT_PORT);
		cluster = builder.build();
		session = cluster.connect();
	}
	
	public List<TF> findAll() {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(DEFAULT_KEYSPACE).append(".").append(TABLE_NAME).append(";");
		
		String query = sb.toString();
		ResultSet resultSet = session.execute(query);
		return resultSet.all().stream()
			.map(row -> new TF(row.getString("fileHash"), row.getString("word"), row.getInt("count")))
			.collect(Collectors.toList());
	}
	
	public List<TF> findByFilename(String fileName) {
		String fileHash = fileName; // technically we should've used a hash but this will do for this exercise
		
		String query = "SELECT * FROM MH_BDEA3.TermFrequenz WHERE fileHash = '" + fileHash + "';";
		ResultSet resultSet = session.execute(query);
		return resultSet.all().stream()
				.map(row -> new TF(row.getString("fileHash"), row.getString("word"), row.getInt("count")))
				.collect(Collectors.toList());
	}
	
	public int countFiles() {
//		String query = "SELECT COUNT(*) FROM MH_BDEA3.TermFrequenz GROUP BY fileName;";
//		ResultSet resultSet = session.execute(query);
//		return resultSet.one().getLong("COUNT");
		File directory = new File("/home/marcel/Documents/uploaded_files/");
		return directory.listFiles().length;
	}
	
	@PreDestroy
	public void destroy() {
		session.close();
		cluster.close();
	}

}
