package com.marcelherd.bdea.serving.service;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.marcelherd.bdea.serving.model.DokFreq;

@Service
public class CassandraService {
	
	public static final String DEFAULT_NODE = "hoppy.informatik.hs-mannheim.de";
	public static final int DEFAULT_PORT = 9042;
	public static final String DEFAULT_KEYSPACE = "MH_BDEA3";	
	public static final String TABLE_NAME = "DokumentenFrequenz";
	
	private Cluster cluster;
	
	private Session session;
	
	@PostConstruct
	public void init() {
		Builder builder = Cluster.builder().addContactPoint(DEFAULT_NODE).withPort(DEFAULT_PORT);
		cluster = builder.build();
		session = cluster.connect();
	}
	
	public List<DokFreq> findAll() {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(DEFAULT_KEYSPACE).append(".").append(TABLE_NAME).append(";");
		
		String query = sb.toString();
		ResultSet resultSet = session.execute(query);
		return resultSet.all().stream()
			.map(row -> new DokFreq(row.getString("term"), row.getInt("df")))
			.collect(Collectors.toList());
	}
	
	public DokFreq findByTerm(String term) {
		String query = "SELECT * FROM MH_BDEA3.DokumentenFrequenz WHERE term = '" + term + "';";
		ResultSet resultSet = session.execute(query);
		Row row = resultSet.one();
		if (row != null) {
			return new DokFreq(row.getString("term"), row.getInt("df"));
		} else {
			return null;
		}		
	}
	
	@PreDestroy
	public void destroy() {
		session.close();
		cluster.close();
	}

}
