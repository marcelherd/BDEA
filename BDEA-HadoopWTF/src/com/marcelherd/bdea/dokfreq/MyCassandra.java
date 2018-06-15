package com.marcelherd.bdea.dokfreq;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MyCassandra implements AutoCloseable {
	
	public static final String DEFAULT_NODE = "hoppy.informatik.hs-mannheim.de";
	public static final int DEFAULT_PORT = 9042;

	public static final String DEFAULT_KEYSPACE = "MH_BDEA3";
	public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
	public static final int DEFAULT_REPLICATION_FACTOR = 1;
	
	public static final String TABLE_NAME = "DokumentenFrequenz";
	
	private Cluster cluster;
	
	private Session session;
	
	public MyCassandra() {
		connect(DEFAULT_NODE, DEFAULT_PORT);
		createKeyspace(DEFAULT_KEYSPACE, DEFAULT_REPLICATION_STRATEGY, DEFAULT_REPLICATION_FACTOR); // also drops it
		createDFTable(TABLE_NAME);
		createTFTable();
	}
	
	private void createTFTable() {
		session.execute("CREATE TABLE IF NOT EXISTS MH_BDEA3.TermFrequenz (fileHash text, word text, count int, PRIMARY KEY (fileHash, word));");
		System.out.println("Cassandra TermFrequenz table created");
	}
	
	public void connect(String node, int port) {
		Builder builder = Cluster.builder().addContactPoint(node).withPort(port);
		cluster = builder.build();
		session = cluster.connect();
		System.out.println("Cassandra connection established");
	}
	
	public void createKeyspace(String name, String replicationStrategy, int replicationFactor) {
		session.execute("DROP KEYSPACE IF EXISTS " + name + ";");
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE KEYSPACE IF NOT EXISTS ").append(name)
			.append(" WITH replication = {").append("'class':'").append(replicationStrategy)
			.append("','replication_factor':").append(replicationFactor).append("};");
		
		String query = sb.toString();
		session.execute(query);
		System.out.println("Cassandra keyspace created");
	}
	
	public List<String> findKeyspaces() {
		ResultSet resultSet = session.execute("SELECT * FROM system_schema.keyspaces;");
		return resultSet.all().stream().map(r -> r.getString(0)).collect(Collectors.toList());
	}
	
	public void createDFTable(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE IF NOT EXISTS ").append(DEFAULT_KEYSPACE).append(".").append(name).append("(")
			.append("term text PRIMARY KEY,")
			.append("df int);");
		
		String query = sb.toString();
		session.execute(query);
		System.out.println("Cassandra DokumentenFrequenz table created");
	}
	
	public List<String> findColumns() {
		ResultSet resultSet = session.execute("SELECT * FROM " + DEFAULT_KEYSPACE + "." + TABLE_NAME);
		return resultSet.getColumnDefinitions().asList().stream().map(cl -> cl.getName()).collect(Collectors.toList());
	}
	
	public void insert(String[] values) {
		insert(values[0], Long.parseLong(values[1]));
	}
	
	public void insert(String term, long dokumentenFrequenz) {
		// INSERT INTO TABLE_NAME_BY_TITLE (id, title) VALUES (book.getId(), 'test');
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(DEFAULT_KEYSPACE).append(".").append(TABLE_NAME)
			.append(" (term, df) VALUES ('")
			.append(term).append("',").append(dokumentenFrequenz).append(");");
		
		String query = sb.toString();
		session.execute(query);
	}
	
	public void printAll() {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(DEFAULT_KEYSPACE).append(".").append(TABLE_NAME)
			.append(";");
		
		String query = sb.toString();
		ResultSet resultSet = session.execute(query);
		for (Row row : resultSet.all()) {
			System.out.println(row.getString("term"));
			System.out.println(row.getInt("df"));
			System.out.println();
		}
	}
	
	@Override
	public void close() throws Exception {
		session.close();
		cluster.close();
		System.out.println("Cassandra connection closed");
	}

}
