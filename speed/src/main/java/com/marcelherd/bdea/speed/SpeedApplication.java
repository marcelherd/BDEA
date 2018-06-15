package com.marcelherd.bdea.speed;

import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 *
 */
public class SpeedApplication {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "hoppy.informatik.hs-mannheim.de:9092");
		props.put("group.id", "BdeaConsumer");
		
		FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("mhLambdaTopic3", new SimpleStringSchema(), props);
		consumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(consumer);
		DataStream<Tuple3<String, String, Integer>> termFrequenz = 
				stream
					.map(line -> line.split("\\W+"))
					.flatMap((String[] tokens, Collector<Tuple3<String, String, Integer>> out) -> {
						final String fileHash = tokens[0].substring("BEGIN".length());
						Arrays.stream(tokens)
							.filter(t -> t.length() > 0)
							.filter(t -> !t.startsWith("BEGIN"))
//							.map(t -> t.replaceAll("[^a-zA-Z]", ""))
							.forEach(t -> out.collect(new Tuple3<String, String, Integer>(fileHash, t, 1)));
					})
					.returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
					.keyBy(0, 1)
					.sum(2);
		
//		termFrequenz.print();
		
		CassandraSink.addSink(termFrequenz)
			.setQuery("INSERT INTO MH_BDEA3.TermFrequenz(fileHash, word, count) values(?, ?, ?);")
			.setHost("hoppy.informatik.hs-mannheim.de", 9042)
			.build();
		
		env.execute();
	}
	
}
