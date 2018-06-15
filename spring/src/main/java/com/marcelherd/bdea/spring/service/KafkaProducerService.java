package com.marcelherd.bdea.spring.service;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class KafkaProducerService {
	
	public static final String TOPIC = "mhLambdaTopic3";
	public static final String BOOTSTRAP_SERVERS = "hoppy.informatik.hs-mannheim.de:9092";
	
	public void publish(MultipartFile file) throws IOException {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "BdeaProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		String contents = "BEGIN" + file.getOriginalFilename() + " " + new String(file.getBytes());
		
		System.out.println(new String(file.getBytes()));
		
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, contents); // (Topic, Key, Value)? oder BEGIN oder append to line
		
		producer.send(record);
		producer.close();
		
		// zum Testen
//		Properties consumerProperties = new Properties();
//		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "BdeaConsumer");
//		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		
//		Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
//		consumer.subscribe(Collections.singletonList(TOPIC));
//		
//		for (int i = 0; i < 4200; i++) {
//			ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
//			consumerRecords.forEach(theRecord -> {
//				System.out.printf("Record: (%d, %s, %d, %d)\n", theRecord.key(), theRecord.value(), theRecord.partition(), theRecord.offset());
//			});
//			consumer.commitAsync();
//		}
//		
//		consumer.close();
	}

}
