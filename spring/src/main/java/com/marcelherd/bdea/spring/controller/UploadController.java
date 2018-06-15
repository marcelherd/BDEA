package com.marcelherd.bdea.spring.controller;

import java.io.File;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.marcelherd.bdea.spring.service.KafkaProducerService;

@RestController
public class UploadController {
	
	@Autowired
	private KafkaProducerService kafkaProducerService;
	
	@PostMapping("/upload")
	public String handleFileUplad(@RequestParam("file") MultipartFile file) throws Exception {
		// save the file to the filesystem for the batch layer
		File outputDirectory = new File("/home/marcel/Documents/uploaded_files/" + file.getOriginalFilename());
		file.transferTo(outputDirectory);
		
		// send the file to Kafka for the speed layer
		kafkaProducerService.publish(file);
		
		// serving layer benutzt hier cassandra, spring boot microservice generiert html output
		// ein microservice, zwei controller, einer zeigt tfidf an, einer zeigt df an, kriegt das input aus cassandra
		// batch layer legt output auch in cassandra ab
		return "Done. <br /><a href=\"/\">Back</a>";
	}

}
