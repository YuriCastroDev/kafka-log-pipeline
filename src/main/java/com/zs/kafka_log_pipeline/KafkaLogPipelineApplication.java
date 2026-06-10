package com.zs.kafka_log_pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaLogPipelineApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaLogPipelineApplication.class, args);
	}

}
