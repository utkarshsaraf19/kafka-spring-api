package com.example.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public AdminClient adminClient() {
		return KafkaAdminClient.create(adminClientConfigs());
	}

	public Map<String, Object> adminClientConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
		return props;
	}
}
