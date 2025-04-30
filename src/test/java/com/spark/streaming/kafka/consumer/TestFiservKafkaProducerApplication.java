package com.spark.streaming.kafka.consumer;

import org.springframework.boot.SpringApplication;

public class TestFiservKafkaProducerApplication {

    public static void main(String[] args) {
        SpringApplication.from(FiservSparkStructuredStreamingApp::main).with(TestcontainersConfiguration.class).run(args);
    }

}
