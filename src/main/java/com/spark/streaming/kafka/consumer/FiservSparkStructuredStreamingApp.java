package com.spark.streaming.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class FiservSparkStructuredStreamingApp {

    public static void main(String[] args) {
        SpringApplication.run(FiservSparkStructuredStreamingApp.class, args);
    }

}
