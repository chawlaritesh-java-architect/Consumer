package com.spark.streaming.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkStructuredStreamingConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "fiserv-coforge-event-produced-spark-stream")
    private String topicReceivingInput;

    @Value(value = "${spring.spark.structured.streaming.with.window.output}")
    private String topicNameWithWindowsFunction;

    @Value(value = "${spring.spark.structured.streaming.without.window.output}")
    private String topicNameWithoutWindowFunction;

    @Bean
    public NewTopic topicReceivingInput() {
        return new NewTopic(topicReceivingInput, 1, (short) 1);
    }

    @Bean
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

}
