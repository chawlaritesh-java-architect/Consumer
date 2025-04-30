package com.spark.streaming.kafka.consumer.rest.controllers;

import com.spark.streaming.kafka.consumer.structured.streaming.SparkStructuredStreamingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
@ResponseBody
@RequestMapping("/fiserv/spark/structured/streaming/api")
public class SparkStructuredStreamingRestController {

    @Autowired
    private SparkStructuredStreamingService sparkStructuredStreamingService;

    @GetMapping("/invoke/structuredStreamingWindowFn")
    private ResponseEntity<String> structuredStreamingWindowFn() {
        sparkStructuredStreamingService.startSparkStructuredStreaming();
        return ResponseEntity.of(Optional.of("Structured Streaming Started Successfully"));
    }

}
