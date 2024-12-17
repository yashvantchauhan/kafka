package com.example.kafka.demo;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "widgets", groupId = "my-group")
    public void consume(String message) {
        System.out.println("Consumed message: " + message);
    }
}
