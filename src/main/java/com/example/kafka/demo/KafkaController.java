package com.example.kafka.demo;

import com.example.kafka.demo.dynamic.DynamicConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaProducerService producerService;

    @Autowired
    private DynamicConsumerService dynamicConsumerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam String message) {
        producerService.sendMessage("widgets", message);
        return "Message sent: " + message;
    }


    @PostMapping("/consume")
    public String consumeFromTopic(
            @RequestParam String topic,
            @RequestParam String groupId) {

        dynamicConsumerService.addNewGroup(topic, groupId);
        return "Consumer started for group: " + groupId + " on topic: " + topic;
    }

    @PostMapping("/remove")
    public String removeGroup(
            @RequestParam String groupId) {

        dynamicConsumerService.removeGroup(groupId);
        return "Remove started for group: " + groupId;
    }
}
