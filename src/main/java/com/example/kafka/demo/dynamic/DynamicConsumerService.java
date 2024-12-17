package com.example.kafka.demo.dynamic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Service
public class DynamicConsumerService {

    private static final List<String> DYNAMIC_GROUPS = new ArrayList<>();
    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> dynamicKafkaListenerContainerFactory;
    @Autowired
    private AdminClient adminClient;  // To interact with Kafka
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, ConcurrentMessageListenerContainer> map = new HashMap<>();


    public void addNewGroup(String topicName, String groupId) {
        if (!DYNAMIC_GROUPS.contains(groupId)) {
            DYNAMIC_GROUPS.add(groupId);
        }
    }

    public void removeGroup(String groupId) {
        DYNAMIC_GROUPS.remove(groupId);
    }


    @Scheduled(fixedDelay = 60000)  // This will run every 1 minute
    public void manageConsumerGroups() {
        try {
            // Get list of existing consumer groups
            ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
            List<ConsumerGroupListing> existingGroups = (List<ConsumerGroupListing>) groupsResult.all().get(10, TimeUnit.SECONDS);

            // Get a list of group names
            List<String> existingGroupNames = existingGroups.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());

            // Compare and create new groups if needed
            for (String group : DYNAMIC_GROUPS) {
                if (!existingGroupNames.contains(group) || (map.get(group) ==null)) {
                    // Group does not exist, so create it dynamically
                    createConsumerGroup(group);
                }
            }
            for (String cGroup : existingGroupNames) {
                if (cGroup != null && !DYNAMIC_GROUPS.contains(cGroup)) {
                    // Group does not exist, so create it dynamically
                    stopConsumerGroup(map.get(cGroup));
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void createConsumerGroup(String groupName) {
        // Logic to create a consumer group dynamically.
        // In Kafka, consumer groups are created automatically when consumers join with the group.
        // However, you may need to implement a logic to ensure consumers are created if not already running
        // Here's an example of initiating a Kafka listener dynamically for the group.
        startDynamicConsumer(groupName);
    }

    private void stopConsumerGroup(ConcurrentMessageListenerContainer<String, String> container) {
        if (container != null) {
            container.stop(true);
        }

    }

    private void startDynamicConsumer(String groupName) {
        // Start a new Kafka consumer dynamically with the given group name

        ConcurrentMessageListenerContainer<String, String> container = dynamicKafkaListenerContainerFactory.createContainer("widgets");

        // Update the consumer factory to use the dynamic group ID
        container.getContainerProperties().setGroupId(groupName);

        // Define a simple message listener
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) message -> {
            System.out.println("Received message in group " + groupName + ": " + message.value());
        });

        // Start the container
        container.start();  // Start the consumer for the new group

        container.resume();

        map.put(groupName, container);
    }
}
