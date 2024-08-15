package co.com.famisanar.kafka.topics.application.services;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class KafkaCountAllService {
	
	@Autowired
    private KafkaAdmin kafkaAdmin;
	
	@Autowired
	private ConsumerFactory<String, String> consumerFactory;

    public int getTopicCount() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            return topics.size();
        }
    }
    
    public int getTotalPartitionCount() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Obtener la lista de nombres de tópicos
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();

            // Obtener detalles de los tópicos
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
            @SuppressWarnings("deprecation")
			Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

            // Contar el número total de particiones
            int totalPartitions = 0;
            for (TopicDescription topicDescription : topicDescriptions.values()) {
                totalPartitions += topicDescription.partitions().size();
            }

            return totalPartitions;
        }
    }
    
    public int getTotalConsumerCount() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> consumerGroups = listConsumerGroupsResult.all().get();
            return consumerGroups.size();
        }
    }
    
    public int getTotalMessageCount() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Obtener la lista de nombres de tópicos
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();

            // Obtener detalles de los tópicos
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
            @SuppressWarnings("deprecation")
			Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

            // Contar el número total de mensajes
            int totalMessages = 0;
            try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
                for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
                    String topic = entry.getKey();
                    TopicDescription topicDescription = entry.getValue();
                    for (var partitionInfo : topicDescription.partitions()) {
                        TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
                        consumer.assign(Collections.singletonList(partition));
                        consumer.seekToEnd(Collections.singletonList(partition));
                        long endOffset = consumer.position(partition);
                        consumer.seekToBeginning(Collections.singletonList(partition));
                        long startOffset = consumer.position(partition);

                        totalMessages += endOffset - startOffset;
                    }
                }
            }
            return totalMessages;
        }
    }
    
}
