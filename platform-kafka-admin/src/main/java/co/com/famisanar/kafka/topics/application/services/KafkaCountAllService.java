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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.adapter.out.exceptions.RespuestaHttpHandler;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaCountAll;

@Service
public class KafkaCountAllService implements IKafkaCountAll{
	
	@Autowired
    private KafkaBrokerChange kafkaBrokerChange;
	
	@Autowired
	RespuestaHttpHandler respuestaHttpHandler;
	
	@Autowired
	private ConsumerFactory<String, String> consumerFactory;

	public ResponseEntity<Object> getTopicCount() throws ExecutionException, InterruptedException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> topics = topicsResult.names().get();
        return ResponseEntity.status(HttpStatus.OK)
				.body(topics.size());
    }
    
    public ResponseEntity<Object> getTotalPartitionCount() throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
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

            return ResponseEntity.status(HttpStatus.OK)
					.body(totalPartitions);

    }
    
    public ResponseEntity<Object> getTotalConsumerCount() throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
            ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> consumerGroups = listConsumerGroupsResult.all().get();
            return ResponseEntity.status(HttpStatus.OK)
					.body(consumerGroups.size());
    }
    
    public ResponseEntity<Object> getTotalMessageCount() throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;// Obtener la lista de nombres de tópicos
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
            
            return ResponseEntity.status(HttpStatus.OK)
					.body(totalMessages);

    }
    
}
