package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.com.famisanar.kafka.topics.adapter.out.exceptions.RespuestaHttpHandler;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaTopics;

@Service
public class KafkaTopicsService implements IKafkaTopics{
	
	@Autowired
    private KafkaBrokerChange kafkaBrokerChange;
	
	@Autowired
	RespuestaHttpHandler respuestaHttpHandler;

	public ResponseEntity<Object> createTopic(String topicName, int numPartitions, short replicationFactor) {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
		try {

            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topicName)) {
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body("El topic '" + topicName + "' ya existe.");
            }
            
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

            return ResponseEntity.ok("Topic creado exitosamente: " + topicName);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Error al crear el topic: " + e.getMessage());
        }
    }
    
    public ResponseEntity<Object> getTopicDetails() throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
            @SuppressWarnings("deprecation")
			Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(cg -> cg.groupId())
                .collect(Collectors.toSet());

            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

            Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = new HashMap<>();
            for (String topicName : topicNames) {
                TopicDescription topicDescription = topicDescriptions.get(topicName);
                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    topicPartitionOffsetSpecs.put(new TopicPartition(topicName, partitionInfo.partition()), OffsetSpec.latest());
                }
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(topicPartitionOffsetSpecs).all().get();

            Map<String, Map<String, Object>> topicDetailsMap = new HashMap<>();
            List<Map<String, Object>> topicDetailsList = null;
            for (String topicName : topicNames) {
            	
                TopicDescription topicDescription = topicDescriptions.get(topicName);
                
                Set<Map<String, Object>> consumers = consumerGroupDescriptions.values().stream()
                	    .filter(cg -> cg.members().stream()
                	        .flatMap(member -> member.assignment().topicPartitions().stream())
                	        .anyMatch(tp -> tp.topic().equals(topicName)))
                	    .map(cg -> {
                	        Map<String, Object> consumerInfo = new HashMap<>();
                	        consumerInfo.put("consumerGroup", cg.groupId());
                	        consumerInfo.put("threadCount", cg.members().size());
                	        return consumerInfo;
                	    })
                	    .collect(Collectors.toSet());

                long totalMessages = 0;
                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    TopicPartition partition = new TopicPartition(topicName, partitionInfo.partition());
                    long latestOffset = latestOffsets.get(partition).offset();
                    TopicPartition earliestPartition = new TopicPartition(topicName, partitionInfo.partition());
                    long earliestOffset = adminClient.listOffsets(Map.of(earliestPartition, OffsetSpec.earliest())).all().get().get(earliestPartition).offset();
                    totalMessages += latestOffset - earliestOffset;
                }

                Map<String, Object> details = new HashMap<>();
                details.put("topicName",topicName);
                details.put("totalPartitions", topicDescription.partitions().size());
                details.put("consumers", consumers);
                details.put("totalMessages", totalMessages);

                topicDetailsMap.put(topicName, details);
                
                topicDetailsList = new ArrayList<>(topicDetailsMap.values());

            }
            
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String topicDetailsListJ = gson.toJson(topicDetailsList);

            return ResponseEntity.status(HttpStatus.OK)
    				.body(topicDetailsListJ);
        
    }
    
    public ResponseEntity<Object> describeTopics(Set<String> topicNames) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        Map<String, KafkaFuture<TopicDescription>> topicNameValues = describeTopicsResult.topicNameValues();
        
        Map<TopicPartition, Long> latestOffsets = new HashMap<>();
        Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
        for (String topicName : topicNames) {
            List<TopicPartitionInfo> partitionInfos = topicNameValues.get(topicName).get().partitions();
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(topicName, partitionInfo.partition());
                latestOffsets.put(topicPartition, adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get().get(topicPartition).offset());
                earliestOffsets.put(topicPartition, adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get().get(topicPartition).offset());
            }
        }
        
        Map<String, Object> topicDescriptions = new HashMap<>();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : topicNameValues.entrySet()) {
            TopicDescription topicDescription = entry.getValue().get();
            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            List<Map<String, Object>> partitions = new ArrayList<>();
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(topicDescription.name(), partitionInfo.partition());
                
                Map<String, Object> partition = new HashMap<>();
                partition.put("partition", partitionInfo.partition());
                partition.put("leader", partitionInfo.leader().id());
                partition.put("latestOffset", latestOffsets.get(topicPartition));
                partition.put("earliestOffset", earliestOffsets.get(topicPartition));
                
                List<Map<String, Object>> replicas = new ArrayList<>();
                for (Node node : partitionInfo.replicas()) {
                    Map<String, Object> replica = new HashMap<>();
                    replica.put("id", node.id());
                    replica.put("host", node.host());
                    replica.put("port", node.port());
                    replicas.add(replica);
                }
                partition.put("replicas", replicas);
                
                List<Map<String, Object>> isr = new ArrayList<>();
                for (Node node : partitionInfo.isr()) {
                    Map<String, Object> isrNode = new HashMap<>();
                    isrNode.put("id", node.id());
                    isrNode.put("host", node.host());
                    isrNode.put("port", node.port());
                    isr.add(isrNode);
                }
                partition.put("isr", isr);
                
                partitions.add(partition);
            }
            
            Map<String, Object> topic = new HashMap<>();
            topic.put("name", topicDescription.name());
            topic.put("partitions", partitions);
            topic.put("internal", topicDescription.isInternal());
            topicDescriptions.put(topicDescription.name(), topic);
        }
        
        return ResponseEntity.status(HttpStatus.OK)
				.body(topicDescriptions);
    }
    
    public ResponseEntity<Object> searchTopics(String searchTerm) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNames = listTopicsResult.names().get();

        Set<String> filteredTopicNames = topicNames.stream()
                .filter(topicName -> topicName.contains(searchTerm))
                .collect(Collectors.toSet());

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(filteredTopicNames);
        @SuppressWarnings("deprecation")
		Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

        ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
        Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(cg -> cg.groupId())
                .collect(Collectors.toSet());

        Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

        Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = new HashMap<>();
        for (String topicName : filteredTopicNames) {
            TopicDescription topicDescription = topicDescriptions.get(topicName);
            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                topicPartitionOffsetSpecs.put(new TopicPartition(topicName, partitionInfo.partition()), OffsetSpec.latest());
            }
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(topicPartitionOffsetSpecs).all().get();

        List<Map<String, Object>> topicDetailsList = new ArrayList<>();
        for (String topicName : filteredTopicNames) {
            TopicDescription topicDescription = topicDescriptions.get(topicName);
            Set<String> consumers = consumerGroupDescriptions.values().stream()
                    .flatMap(cg -> cg.members().stream())
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .filter(tp -> tp.topic().equals(topicName))
                    .map(tp -> tp.topic())
                    .collect(Collectors.toSet());

            long totalMessages = 0;
            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                TopicPartition partition = new TopicPartition(topicName, partitionInfo.partition());
                long latestOffset = latestOffsets.get(partition).offset();
                TopicPartition earliestPartition = new TopicPartition(topicName, partitionInfo.partition());
                long earliestOffset = adminClient.listOffsets(Map.of(earliestPartition, OffsetSpec.earliest())).all().get().get(earliestPartition).offset();
                totalMessages += latestOffset - earliestOffset;
            }

            Map<String, Object> details = new HashMap<>();
            details.put("topicName", topicName);
            details.put("totalPartitions", topicDescription.partitions().size());
            details.put("consumers", consumers);
            details.put("totalMessages", totalMessages);

            topicDetailsList.add(details);
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return ResponseEntity.status(HttpStatus.OK)
				.body(gson.toJson(topicDetailsList));
    }
    
    public ResponseEntity<Object> getTopicDetails(String topicName) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
    	
        List<String> topicNames = Collections.singletonList(topicName);
        
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        
        Map<String, KafkaFuture<TopicDescription>> topicNameValues = describeTopicsResult.topicNameValues();
        
        Map<String, Object> topicDescriptions = new HashMap<>();
        
        KafkaFuture<TopicDescription> topicDescriptionFuture = topicNameValues.get(topicName);
        
        if (topicDescriptionFuture != null) {
            TopicDescription topicDescription = topicDescriptionFuture.get();
            
            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            List<Object> partitions = new ArrayList<>();
            
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                Map<String, Object> partition = new HashMap<>();
                partition.put("partition", partitionInfo.partition());
                partition.put("leader", partitionInfo.leader().id());
                
                List<Object> replicas = new ArrayList<>();
                for (Node node : partitionInfo.replicas()) {
                    Map<String, Object> replica = new HashMap<>();
                    replica.put("id", node.id());
                    replica.put("host", node.host());
                    replica.put("port", node.port());
                    replicas.add(replica);
                }
                partition.put("replicas", replicas);
                
                List<Object> isr = new ArrayList<>();
                for (Node node : partitionInfo.isr()) {
                    Map<String, Object> isrNode = new HashMap<>();
                    isrNode.put("id", node.id());
                    isrNode.put("host", node.host());
                    isrNode.put("port", node.port());
                    isr.add(isrNode);
                }
                partition.put("isr", isr);
                
                partitions.add(partition);
            }
            
            // Crear un mapa para el tópico
            Map<String, Object> topic = new HashMap<>();
            topic.put("name", topicDescription.name());
            topic.put("partitions", partitions);
            topic.put("internal", topicDescription.isInternal());
            
            topicDescriptions.put(topicDescription.name(), topic);
        }
        
        return ResponseEntity.status(HttpStatus.OK)
				.body(topicDescriptions);
    }
    
}