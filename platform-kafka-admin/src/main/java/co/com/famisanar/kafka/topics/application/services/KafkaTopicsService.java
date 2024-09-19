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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.com.famisanar.kafka.topics.application.ports.in.IKafkaTopics;

@Service
public class KafkaTopicsService implements IKafkaTopics{

	@Autowired
    private KafkaAdmin kafkaAdmin;
	
	private AdminClient adminClient;
	
	@Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaTopicsService(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
    
    public String getTopicDetails() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener la lista de nombres de tópicos
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();

            // Obtener detalles de los tópicos
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
            @SuppressWarnings("deprecation")
			Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

            // Obtener los grupos de consumidores
            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(cg -> cg.groupId())
                .collect(Collectors.toSet());

            // Obtener la descripción de los grupos de consumidores
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

            // Obtener offsets para contar los mensajes
            Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = new HashMap<>();
            for (String topicName : topicNames) {
                TopicDescription topicDescription = topicDescriptions.get(topicName);
                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    topicPartitionOffsetSpecs.put(new TopicPartition(topicName, partitionInfo.partition()), OffsetSpec.latest());
                }
            }

            // Obtener los offsets más recientes
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(topicPartitionOffsetSpecs).all().get();

            // Mapear los detalles de los tópicos y consumidores
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
                	        consumerInfo.put("consumerGroup", cg.groupId()); // Nombre del grupo de consumidores
                	        consumerInfo.put("threadCount", cg.members().size()); // Número de hilos (consumidores) activos en este grupo
                	        return consumerInfo;
                	    })
                	    .collect(Collectors.toSet());

                // Calcular el total de mensajes
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
                details.put("totalPartitions", topicDescription.partitions().size()); // Total de particiones
                details.put("consumers", consumers); // Consumidores asociados
                details.put("totalMessages", totalMessages); // Total de mensajes

                topicDetailsMap.put(topicName, details);
                
                topicDetailsList = new ArrayList<>(topicDetailsMap.values());

            }
            
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String json = gson.toJson(topicDetailsList);

            return json;
        }
    }
    
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        ListTopicsResult topics = adminClient.listTopics();
        return topics.names().get();
    }
    
    public Map<String, Object> describeTopics(Set<String> topicNames) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
     // Obtener la descripción de los topics
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        Map<String, KafkaFuture<TopicDescription>> topicNameValues = describeTopicsResult.topicNameValues();
        
        // Obtener la lista de particiones y sus offsets
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
        
        return topicDescriptions;
    }
    
    public int countTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> topics = topicsResult.names().get();
        return topics.size();
    }
    
    public String searchTopics(String searchTerm) throws ExecutionException, InterruptedException {
        // Obtener la lista de nombres de tópicos
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNames = listTopicsResult.names().get();

        // Filtrar tópicos que contengan el término de búsqueda
        Set<String> filteredTopicNames = topicNames.stream()
                .filter(topicName -> topicName.contains(searchTerm))
                .collect(Collectors.toSet());

        // Obtener detalles de los tópicos filtrados
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(filteredTopicNames);
        @SuppressWarnings("deprecation")
		Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

        // Obtener los grupos de consumidores
        ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
        Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(cg -> cg.groupId())
                .collect(Collectors.toSet());

        // Obtener la descripción de los grupos de consumidores
        Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

        // Obtener offsets para contar los mensajes
        Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = new HashMap<>();
        for (String topicName : filteredTopicNames) {
            TopicDescription topicDescription = topicDescriptions.get(topicName);
            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                topicPartitionOffsetSpecs.put(new TopicPartition(topicName, partitionInfo.partition()), OffsetSpec.latest());
            }
        }

        // Obtener los offsets más recientes
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(topicPartitionOffsetSpecs).all().get();

        // Mapear los detalles de los tópicos y consumidores
        List<Map<String, Object>> topicDetailsList = new ArrayList<>();
        for (String topicName : filteredTopicNames) {
            TopicDescription topicDescription = topicDescriptions.get(topicName);
            Set<String> consumers = consumerGroupDescriptions.values().stream()
                    .flatMap(cg -> cg.members().stream())
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .filter(tp -> tp.topic().equals(topicName))
                    .map(tp -> tp.topic())
                    .collect(Collectors.toSet());

            // Calcular el total de mensajes
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
            details.put("totalPartitions", topicDescription.partitions().size()); // Total de particiones
            details.put("consumers", consumers); // Consumidores asociados
            details.put("totalMessages", totalMessages); // Total de mensajes

            topicDetailsList.add(details);
        }

        // Convertir la lista a JSON
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(topicDetailsList);
    }
    
    public Map<String, Object> getTopicDetails(String topicName) throws ExecutionException, InterruptedException {
        // Crear una lista con el nombre del tópico que queremos describir
        List<String> topicNames = Collections.singletonList(topicName);
        
        // Llamar a describeTopics con una lista de un solo tópico
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        
        // Obtener los resultados
        Map<String, KafkaFuture<TopicDescription>> topicNameValues = describeTopicsResult.topicNameValues();
        
        // Crear un mapa para almacenar los detalles del tópico
        Map<String, Object> topicDescriptions = new HashMap<>();
        
        // Obtener la descripción del tópico específico
        KafkaFuture<TopicDescription> topicDescriptionFuture = topicNameValues.get(topicName);
        
        if (topicDescriptionFuture != null) {
            TopicDescription topicDescription = topicDescriptionFuture.get();
            
            // Obtener información sobre particiones
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
                for (Node node : partitionInfo.isr()) { // Debe usar isr() en lugar de replicas() para ISR
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
        
        return topicDescriptions;
    }
    
}