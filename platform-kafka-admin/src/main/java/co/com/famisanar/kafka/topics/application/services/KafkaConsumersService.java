package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class KafkaConsumersService {
	
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

	private final AdminClient adminClient;

    public KafkaConsumersService(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public List<String> listConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
        List<ConsumerGroupListing> groupListings = new ArrayList<>(groupsResult.all().get());
        List<String> groupIds = new ArrayList<>();
        for (ConsumerGroupListing groupListing : groupListings) {
            groupIds.add(groupListing.groupId());
        }
        return groupIds;
    }
    
    public int countConsumerGroups() throws ExecutionException, InterruptedException {
    	ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
        List<ConsumerGroupListing> groupListings = (List<ConsumerGroupListing>) groupsResult.all().get();
        return groupListings.size();
    }
    
    public String searchConsumerGroups(String searchTerm) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener los grupos de consumidores
            ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
            
            // Filtrar por el término de búsqueda
            List<ConsumerGroupListing> groupListings = (List<ConsumerGroupListing>) groupsResult.all().get();
            Set<String> consumerGroupIds = groupListings.stream()
                    .filter(groupListing -> groupListing.groupId().contains(searchTerm))
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());

            // Obtener la descripción de los grupos de consumidores
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroupIds).all().get();

            // Mapear detalles de los grupos de consumidores a una lista
            List<Map<String, Object>> consumerGroupDetailsList = consumerGroupDescriptions.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> details = new HashMap<>();
                    ConsumerGroupDescription description = entry.getValue();

                    List<String> consumerIds = description.members().stream()
                            .map(member -> member.consumerId()) // Asegúrate de que este método devuelve el ID correcto del consumidor
                            .collect(Collectors.toList());
                    details.put("consumerIds", consumerIds);
                    
                    // Obtener los tópicos asociados
                    Set<String> topics = description.members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .map(TopicPartition::topic)
                            .collect(Collectors.toSet());
                    details.put("topics", topics);

                    // Verificar si el grupo de consumidores está activo
                    details.put("active", !description.members().isEmpty());

                    // Obtener la cantidad de miembros en el grupo
                    details.put("threadCount", description.members().size());

                    // Agregar el nombre del grupo de consumidores
                    details.put("name", entry.getKey());

                    return details;
                })
                .collect(Collectors.toList());

            // Convertir la lista a JSON
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(consumerGroupDetailsList);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka consumer groups details", e);
        }
    }
    
    public String getConsumersAndTopics() {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener los grupos de consumidores
            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());

            // Obtener la descripción de los grupos de consumidores
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

            // Mapear consumidores a topics con conteo
            List<Map<String, Object>> consumerDetailsList = consumerGroupDescriptions.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> details = new HashMap<>();
                    ConsumerGroupDescription description = entry.getValue();

                    // Contar los topics únicos asignados a cada consumidor
                    long topicCount = description.members().stream()
                        .flatMap(member -> member.assignment().topicPartitions().stream())
                        .map(TopicPartition::topic)
                        .distinct()
                        .count();
                    
                    details.put("name", entry.getKey());
                    details.put("topicCount", (int) topicCount);
                    details.put("active", !description.members().isEmpty());
                    // Obtener la cantidad de miembros en el grupo
                    details.put("memberCount", description.members().size());

                    return details;
                })
                .collect(Collectors.toList());

            // Convertir la lista a JSON
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(consumerDetailsList);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka consumers and topics", e);
        }
    }    
    
    public List<Map<String, Object>> getTopicsByConsumer(String consumerGroupId) {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener la descripción del grupo de consumidores específico
            DescribeConsumerGroupsResult describeGroupsResult = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroupId));
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = describeGroupsResult.all().get();

            ConsumerGroupDescription description = consumerGroupDescriptions.get(consumerGroupId);
            if (description == null) {
                throw new NoSuchElementException("Consumer group not found: " + consumerGroupId);
            }

            // Mapear información de los tópicos
            List<Map<String, Object>> topicsInfo = new ArrayList<>();

            // Obtener información de los tópicos
            for (String topic : description.members().stream()
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .map(TopicPartition::topic)
                    .distinct()
                    .collect(Collectors.toList())) {

                // Obtener información sobre el tópico
                @SuppressWarnings("deprecation")
				TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topic)).all().get().get(topic);
                if (topicDescription == null) continue;

                // Obtener total de particiones y total de mensajes
                int totalPartitions = topicDescription.partitions().size();
                long totalMessages = getTotalMessagesForTopic(topic); // Método que debes implementar para contar mensajes

                // Construir el objeto del tópico
                Map<String, Object> topicDetails = new HashMap<>();
                topicDetails.put("totalPartitions", totalPartitions);
                topicDetails.put("totalMessages", totalMessages);
                topicDetails.put("topicName", topic);

                // Agregar información de consumidores
                List<Map<String, Object>> consumers = new ArrayList<>();
                for (ConsumerGroupDescription groupDescription : consumerGroupDescriptions.values()) {
                    if (groupDescription.members().stream().anyMatch(member -> member.assignment().topicPartitions().stream()
                            .anyMatch(tp -> tp.topic().equals(topic)))) {
                        Map<String, Object> consumerInfo = new HashMap<>();
                        consumerInfo.put("threadCount", groupDescription.members().size());
                        consumerInfo.put("consumerGroup", groupDescription.groupId());
                        consumers.add(consumerInfo);
                    }
                }

                topicDetails.put("consumers", consumers);
                topicsInfo.add(topicDetails);
            }

            return topicsInfo;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka topics for consumer group", e);
        }
    }

    private long getTotalMessagesForTopic(String topic) {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener la descripción de las particiones del tópico
            @SuppressWarnings("deprecation")
			List<TopicPartitionInfo> partitionInfos = adminClient.describeTopics(Collections.singletonList(topic)).all().get().get(topic).partitions();

            // Obtener el total de mensajes en el tópico
            long totalMessages = 0;
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                int partitionId = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partitionId);

                // Obtener el último offset
                long endOffset = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get().get(topicPartition).offset();
                totalMessages += endOffset; // Contar desde el inicio hasta el último offset
            }

            return totalMessages;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching total messages for topic: " + topic, e);
        }
    }
    
}
