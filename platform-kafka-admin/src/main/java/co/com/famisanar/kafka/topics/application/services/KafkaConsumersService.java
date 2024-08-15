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
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

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
    
    public Map<String, Map<String, Object>> searchConsumerGroups(String searchTerm) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            
            ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroupIds = groupsResult.all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(groupId -> groupId.contains(searchTerm))
                    .collect(Collectors.toSet());

            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroupIds).all().get();

            Map<String, Map<String, Object>> consumerGroupDetails = consumerGroupDescriptions.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Map<String, Object> details = new HashMap<>();
                            ConsumerGroupDescription description = entry.getValue();

                            Set<String> topics = description.members().stream()
                                    .flatMap(member -> member.assignment().topicPartitions().stream())
                                    .map(tp -> tp.topic())
                                    .collect(Collectors.toSet());
                            details.put("topics", topics);

                            details.put("active", !description.members().isEmpty());

                            details.put("memberCount", description.members().size());

                            return details;
                        }
                    ));

            return consumerGroupDetails;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka consumer groups details", e);
        }
    }
    
    public Map<String, Map<String, Object>> getConsumersAndTopics() {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener los grupos de consumidores
            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());

            // Obtener la descripción de los grupos de consumidores
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

            // Mapear consumidores a topics con conteo
            return consumerGroupDescriptions.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        Map<String, Object> details = new HashMap<>();
                        ConsumerGroupDescription description = entry.getValue();

                        // Contar los topics únicos asignados a cada consumidor
                        long topicCount = description.members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .map(tp -> tp.topic())
                            .distinct()
                            .count();
                        details.put("topicCount", (int) topicCount);

                        // Verificar si el grupo de consumidores está activo
                        details.put("active", !description.members().isEmpty());

                        // Obtener la cantidad de miembros en el grupo
                        details.put("memberCount", description.members().size());

                        return details;
                    }
                ));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka consumers and topics", e);
        }
    }
    
    
    public List<String> getTopicsByConsumer(String consumerGroupId) {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener la descripción del grupo de consumidores específico
            DescribeConsumerGroupsResult describeGroupsResult = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroupId));
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = describeGroupsResult.all().get();

            ConsumerGroupDescription description = consumerGroupDescriptions.get(consumerGroupId);
            if (description == null) {
                throw new NoSuchElementException("Consumer group not found: " + consumerGroupId);
            }

            // Mapear topics para el consumidor específico
            List<String> topics = description.members().stream()
                .flatMap(member -> member.assignment().topicPartitions().stream())
                .map(TopicPartition::topic)
                .distinct() // Evitar duplicados
                .sorted()   // Ordenar para una salida consistente
                .collect(Collectors.toList());

            return topics;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka topics for consumer group", e);
        }
    }
    
}
