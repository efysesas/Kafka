package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
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
    
    public List<String> searchConsumerGroups(String searchTerm) throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
        List<ConsumerGroupListing> groupListings = (List<ConsumerGroupListing>) groupsResult.all().get();
        return groupListings.stream()
                .map(ConsumerGroupListing::groupId)
                .filter(groupId -> groupId.contains(searchTerm))
                .collect(Collectors.toList());
    }
    
    public Map<String, Map<String, Object>> getConsumersAndTopics() {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // Obtener los grupos de consumidores
            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Set<String> consumerGroups = consumerGroupsResult.all().get().stream()
                .map(cg -> cg.groupId())
                .collect(Collectors.toSet());

            // Obtener la descripción de los grupos de consumidores
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(consumerGroups).all().get();

            // Mapear consumidores a topics
            Map<String, Map<String, Object>> consumerTopicMap = consumerGroupDescriptions.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        Map<String, Object> details = new HashMap<>();
                        details.put("topics", entry.getValue().members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .map(tp -> tp.topic())
                            .collect(Collectors.toSet()));
                        details.put("active", !entry.getValue().members().isEmpty());
                        return details;
                    }
                ));

            return consumerTopicMap;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error fetching Kafka consumers and topics", e);
        }
    }
    
}
