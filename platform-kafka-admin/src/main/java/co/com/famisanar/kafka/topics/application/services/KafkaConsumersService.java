package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumersService {
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
    
}
