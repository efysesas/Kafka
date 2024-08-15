package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaConsumersService;

@CustomRestController
@RequestMapping("/kafka")
public class AdminConsumersKafka {

	@Autowired
    private KafkaConsumersService kafkaConsumersService;

    @GetMapping("/consumers")
    public List<String> listConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaConsumersService.listConsumerGroups();
    }
    
    @GetMapping("/consumers/count")
    public int countConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaConsumersService.countConsumerGroups();
    }
    
    @GetMapping("/consumers/search")
    public List<String> searchConsumerGroups(@RequestParam String searchTerm) throws ExecutionException, InterruptedException {
        return kafkaConsumersService.searchConsumerGroups(searchTerm);
    }
    
    @GetMapping("/consumersInf")
    public Map<String, Map<String, Object>> getConsumersAndTopics() {
        return kafkaConsumersService.getConsumersAndTopics();
    }
    
}
