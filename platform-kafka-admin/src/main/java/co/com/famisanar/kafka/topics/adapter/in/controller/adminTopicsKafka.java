package co.com.famisanar.kafka.topics.adapter.in.controller;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaTopicsService;

@CustomRestController
@RequestMapping("/kafka")
public class adminTopicsKafka {
	
	@Autowired
    private KafkaTopicsService kafkaService;

    @GetMapping("/topics")
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        return kafkaService.listTopics();
    }
    
    @GetMapping("/topics/count")
    public int countTopics() throws ExecutionException, InterruptedException {
        return kafkaService.countTopics();
    }
    
    @GetMapping("/topics/search")
    public List<String> searchTopics(@RequestParam String searchTerm) throws ExecutionException, InterruptedException {
        return kafkaService.searchTopics(searchTerm);
    }
    
    @GetMapping("/{topicName}/details")
    public Map<String, Object> getTopicDetails(@PathVariable String topicName) throws ExecutionException, InterruptedException {
        return kafkaService.getTopicDetails(topicName);
    }
    
    @GetMapping("/topics/details")
    public ResponseEntity<Map<String, TopicDescription>> getTopicDetails() {
        try {
            Map<String, TopicDescription> topicDetails = kafkaService.listTopicDetails();
            return ResponseEntity.ok(topicDetails);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
}
