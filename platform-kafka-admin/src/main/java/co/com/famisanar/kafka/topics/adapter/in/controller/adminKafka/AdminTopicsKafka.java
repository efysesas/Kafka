package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaTopics;

@CustomRestController
@RequestMapping("/kafka")
public class AdminTopicsKafka {
	
	@Autowired
    private IKafkaTopics iKafkaTopics;
    
	@GetMapping("/topics")
	public String getTopicDetails() throws ExecutionException, InterruptedException {
	    return iKafkaTopics.getTopicDetails();
	}
    
    @GetMapping("/topics/search")
    public String searchTopics(@RequestParam String topic) throws ExecutionException, InterruptedException {
        return iKafkaTopics.searchTopics(topic);
    }
    
    @GetMapping("/{topicName}/details/byTopic")
    public Map<String, Object> getTopicDetails(@PathVariable String topicName) throws ExecutionException, InterruptedException {
        return iKafkaTopics.getTopicDetails(topicName);
    }
    
}
