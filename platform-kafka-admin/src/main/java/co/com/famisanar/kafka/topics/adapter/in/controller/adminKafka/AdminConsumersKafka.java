package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    public String getConsumersAndTopics() throws InterruptedException, ExecutionException {
        return kafkaConsumersService.getConsumersAndTopics();
    }
    
    @GetMapping("/consumers/count")
    public int countConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaConsumersService.countConsumerGroups();
    }
    
    @GetMapping("/consumers/search")
    public String searchConsumerGroups(@RequestParam String consumer)throws ExecutionException, InterruptedException  {
        return kafkaConsumersService.searchConsumerGroups(consumer);
    }
    
    @GetMapping("/topicsByConsumer/{consumerGroupId}")
    public List<Map<String, Object>> getTopicsByConsumer(@PathVariable String consumerGroupId) throws InterruptedException, ExecutionException {
        return kafkaConsumersService.getTopicsByConsumer(consumerGroupId);
    }
    
}
