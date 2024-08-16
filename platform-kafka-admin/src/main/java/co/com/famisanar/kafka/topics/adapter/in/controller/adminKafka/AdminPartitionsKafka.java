package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaPartitionsService;

@CustomRestController
@RequestMapping("/kafka")
public class AdminPartitionsKafka {
	
	@Autowired
    private KafkaPartitionsService kafkaService;
	
	@GetMapping("/partition-details")
    public Map<String, Map<String, Map<String, Object>>> getAllPartitionDetails() {
        try {
            return kafkaService.getAllPartitionDetails();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Error fetching partition details for all topics", e);
        }
    }
	
	@GetMapping("/topics/{topic}/partitions/details/byTopic")
    public Map<String, Object> getPartitionDetails(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionDetails(topic);
    }
	
	@GetMapping("/topics/{topic}/partitions/count/byTopic")
    public int getPartitionCount(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionCount(topic);
    }
	
	@GetMapping("/topics/{topic}/partitions/search/{count}")
    public Map<String, Object> getPartitionSearch(@PathVariable String topic,@PathVariable int count) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionSearch(topic,count);
    }
	
}
