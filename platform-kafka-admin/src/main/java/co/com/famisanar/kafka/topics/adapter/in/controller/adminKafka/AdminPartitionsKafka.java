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
import co.com.famisanar.kafka.topics.application.services.KafkaPartitionsService;

@CustomRestController
@RequestMapping("/kafka")
public class AdminPartitionsKafka {
	
	@Autowired
    private KafkaPartitionsService kafkaService;
	
	@GetMapping("/partitions")
    public List<Map<String, Object>> getAllPartitionDetails() throws ExecutionException, InterruptedException  {
        return kafkaService.getAllPartitionDetails();
    }
	
	@GetMapping("/topics/{topic}/partitions/search")
    public Map<String, Object> getPartitionSearch(@PathVariable String topic,@RequestParam int partition) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionSearch(topic,partition);
    }
	
	@GetMapping("/topics/{topic}/partitions/details/byTopic")
    public List<Map<String, Object>> getPartitionDetails(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionDetails(topic);
    }
	
	@GetMapping("/topics/{topic}/partitions/count/byTopic")
    public int getPartitionCount(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionCount(topic);
    }
	
}
