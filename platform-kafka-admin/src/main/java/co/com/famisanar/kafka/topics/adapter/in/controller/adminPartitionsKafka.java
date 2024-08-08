package co.com.famisanar.kafka.topics.adapter.in.controller;

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
public class adminPartitionsKafka {
	
	@Autowired
    private KafkaPartitionsService kafkaService;
	
	@GetMapping("/topics/{topic}/partitions/details")
    public Map<String, Object> getPartitionDetails(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return kafkaService.getPartitionDetails(topic);
    }
}
