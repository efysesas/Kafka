package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaCountAllService;

@CustomRestController
@RequestMapping("/kafka")
public class AdminCountAllKafka {

	@Autowired
	KafkaCountAllService kafkaCountAllService;
	
	@GetMapping("/topic-count")
    public int getTopicCount() throws Exception {
        return kafkaCountAllService.getTopicCount();
    }
	
	@GetMapping("/partition-count")
    public int getTotalPartitionCount() throws Exception {
        return kafkaCountAllService.getTotalPartitionCount();
    }
	
	@GetMapping("/consumer-count")
    public int getTotalConsumerCount() throws Exception {
        return kafkaCountAllService.getTotalConsumerCount();
    }
	
	@GetMapping("/message-count")
    public int getTotalMessageCount() throws Exception {
        return kafkaCountAllService.getTotalMessageCount();
    }
	
}
