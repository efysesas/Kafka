package co.com.famisanar.kafka.topics.adapter.in.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaKlusterService;

@CustomRestController
@RequestMapping("/kafka")
public class adminKlusterKafka {
	
	@Autowired
	KafkaKlusterService kafkaService;
	
	@GetMapping("/{brokerId}/detailsBroker")
    public Map<String, Object> getBrokerDetails(@PathVariable int brokerId) throws ExecutionException, InterruptedException {
        return kafkaService.getBrokerDetails(brokerId);
    }
	
	@GetMapping("/brokers")
    public List<Node> getBrokers() {
        try {
            return kafkaService.getBrokers();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Error al obtener la lista de brokers", e);
        }
    }
	
}
