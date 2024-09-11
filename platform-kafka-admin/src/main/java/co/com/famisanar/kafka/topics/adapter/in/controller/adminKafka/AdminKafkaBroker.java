package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaBroker {
	
	@PostMapping("/setBroker")
	public ResponseEntity<String> setBroker(@RequestParam String brokerIp){
		//kafkaService.updateKafkaBroker(brokerIp);
		//return ResponseEntity.ok("Kafka broker update to: "+brokerIp)
		return null;
	}
}
