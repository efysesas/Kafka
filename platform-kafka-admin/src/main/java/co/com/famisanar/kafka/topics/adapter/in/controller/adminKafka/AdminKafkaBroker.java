package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaBrokerChange;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaBroker {
	
	@Autowired
	KafkaBrokerChange kafkaBrokerChange;
	
	@PostMapping("/changeBroker")
    public ResponseEntity<String> setBroker(@RequestParam String brokerIp) throws IOException {
		kafkaBrokerChange.updateKafkaBroker(brokerIp);
        return ResponseEntity.ok("Kafka broker updated to: " + brokerIp);
    }
	
}
