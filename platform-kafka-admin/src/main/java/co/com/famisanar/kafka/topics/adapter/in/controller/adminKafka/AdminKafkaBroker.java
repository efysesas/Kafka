package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaBrokerChange;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaBroker {
	
	@Autowired
	KafkaBrokerChange kafkaBrokerChange;
	
	@GetMapping("/connect/{broker}")
    public String connectToBroker(@PathVariable String broker) {
		kafkaBrokerChange.connectToBroker(broker);
        return "Conectado a broker: " + broker;
    }
	
}
