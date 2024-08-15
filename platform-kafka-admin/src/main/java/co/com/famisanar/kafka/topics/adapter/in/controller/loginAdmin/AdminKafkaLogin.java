package co.com.famisanar.kafka.topics.adapter.in.controller.loginAdmin;

import java.util.concurrent.ExecutionException;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaLogin {
	
	@GetMapping("/user/login")
    public boolean getUserLogin(@PathVariable String user, @PathVariable String pass) {
        return true;//kafkaLogin.getUserDetails(user, pass);
    }
	
	@GetMapping("/user/register")
    public boolean getPartitionDetails(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return true;//kafkaLogin.getPartitionDetails(topic);
    }
	
}
