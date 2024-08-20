package co.com.famisanar.kafka.topics.adapter.in.controller.loginAdmin;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;
import co.com.famisanar.kafka.topics.application.services.loginService.LoginService;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaLogin {
	
	@Autowired
	LoginService kafkaLogin;
	
	 @Autowired
	 PasswordEncoder passwordEncoder;
	
	@GetMapping("/user/login")
    public boolean getUserLogin(@PathVariable String user, @PathVariable String pass) {
		passwordEncoder.encode(pass);
        return kafkaLogin.getUserDetails(user, pass);
    }
	
	@GetMapping("/user/register")
    public boolean registerUser(@RequestBody LoginRegister loginRegister) throws ExecutionException, InterruptedException {
        return kafkaLogin.registerUser(loginRegister);
    }
	
}
