package co.com.famisanar.kafka.topics.adapter.in.controller.loginAdmin;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.Login;
import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;
import co.com.famisanar.kafka.topics.application.ports.in.ILogin.IKafkaLoginAdmin;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaLogin {
	
	@Autowired
	private IKafkaLoginAdmin iKafkaLoginAdmin;
	
	 @Autowired
	 PasswordEncoder passwordEncoder;
	
	@GetMapping("/user/login")
    public ResponseEntity<Object> getUserLogin(@RequestBody Login login) {
		login.setPasswordKafka(passwordEncoder.encode(login.getPasswordKafka()));
        return iKafkaLoginAdmin.getUserDetails(login.getUserKafka(), login.getPasswordKafka(), login.getUserDomain());
    }
	
	@PostMapping("/user/register")
    public ResponseEntity<String> registerUser(@RequestBody LoginRegister loginRegister) throws ExecutionException, InterruptedException {
		loginRegister.setPasswordKafka(passwordEncoder.encode(loginRegister.getPasswordKafka()));
		return iKafkaLoginAdmin.registerUser(loginRegister);
    }
	
}
