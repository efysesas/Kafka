package co.com.famisanar.kafka.topics.application.ports.in.ILogin;

import org.springframework.http.ResponseEntity;

import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;

public interface IKafkaLoginAdmin {
	public ResponseEntity<Object> getUserDetails(String user,String pass, String userDomain);
	public ResponseEntity<String> registerUser(LoginRegister loginRegister);
}
