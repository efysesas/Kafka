package co.com.famisanar.kafka.topics.application.services.loginService;

import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;
import co.com.famisanar.kafka.topics.application.ports.in.ILogin.IKafkaLoginAdmin;

@Service
public class LoginService implements IKafkaLoginAdmin{
	
	public boolean getUserDetails(String user,String pass){
		return true;
	}
	public boolean registerUser(LoginRegister loginRegister) {
		return true;
	}
}
