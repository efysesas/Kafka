package co.com.famisanar.kafka.topics.application.ports.in.ILogin;

import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;

public interface IKafkaLoginAdmin {
	public boolean getUserDetails(String user,String pass);
	public boolean registerUser(LoginRegister loginRegister);
}
