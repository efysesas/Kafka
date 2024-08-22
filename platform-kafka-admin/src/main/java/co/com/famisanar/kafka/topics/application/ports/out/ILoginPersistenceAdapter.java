package co.com.famisanar.kafka.topics.application.ports.out;

import org.springframework.http.ResponseEntity;

import co.com.famisanar.kafka.topics.adapter.out.entity.LoginEntity;

public interface ILoginPersistenceAdapter {
	public ResponseEntity<String> registerUser(LoginEntity loginEntity);
	public ResponseEntity<Object> getUserLogin(String user,String pass, String userDomain);
}
