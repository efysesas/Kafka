package co.com.famisanar.kafka.topics.application.services.loginService;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;
import co.com.famisanar.kafka.topics.adapter.out.entity.LoginEntity;
import co.com.famisanar.kafka.topics.application.ports.in.ILogin.IKafkaLoginAdmin;
import co.com.famisanar.kafka.topics.application.ports.out.ILoginPersistenceAdapter;

@Service
public class LoginService implements IKafkaLoginAdmin{
	
	@Autowired
	ILoginPersistenceAdapter iLoginPersistenceAdapter;
	
	@Autowired
    private ModelMapper modelMapper;
	
	public ResponseEntity<Object> getUserDetails(String user,String pass, String userDomain){
		return iLoginPersistenceAdapter.getUserLogin(user, pass, userDomain);
	}
	public ResponseEntity<Object> registerUser(LoginRegister loginRegister) {
		LoginEntity loginEntity = modelMapper.map(loginRegister, LoginEntity.class);
		return iLoginPersistenceAdapter.registerUser(loginEntity);
	}
	
	
}
