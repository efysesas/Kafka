package co.com.famisanar.kafka.topics.adapter.out.persistence;

import java.util.Optional;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;

import co.com.famisanar.kafka.shared.annotations.PersistenceAdapter;
import co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka.LoginRegister;
import co.com.famisanar.kafka.topics.adapter.out.entity.LoginEntity;
import co.com.famisanar.kafka.topics.adapter.out.exceptions.RespuestaHttpHandler;
import co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces.ILogin;
import co.com.famisanar.kafka.topics.application.ports.out.ILoginPersistenceAdapter;

@PersistenceAdapter
public class LoginKafkaPersistenceAdapter implements ILoginPersistenceAdapter{

	@Autowired
    private PasswordEncoder passwordEncoder;
	
	@Autowired
    private ModelMapper modelMapper;
	
	@Autowired
	ILogin iLoginRepository;
	
	@Autowired
	RespuestaHttpHandler respuestaHttpHandler;
	
	@Override
	public ResponseEntity<String> registerUser(LoginEntity loginEntity) {
		if (!iLoginRepository.existsById(loginEntity.getIdentification())) {
			iLoginRepository.save(loginEntity);
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.respuestaPeticiones(HttpStatus.CREATED, "Creacion Ok", "201", null));
		} else {
			return ResponseEntity.status(HttpStatus.CONFLICT).body(respuestaHttpHandler
					.respuestaPeticiones(HttpStatus.CONFLICT, "Ya existe alguien con el mismo documento", "409", null));
		}
	}

	public ResponseEntity<Object> getUserLogin(String user, String pass, String userDomain) {

	    try {
	        Optional<LoginEntity> loginEntityOpt = iLoginRepository.findByUserDomain(userDomain);

	        if (loginEntityOpt.isPresent()) {
	            LoginEntity loginEntity = loginEntityOpt.get();
	            boolean isMatch = this.matchPasswords(loginEntity.getPasswordKafka(), pass);

	            if (loginEntity.getUserKafka().equals(user) && isMatch) {
	                // Mapear LoginEntity a LoginRegister
	                LoginRegister loginRegister = modelMapper.map(loginEntity, LoginRegister.class);
	                return ResponseEntity.ok(loginRegister);
	            } else {
	                String errorJson = respuestaHttpHandler.respuestaPeticiones(
	                        HttpStatus.BAD_REQUEST, 
	                        "Usuario o contraseña incorrectos", 
	                        "Error", 
	                        null
	                );
	                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorJson);
	            }
	        } else {
	            String errorJson = respuestaHttpHandler.respuestaPeticiones(
	                    HttpStatus.BAD_REQUEST, 
	                    "Usuario de dominio no existe", 
	                    "Error", 
	                    null
	            );
	            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorJson);
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        String errorJson = respuestaHttpHandler.respuestaPeticiones(
	                HttpStatus.INTERNAL_SERVER_ERROR, 
	                "Error interno del servidor", 
	                "Error", 
	                null
	        );
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorJson);
	    }
	}
	
	public boolean matchPasswords(String plainPassword, String encodedPassword) {
        return passwordEncoder.matches(plainPassword, encodedPassword);
    }
	
}
