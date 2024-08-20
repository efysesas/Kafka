package co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka;

import lombok.Data;

@Data
public class Login {
	private String user;
	private String password;
}
