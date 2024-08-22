package co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka;

import lombok.Data;

@Data
public class Login {
	private String userKafka;
	private String passwordKafka;
	private String userDomain;
}
