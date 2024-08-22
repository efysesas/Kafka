package co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka;

import lombok.Data;

@Data
public class LoginRegister {
    private Long identification;
	private String name;
	private String lastname;
	private String userDomain;
	private String email;
	private String area;
	private String userKafka;
	private String passwordKafka;
	private int active;
}
