package co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage;

import lombok.Data;

@Data
public class LogMessageChange {
	
	private String topic;
	private int partition;
	private String newMessage;
	private String previousMessage;
	
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
