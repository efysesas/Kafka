package co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage;

import lombok.Data;

@Data
public class RelaunchingMessage {
	private String originalMessage;
	private String userDomain;
	private Long identification;
	private String dateUpdate;
	private String newMessage;
}
