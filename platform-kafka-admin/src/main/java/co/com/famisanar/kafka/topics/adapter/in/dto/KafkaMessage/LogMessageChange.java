package co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage;

import lombok.Data;

@Data
public class LogMessageChange {
	
	private String topic;
	private int partition;
	private String newMessage;
	private String previousMessage;
	private String idMessagePrevious;
	private String userDomain;
	
}
