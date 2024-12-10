package co.com.famisanar.kafka.topics.adapter.in.dto.kafkaAdmin;

import lombok.Data;

@Data
public class SendMessage {
	private String topic;
	private int partition;
	private String message;
	private long offset;
}
