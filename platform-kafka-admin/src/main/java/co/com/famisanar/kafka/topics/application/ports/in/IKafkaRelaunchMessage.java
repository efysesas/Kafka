package co.com.famisanar.kafka.topics.application.ports.in;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage.LogMessageChange;
import co.com.famisanar.kafka.topics.adapter.in.dto.kafkaAdmin.SendMessage;

public interface IKafkaRelaunchMessage {
	
	public List<Map<String, Object>> getMessages(String topic, int partition, int offset, int limit);
	public List<ConsumerRecord<String, String>> getMessagesByDateRange(String topic, int partition, Instant startTime, Instant endTime);
	public List<ConsumerRecord<String, String>> getMessagesByValue(SendMessage sendMessage);
	public boolean send(SendMessage sendMessage);
	public boolean reSend(LogMessageChange logMessageChange);
	
}
