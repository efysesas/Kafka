package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.List;
import java.util.Map;

public interface IKafkaPartitions {
	public List<Map<String, Object>> getPartitionDetails(String topic);
	public int getPartitionCount(String topic);
	public Map<String, Object> getPartitionSearch(String topic, int partition);
	public List<Map<String, Object>> getAllPartitionDetails();
}
