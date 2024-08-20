package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.Map;
import java.util.Set;

public interface IKafkaTopics {
	public String getTopicDetails();
	public Set<String> listTopics();
	public Map<String, Object> describeTopics(Set<String> topicNames);
	public int countTopics();
	public String searchTopics(String searchTerm);
	public Map<String, Object> getTopicDetails(String topicName);
}
