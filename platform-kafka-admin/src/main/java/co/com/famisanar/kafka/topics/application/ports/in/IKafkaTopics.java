package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface IKafkaTopics {
	public String getTopicDetails() throws ExecutionException, InterruptedException;
	public Set<String> listTopics() throws ExecutionException, InterruptedException;
	public Map<String, Object> describeTopics(Set<String> topicNames) throws ExecutionException, InterruptedException;
	public int countTopics() throws ExecutionException, InterruptedException;
	public String searchTopics(String searchTerm) throws ExecutionException, InterruptedException;
	public Map<String, Object> getTopicDetails(String topicName) throws ExecutionException, InterruptedException;
}
