package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.springframework.http.ResponseEntity;

public interface IKafkaTopics {
	public ResponseEntity<Object> getTopicDetails() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> describeTopics(Set<String> topicNames) throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> searchTopics(String searchTerm) throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getTopicDetails(String topicName) throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> createTopic(String topicName, int numPartitions, short replicationFactor);
}
