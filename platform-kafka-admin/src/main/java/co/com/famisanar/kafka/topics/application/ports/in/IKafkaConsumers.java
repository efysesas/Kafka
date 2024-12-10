package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.concurrent.ExecutionException;

import org.springframework.http.ResponseEntity;

public interface IKafkaConsumers {
	public ResponseEntity<Object> listConsumerGroups() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> countConsumerGroups() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> searchConsumerGroups(String searchTerm) throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getConsumersAndTopics() throws InterruptedException, ExecutionException;
	public ResponseEntity<Object> getTopicsByConsumer(String consumerGroupId) throws InterruptedException, ExecutionException;
}
