package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.concurrent.ExecutionException;

import org.springframework.http.ResponseEntity;

public interface IKafkaCountAll {
	public ResponseEntity<Object> getTopicCount() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getTotalPartitionCount() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getTotalConsumerCount() throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getTotalMessageCount() throws ExecutionException, InterruptedException;
}
