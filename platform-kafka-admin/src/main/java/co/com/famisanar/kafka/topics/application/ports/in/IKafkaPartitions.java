package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.concurrent.ExecutionException;

import org.springframework.http.ResponseEntity;

public interface IKafkaPartitions {
	public ResponseEntity<Object> getPartitionDetails(String topic)throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getPartitionCount(String topic)throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getPartitionSearch(String topic, int partition)throws ExecutionException, InterruptedException;
	public ResponseEntity<Object> getAllPartitionDetails()throws ExecutionException, InterruptedException;
}
