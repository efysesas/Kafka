package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.concurrent.ExecutionException;

public interface IKafkaCountAll {
	public int getTopicCount() throws ExecutionException, InterruptedException;
	public int getTotalPartitionCount() throws ExecutionException, InterruptedException;
	public int getTotalConsumerCount() throws ExecutionException, InterruptedException;
	public int getTotalMessageCount() throws ExecutionException, InterruptedException;
}
