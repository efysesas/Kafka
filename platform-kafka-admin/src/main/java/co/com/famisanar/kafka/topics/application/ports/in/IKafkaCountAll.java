package co.com.famisanar.kafka.topics.application.ports.in;

public interface IKafkaCountAll {
	public int getTopicCount();
	public int getTotalPartitionCount();
	public int getTotalConsumerCount();
	public int getTotalMessageCount();
}
