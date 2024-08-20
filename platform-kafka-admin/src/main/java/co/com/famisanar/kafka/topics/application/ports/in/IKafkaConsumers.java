package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.List;

public interface IKafkaConsumers {
	public List<String> listConsumerGroups();
	public int countConsumerGroups();
	public String searchConsumerGroups(String searchTerm);
	public String getConsumersAndTopics();
	public List<String> getTopicsByConsumer(String consumerGroupId);
}
