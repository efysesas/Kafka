
package co.com.famisanar.kafka.topics.application.ports.in;

import java.util.Map;

public interface IKafkaMetrics {
	public Map<String, String> getKafkaMetrics();
}
