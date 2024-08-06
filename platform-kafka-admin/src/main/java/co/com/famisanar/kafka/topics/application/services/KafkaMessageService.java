package co.com.famisanar.kafka.topics.application.services;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageService {
	
	@Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
	
	@Autowired
    private ConsumerFactory<String, String> consumerFactory;
	
	public List<ConsumerRecord<String, String>> getMessages(String topic, int partition, int offset, int limit) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "backend-architecture");//PENDIENTE DE LISTAR TAMBIEN LOS GROUP_ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);

        List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofSeconds(1)).records(topicPartition);
        consumer.close();

        return records.subList(0, Math.min(records.size(), limit));
    }
	
	public List<ConsumerRecord<String, String>> getMessagesByDateRange(String topic, int partition, Instant startTime, Instant endTime) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        timestampsToSearch.put(topicPartition, startTime.toEpochMilli());

        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
            consumer.assign(Collections.singletonList(topicPartition));
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);

            long startOffset = offsetsForTimes.get(topicPartition).offset();
            consumer.seek(topicPartition, startOffset);

            List<ConsumerRecord<String, String>> records = new ArrayList<>();

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    if (record.timestamp() >= endTime.toEpochMilli()) {
                        return records;
                    }

                    records.add(record);
                }

                if (consumerRecords.isEmpty()) {
                    break;
                }
            }

            return records;
        }
    }
	
}
