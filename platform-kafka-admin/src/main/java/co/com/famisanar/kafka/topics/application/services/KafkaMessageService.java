package co.com.famisanar.kafka.topics.application.services;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.adapter.in.dto.SendMessage;
import co.com.famisanar.kafka.topics.adapter.out.entity.MessageEntity;
import co.com.famisanar.kafka.topics.application.ports.out.IAdminKafkaPersistenceAdapter;
import jakarta.servlet.ServletContext;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaMessageService {
	
	@Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
	
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
    public KafkaMessageService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
	
	@Autowired
    public ServletContext servletContext;
	
	@Autowired
    private ConsumerFactory<String, String> consumerFactory;
	
	@Autowired
    private IAdminKafkaPersistenceAdapter departmentPersistenceAdapter;
	
	public List<Map<String, Object>> getMessages(String topic, int partition, int offset, int limit) {
	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

	    try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
	        TopicPartition topicPartition = new TopicPartition(topic, partition);
	        consumer.assign(Collections.singletonList(topicPartition));
	        consumer.seek(topicPartition, offset);

	        List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofSeconds(1)).records(topicPartition);

	        // Cerrar el consumidor después de obtener los registros
	        consumer.close();

	        // Mapear los registros a una lista de mapas con timestamp formateado
	        return records.subList(0, Math.min(records.size(), limit)).stream().map(record -> {
	            Map<String, Object> messageDetails = new HashMap<>();
	            messageDetails.put("key", record.key());
	            messageDetails.put("value", record.value());
	            messageDetails.put("timestamp", formatter.format(Instant.ofEpochMilli(record.timestamp())));
	            messageDetails.put("partition", record.partition());
	            messageDetails.put("offset", record.offset());
	            return messageDetails;
	        }).collect(Collectors.toList());
	    }
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
	
	public List<ConsumerRecord<String, String>> getMessagesByValue(SendMessage sendMessage) {

        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
        	TopicPartition topicPartition = new TopicPartition(sendMessage.getTopic(), sendMessage.getPartition());
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, sendMessage.getOffset());
			
			    List<ConsumerRecord<String, String>> records = new ArrayList<>();

			    while (true) {
	                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

	                for (ConsumerRecord<String, String> record : consumerRecords) {
	                	
	                	String value = record.value().toLowerCase();
	                    if (value.contains(sendMessage.getMessage())) {
	                        records.add(record);
	                    }
	                    
	                }

	                if (consumerRecords.isEmpty()) {
	                    break;
	                }
	            }

			    return records;
		}
    }
	
	public boolean send(SendMessage sendMessage) {
        String idMessage = UUID.randomUUID().toString();
        try {
            kafkaTemplate.send(sendMessage.getTopic(), sendMessage.getPartition(), idMessage, sendMessage.getMessage()).get();
            return true;
        } catch (Exception e) {
            MessageEntity messageEntity = new MessageEntity();
            messageEntity.setMessageId(idMessage);
            messageEntity.setAttempts(1);
            messageEntity.setTopicKafka(sendMessage.getTopic());
            messageEntity.setMessage(sendMessage.getMessage());
            messageEntity.setApplication(servletContext.getContextPath());
            messageEntity.setStatus("PENDING");
            departmentPersistenceAdapter.saveMessageError(messageEntity);
            log.error("ERROR ENVIANDO MENSAJE", e);
            return false;
        }
    }
	
}
