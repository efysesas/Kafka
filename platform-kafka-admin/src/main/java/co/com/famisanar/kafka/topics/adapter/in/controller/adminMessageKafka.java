package co.com.famisanar.kafka.topics.adapter.in.controller;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.services.KafkaMessageService;

@CustomRestController
@RequestMapping("/kafka")
public class adminMessageKafka {

	@Autowired
    private KafkaMessageService kafkaMessageService;
	
    @GetMapping("/topics/{topic}/partitions/{partition}/messages")
    public List<Map<String, Object>> getMessages(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam int offset,
            @RequestParam(defaultValue = "10") int limit) {
        List<ConsumerRecord<String, String>> records = kafkaMessageService.getMessages(topic, partition, offset, limit);
        return records.stream().map(record -> {
            Map<String, Object> message = new HashMap<>();
            message.put("offset", record.offset());
            message.put("key", record.key());
            message.put("value", record.value());
            message.put("partition", record.partition());
            message.put("timestamp", record.timestamp());
            return message;
        }).collect(Collectors.toList());
    }
    
    @GetMapping("/topics/{topic}/partitions/{partition}/messagesByDate")
    public List<Map<String, Object>> getMessagesByDateRange(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Instant start = Instant.parse(startTime);
        Instant end = Instant.parse(endTime);
        List<ConsumerRecord<String, String>> records = kafkaMessageService.getMessagesByDateRange(topic, partition, start, end);
        return records.stream().map(record -> {
            Map<String, Object> message = new HashMap<>();
            message.put("offset", record.offset());
            message.put("key", record.key());
            message.put("value", record.value());
            message.put("partition", record.partition());
            message.put("timestamp", record.timestamp());
            return message;
        }).collect(Collectors.toList());
    }
    
}
