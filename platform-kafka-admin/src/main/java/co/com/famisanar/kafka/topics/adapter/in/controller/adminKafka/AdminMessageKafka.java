package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage.LogMessageChange;
import co.com.famisanar.kafka.topics.adapter.in.dto.kafkaAdmin.SendMessage;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaRelaunchMessage;

@CustomRestController
@RequestMapping("/kafka")
public class AdminMessageKafka {

	@Autowired
    private IKafkaRelaunchMessage iKafkaRelaunchMessage;
	
	@GetMapping("/topics/{topic}/partitions/{partition}/messages")
    public List<Map<String, Object>> getMessages(
    		@PathVariable String topic,
            @PathVariable int partition,
            @RequestParam int offset,
            @RequestParam int limit) throws ExecutionException, InterruptedException {
        return iKafkaRelaunchMessage.getMessages(topic, partition, offset, limit);
    }
    
    @GetMapping("/topics/{topic}/partitions/{partition}/messagesByDate")
    public List<Map<String, Object>> getMessagesByDateRange(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Instant start = Instant.parse(startTime);
        Instant end = Instant.parse(endTime);
        List<ConsumerRecord<String, String>> records = iKafkaRelaunchMessage.getMessagesByDateRange(topic, partition, start, end);
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
    
    @GetMapping("/messages/search")
    public List<Map<String, Object>> getMessagesByValue(@RequestBody SendMessage sendMessage) {
        List<ConsumerRecord<String, String>> messages = iKafkaRelaunchMessage.getMessagesByValue(sendMessage);
        return messages.stream().map(record -> {
            Map<String, Object> message = new HashMap<>();
            message.put("offset", record.offset());
            message.put("key", record.key());
            message.put("value", record.value());
            message.put("partition", record.partition());
            message.put("timestamp", record.timestamp());
            return message;
        }).collect(Collectors.toList());
    }
    
    @PostMapping(value = "/sendMessage")
    public ResponseEntity<String> sendMessage(@RequestBody SendMessage sendMessage) {
        return iKafkaRelaunchMessage.send(sendMessage) ?  ResponseEntity.ok("Mensaje enviado ") :  ResponseEntity.internalServerError().body("No se pudo enviar el mensaje");
    }
    
    @PostMapping(value = "/logChangeMessage")
    public ResponseEntity<String> logChangeMessage(@RequestBody LogMessageChange logMessageChange) {
        return iKafkaRelaunchMessage.reSend(logMessageChange) ?  ResponseEntity.ok("Mensaje relanzado ") :  ResponseEntity.internalServerError().body("No se pudo enviar el mensaje");
    }
    
}
