package co.com.famisanar.kafka.topics.adapter.out.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import co.com.famisanar.kafka.shared.annotations.QueueManager;
import co.com.famisanar.kafka.topics.application.ports.out.IMessagePersistenceAdapter;

@QueueManager
public class KafkaProducerAdmin implements IMessagePersistenceAdapter {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message, String idMessage, String topic) {
        kafkaTemplate.send(topic, idMessage, message);
    }

}
