package co.com.famisanar.kafka.topics.application.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.shared.config.KafkaDynamicConfig;

@Service
public class KafkaBrokerChange {

    @Autowired
    private KafkaDynamicConfig kafkaDynamicConfig;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaBrokerChange(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void updateKafkaBroker(String brokerIp) {
        kafkaDynamicConfig.updateBootstrapServers(brokerIp);
        kafkaTemplate = kafkaDynamicConfig.kafkaTemplate(); // <--- Update the kafkaTemplate instance
    }
}