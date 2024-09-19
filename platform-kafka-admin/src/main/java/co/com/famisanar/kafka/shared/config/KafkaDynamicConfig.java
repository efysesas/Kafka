package co.com.famisanar.kafka.shared.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaDynamicConfig {

    @Value("${spring.kafka.bootstrap-servers:}")
    private String bootstrapServers;

    private ProducerFactory<String, String> producerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return createProducerFactoryInternal(bootstrapServers);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        if (kafkaTemplate == null) {
            kafkaTemplate = new KafkaTemplate<>(producerFactory());
        }
        return kafkaTemplate;
    }

    public void updateBootstrapServers(String newBootstrapServers) {
        this.bootstrapServers = newBootstrapServers;
        producerFactory = createProducerFactoryInternal(bootstrapServers);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    private ProducerFactory<String, String> createProducerFactoryInternal(String bootstrapServers) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
}