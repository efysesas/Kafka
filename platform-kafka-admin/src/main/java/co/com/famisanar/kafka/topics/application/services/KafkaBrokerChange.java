package co.com.famisanar.kafka.topics.application.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.shared.config.KafkaDynamicConfig;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaChangeKafka;

@Service
public class KafkaBrokerChange implements IKafkaChangeKafka{

    private final KafkaDynamicConfig kafkaDynamicConfig;
    public AdminClient adminClient;

    public KafkaBrokerChange(KafkaDynamicConfig kafkaDynamicConfig) {
        this.kafkaDynamicConfig = kafkaDynamicConfig;
    }
    
    public String changeKafkaConnect(String brokerAddress) {
        try {
            close();
            this.adminClient = kafkaDynamicConfig.createAdminClient(brokerAddress);
            adminClient.describeCluster().clusterId().get();
            return "{\"status\":\"success\",\"message\":\"Conexión exitosa al broker\"}";
        } catch (Exception e) {
            this.adminClient = null;
            return "{\"status\":\"error\",\"message\":\"No se pudo conectar al broker " + brokerAddress + "\"}";
        }
    }
    
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
    
}