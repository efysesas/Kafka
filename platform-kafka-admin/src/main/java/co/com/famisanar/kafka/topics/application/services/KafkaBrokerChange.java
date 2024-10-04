package co.com.famisanar.kafka.topics.application.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.shared.config.KafkaDynamicConfig;

@Service
public class KafkaBrokerChange {

    private final KafkaDynamicConfig kafkaDynamicConfig;
    public AdminClient adminClient;

    public KafkaBrokerChange(KafkaDynamicConfig kafkaDynamicConfig) {
        this.kafkaDynamicConfig = kafkaDynamicConfig;
    }

    public void connectToBroker(String brokerAddress) {
        if (adminClient != null) {
            adminClient.close();
        }
        this.adminClient = kafkaDynamicConfig.createAdminClient(brokerAddress);
    }
    
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
    
}