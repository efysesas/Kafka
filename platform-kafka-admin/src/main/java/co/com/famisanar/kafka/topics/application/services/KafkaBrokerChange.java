package co.com.famisanar.kafka.topics.application.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.shared.config.KafkaDynamicConfig;

@Service
public class KafkaBrokerChange {

    private final KafkaDynamicConfig kafkaDynamicConfig;
    AdminClient adminClient;

    public KafkaBrokerChange(KafkaDynamicConfig kafkaDynamicConfig) {
        this.kafkaDynamicConfig = kafkaDynamicConfig;
    }

    public void connectToBroker(String brokerAddress) {
        if (adminClient != null) {
            // Cerrar la conexión anterior antes de crear una nueva
            adminClient.close();
        }
        this.adminClient = kafkaDynamicConfig.createAdminClient(brokerAddress);
    }

    // Considera agregar un método para cerrar la conexión cuando se desecha el servicio
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}