package co.com.famisanar.kafka.shared.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaDynamicConfig {

    public AdminClient createAdminClient(String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        config.put(AdminClientConfig.RETRIES_CONFIG, 0);
        return AdminClient.create(config);
    }
    
}