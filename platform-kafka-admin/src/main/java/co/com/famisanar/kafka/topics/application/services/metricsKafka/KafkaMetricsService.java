package co.com.famisanar.kafka.topics.application.services.metricsKafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.application.ports.in.IKafkaMetrics;
import co.com.famisanar.kafka.topics.application.services.KafkaBrokerChange;

@Service
public class KafkaMetricsService implements IKafkaMetrics{

	private final KafkaBrokerChange kafkaBrokerChange;

    public KafkaMetricsService(KafkaBrokerChange kafkaBrokerChange) {
        this.kafkaBrokerChange = kafkaBrokerChange;
    }

    public Map<String, String> getKafkaMetrics() {
        AdminClient adminClient = kafkaBrokerChange.adminClient;
        if (adminClient == null) {
            throw new IllegalStateException("No hay conexión activa al broker Kafka");
        }

        // Obtención de métricas (ejemplo con métricas del AdminClient)
        Map<MetricName, ? extends Metric> metrics = adminClient.metrics();
        Map<String, String> formattedMetrics = new HashMap<>();
        for (MetricName metricName : metrics.keySet()) {
            formattedMetrics.put(metricName.name(), metrics.get(metricName).metricValue().toString());
        }

        return formattedMetrics;
    }
    
}
