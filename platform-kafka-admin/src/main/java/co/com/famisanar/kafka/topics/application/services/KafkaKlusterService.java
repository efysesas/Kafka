package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

@Service
public class KafkaKlusterService {
	
	private AdminClient adminClient;
	
	public Map<String, Object> getBrokerDetails(int brokerId) throws ExecutionException, InterruptedException {
        Map<String, Object> brokerDetails = new HashMap<>();

        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        List<Node> nodes = new ArrayList<>(describeClusterResult.nodes().get());
        Node brokerNode = nodes.stream().filter(node -> node.id() == brokerId).findFirst().orElse(null);

        if (brokerNode != null) {
            brokerDetails.put("id", brokerNode.id());
            brokerDetails.put("host", brokerNode.host());
            brokerDetails.put("port", brokerNode.port());
            brokerDetails.put("rack", brokerNode.rack());

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
            Config config = adminClient.describeConfigs(List.of(configResource)).all().get().get(configResource);
            Map<String, String> configs = new HashMap<>();
            for (ConfigEntry entry : config.entries()) {
                configs.put(entry.name(), entry.value());
            }
            brokerDetails.put("configurations", configs);
        }

        return brokerDetails;
    }
	
	
	public List<Node> getBrokers() throws ExecutionException, InterruptedException {
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        return (List<Node>) clusterResult.nodes().get();
    }
	
}
