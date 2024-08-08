package co.com.famisanar.kafka.topics.application.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaPartitionsService {

    private AdminClient adminClient;

    public KafkaPartitionsService(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    /**
     * Obtiene los detalles de las particiones de un tópico de Kafka.
     *
     * @param topic Nombre del tópico
     * @return Mapa con los detalles de las particiones
     * @throws ExecutionException   Si ocurre un error durante la ejecución
     * @throws InterruptedException Si el hilo es interrumpido
     */
    public Map<String, Object> getPartitionDetails(String topic) throws ExecutionException, InterruptedException {
        // Describe el tópico y obtener su descripción
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        @SuppressWarnings("deprecation")
		TopicDescription topicDescription = topicsResult.all().get().get(topic);

        if (topicDescription == null) {
            throw new IllegalArgumentException("Tópico no encontrado: " + topic);
        }

        Map<String, Object> partitionDetails = new HashMap<>();

        
        for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
            int partition = partitionInfo.partition();// Iterar sobre las particiones del tópico
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            
            Map<String, Object> partitionInfoMap = new HashMap<>();// Crear un mapa para almacenar la información de la partición

            
            Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
            ListOffsetsResultInfo offsetSpec = offsets.get(topicPartition);// Obtener los offsets de inicio y fin de la partición

            long lastOffset = offsetSpec.offset();

            offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get();
            offsetSpec = offsets.get(topicPartition);
            long firstOffset = offsetSpec.offset();

            long size = lastOffset - firstOffset;

            partitionInfoMap.put("firstOffset", firstOffset);
            partitionInfoMap.put("lastOffset", lastOffset);
            partitionInfoMap.put("size", size);

            // Obtener los nodos réplica y réplicas en sincronía
            List<Node> replicaNodes = partitionInfo.replicas();
            List<Node> inSyncReplicaNodes = partitionInfo.isr();

            List<Integer> replicaNodeIds = replicaNodes.stream().map(Node::id).collect(Collectors.toList());
            List<Integer> inSyncReplicaNodeIds = inSyncReplicaNodes.stream().map(Node::id).collect(Collectors.toList());

            // Calcular los nodos réplica fuera de línea
            List<Integer> offlineReplicaNodeIds = new ArrayList<>(replicaNodeIds);
            offlineReplicaNodeIds.removeAll(inSyncReplicaNodeIds);

            // Obtener el líder de la partición
            Node leaderNode = partitionInfo.leader();

            partitionInfoMap.put("firstOffset", firstOffset);
            partitionInfoMap.put("lastOffset", lastOffset);
            partitionInfoMap.put("size", size);
            partitionInfoMap.put("leaderNode", leaderNode.id());
            partitionInfoMap.put("replicaNodes", replicaNodeIds);
            partitionInfoMap.put("inSyncReplicaNodes", inSyncReplicaNodeIds);
            partitionInfoMap.put("offlineReplicaNodes", offlineReplicaNodeIds);

            // Agregar la información de la partición al mapa de detalles
            partitionDetails.put("partition-" + partition, partitionInfoMap);
        }

        return partitionDetails;
    }
    
    public int getPartitionCount(String topic) throws ExecutionException, InterruptedException {
    	int countPartition = 0;
        // Describe el tópico y obtener su descripción
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        @SuppressWarnings("deprecation")
		TopicDescription topicDescription = topicsResult.all().get().get(topic);

        if (topicDescription == null) {
            throw new IllegalArgumentException("Tópico no encontrado: " + topic);
        }

        TopicPartitionInfo[] partitionInfos = topicDescription.partitions().toArray(new TopicPartitionInfo[0]);
        for (int i = 0; i < partitionInfos.length; i++) {
            countPartition++;
        }

        return countPartition;
    }
    
    /**
     * Obtiene los detalles de las particiones de un tópico de Kafka.
     *
     * @param topic Nombre del tópico
     * @return Mapa con los detalles de las particiones
     * @throws ExecutionException   Si ocurre un error durante la ejecución
     * @throws InterruptedException Si el hilo es interrumpido
     */
    public Map<String, Object> getPartitionSearch(String topic, int partition) throws ExecutionException, InterruptedException {
        // Describe el tópico y obtener su descripción
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        @SuppressWarnings("deprecation")
        TopicDescription topicDescription = topicsResult.all().get().get(topic);

        if (topicDescription == null) {
            throw new IllegalArgumentException("Tópico no encontrado: " + topic);
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        Map<String, Object> partitionDetails = new HashMap<>();

        // Obtener los offsets de inicio y fin de la partición
        Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
        ListOffsetsResultInfo offsetSpec = offsets.get(topicPartition);
        long lastOffset = offsetSpec.offset();

        offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get();
        offsetSpec = offsets.get(topicPartition);
        long firstOffset = offsetSpec.offset();

        long size = lastOffset - firstOffset;

        Map<String, Object> partitionInfoMap = new HashMap<>();
        partitionInfoMap.put("firstOffset", firstOffset);
        partitionInfoMap.put("lastOffset", lastOffset);
        partitionInfoMap.put("size", size);

        // Obtener los nodos réplica y réplicas en sincronía
        List<Node> replicaNodes = topicDescription.partitions().get(partition).replicas();
        List<Node> inSyncReplicaNodes = topicDescription.partitions().get(partition).isr();

        List<Integer> replicaNodeIds = replicaNodes.stream().map(Node::id).collect(Collectors.toList());
        List<Integer> inSyncReplicaNodeIds = inSyncReplicaNodes.stream().map(Node::id).collect(Collectors.toList());

        // Calcular los nodos réplica fuera de línea
        List<Integer> offlineReplicaNodeIds = new ArrayList<>(replicaNodeIds);
        offlineReplicaNodeIds.removeAll(inSyncReplicaNodeIds);

        // Obtener el líder de la partición
        Node leaderNode = topicDescription.partitions().get(partition).leader();

        partitionInfoMap.put("leaderNode", leaderNode.id());
        partitionInfoMap.put("replicaNodes", replicaNodeIds);
        partitionInfoMap.put("inSyncReplicaNodes", inSyncReplicaNodeIds);
        partitionInfoMap.put("offlineReplicaNodes", offlineReplicaNodeIds);

        // Agregar la información de la partición al mapa de detalles
        partitionDetails.put("partition-" + partition, partitionInfoMap);

        return partitionDetails;
    }
    
}
