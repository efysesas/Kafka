package co.com.famisanar.kafka.topics.application.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import co.com.famisanar.kafka.topics.adapter.out.exceptions.RespuestaHttpHandler;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaPartitions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaPartitionsService implements IKafkaPartitions{

	@Autowired
    private KafkaBrokerChange kafkaBrokerChange;
	
	@Autowired
	RespuestaHttpHandler respuestaHttpHandler;
	
    /**
     * Obtiene los detalles de las particiones de un tópico de Kafka.
     *
     * @param topic Nombre del tópico
     * @return Mapa con los detalles de las particiones
     * @throws ExecutionException   Si ocurre un error durante la ejecución
     * @throws InterruptedException Si el hilo es interrumpido
     */
    public ResponseEntity<Object> getPartitionDetails(String topic) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
    	// Describe el tópico y obtener su descripción
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        @SuppressWarnings("deprecation")
        TopicDescription topicDescription = topicsResult.all().get().get(topic);

        if (topicDescription == null) {
            throw new IllegalArgumentException("Tópico no encontrado: " + topic);
        }

        List<Map<String, Object>> partitionDetailsList = new ArrayList<>();

        // Iterar sobre las particiones del tópico
        for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
            int partition = partitionInfo.partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // Crear un mapa para almacenar la información de la partición
            Map<String, Object> partitionInfoMap = new HashMap<>();

            // Obtener los offsets de inicio y fin de la partición
            Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
            ListOffsetsResultInfo offsetSpec = offsets.get(topicPartition);
            long lastOffset = offsetSpec.offset();

            offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get();
            offsetSpec = offsets.get(topicPartition);
            long firstOffset = offsetSpec.offset();

            long size = lastOffset - firstOffset;

            partitionInfoMap.put("partitionName", "partition-" + partition);
            partitionInfoMap.put("topicName", topic);
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

            partitionInfoMap.put("leaderNode", leaderNode.id());
            partitionInfoMap.put("replicaNodes", replicaNodeIds);
            partitionInfoMap.put("inSyncReplicaNodes", inSyncReplicaNodeIds);
            partitionInfoMap.put("offlineReplicaNodes", offlineReplicaNodeIds);

            // Agregar la información de la partición a la lista de detalles
            partitionDetailsList.add(partitionInfoMap);
        }

        return ResponseEntity.status(HttpStatus.OK)
				.body(partitionDetailsList);
    }
    
    public ResponseEntity<Object> getPartitionCount(String topic) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
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

        return ResponseEntity.status(HttpStatus.OK)
				.body(countPartition);
    }
    
    /**
     * Obtiene los detalles de las particiones de un tópico de Kafka.
     *
     * @param topic Nombre del tópico
     * @return Mapa con los detalles de las particiones
     * @throws ExecutionException   Si ocurre un error durante la ejecución
     * @throws InterruptedException Si el hilo es interrumpido
     */
    public ResponseEntity<Object> getPartitionSearch(String topic, int partition) throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
    	// Describe el tópico y obtener su descripción
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        @SuppressWarnings("deprecation")
        TopicDescription topicDescription = topicsResult.all().get().get(topic);

        if (topicDescription == null) {
            throw new IllegalArgumentException("Tópico no encontrado: " + topic);
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        // Obtener los offsets de inicio y fin de la partición
        Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
        ListOffsetsResultInfo offsetSpec = offsets.get(topicPartition);
        long lastOffset = offsetSpec.offset();

        offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get();
        offsetSpec = offsets.get(topicPartition);
        long firstOffset = offsetSpec.offset();

        long size = lastOffset - firstOffset;

        // Crear un mapa para contener toda la información de la partición
        Map<String, Object> partitionInfoMap = new HashMap<>();
        partitionInfoMap.put("partitionName", "partition-" + partition);
        partitionInfoMap.put("topicName", topic);
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

        return ResponseEntity.status(HttpStatus.OK)
				.body(partitionInfoMap);
    }
    
    public ResponseEntity<Object> getAllPartitionDetails() throws ExecutionException, InterruptedException {
    	if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
    	AdminClient adminClient = kafkaBrokerChange.adminClient;
    	List<Map<String, Object>> allPartitionDetails = new ArrayList<>();
        
        try {
            // Obtener la lista de todos los tópicos
            DescribeTopicsResult topicsResult = adminClient.describeTopics(adminClient.listTopics().names().get());
            @SuppressWarnings("deprecation")
            Map<String, TopicDescription> topicDescriptions = topicsResult.all().get();

            for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
                String topic = entry.getKey();
                TopicDescription topicDescription = entry.getValue();

                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    int partition = partitionInfo.partition();
                    TopicPartition topicPartition = new TopicPartition(topic, partition);

                    Map<String, Object> partitionInfoMap = new HashMap<>();

                    Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
                    ListOffsetsResultInfo offsetSpec = offsets.get(topicPartition);
                    long lastOffset = offsetSpec.offset();

                    offsets = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.earliest())).all().get();
                    offsetSpec = offsets.get(topicPartition);
                    long firstOffset = offsetSpec.offset();
                    long size = lastOffset - firstOffset;

                    List<Node> replicaNodes = partitionInfo.replicas();
                    List<Node> inSyncReplicaNodes = partitionInfo.isr();

                    List<Integer> replicaNodeIds = replicaNodes.stream().map(Node::id).collect(Collectors.toList());
                    List<Integer> inSyncReplicaNodeIds = inSyncReplicaNodes.stream().map(Node::id).collect(Collectors.toList());
                    List<Integer> offlineReplicaNodeIds = new ArrayList<>(replicaNodeIds);
                    offlineReplicaNodeIds.removeAll(inSyncReplicaNodeIds);

                    Node leaderNode = partitionInfo.leader();

                    partitionInfoMap.put("partitionName", "partition-" + partition);
                    partitionInfoMap.put("topicName", topic);
                    partitionInfoMap.put("firstOffset", firstOffset);
                    partitionInfoMap.put("lastOffset", lastOffset);
                    partitionInfoMap.put("size", size);
                    partitionInfoMap.put("leaderNode", leaderNode.id());
                    partitionInfoMap.put("replicaNodes", replicaNodeIds);
                    partitionInfoMap.put("inSyncReplicaNodes", inSyncReplicaNodeIds);
                    partitionInfoMap.put("offlineReplicaNodes", offlineReplicaNodeIds);

                    allPartitionDetails.add(partitionInfoMap);
                }
            }
        } catch (TimeoutException e) {
            System.err.println("Error: El nodo Kafka no está disponible. Verifique la conexión al broker.");
        } catch (ExecutionException e) {
            System.err.println("Error: No se pudo ejecutar la consulta a Kafka. Verifique la configuración del cliente Kafka.");
        } catch (Exception e) {
            System.err.println("Error inesperado: " + e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.OK)
				.body(allPartitionDetails);
    }
}
