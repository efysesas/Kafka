package co.com.famisanar.kafka.topics.application.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.com.famisanar.kafka.shared.annotations.UseCase;
import co.com.famisanar.kafka.topics.adapter.out.exceptions.RespuestaHttpHandler;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaConsumers;

@UseCase
public class KafkaConsumersService implements IKafkaConsumers{

	@Autowired
	private KafkaBrokerChange kafkaBrokerChange;
	
	@Autowired
	RespuestaHttpHandler respuestaHttpHandler;

	public ResponseEntity<Object> listConsumerGroups() throws ExecutionException, InterruptedException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
		ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
		List<ConsumerGroupListing> groupListings = new ArrayList<>(groupsResult.all().get());
		List<String> groupIds = new ArrayList<>();
		for (ConsumerGroupListing groupListing : groupListings) {
			groupIds.add(groupListing.groupId());
		}
		return ResponseEntity.status(HttpStatus.OK)
				.body(groupIds);
	}

	public ResponseEntity<Object> countConsumerGroups() throws ExecutionException, InterruptedException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
		ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
		List<ConsumerGroupListing> groupListings = (List<ConsumerGroupListing>) groupsResult.all().get();
		return ResponseEntity.status(HttpStatus.OK)
				.body(groupListings.size());
	}

	public ResponseEntity<Object> searchConsumerGroups(String searchTerm) throws ExecutionException, InterruptedException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
		ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();

		List<ConsumerGroupListing> groupListings = (List<ConsumerGroupListing>) groupsResult.all().get();
		Set<String> consumerGroupIds = groupListings.stream()// Filtrar por el término de búsqueda
				.filter(groupListing -> groupListing.groupId().contains(searchTerm)).map(ConsumerGroupListing::groupId)
				.collect(Collectors.toSet());

		Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient// Obtener la descripción de los grupos de consumidores
				.describeConsumerGroups(consumerGroupIds).all().get();

		List<Map<String, Object>> consumerGroupDetailsList = consumerGroupDescriptions.entrySet().stream()
				.map(entry -> {
					Map<String, Object> details = new HashMap<>();// Mapear detalles de los grupos de consumidores a una lista
					ConsumerGroupDescription description = entry.getValue();

					List<String> consumerIds = description.members().stream().map(member -> member.consumerId())																			// consumidor
							.collect(Collectors.toList());
					details.put("consumerIds", consumerIds);

					Set<String> topics = description.members().stream()// Obtener los tópicos asociados
							.flatMap(member -> member.assignment().topicPartitions().stream())
							.map(TopicPartition::topic).collect(Collectors.toSet());
					details.put("topics", topics);

					details.put("active", !description.members().isEmpty());// Verificar si el grupo de consumidores está activo

					details.put("threadCount", description.members().size());// Obtener la cantidad de miembros en el grupo

					details.put("name", entry.getKey());// Agregar el nombre del grupo de consumidores

					return details;
				}).collect(Collectors.toList());

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return ResponseEntity.status(HttpStatus.OK)
				.body(gson.toJson(consumerGroupDetailsList));
	}

	public ResponseEntity<Object> getConsumersAndTopics() throws InterruptedException, ExecutionException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
		ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
		Set<String> consumerGroups = consumerGroupsResult.all().get().stream().map(ConsumerGroupListing::groupId)
				.collect(Collectors.toSet());

		Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient
				.describeConsumerGroups(consumerGroups).all().get();// Obtener la descripción de los grupos de consumidores

		List<Map<String, Object>> consumerDetailsList = consumerGroupDescriptions.entrySet().stream().map(entry -> {
			Map<String, Object> details = new HashMap<>();// Mapear consumidores a topics con conteo
			ConsumerGroupDescription description = entry.getValue();

			long topicCount = description.members().stream()// Contar los topics únicos asignados a cada consumidor
					.flatMap(member -> member.assignment().topicPartitions().stream()).map(TopicPartition::topic)
					.distinct().count();

			details.put("name", entry.getKey());
			details.put("topicCount", (int) topicCount);
			details.put("active", !description.members().isEmpty());
			details.put("memberCount", description.members().size());// Obtener la cantidad de miembros en el grupo

			return details;
		}).collect(Collectors.toList());

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return ResponseEntity.status(HttpStatus.OK)
				.body(gson.toJson(consumerDetailsList));
	}

	public ResponseEntity<Object> getTopicsByConsumer(String consumerGroupId)
			throws InterruptedException, ExecutionException {
		if (respuestaHttpHandler.validateAdminClient() != null) {
			return ResponseEntity.status(HttpStatus.OK)
					.body(respuestaHttpHandler.validateAdminClient());
	    }
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
		DescribeConsumerGroupsResult describeGroupsResult = adminClient
				.describeConsumerGroups(Collections.singletonList(consumerGroupId));
		Map<String, ConsumerGroupDescription> consumerGroupDescriptions = describeGroupsResult.all().get();

		ConsumerGroupDescription description = consumerGroupDescriptions.get(consumerGroupId);
		if (description == null) {
			throw new NoSuchElementException("Consumer group not found: " + consumerGroupId);
		}

		List<Map<String, Object>> topicsInfo = new ArrayList<>();// Mapear información de los tópicos

		for (String topic : description.members().stream()// Obtener información de los tópicos
				.flatMap(member -> member.assignment().topicPartitions().stream()).map(TopicPartition::topic).distinct()
				.collect(Collectors.toList())) {

			@SuppressWarnings("deprecation")// Obtener información sobre el tópico
			TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topic)).all().get()
					.get(topic);
			if (topicDescription == null)
				continue;

			int totalPartitions = topicDescription.partitions().size();// Obtener total de particiones y total de mensajes
			long totalMessages = getTotalMessagesForTopic(topic,adminClient); // Método que debes implementar para contar mensajes

			Map<String, Object> topicDetails = new HashMap<>();// Construir el objeto del tópico
			topicDetails.put("totalPartitions", totalPartitions);
			topicDetails.put("totalMessages", totalMessages);
			topicDetails.put("topicName", topic);

			List<Map<String, Object>> consumers = new ArrayList<>();// Agregar información de consumidores
			for (ConsumerGroupDescription groupDescription : consumerGroupDescriptions.values()) {
				if (groupDescription.members().stream().anyMatch(member -> member.assignment().topicPartitions()
						.stream().anyMatch(tp -> tp.topic().equals(topic)))) {
					Map<String, Object> consumerInfo = new HashMap<>();
					consumerInfo.put("threadCount", groupDescription.members().size());
					consumerInfo.put("consumerGroup", groupDescription.groupId());
					consumers.add(consumerInfo);
				}
			}

			topicDetails.put("consumers", consumers);
			topicsInfo.add(topicDetails);
		}

		return ResponseEntity.status(HttpStatus.OK)
				.body(topicsInfo);
	}

	private long getTotalMessagesForTopic(String topic,AdminClient adminClient) throws InterruptedException, ExecutionException {
		@SuppressWarnings("deprecation")
		List<TopicPartitionInfo> partitionInfos = adminClient.describeTopics(Collections.singletonList(topic)).all()
				.get().get(topic).partitions();

		long totalMessages = 0;// Obtener el total de mensajes en el tópico
		for (TopicPartitionInfo partitionInfo : partitionInfos) {
			int partitionId = partitionInfo.partition();
			TopicPartition topicPartition = new TopicPartition(topic, partitionId);

			
			long endOffset = adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest()))
					.all().get().get(topicPartition).offset();// Obtener el último offset
			totalMessages += endOffset; // Contar desde el inicio hasta el último offset
		}

		return totalMessages;
	}
	
}
