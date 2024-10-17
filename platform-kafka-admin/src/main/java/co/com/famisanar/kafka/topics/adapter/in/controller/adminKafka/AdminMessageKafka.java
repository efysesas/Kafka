package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage.LogMessageChange;
import co.com.famisanar.kafka.topics.adapter.in.dto.kafkaAdmin.SendMessage;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaRelaunchMessage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@CustomRestController
@RequestMapping("/kafka")
public class AdminMessageKafka {

	@Autowired
    private IKafkaRelaunchMessage iKafkaRelaunchMessage;
	
	@GetMapping("/topics/{topic}/partitions/{partition}/messages")
	@Operation(summary = "Lista mensajes por Topic y Particion", description = "Lista mensajes por Topic y Particion")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public List<Map<String, Object>> getMessages(
    		@PathVariable String topic,
            @PathVariable int partition,
            @RequestParam int offset,
            @RequestParam int limit){
        return iKafkaRelaunchMessage.getMessages(topic, partition, offset, limit);
    }
    
    @GetMapping("/topics/{topic}/partitions/{partition}/messagesByDate")
    @Operation(summary = "Lista mensajes por Topic y Particion y rango de fecha", description = "Lista mensajes por Topic y Particion y rango de fecha")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public List<Map<String, Object>> getMessagesByDateRange(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Instant start = Instant.parse(startTime);
        Instant end = Instant.parse(endTime);
        List<ConsumerRecord<String, String>> records = iKafkaRelaunchMessage.getMessagesByDateRange(topic, partition, start, end);
        return records.stream().map(record -> {
            Map<String, Object> message = new HashMap<>();
            message.put("offset", record.offset());
            message.put("key", record.key());
            message.put("value", record.value());
            message.put("partition", record.partition());
            message.put("timestamp", record.timestamp());
            return message;
        }).collect(Collectors.toList());
    }
    
    @GetMapping("/messages/search")
    @Operation(summary = "Lista mensajes por Value", description = "Lista mensajes por Value")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public List<Map<String, Object>> getMessagesByValue(@RequestBody SendMessage sendMessage) {
        List<ConsumerRecord<String, String>> messages = iKafkaRelaunchMessage.getMessagesByValue(sendMessage);
        return messages.stream().map(record -> {
            Map<String, Object> message = new HashMap<>();
            message.put("offset", record.offset());
            message.put("key", record.key());
            message.put("value", record.value());
            message.put("partition", record.partition());
            message.put("timestamp", record.timestamp());
            return message;
        }).collect(Collectors.toList());
    }
    
    @PostMapping(value = "/sendMessage")
    @Operation(summary = "lanzar mensaje a topic y particion especifica", description = "lanzar mensaje a topic y particion especifica")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<String> sendMessage(@RequestBody SendMessage sendMessage) {
        return iKafkaRelaunchMessage.send(sendMessage) ?  ResponseEntity.ok("Mensaje enviado ") :  ResponseEntity.internalServerError().body("No se pudo enviar el mensaje");
    }
    
    @PostMapping(value = "/logChangeMessage")
    @Operation(summary = "Relanzamiento de mensaje y almacenamiento de informacion log", description = "Relanzamiento de mensaje y almacenamiento de informacion log")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<String> logChangeMessage(@RequestBody LogMessageChange logMessageChange) {
        return iKafkaRelaunchMessage.reSend(logMessageChange) ?  ResponseEntity.ok("Mensaje relanzado ") :  ResponseEntity.internalServerError().body("No se pudo enviar el mensaje");
    }
    
    @GetMapping("/search/{id}")
    @Operation(summary = "Busqueda de mensajes por Id", description = "Busqueda de mensajes por Id")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<List<ConsumerRecord<String, String>>> searchMessagesById(@PathVariable("id") String messageId) {
        List<ConsumerRecord<String, String>> messages = iKafkaRelaunchMessage.searchMessagesById(messageId);
        if (messages.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(messages);
    }
    
    @GetMapping("/topics/{topic}/messages")
	@Operation(summary = "Lista mensajes por Topic", description = "Lista mensajes por Topic")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public List<Map<String, Object>> getMessagesTopic(
    		@PathVariable String topic,
            @RequestParam int offset,
            @RequestParam int limit){
        return iKafkaRelaunchMessage.getMessagesFromTopic(topic, offset, limit);
    }
    
}
