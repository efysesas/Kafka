package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaConsumers;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.Content;

@CustomRestController
@RequestMapping("/kafka")
public class AdminConsumersKafka {

	@Autowired
    private IKafkaConsumers iKafkaConsumers;
	
    @GetMapping("/consumers")
    @Operation(summary = "obtener informacion de los consumidores", description = "obtener informacion de los consumidores")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getConsumersAndTopics() throws InterruptedException, ExecutionException {
        return iKafkaConsumers.getConsumersAndTopics();
    }
    
    @GetMapping("/consumers/count")
    @Operation(summary = "obtener conteo de los consumidores", description = "obtener conteo de los consumidores")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> countConsumerGroups() throws ExecutionException, InterruptedException {
        return iKafkaConsumers.countConsumerGroups();
    }
    
    @GetMapping("/consumers/search")
    @Operation(summary = "busqueda de los consumidores", description = "busqueda de los consumidores")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> searchConsumerGroups(@RequestParam String consumer)throws ExecutionException, InterruptedException  {
        return iKafkaConsumers.searchConsumerGroups(consumer);
    }
    
    @GetMapping("/topicsByConsumer/{consumerGroupId}")
    @Operation(summary = "busqueda topics por consumidores", description = "busqueda topics por consumidores")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getTopicsByConsumer(@PathVariable String consumerGroupId) throws InterruptedException, ExecutionException {
        return iKafkaConsumers.getTopicsByConsumer(consumerGroupId);
    }
    
}
