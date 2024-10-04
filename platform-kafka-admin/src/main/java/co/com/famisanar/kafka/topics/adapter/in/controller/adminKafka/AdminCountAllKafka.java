package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaCountAll;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@CustomRestController
@RequestMapping("/kafka")
public class AdminCountAllKafka {

	@Autowired
	IKafkaCountAll iKafkaCountAll;
	
	@GetMapping("/topic-count")
	@Operation(summary = "obtener conteo de los topics", description = "obtener conteo de los topics")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public int getTopicCount() throws Exception {
        return iKafkaCountAll.getTopicCount();
    }
	
	@GetMapping("/partition-count")
	@Operation(summary = "obtener conteo de las particiones", description = "obtener conteo de las particiones")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public int getTotalPartitionCount() throws Exception {
        return iKafkaCountAll.getTotalPartitionCount();
    }
	
	@GetMapping("/consumer-count")
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
    public int getTotalConsumerCount() throws Exception {
        return iKafkaCountAll.getTotalConsumerCount();
    }
	
	@GetMapping("/message-count")
	@Operation(summary = "obtener conteo de los mensajes", description = "obtener conteo de los mensajes")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public int getTotalMessageCount() throws Exception {
        return iKafkaCountAll.getTotalMessageCount();
    }
	
}
