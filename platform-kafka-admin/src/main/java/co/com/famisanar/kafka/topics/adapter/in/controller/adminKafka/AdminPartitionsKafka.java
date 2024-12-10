package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaPartitions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@CustomRestController
@RequestMapping("/kafka")
public class AdminPartitionsKafka {
	
	@Autowired
	IKafkaPartitions iKafkaPartitions;
	
	@GetMapping("/partitions")
	@Operation(summary = "obtencion de particiones", description = "obtencion de particiones")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getAllPartitionDetails() throws ExecutionException, InterruptedException  {
        return iKafkaPartitions.getAllPartitionDetails();
    }
	
	@GetMapping("/topics/{topic}/partitions/search")
	@Operation(summary = "obtencion de particiones por topic", description = "obtencion de particiones por topic")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getPartitionSearch(@PathVariable String topic,@RequestParam int partition) throws ExecutionException, InterruptedException {
        return iKafkaPartitions.getPartitionSearch(topic,partition);
    }
	
	@GetMapping("/topics/{topic}/partitions/details/byTopic")
	@Operation(summary = "obtencion de particiones por topic con detalles", description = "obtencion de particiones por topic con detalles")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getPartitionDetails(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return iKafkaPartitions.getPartitionDetails(topic);
    }
	
	@GetMapping("/topics/{topic}/partitions/count/byTopic")
	@Operation(summary = "obtencion de particiones por topic con detalles", description = "obtencion de particiones por topic con detalles")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public ResponseEntity<Object> getPartitionCount(@PathVariable String topic) throws ExecutionException, InterruptedException {
        return iKafkaPartitions.getPartitionCount(topic);
    }
	
}
