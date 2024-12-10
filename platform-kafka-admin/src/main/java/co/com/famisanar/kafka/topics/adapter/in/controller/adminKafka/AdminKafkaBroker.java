package co.com.famisanar.kafka.topics.adapter.in.controller.adminKafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaChangeKafka;
import co.com.famisanar.kafka.topics.application.services.KafkaBrokerChange;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@CustomRestController
@RequestMapping("/kafka")
public class AdminKafkaBroker {
		
	@Autowired
	IKafkaChangeKafka iKafkaChangeKafka;
	
	@Autowired
	KafkaBrokerChange kafkaBrokerChange;
	
	@GetMapping("/connect/{broker}")
	@Operation(summary = "Conexion a broker de kafka", description = "Conexion a broker de kafka")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "400", description = "Solicitud inválida", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "404", description = "Recurso no encontrado", content = {
					@Content(mediaType = "application/json") }),
			@ApiResponse(responseCode = "500", description = "Error interno del servidor", content = {
					@Content(mediaType = "application/json") }) })
    public String connectToBroker(@PathVariable String broker) {
        return iKafkaChangeKafka.changeKafkaConnect(broker);
    }
	
}
