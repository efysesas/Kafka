package co.com.famisanar.kafka.topics.adapter.in.controller.metricsKafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import co.com.famisanar.kafka.shared.annotations.CustomRestController;
import co.com.famisanar.kafka.topics.application.ports.in.IKafkaMetrics;

@CustomRestController
@RequestMapping("/kafka")
public class KafkaMetrics {
	
	@Autowired
	IKafkaMetrics iKafkaMetrics;

	@GetMapping("/api/metrics")
    public ResponseEntity<Object> getKafkaMetrics() {
        try {
            Map<String, String> metrics = iKafkaMetrics.getKafkaMetrics();
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
        }
    }
	
	@GetMapping(value = "/metrics/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamKafkaMetrics() {
        SseEmitter emitter = new SseEmitter();
        new Thread(() -> {
            try {
                while (true) {
                    Map<String, String> metrics = iKafkaMetrics.getKafkaMetrics();
                    emitter.send(metrics);
                    Thread.sleep(2000);
                }
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        }).start();
        return emitter;
    }
}
