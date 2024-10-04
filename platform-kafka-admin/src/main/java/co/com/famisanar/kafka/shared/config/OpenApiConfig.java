package co.com.famisanar.kafka.shared.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;

import org.springdoc.core.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@OpenAPIDefinition(info = @Info(title = "API de Administracion Kafka FAMISANAR EPS", 
		description = "Administrar Cluster Kafka", version = "v1"), servers = {
        @Server(url = "${server.servlet.context-path}", description = "Generate server url"),
})

@Configuration
public class OpenApiConfig {
	
	@Bean
	GroupedOpenApi api() {
		return GroupedOpenApi.builder().group("api").pathsToMatch("/**").build();
	}
	
}
