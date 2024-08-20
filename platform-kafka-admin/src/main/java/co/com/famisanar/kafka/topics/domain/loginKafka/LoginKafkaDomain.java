package co.com.famisanar.kafka.topics.domain.loginKafka;

import lombok.Data;

@Data
public class LoginKafkaDomain {
		private String nombre;
		private String apellido;
		private String usuarioDominio;
		private String correo;
		private String area;
		private String usuarioKafka;
		private String passwordKafka;
		private int active;
}
