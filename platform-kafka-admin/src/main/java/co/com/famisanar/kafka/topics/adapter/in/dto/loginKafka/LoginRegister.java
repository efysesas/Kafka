package co.com.famisanar.kafka.topics.adapter.in.dto.loginKafka;

import lombok.Data;

@Data
public class LoginRegister {
	private String nombre;
	private String apellido;
	private String usuarioDominio;
	private String correo;
	private String area;
	private String usuarioKafka;
	private String passwordKafka;
	private int active;
}
