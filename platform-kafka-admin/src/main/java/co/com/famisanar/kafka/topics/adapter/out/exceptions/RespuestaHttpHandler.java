package co.com.famisanar.kafka.topics.adapter.out.exceptions;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@Component
public class RespuestaHttpHandler {
	
	private static final Gson gson = new Gson();

	public String respuestaPeticiones(HttpStatus codigo, String descripcion, String estado, List<String> errorValid) {
		JsonObject respuesta = new JsonObject();
		respuesta.addProperty("estate", estado);
		respuesta.addProperty("code", codigo.toString());
		respuesta.addProperty("message", descripcion);

		if (errorValid != null && !errorValid.isEmpty()) {
			JsonArray errores = new JsonArray();
			for (String error : errorValid) {
				errores.add(error);
			}
			respuesta.add("validationsError", errores);
		}

		return gson.toJson(respuesta);
	}
	
}
