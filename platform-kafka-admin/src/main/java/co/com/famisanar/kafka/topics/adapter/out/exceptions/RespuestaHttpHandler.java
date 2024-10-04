package co.com.famisanar.kafka.topics.adapter.out.exceptions;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import co.com.famisanar.kafka.topics.application.services.KafkaBrokerChange;

@Component
public class RespuestaHttpHandler {
	
	private static final Gson gson = new Gson();
	
	@Autowired
	private KafkaBrokerChange kafkaBrokerChange;

	public Object responseExceptions(HttpStatus codigo, String descripcion, String estado, List<String> errorValid) {
		JsonObject response = new JsonObject();
		response.addProperty("estate", estado);
		response.addProperty("code", codigo.toString());
		response.addProperty("message", descripcion);

		if (errorValid != null && !errorValid.isEmpty()) {
			JsonArray errores = new JsonArray();
			for (String error : errorValid) {
				errores.add(error);
			}
			response.add("validationsError", errores);
		}

		return gson.toJson(response);
	}
	
	public Object validateAdminClient() {
	    AdminClient adminClient = kafkaBrokerChange.adminClient;
	    if (adminClient == null) {
	    	JsonObject response = new JsonObject();
	    	response.addProperty("estate", "Error Connect");
			response.addProperty("code", "500");
			response.addProperty("message", "AdminClient is not initialized. Please connect to a broker first.");
	        return gson.toJson(response);
	    }
	    return null;
	}
	
}
