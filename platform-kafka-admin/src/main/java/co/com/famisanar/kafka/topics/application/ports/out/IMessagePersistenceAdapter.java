package co.com.famisanar.kafka.topics.application.ports.out;

public interface IMessagePersistenceAdapter {

     void sendMessage(String message, String idMessage, String topic);

}
