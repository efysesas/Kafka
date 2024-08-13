package co.com.famisanar.kafka.topics.application.ports.out;

import java.util.List;

import co.com.famisanar.kafka.topics.adapter.out.entity.MessageEntity;

public interface IAdminKafkaPersistenceAdapter {
    void saveMessageError(MessageEntity messageEntity);
    List<MessageEntity> getMessageError();
}
