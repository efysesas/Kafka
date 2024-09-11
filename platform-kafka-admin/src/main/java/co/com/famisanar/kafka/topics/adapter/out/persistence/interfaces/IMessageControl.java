package co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces;

import org.springframework.data.jpa.repository.JpaRepository;

import co.com.famisanar.kafka.topics.adapter.out.entity.MessageEntity;

import java.util.List;

public interface IMessageControl extends JpaRepository <MessageEntity, String> {

    List<MessageEntity> findByStatusAndApplication(String status, String application);

}
