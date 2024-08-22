package co.com.famisanar.kafka.topics.adapter.out.persistence;

import java.util.List;

import jakarta.servlet.ServletContext;
import jakarta.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;

import co.com.famisanar.kafka.shared.annotations.PersistenceAdapter;
import co.com.famisanar.kafka.topics.adapter.out.entity.MessageEntity;
import co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces.IMessageControlRepository;
import co.com.famisanar.kafka.topics.application.ports.out.IAdminKafkaPersistenceAdapter;

@PersistenceAdapter
@Transactional
public class AdminKafkaPersistenceAdapter implements IAdminKafkaPersistenceAdapter {
	
    @Autowired
    private IMessageControlRepository messageControlRepository;

    @Autowired
    public ServletContext servletContext;

    public void saveMessageError(MessageEntity messageEntity) {
        messageControlRepository.save(messageEntity);
    }

    public  List<MessageEntity> getMessageError() {
        return messageControlRepository.findByStatusAndApplication("PENDING", servletContext.getContextPath()).stream().limit(5).toList();
    }

}
