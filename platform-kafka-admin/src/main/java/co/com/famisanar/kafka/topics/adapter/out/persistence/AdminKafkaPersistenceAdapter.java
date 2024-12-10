package co.com.famisanar.kafka.topics.adapter.out.persistence;

import java.util.Date;
import java.util.List;

import jakarta.servlet.ServletContext;
import jakarta.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import co.com.famisanar.kafka.shared.annotations.PersistenceAdapter;
import co.com.famisanar.kafka.topics.adapter.in.dto.KafkaMessage.LogMessageChange;
import co.com.famisanar.kafka.topics.adapter.out.entity.LogsRelaunch;
import co.com.famisanar.kafka.topics.adapter.out.entity.MessageEntity;
import co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces.IMessageControl;
import co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces.IMessageRelaunch;
import co.com.famisanar.kafka.topics.application.ports.out.IAdminKafkaPersistenceAdapter;

@PersistenceAdapter
@Transactional
public class AdminKafkaPersistenceAdapter implements IAdminKafkaPersistenceAdapter {
	
    @Autowired
    private IMessageControl messageControlRepository;
    
    @Autowired
    private IMessageRelaunch iMessageRelaunch;

    @Autowired
    public ServletContext servletContext;

    public void saveMessageError(MessageEntity messageEntity) {
        messageControlRepository.save(messageEntity);
    }

    public  List<MessageEntity> getMessageError() {
        return messageControlRepository.findByStatusAndApplication("PENDING", servletContext.getContextPath()).stream().limit(5).toList();
    }
    
    public ResponseEntity<String> logMessageRelaunch(LogMessageChange logMessageChange){
    	try {
    		LogsRelaunch logsRelaunch = new LogsRelaunch();
    		logsRelaunch.setNewMessage(logMessageChange.getNewMessage());
    		logsRelaunch.setPartition(logMessageChange.getPartition());
    		logsRelaunch.setIdNewMessage(logMessageChange.getIdMessagePrevious());
    		logsRelaunch.setPreviousMessage(logMessageChange.getPreviousMessage());
    		logsRelaunch.setTopic(logMessageChange.getTopic());
    		logsRelaunch.setUserDomain(logMessageChange.getUserDomain());
    		logsRelaunch.setModificationDate(new Date());
    		iMessageRelaunch.save(logsRelaunch);
    		return ResponseEntity.status(HttpStatus.OK).body("Mensaje Guardado");
    	}catch(Exception e){
    		return ResponseEntity.status(HttpStatus.CONFLICT).body("Mensaje No Guardado");
    	}
    }

}
