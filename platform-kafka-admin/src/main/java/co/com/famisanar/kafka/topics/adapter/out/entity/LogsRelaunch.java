package co.com.famisanar.kafka.topics.adapter.out.entity;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Entity
@Data
@Table(name = "LOG_MESSAGE_RELAUNCH")
public class LogsRelaunch {
	
	@Column(name = "NAME_TOPIC", length = 60)
	private String topic;
	@Column(name = "PARTITION", length = 2)
	private int partition;
	@Column(name = "NEW_MESSAGE", length = 200)
	private String newMessage;
	@Id
	@Column(name = "ID_NEW_MESSAGE", length = 200)
	private String idNewMessage;
	@Column(name = "PREVIOUS_MESSAGE", length = 200)
	private String previousMessage;
	@Column(name = "DOMAIN_MODIFIER", length = 50)
	private String userDomain;
	@Column(name = "MODIFICATION_DATE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date modificationDate;
	
}
