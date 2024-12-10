package co.com.famisanar.kafka.topics.adapter.out.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "MESSAGE_CONTROL")
public class MessageEntity {

    @Id
    @Column(name = "MESSAGE_ID")
    private String messageId;
    @Column(name = "TOPIC_KAFKA", length = 50)
    private String topicKafka;
    @Column(name = "MESSAGE", length = 250)
    private String message;
    @Column(name = "STATUS", length = 50)
    private String status;
    @Column(name = "ATTEMPTS", length = 5)
    private Integer attempts;
    @Column(name = "APPLICATION", length = 50)
    private String application;
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CREATED_DATE")
    private Date createDate;

    @PrePersist
    protected void onCreate() {
        createDate = new Date();
    }

}
