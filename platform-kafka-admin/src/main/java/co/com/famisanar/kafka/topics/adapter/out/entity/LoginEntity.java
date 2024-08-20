package co.com.famisanar.kafka.topics.adapter.out.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "LOGIN_CONTROL")
public class LoginEntity {
	@Id
    @Column(name = "USER_ID")
    private String identificaTion;
    @Column(name = "NAME", length = 50)
	private String name;
    @Column(name = "LASTNAME", length = 50)
	private String lastname;
    @Column(name = "USERDOMAIN", length = 50)
	private String userDomain;
    @Column(name = "EMAIL", length = 50)
	private String email;
    @Column(name = "area", length = 50)
	private String area;
    @Column(name = "USERKAFKA", length = 50)
	private String userKafka;
    @Column(name = "PASSWORDKAFKA", length = 50)
	private String passwordKafka;
    @Column(name = "ACTIVE", length = 1)
	private int active;
}
