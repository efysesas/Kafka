package co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import co.com.famisanar.kafka.topics.adapter.out.entity.LoginEntity;

public interface ILogin extends JpaRepository<LoginEntity,Long>{
	Optional<LoginEntity> findByUserDomain(String userDomain);
}