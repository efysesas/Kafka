package co.com.famisanar.kafka.topics.adapter.out.persistence.interfaces;

import org.springframework.data.jpa.repository.JpaRepository;

import co.com.famisanar.kafka.topics.adapter.out.entity.LogsRelaunch;

public interface IMessageRelaunch extends JpaRepository<LogsRelaunch,Long>{

}
