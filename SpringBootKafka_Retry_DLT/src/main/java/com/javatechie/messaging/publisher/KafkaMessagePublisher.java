package com.javatechie.messaging.publisher;

import com.javatechie.dto.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topicName;



    public void sendEvents(User user) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, user);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent message=[ {} ] with offset=[ {} ]",user.toString(),result.getRecordMetadata().offset());
                } else {
                    log.error("Unable to send message=[ {} ] due to : {} ", user.toString(),ex.getMessage());
                }
            });
        } catch (Exception ex) {
            log.error(String.format("Exception Details : %s", String.valueOf(ex.getMessage())));
        }
    }


}