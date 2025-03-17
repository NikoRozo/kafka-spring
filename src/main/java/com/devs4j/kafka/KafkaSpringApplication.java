package com.devs4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
public class KafkaSpringApplication implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(KafkaSpringApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "devs4j-topic", groupId = "devs4j-group")
    public void listen(String message){
        log.info("Message received {} ", message);
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        kafkaTemplate.send("devs4j-topic", "Sample message").get(100, TimeUnit.MILLISECONDS);
        /*
        // Entrega del mensaje asincrona
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample message");
        future.whenComplete((result, e) -> {
            if (e == null){
                log.info("Message sent successfully: {}", result.getRecordMetadata().offset());
            } else {
                log.error("Error sending message", e);
            }
        });*/
    }
}
