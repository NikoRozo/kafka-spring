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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
public class KafkaSpringApplication implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(KafkaSpringApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "devs4j-topic", containerFactory = "listenerContainerFactory", groupId = "devs4j-group",
                    properties = {"max.poll.interval.ms: 4000", "max.poll.records: 10"})
    public void listen(List<String> message){
        log.info("Start reading messages");
        for (String msg : message){
            log.info("Message received {} ", msg);
        }
        log.info("End reading messages");
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        //kafkaTemplate.send("devs4j-topic", "Sample message");
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("devs4j-topic", String.format("Sample message %d", i));
        }
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
