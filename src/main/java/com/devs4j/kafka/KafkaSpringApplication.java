package com.devs4j.kafka;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
@EnableScheduling
public class KafkaSpringApplication /*implements CommandLineRunner*/ {
    private final Logger log = LoggerFactory.getLogger(KafkaSpringApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private MeterRegistry meterRegistry;
    @Autowired
    private BeanFactoryPostProcessor forceAutoProxyCreatorToUseClassProxying;

    @KafkaListener(id = "devs4j-id", autoStartup = "true", topics = "devs4j-topic", containerFactory = "listenerContainerFactory", groupId = "devs4j-group",
                    properties = {"max.poll.interval.ms: 4000", "max.poll.records: 50"})
    public void listen(List<ConsumerRecord<String, String>> message){
        log.info("Start reading messages");
        for (ConsumerRecord<String, String> msg : message){
            log.info("Partition: {}, OffSet: {}, Key: {}, Value: {}", msg.partition(), msg.offset(), msg.key(), msg.value());
        }
        log.info("End reading messages");
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringApplication.class, args);
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 100)
    public void sendMessageKafka(){
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void printMetrics(){
        List<Meter> meters = meterRegistry.getMeters();
        for (Meter meter : meters){
            log.info("Meter: {}", meter.getId().getName());
        }

        double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
        log.info("Count: {}", count);
    }

    /*@Override
    public void run(String... args) throws Exception {
        //kafkaTemplate.send("devs4j-topic", "Sample message");
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
        *//*
        // Entrega del mensaje asincrona
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample message");
        future.whenComplete((result, e) -> {
            if (e == null){
                log.info("Message sent successfully: {}", result.getRecordMetadata().offset());
            } else {
                log.error("Error sending message", e);
            }
        });*//*
        log.info("Waiting 5 seconds");
        Thread.sleep(5000);
        log.info("Started");
        registry.getListenerContainer("devs4j-id").start();
        Thread.sleep(5000);
        registry.getListenerContainer("devs4j-id").stop();
    }*/
}
