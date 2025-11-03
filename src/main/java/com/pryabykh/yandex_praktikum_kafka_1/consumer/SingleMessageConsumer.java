package com.pryabykh.yandex_praktikum_kafka_1.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class SingleMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

    @Async
    public void startConsuming(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                log.info(
                        "Получено сообщение в SingleMessageConsumer: value = {}, offset = {}",
                        record.value(),
                        record.offset()
                );
            }
        }
    }
}
