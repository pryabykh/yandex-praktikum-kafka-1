package com.pryabykh.yandex_praktikum_kafka_1.consumer;

import com.pryabykh.yandex_praktikum_kafka_1.dto.WeatherMessageDto;
import com.pryabykh.yandex_praktikum_kafka_1.mapper.WeatherMessageHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class KafkaConsumerRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);
    @Autowired
    private WeatherMessageHelper weatherMessageHelper;

    @Async
    public <T> void startConsuming(KafkaConsumer<String, String> consumer,
                                   long pollTimeout,
                                   Class<T> source,
                                   boolean manualCommit) {
        log.info("Запуск консьюмера: {}", source.getName());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    WeatherMessageDto weatherDto = weatherMessageHelper.deserialize(record.value());
                    log.info(
                            "Получено сообщение в {}: value = {}",
                            source.getName(),
                            weatherDto.getValue()
                    );
                } catch (Exception e) {
                    log.warn("Произошла ошибка при обработке сообщения: {} для консьюмера: {}", record.key(), source.getName());
                }
            }
            if (manualCommit && records.count() > 0) {
                log.info("Ручной коммит пачки {}", records.count());
                consumer.commitSync();
            }
        }
    }
}
