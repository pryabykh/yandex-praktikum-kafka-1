package com.pryabykh.yandex_praktikum_kafka_1.configuration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.BOOTSTRAP_SERVERS;
import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.TOPIC_NAME;

@Component
public class KafkaTopicConfiguration  implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConfiguration.class);

    @Override
    public void run(String... args) {
        try (Admin admin = Admin.create(Collections.singletonMap("bootstrap.servers", BOOTSTRAP_SERVERS))) {
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 2);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            log.info("Топик успешно создан!");
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof ExecutionException && ((ExecutionException) e).getCause() instanceof TopicExistsException) {
                log.info("Топик уже существует. Создание будет проигнорировано");
            } else {
                log.error("Ошибка при создании топика", e);
            }
        }
    }
}
