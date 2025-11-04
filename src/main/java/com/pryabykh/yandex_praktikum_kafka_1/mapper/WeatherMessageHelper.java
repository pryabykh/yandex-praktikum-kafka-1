package com.pryabykh.yandex_praktikum_kafka_1.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pryabykh.yandex_praktikum_kafka_1.dto.WeatherMessageDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WeatherMessageHelper {
    private static final Logger log = LoggerFactory.getLogger(WeatherMessageHelper.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public String serialize(WeatherMessageDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            log.error("Возникла ошибка сериализации сообщения", e);
            throw new RuntimeException(e);
        }
    }

    public WeatherMessageDto deserialize(String json) {
        try {
            return objectMapper.readValue(json, WeatherMessageDto.class);
        } catch (JsonProcessingException e) {
            log.error("Возникла ошибка десериализации сообщения", e);
            throw new RuntimeException(e);
        }
    }
}
