package com.pryabykh.yandex_praktikum_kafka_1.dto;

public class WeatherMessageDto {

    private double value = 1.0;

    public WeatherMessageDto() {
    }

    public WeatherMessageDto(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
