package com.learnkafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Country;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountrySerializer implements Serializer<Country> {
    private static final Logger logger = LoggerFactory.getLogger(CountrySerializer.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Country data) {
        logger.info("inside serialization logic");
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            logger.error("Unable to serialize object {}", data, e);
            return null;
        }
    }
}
