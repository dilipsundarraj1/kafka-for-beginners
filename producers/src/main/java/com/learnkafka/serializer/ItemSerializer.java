package com.learnkafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Item;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemSerializer implements Serializer<Item> {
    private static final Logger logger = LoggerFactory.getLogger(ItemSerializer.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Item data) {
        logger.info("inside serialization logic");
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            logger.error("Unable to serialize object {}", data, e);
            return null;
        }
    }
}
