package com.learnkafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Country;
import com.learnkafka.domain.Item;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ItemDeserializer implements Deserializer<Item> {
    private static final Logger logger = LoggerFactory.getLogger(ItemDeserializer.class);

    @Override
    public Item deserialize(String topic, byte[] data) {
        logger.info("Inside Consumer Deserialization");
        ObjectMapper mapper = new ObjectMapper();
        Item item = null;
        try {
            item = mapper.readValue(data, Item.class);
        } catch (Exception e) {

            logger.error("Exception Occurred during deserializing"+e);
        }
        return item;
    }
}
