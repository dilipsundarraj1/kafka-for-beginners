package com.learnkafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Country;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountryDeserializer implements Deserializer<Country> {

    private static final Logger logger = LoggerFactory.getLogger(CountryDeserializer.class);

    @Override
    public Country deserialize(String topic, byte[] data) {
        logger.info("Inside Consumer Deserialization");
        ObjectMapper mapper = new ObjectMapper();
        Country message = null;
        try {
            message = mapper.readValue(data, Country.class);
        } catch (Exception e) {

            logger.error("Exception Occurred during deserializing"+e);
        }
        return message;
    }
}
