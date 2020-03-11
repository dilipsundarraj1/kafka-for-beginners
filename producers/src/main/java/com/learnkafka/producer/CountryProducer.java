package com.learnkafka.producer;

import com.learnkafka.domain.Country;
import com.learnkafka.serializer.CountrySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CountryProducer {

    private static final Logger logger = LoggerFactory.getLogger(CountryProducer.class);
    KafkaProducer kafkaProducer;
    String topicName = "country";

    public CountryProducer(Map<String, Object> producerProps) {
        kafkaProducer = new KafkaProducer(producerProps);
    }

    public static Map<String, Object> propsMap() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CountrySerializer.class.getName());
        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRIES_CONFIG, "10");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");

        return propsMap;
    }

    private void publishMessageSync(Country country) {


        ProducerRecord<String, Country> producerRecord = new ProducerRecord<>(topicName, country.getCountryCode(),country);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
        }
    }

    public static void main(String[] args) {
        CountryProducer countryProducer = new CountryProducer(propsMap());
        Country country = new Country("USA", "United States of America");
        countryProducer.publishMessageSync(country);
    }


}
