package com.learnjava.producer;

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

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic";
    KafkaProducer kafkaProducer;

    public MessageProducer(Map<String, String> producerProps) {
        kafkaProducer = new KafkaProducer(producerProps);
    }


    public RecordMetadata publishMessageSync(String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            logger.info("recordMetadata : {} ", recordMetadata);
        }
        /*catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("");
        }*/
        catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync : {} ", e);
        }
        return recordMetadata;
    }

    public static Map<String, String> buildProducerProperties() {

        Map<String, String> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return propsMap;
    }

    public static void main(String[] args) {

        Map<String, String> producerProps = buildProducerProperties();
        MessageProducer messageProducer = new MessageProducer(producerProps);
        RecordMetadata recordMetadata = messageProducer.publishMessageSync(null, "ABC");
        System.out.println("recordMetadata: " + recordMetadata);
    }
}
