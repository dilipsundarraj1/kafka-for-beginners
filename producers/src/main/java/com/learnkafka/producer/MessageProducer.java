package com.learnkafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic";
    KafkaProducer kafkaProducer;

    public MessageProducer(Map<String, Object> producerProps) {
        kafkaProducer = new KafkaProducer(producerProps);
    }

    Callback callback = (recordMetadata, exception) -> {
        if (exception != null) {
            logger.error("Exception is {} ", exception.getMessage());
        } else {
            logger.info("Record MetaData Async in CallBack Offset : {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        }
    };

    public void close(){
        kafkaProducer.close();
    }


    public void publishMessageSync(String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            logSuccessResponse(key, message, recordMetadata);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
        }

    }

    public void logSuccessResponse(String message, String key, RecordMetadata recordMetadata) {
        logger.info("Message ** {} ** sent successfully with the key  **{}** . ", message, key);
        logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
    }

    public void publishMessageAsync(String key, String message) throws InterruptedException, ExecutionException, TimeoutException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
        kafkaProducer.send(producerRecord, callback);
        // kafkaProducer.send(producerRecord).get();
    }

    public static Map<String, Object> propsMap() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");*/

        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
        return propsMap;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        Map<String, Object> producerProps = propsMap();
        MessageProducer messageProducer = new MessageProducer(producerProps);
        messageProducer.publishMessageSync(null, "ABC");
        messageProducer.publishMessageAsync(null, "ABC");
        Thread.sleep(3000);
    }
}
