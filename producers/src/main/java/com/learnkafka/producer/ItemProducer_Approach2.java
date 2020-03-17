package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Item;
import com.learnkafka.serializer.ItemSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ItemProducer_Approach2 {

    private static final Logger logger = LoggerFactory.getLogger(ItemProducer_Approach2.class);
    KafkaProducer kafkaProducer;
    String topicName = "items";
    ObjectMapper objectMapper = new ObjectMapper();

    public ItemProducer_Approach2(Map<String, Object> producerProps) {
        kafkaProducer = new KafkaProducer(producerProps);
    }

    public static Map<String, Object> propsMap() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        //propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRIES_CONFIG, "10");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");

        return propsMap;
    }

    private void publishMessageSync(Item item) throws JsonProcessingException {

        String itemStr = objectMapper.writeValueAsString(item);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, item.getId(), itemStr);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
        }
    }

    public static void main(String[] args) {
        ItemProducer_Approach2 itemProducer = new ItemProducer_Approach2(propsMap());
        List<Item> items = List.of(new Item(1, "LG TV", 200.00),
                new Item(2, "Iphone 10 Pro Max", 949.99));

        items.forEach((item -> {
            try {
                itemProducer.publishMessageSync(item);
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException in main : ", e);
            }
        }));
    }


}
