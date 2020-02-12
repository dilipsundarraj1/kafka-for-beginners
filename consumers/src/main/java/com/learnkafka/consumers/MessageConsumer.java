package com.learnkafka.consumers;

import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    KafkaConsumer<String, String> kafkaConsumer;

    public  MessageConsumer(Map<String, Object> propsMap){
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageConsumer");
        return propsMap;
    }

   public void pollKafka(){
       kafkaConsumer.subscribe(List.of("test-topic"));
       Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
       try{
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
            consumerRecords.forEach((record)->{
                logger.info("Consumed Record key is {}  and the value is {} and the partition is {} ", record.key(), record.value(), record.partition());
            });
        }
       }catch (Exception e){
            logger.error("Exception in pollKafka : "+ e);
       }finally {
           kafkaConsumer.close();
       }
    }

    public static void main(String[] args) {

        Map<String, Object> propsMap = buildConsumerProperties();
        MessageConsumer messageConsumer = new MessageConsumer(propsMap);
        messageConsumer.pollKafka();
    }
}
