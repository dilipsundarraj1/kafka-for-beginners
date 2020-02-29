package com.learnkafka.consumers;

import com.learnkafka.listeners.MessageRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerRebalanceListener.class);

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic-replicated";


    public MessageConsumerRebalanceListener(Map<String, Object> propsMap){
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageConsumer");

        //max.poll.interval.ms
       // propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");

        //commit strategy
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return propsMap;
    }

   public void pollKafka(){
       kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer));
       Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
       try{
        while(true){

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
            //Thread.sleep(6000);
            consumerRecords.forEach((record)->{
                logger.info("Consumed Record key is {}  and the value is {} and the partition is {} ", record.key(), record.value(), record.partition());
            });
        }
       }catch (Exception e){
            logger.error("Exception in pollKafka : "+ e);
       }finally {
           kafkaConsumer.close(); // always close the consumer for releasing the connections and sockets
       }
    }

    public static void main(String[] args) {

        Map<String, Object> propsMap = buildConsumerProperties();
        MessageConsumerRebalanceListener messageConsumer = new MessageConsumerRebalanceListener(propsMap);
        messageConsumer.pollKafka();
    }
}
