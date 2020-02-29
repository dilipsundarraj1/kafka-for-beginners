package com.learnkafka.listeners;

import com.learnkafka.consumers.MessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MessageRebalanceListener implements ConsumerRebalanceListener {
    private KafkaConsumer<String,String> kafkaConsumer;

    public MessageRebalanceListener(KafkaConsumer<String,String> kafkaConsumer){
        this.kafkaConsumer = kafkaConsumer;
    }
    private static final Logger logger = LoggerFactory.getLogger(MessageRebalanceListener.class);


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Inside onPartitionsRevoked : {} ", partitions);
        kafkaConsumer.commitSync();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Inside onPartitionsAssigned : {} ", partitions);
    }
}
