package com.learnkafka.deserializer;

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
        Item item = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bis);
            item = (Item) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.error(" exception closing the stream ", e);
        } finally {
            if (bis != null || ois != null) {
                try {
                    bis.close();
                    ois.close();
                } catch (IOException e) {
                    logger.error("IO exception closing the stream ", e);
                }
            }
            return item;
        }
    }
}
