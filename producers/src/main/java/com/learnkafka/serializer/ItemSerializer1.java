package com.learnkafka.serializer;

import com.learnkafka.domain.Item;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ItemSerializer1 implements Serializer<Item> {
    private static final Logger logger = LoggerFactory.getLogger(ItemSerializer1.class);

    @Override
    public byte[] serialize(String topic, Item data) {

        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(data);
            oos.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            logger.error("IO Exception in serialize ", e);
        } finally {
            if (oos != null && bos != null) {
                try {
                    oos.close();
                    bos.close();
                } catch (IOException e) {
                    logger.error("Exception Closing the output stream ", e);
                }
            }
        }
        return bytes;

    }
}
