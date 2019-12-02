package com.github.anpks.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger LOGGER  = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer =
                new KafkaProducer<String, String>(properties);

        final ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("myFirstTopic", "Hello World !!");

        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    LOGGER.info("Exception Occurred :: "+e.getMessage());
                } else {
                    LOGGER.info("Offset --> "+recordMetadata.offset());
                    LOGGER.info("Topic --> "+recordMetadata.topic());
                    LOGGER.info("Partition --> "+recordMetadata.partition());
                    LOGGER.info("Timestamp --> "+recordMetadata.timestamp());

                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}