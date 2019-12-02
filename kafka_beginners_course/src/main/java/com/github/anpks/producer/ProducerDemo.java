package com.github.anpks.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Create Producer Properties.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> kafkaProducer =
                                    new KafkaProducer<String, String>(properties);

        //Create Producer Record
        ProducerRecord<String, String> record =
                                    new ProducerRecord<String, String>("myFirstTopic","Hello World !!");

        // Send Data
        kafkaProducer.send(record);

        //Flush Producer Data
        kafkaProducer.flush();

        //Flush and Close Producer Data
        kafkaProducer.close();
    }
}
