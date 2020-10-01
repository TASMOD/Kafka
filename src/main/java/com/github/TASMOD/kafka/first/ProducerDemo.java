package com.github.TASMOD.kafka.first;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        //Steps to create a Producer:
        // 1. Create producer properties
        Properties properties = new Properties();

        /* Old way
        //Properties can be found at kafka documentation (https://kafka.apache.org/documentation/#producerconfigs)
        properties.setProperty("bootstrap.servers", bootstrapServers);
        // These will help the producer understand the data types we are sending to kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        */

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the producer
        // Producer extends <K, V> both of value string.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a producer record. Topic is auto generated, not hard coded (it appears magically dont be afraid!)
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        // 3. Send data (asynchronous)
        producer.send(record);
        // flush data to finish async call
        producer.flush();
        // alternatively flush and close
        producer.close();
    }
}
