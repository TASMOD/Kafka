package com.github.TASMOD.kafka.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

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
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a producer record. Topic is auto generated, not hard coded (it appears magically dont be afraid!)
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        // 3. Send data (asynchronous)
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes on successful send or exception
                if (e == null){
                    logger.info("Received new success metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });
        // flush data to finish async call
        producer.flush();
        // alternatively flush and close
        producer.close();
    }
}
