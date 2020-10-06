package com.github.TASMOD.kafka.first;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    public static void main(String[] args) {
        new ConsumerDemo().run();
    }

    private ConsumerDemo(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-app";
        String topic = "topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer");

        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interripted", e);

        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootsrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch
                              ){
                this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootsrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                    groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "earliest");

                consumer = new KafkaConsumer<String, String>(properties);
                consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() +
                                " Value: " + record.value());
                        logger.info("Partition: " + record.partition() +
                                "Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                //notify main code the consumer is done
                latch.countDown();
            }
        }
        public void shutdown(){
            // interrupt consumer.poll and throw exception
            consumer.wakeup();
        }
    }
}
