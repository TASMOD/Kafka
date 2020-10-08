package com.TASMOD.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey ="consumerKey";
    String consumerSecret = "consumerSecret";
    String token = "token";
    String secret = "scret";


    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();
        // Create a kafka producer
        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5,TimeUnit.SECONDS);
            } catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
            }
            logger.info(msg);
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("bitcoin", "api");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
