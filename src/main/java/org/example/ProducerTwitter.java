package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class ProducerTwitter {
    private static final Logger log = LoggerFactory.getLogger(ProducerTwitter.class);
    public final static String TOPIC_NAME = "rawTweets";
    public static void main(String[] args){
        String apiKey = args[0];
        String apiSecret = args[1];
        String tokenValue = args[2];
        String tokenSecret = args[3];

        log.info("I am a Kafka Producer");
        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.put("acks", "1");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthAccessToken(tokenValue);
        cb.setOAuthAccessTokenSecret(tokenSecret);
        cb.setOAuthConsumerKey(apiKey);
        cb.setOAuthConsumerSecret(apiSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        final TwitterStream ts = new TwitterStreamFactory(cb.build()).getInstance();

        try {
            StatusListener st = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    HashtagEntity[] hashtags = status.getHashtagEntities();
                    if(hashtags.length > 0){
                        String value = TwitterObjectFactory.getRawJSON(status);
                        String lang = status.getLang();
                        producer.send(new ProducerRecord<>(ProducerTwitter.TOPIC_NAME, lang, value));

                    }
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

                @Override
                public void onTrackLimitationNotice(int i) {}

                @Override
                public void onScrubGeo(long l, long l1) {}

                @Override
                public void onStallWarning(StallWarning stallWarning) {}

                @Override
                public void onException(Exception e) {}
            };

            ts.addListener(st);
            ts.sample();
        } catch (Exception e){
            System.out.println(e);
            producer.close();
        }

    }
}
