package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class CounterTweets {

    public static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(CounterTweets.class);
    public static void main(String[] args){
        log.info("I am a Kafka Consumer");
        String bootstrapServers = "localhost:9092";
        String groupId = "grupo1";
        String topic = "rawTweets";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> tweets = streamsBuilder.stream(topic);

        tweets.flatMapValues(value -> Collections.singletonList(getHashtags(value)))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .print(Printed.toSysOut());

        Topology topology = streamsBuilder.build();
        final KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }

    public static String getHashtags(String input){
        JsonNode root;
        try{
            root = objectMapper.readTree(input);
            JsonNode hashtagNode = root.path("entities").path("hashtags");

            if(!hashtagNode.toString().equals("[]")){
                return hashtagNode.get(0).path("text").asText();
            }

        } catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }
}
