package com.streamer.streamer_api.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;

@Configuration
public class KafkaConfig {

    @Value("${streamer.id.topic.name}") 
    private String idTopic;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.application.id}")
    private String kafkaApplicationId; 

    private String storeName = idTopic + "-store";
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    // Validate env vars 
   @PostConstruct
    public void validate() {
        if (idTopic == null || idTopic.isEmpty()) {
            throw new IllegalArgumentException("STREAMER_ID_TOPIC_NAME is not set or is empty");
        }
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("KAFKA_BOOTSTRAP_SERVERS is not set or is empty");
        }
        if (kafkaApplicationId == null || kafkaApplicationId.isEmpty()) {
            throw new IllegalArgumentException("KAFKA_APPLICATION_ID is not set or is empty");
        }
    }

    @Bean
    public Properties kStreamConfigs() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        StreamsBuilder builder = new StreamsBuilder();

        // Can add other Topologies here
        IdIdTopology(builder);

        return builder;
    }

    private void IdIdTopology(StreamsBuilder builder) {
        final Serde<String> stringSerde = getStringSerde();

        KStream<String, String> myDataStream = builder.stream(idTopic, Consumed.with(stringSerde, stringSerde));
        myDataStream.groupByKey()
            .count(Materialized.as(storeName));
    }


    // Helper methods
    private Serde<String> getStringSerde() {
        return Serdes.String();
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder, Properties kStreamConfigs) throws InterruptedException {
        // Wait for the topic to become available
        waitForTopic(idTopic, kStreamConfigs);

        logger.info("Topic '{}' is available. Creating KafkaStreams...", idTopic);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamConfigs);
        kafkaStreams.start();

        logger.info("KafkaStreams started successfully.");
        return kafkaStreams;
    }

    private void waitForTopic(String topicName, Properties kStreamConfigs) throws InterruptedException {
        boolean topicAvailable = false;
        long startTime = System.currentTimeMillis();
        long maxRetryTimeMillis = TimeUnit.MINUTES.toMillis(10);

        while (!topicAvailable && System.currentTimeMillis() - startTime < maxRetryTimeMillis) {
            try {
                AdminClient adminClient = AdminClient.create(kStreamConfigs);
                ListTopicsOptions listTopicsOptions = new ListTopicsOptions().timeoutMs(5000); // Set a timeout for listing topics
                Set<String> topicNames = adminClient.listTopics(listTopicsOptions).names().get();
                boolean topicExists = topicNames.contains(topicName);

                adminClient.close();

                if (topicExists) {
                    topicAvailable = true;
                    logger.info("Topic '{}' is now available.", topicName);
                } else {
                    logger.info("Topic '{}' not found. Waiting for 5 seconds before retrying...", topicName);
                    Thread.sleep(5000); // Wait for 5 seconds before retrying
                }
            } catch (ExecutionException | TimeoutException e) {
                // Handle exceptions
                logger.error("Error checking topic '{}': {}", topicName, e.getMessage());
                Thread.sleep(5000); // Wait for 5 seconds before retrying
            }
        }

        if (!topicAvailable) {
            logger.error("Topic '{}' not found after waiting for 10 minutes. Exiting...", topicName);
            throw new RuntimeException("Topic '" + topicName + "' not found after waiting for 10 minutes.");
        }
    }

}