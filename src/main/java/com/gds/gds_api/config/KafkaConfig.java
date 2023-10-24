package com.gds.gds_api.config;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${gds.party.topic.name}") 
    private String partyTopic;

    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String bootstrapServers;

    @Value("${kafka.application.id}")
    private String kafkaApplicationId; 

    private String storeName = partyTopic + "-store";
    
    // Validate env vars 
   @PostConstruct
    public void validate() {
        if (partyTopic == null || partyTopic.isEmpty()) {
            throw new IllegalArgumentException("GDS_PARTY_TOPIC_NAME is not set or is empty");
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
        PartyIdTopology(builder);

        return builder;
    }

    private void PartyIdTopology(StreamsBuilder builder) {
        final Serde<String> stringSerde = getStringSerde();

        KStream<String, String> myDataStream = builder.stream(partyTopic, Consumed.with(stringSerde, stringSerde));
        myDataStream.groupByKey()
            .count(Materialized.as(storeName));
    }


    // Helper methods
    private Serde<String> getStringSerde() {
        return Serdes.String();
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder, Properties kStreamConfigs) {
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamConfigs);
        kafkaStreams.start();
        return kafkaStreams;
    }
}
