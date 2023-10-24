package com.gds.gds_api.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaStreamsService {

    @Value("${gds.party.topic.name}") 
    private String partyTopic;

    private String storeName = partyTopic + "-store";

    private final KafkaStreams kafkaStreams;

    public KafkaStreamsService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public boolean doesPartyIdExist(String partyId) {
     KafkaStreams.State state = kafkaStreams.state();
 
     if (state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING) {
         try {
             ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                 storeName, QueryableStoreTypes.keyValueStore()));
             
             return keyValueStore.get(partyId) != null;
         } catch (Exception e) {
             // Log the exception or handle it accordingly
             throw new RuntimeException("Error querying state store: ", e);
         }
     } else {
         throw new IllegalStateException("KafkaStreams is not running. Current state is: " + state);
     }
 }
 
}
