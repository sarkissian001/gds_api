package com.streamer.streamer_api.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaStreamsService {

    @Value("${streamer.id.topic.name}") 
    private String idTopic;

    private String storeName = idTopic + "-store";

    private final KafkaStreams kafkaStreams;

    public KafkaStreamsService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public boolean doesRecordIdExist(String record_id) {
     KafkaStreams.State state = kafkaStreams.state();
 
     if (state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING) {
         try {
             ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                 storeName, QueryableStoreTypes.keyValueStore()));
             
             return keyValueStore.get(record_id) != null;
         } catch (Exception e) {
             // Log the exception or handle it accordingly
             throw new RuntimeException("Error querying state store: ", e);
         }
     } else {
         throw new IllegalStateException("KafkaStreams is not running. Current state is: " + state);
     }
 }
 
}
