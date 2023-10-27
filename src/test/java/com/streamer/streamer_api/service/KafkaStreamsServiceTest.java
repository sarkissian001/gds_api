package com.streamer.streamer_api.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsServiceTest {

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, String> keyValueStore;

    private KafkaStreamsService kafkaStreamsService;

    private final String storeName = "idTopic-store";

    @BeforeEach
    void setUp() {
        kafkaStreamsService = new KafkaStreamsService(kafkaStreams);
    }

    @Test
    void whenRecordIdExists_thenShouldReturnTrue() {
        // Arrange
        String recordId = "existingRecordId";
        KafkaStreams.State state = KafkaStreams.State.RUNNING;

        when(kafkaStreams.state()).thenReturn(state);
        when(kafkaStreams.store(any())).thenReturn(keyValueStore);
        when(keyValueStore.get(recordId)).thenReturn("SomeValue");

        // Act
        boolean result = kafkaStreamsService.doesRecordIdExist(recordId);

        // Assert
        assertTrue(result);
    }

    @Test
    void whenRecordIdDoesNotExist_thenShouldReturnFalse() {
        // Arrange
        String recordId = "nonExistingRecordId";
        KafkaStreams.State state = KafkaStreams.State.RUNNING;

        when(kafkaStreams.state()).thenReturn(state);
        when(kafkaStreams.store(any())).thenReturn(keyValueStore);
        when(keyValueStore.get(recordId)).thenReturn(null);

        // Act
        boolean result = kafkaStreamsService.doesRecordIdExist(recordId);

        // Assert
        assertFalse(result);
    }

    @Test
    void whenKafkaStreamsIsNotRunning_thenShouldThrowException() {
        // Arrange
        String recordId = "anyRecordId";
        KafkaStreams.State state = KafkaStreams.State.NOT_RUNNING;

        when(kafkaStreams.state()).thenReturn(state);

        // Assert
        assertThrows(IllegalStateException.class, () -> kafkaStreamsService.doesRecordIdExist(recordId));
    }
}
