package com.streamer.streamer_api.controller;

import com.streamer.streamer_api.service.KafkaStreamsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdLookupControllerTest {

    @Mock
    private KafkaStreamsService kafkaStreamsService;

    @InjectMocks
    private IdLookupController idLookupController;

    @BeforeEach
    void setUp() {
        idLookupController = new IdLookupController(kafkaStreamsService);
    }

    @Test
    void whenRecordIdExists_thenResponseShouldBeTrue() {
        // Arrange
        String record_id = "testRecordId";
        when(kafkaStreamsService.doesRecordIdExist(record_id)).thenReturn(true);

        // Act
        ResponseEntity<Map<String, Boolean>> response = idLookupController.checkRecordId(record_id);

        // Assert
        assertEquals(true, response.getBody().get("exists"));
    }

    @Test
    void whenRecordIdDoesNotExist_thenResponseShouldBeFalse() {
        // Arrange
        String record_id = "nonExistingRecordId";
        when(kafkaStreamsService.doesRecordIdExist(record_id)).thenReturn(false);

        // Act
        ResponseEntity<Map<String, Boolean>> response = idLookupController.checkRecordId(record_id);

        // Assert
        assertEquals(false, response.getBody().get("exists"));
    }
}
