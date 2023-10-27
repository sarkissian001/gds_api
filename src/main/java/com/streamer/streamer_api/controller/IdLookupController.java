package com.streamer.streamer_api.controller;

import com.streamer.streamer_api.service.KafkaStreamsService;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;



@RestController
@RequestMapping("/v1")
public class IdLookupController {

    private final KafkaStreamsService kafkaStreamsService;

    @Autowired
    public IdLookupController(KafkaStreamsService kafkaStreamsService) {
        this.kafkaStreamsService = kafkaStreamsService;
    }

    @GetMapping("/id-lookup")
    public ResponseEntity<Map<String, Boolean>> checkRecordId(@RequestParam String record_id) {
        boolean exists = kafkaStreamsService.doesRecordIdExist(record_id);
        return ResponseEntity.ok(Collections.singletonMap("exists", exists));
    }
}
