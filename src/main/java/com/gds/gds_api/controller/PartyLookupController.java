package com.gds.gds_api.controller;

import com.gds.gds_api.service.KafkaStreamsService;

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
public class PartyLookupController {

    private final KafkaStreamsService kafkaStreamsService;

    @Autowired
    public PartyLookupController(KafkaStreamsService kafkaStreamsService) {
        this.kafkaStreamsService = kafkaStreamsService;
    }

    @GetMapping("/party-lookup")
    public ResponseEntity<Map<String, Boolean>> checkPartyId(@RequestParam String party_id) {
        boolean exists = kafkaStreamsService.doesPartyIdExist(party_id);
        return ResponseEntity.ok(Collections.singletonMap("exists", exists));
    }
}
