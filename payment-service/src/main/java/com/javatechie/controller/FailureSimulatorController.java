package com.javatechie.controller;

import com.javatechie.simulator.FailureSimulator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// Demo only, active with the "dev" profile: switch how payment-service fails

/*
How your diagram maps to the saga

A booking-service ─► [Q1 movie-booking-events] ─► B seat-inventory ─► [Q2 seat-reserved-topic] ─► C payment-service ✗ ─► [Q3 payment-events]
                                                                                                         │ keeps failing
                                                                                                         ▼
                                                                                          [seat-reserved-topic-dlt] ─► DeadLetterListener (logs it)
 */
@RestController
@RequestMapping("/payment-service/simulate-failure")
@Profile("dev")
@Tag(name = "Failure simulator", description = "Demo only (dev profile): make payment-service fail to see retry / DLT")
public class FailureSimulatorController {

    private final FailureSimulator failureSimulator;

    public FailureSimulatorController(FailureSimulator failureSimulator) {
        this.failureSimulator = failureSimulator;
    }

    @GetMapping
    @Operation(summary = "Current failure mode")
    public ResponseEntity<FailureSimulator.Mode> getMode() {
        return ResponseEntity.ok(failureSimulator.getMode());
    }

    // POST /payment-service/simulate-failure/{NONE|FLAKY|TRANSIENT|POISON}
    @PostMapping("/{mode}")
    @Operation(summary = "Set failure mode",
            description = "NONE = normal, FLAKY = fails twice then succeeds, "
                    + "TRANSIENT = always fails -> retried 3x -> DLT, POISON = no retry -> DLT")
    public ResponseEntity<FailureSimulator.Mode> setMode(@PathVariable FailureSimulator.Mode mode) {
        failureSimulator.setMode(mode);
        return ResponseEntity.ok(mode);
    }
}
