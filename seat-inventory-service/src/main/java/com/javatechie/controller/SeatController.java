package com.javatechie.controller;

import com.javatechie.response.SeatResponse;
import com.javatechie.service.SeatInventoryService;
import com.javatechie.utils.enums.SeatStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// Read-only: seats are changed only by saga events, never through this API
@RestController
@RequestMapping("/seat-inventory-service")
@Tag(name = "Seats", description = "Seat availability (read only)")
public class SeatController {

    private final SeatInventoryService seatInventoryService;

    public SeatController(SeatInventoryService seatInventoryService) {
        this.seatInventoryService = seatInventoryService;
    }

    @GetMapping("/seats")
    @Operation(summary = "List seats", description = "Optional filters: showId, status (AVAILABLE, LOCKED, RESERVED)")
    public ResponseEntity<List<SeatResponse>> getSeats(@RequestParam(required = false) String showId,
                                                       @RequestParam(required = false) SeatStatus status) {
        return ResponseEntity.ok(seatInventoryService.findSeats(showId, status));
    }
}
