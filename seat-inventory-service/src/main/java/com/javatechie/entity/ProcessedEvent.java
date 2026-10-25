package com.javatechie.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Idempotency record: one row per event already handled by this service.
 * Saved in the SAME database transaction as the business update, so a redelivered event is detected
 * and the work is not done twice.
 */
@Entity
@Table(name = "processed_event")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    private String eventKey; // e.g. BOOKING_CREATED:<bookingId>

    private Instant processedAt;
}
