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
 * <p>
 * Distinct table name on purpose: booking-service and seat-inventory-service both handle payment events with
 * the same keys (PAYMENT_COMPLETED:<id>). When they shared one schema, a shared table made each service see the
 * other's row as "already processed" and skip its own work. Now each service has its own database
 * (booking_db / seat_inventory_db); the name keeps them apart even if they are ever pointed at one schema again.
 */
@Entity
@Table(name = "booking_processed_event")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    private String eventKey; // e.g. BOOKING_CREATED:<bookingId>

    private Instant processedAt;
}
