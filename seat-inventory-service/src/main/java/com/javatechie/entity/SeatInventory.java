package com.javatechie.entity;

import com.javatechie.utils.enums.SeatStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "seat_inventory")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SeatInventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String theaterId;
    private String showId;
    private String seatNumber;
    private String screenId;

    @Enumerated(EnumType.STRING)
    private SeatStatus status; // AVAILABLE, LOCKED, RESERVED

    private String currentBookingId; // if seat is locked/reserved for booking

    private LocalDateTime lockedUntil; // LOCKED seats are released by SeatLockExpiryScheduler after this time

    private LocalDateTime lastUpdated;

    /*
     * Optimistic locking: every UPDATE runs "... WHERE id = ? AND version = ?" and bumps the version.
     * If two bookings read the same AVAILABLE seat concurrently, only the first commit wins; the second gets
     * ObjectOptimisticLockingFailureException -> its transaction rolls back -> the Kafka error handler retries it
     * (transient) -> on retry the seat is already LOCKED -> that booking gets reserved=false. No double booking.
     * DEFAULT 0 so ddl-auto can add the column to existing rows.
     */
    @Version
    @Column(columnDefinition = "BIGINT NOT NULL DEFAULT 0")
    private Long version;

    @PrePersist
    @PreUpdate
    public void onUpdate() {
        lastUpdated = LocalDateTime.now();
    }
}
