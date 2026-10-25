package com.javatechie.utils.enums;

/**
 * Booking saga state machine.
 * <pre>
 *   PENDING ──► SEATS_LOCKED ──► CONFIRMED
 *      │             │               │
 *      └─────────────┴───────────────┴──► FAILED (terminal)
 * </pre>
 * Events come from two topics (seat-reserved-topic, payment-events), so they can arrive out of order:
 * e.g. the payment result before "seats locked". Only forward moves are applied, anything else is ignored.
 */
public enum BookingStatus {
    PENDING,
    SEATS_LOCKED,
    CONFIRMED,
    FAILED;

    public boolean canMoveTo(BookingStatus next) {
        return switch (this) {
            case PENDING -> next != PENDING;
            case SEATS_LOCKED -> next == CONFIRMED || next == FAILED;
            // compensation: payment succeeded but the seat lock had already expired -> seats are gone
            case CONFIRMED -> next == FAILED;
            case FAILED -> false;
        };
    }
}
