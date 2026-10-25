package com.javatechie.exception;

// No booking with this reservation id -> 404 (GlobalExceptionHandler)
public class BookingNotFoundException extends RuntimeException {

    public BookingNotFoundException(String reservationId) {
        super("No booking found with reservationId " + reservationId);
    }
}
