package com.javatechie.response;

// Read model of a seat, served by seat-inventory-service (GET /seat-inventory-service/seats)
public record SeatResponse(String showId, String seatNumber, String status, String currentBookingId) {
}
