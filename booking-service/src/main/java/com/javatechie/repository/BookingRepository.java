package com.javatechie.repository;
import com.javatechie.entity.Booking;
import com.javatechie.utils.enums.BookingStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface BookingRepository extends JpaRepository<Booking, Long> {
    Booking findByBookingCode(String bookingId);

    Optional<Booking> findByIdempotencyKey(String idempotencyKey);

    List<Booking> findByStatusAndCreatedAtBefore(BookingStatus status, Instant cutoff);
}
