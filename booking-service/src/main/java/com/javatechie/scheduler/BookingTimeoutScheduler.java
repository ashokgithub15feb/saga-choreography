package com.javatechie.scheduler;

import com.javatechie.entity.Booking;
import com.javatechie.repository.BookingRepository;
import com.javatechie.service.BookingService;
import com.javatechie.utils.enums.BookingStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Saga timeout on the booking side: fails bookings stuck in PENDING (seat-inventory never answered).
 * Bookings in SEATS_LOCKED are not touched here: seat-inventory owns that timeout (seat.lock.ttl) and reports
 * the expiry back as a seat-reservation failure. booking.pending-timeout must stay well above seat.lock.ttl.
 */
@Component
@Slf4j
public class BookingTimeoutScheduler {

    private final BookingRepository bookingRepository;
    private final BookingService bookingService;
    private final Duration pendingTimeout;

    public BookingTimeoutScheduler(BookingRepository bookingRepository, BookingService bookingService,
                                   @Value("${booking.pending-timeout:PT15M}") Duration pendingTimeout) {
        this.bookingRepository = bookingRepository;
        this.bookingService = bookingService;
        this.pendingTimeout = pendingTimeout;
    }

    @Scheduled(fixedDelayString = "${booking.pending-sweep-interval:PT1M}")
    public void failStalePendingBookings() {
        Instant cutoff = Instant.now().minus(pendingTimeout);
        for (Booking booking : bookingRepository.findByStatusAndCreatedAtBefore(BookingStatus.PENDING, cutoff)) {
            try {
                bookingService.expirePendingBooking(booking.getBookingCode());
            } catch (Exception e) {
                log.warn("BookingTimeoutScheduler:: Could not expire booking {}, will retry on next sweep: {}",
                        booking.getBookingCode(), e.getMessage());
            }
        }
    }
}
