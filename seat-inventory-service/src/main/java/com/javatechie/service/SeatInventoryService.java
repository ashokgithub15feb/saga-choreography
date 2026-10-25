package com.javatechie.service;

import com.javatechie.entity.ProcessedEvent;
import com.javatechie.entity.SeatInventory;
import com.javatechie.events.BookingCreatedEvent;
import com.javatechie.events.SeatReservedEvent;
import com.javatechie.exception.PoisonMessageException;
import com.javatechie.messaging.SeatReserveProducer;
import com.javatechie.repository.ProcessedEventRepository;
import com.javatechie.repository.SeatInventoryRepository;
import com.javatechie.response.SeatResponse;
import com.javatechie.utils.enums.SeatStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
public class SeatInventoryService {

    private final SeatInventoryRepository seatInventoryRepository;
    private final SeatReserveProducer seatReserveProducer;
    private final ProcessedEventRepository processedEventRepository;
    private final Duration lockTtl;

    public SeatInventoryService(SeatInventoryRepository seatInventoryRepository,
                                SeatReserveProducer seatReserveProducer,
                                ProcessedEventRepository processedEventRepository,
                                @Value("${seat.lock.ttl:PT5M}") Duration lockTtl) {
        this.seatReserveProducer = seatReserveProducer;
        this.seatInventoryRepository = seatInventoryRepository;
        this.processedEventRepository = processedEventRepository;
        this.lockTtl = lockTtl;
    }


    // Add service methods to manage seat inventory

    /*
     * Seat update + idempotency record commit in one DB transaction (@Transactional = JPA),
     * the SeatReservedEvent and the consumed offset commit in the container's Kafka transaction right after.
     * If the DB commits but the Kafka commit fails, the event is redelivered: we detect the duplicate,
     * skip the DB work and publish the outcome again (downstream consumers are idempotent too).
     */
    @Transactional
    public void handleBooking(BookingCreatedEvent event) {

        log.info("SeatInventoryService:: Processing bookingCreated for bookingId {}", event.bookingId());

        if (event.bookingId() == null || event.bookingId().isBlank()
                || event.showId() == null || event.seatIds() == null || event.seatIds().isEmpty()) {
            throw new PoisonMessageException("Invalid BookingCreatedEvent: " + event);
        }

        String eventKey = "BOOKING_CREATED:" + event.bookingId();
        if (processedEventRepository.existsById(eventKey)) {
            boolean reserved = !seatInventoryRepository.findByCurrentBookingId(event.bookingId()).isEmpty();
            log.warn("SeatInventoryService:: Duplicate event {} - re-publishing outcome reserved={}", eventKey, reserved);
            seatReserveProducer
                    .publishSeatReserveEvents(new SeatReservedEvent(event.bookingId(), reserved, event.amount()));
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventKey, Instant.now()));

        // Fetch seat inventories for the given show and seat numbers
        List<SeatInventory> seats = seatInventoryRepository
                .findByShowIdAndSeatNumberIn(event.showId(), event.seatIds());

        // Check if all seats are available (an unknown seat number counts as not available)
        boolean allAvailable = seats.size() == event.seatIds().size() && seats.stream()
                .allMatch(s -> s.getStatus() == SeatStatus.AVAILABLE);

        if (allAvailable) {
            // Update seat status to LOCKED and set current booking ID; the lock expires if the saga stalls
            LocalDateTime lockedUntil = LocalDateTime.now().plus(lockTtl);
            seats.forEach(s -> {
                s.setStatus(SeatStatus.LOCKED);
                s.setCurrentBookingId(event.bookingId());
                s.setLockedUntil(lockedUntil);
            });
            seatInventoryRepository.saveAll(seats);
            // Publish seat reserved event
            seatReserveProducer
                    .publishSeatReserveEvents(new SeatReservedEvent(event.bookingId(), true, event.amount()));
            log.info("SeatInventoryService:: Seats locked successfully for bookingId {}", event.bookingId());
        }else{
            log.warn("SeatInventoryService:: Seat locking failed for bookingId {}. Some seats are not available.", event.bookingId());
            // Publish seat reserved event with failure
            seatReserveProducer
                    .publishSeatReserveEvents(new SeatReservedEvent(event.bookingId(), false, event.amount()));
        }
    }

    // Read-only seat listing, both filters optional
    @Transactional(readOnly = true)
    public List<SeatResponse> findSeats(String showId, SeatStatus status) {
        List<SeatInventory> seats;
        if (showId != null && status != null) {
            seats = seatInventoryRepository.findByShowIdAndStatus(showId, status);
        } else if (showId != null) {
            seats = seatInventoryRepository.findByShowId(showId);
        } else if (status != null) {
            seats = seatInventoryRepository.findByStatus(status);
        } else {
            seats = seatInventoryRepository.findAll();
        }
        return seats.stream()
                .map(s -> new SeatResponse(s.getShowId(), s.getSeatNumber(), s.getStatus().name(), s.getCurrentBookingId()))
                .toList();
    }

    @Transactional
    public void releaseSeatsOnPaymentFailure(String bookingId) {
        log.info("SeatInventoryService:: Releasing seats for bookingId {}", bookingId);

        String eventKey = "PAYMENT_FAILED:" + bookingId;
        if (processedEventRepository.existsById(eventKey)) {
            log.warn("SeatInventoryService:: Duplicate event {} - re-publishing failure", eventKey);
            seatReserveProducer.publishSeatReserveEvents(new SeatReservedEvent(bookingId, false, 0));
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventKey, Instant.now()));

        List<SeatInventory> bookingSeats = seatInventoryRepository.findByCurrentBookingId(bookingId);

        bookingSeats.forEach(this::release);

        seatInventoryRepository.saveAll(bookingSeats);
        log.info("SeatInventoryService:: Seats released successfully for bookingId {}", bookingId);

        //send failed event to downstream (booking-service)

        seatReserveProducer
                .publishSeatReserveEvents(new SeatReservedEvent(bookingId, false, 0));
    }

    /*
     * Payment succeeded -> LOCKED seats become RESERVED (final). If the lock already expired, the seats were
     * released (maybe re-booked by someone else): we must not take them back. The booking is failed instead
     * and the captured payment has to be refunded.
     */
    @Transactional
    public void confirmSeatsOnPaymentSuccess(String bookingId) {
        log.info("SeatInventoryService:: Confirming seats for bookingId {}", bookingId);

        String eventKey = "PAYMENT_COMPLETED:" + bookingId;
        if (processedEventRepository.existsById(eventKey)) {
            boolean reserved = seatInventoryRepository.findByCurrentBookingId(bookingId).stream()
                    .anyMatch(s -> s.getStatus() == SeatStatus.RESERVED);
            log.warn("SeatInventoryService:: Duplicate event {} - seats reserved={}", eventKey, reserved);
            if (!reserved) {
                seatReserveProducer.publishSeatReserveEvents(new SeatReservedEvent(bookingId, false, 0));
            }
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventKey, Instant.now()));

        List<SeatInventory> lockedSeats = seatInventoryRepository.findByCurrentBookingId(bookingId).stream()
                .filter(s -> s.getStatus() == SeatStatus.LOCKED)
                .toList();

        if (lockedSeats.isEmpty()) {
            log.error("SeatInventoryService:: Payment completed for bookingId {} but its seat lock expired "
                    + "- booking failed, REFUND REQUIRED", bookingId);
            seatReserveProducer.publishSeatReserveEvents(new SeatReservedEvent(bookingId, false, 0));
            return;
        }

        lockedSeats.forEach(s -> {
            s.setStatus(SeatStatus.RESERVED);
            s.setLockedUntil(null);
        });
        seatInventoryRepository.saveAll(lockedSeats);
        log.info("SeatInventoryService:: Seats reserved for bookingId {}", bookingId);
    }

    /*
     * Saga timeout: called by SeatLockExpiryScheduler for a booking whose LOCK expired (payment never answered,
     * e.g. the message is in a DLT). Releases the seats and tells booking-service the reservation failed.
     * Runs in its own DB transaction; the Kafka send is synchronized with it. A concurrent payment-success
     * update on the same rows makes one side fail on @Version and retry.
     */
    @Transactional
    public void releaseExpiredLock(String bookingId) {
        LocalDateTime now = LocalDateTime.now();
        List<SeatInventory> expiredSeats = seatInventoryRepository.findByCurrentBookingId(bookingId).stream()
                .filter(s -> s.getStatus() == SeatStatus.LOCKED
                        && s.getLockedUntil() != null && s.getLockedUntil().isBefore(now))
                .toList();
        if (expiredSeats.isEmpty()) {
            return; // confirmed or released in the meantime
        }

        expiredSeats.forEach(this::release);
        seatInventoryRepository.saveAll(expiredSeats);
        log.warn("SeatInventoryService:: Seat lock expired for bookingId {} - {} seat(s) released",
                bookingId, expiredSeats.size());

        seatReserveProducer.publishSeatReserveEvents(new SeatReservedEvent(bookingId, false, 0));
    }

    private void release(SeatInventory seat) {
        seat.setStatus(SeatStatus.AVAILABLE);
        seat.setCurrentBookingId(null);
        seat.setLockedUntil(null);
    }
}
