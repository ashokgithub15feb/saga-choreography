package com.javatechie.scheduler;

import com.javatechie.entity.SeatInventory;
import com.javatechie.repository.SeatInventoryRepository;
import com.javatechie.service.SeatInventoryService;
import com.javatechie.utils.enums.SeatStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Saga timeout: seats LOCKED longer than seat.lock.ttl are released, so a stalled saga
 * (payment never answered, message parked in a DLT, service down) can't block seats forever.
 * Each booking is released in its own transaction; a failure is logged and retried on the next sweep.
 * With several instances every instance sweeps; that is safe (re-check + @Version), ShedLock would avoid the extra work.
 */
@Component
@Slf4j
public class SeatLockExpiryScheduler {

    private final SeatInventoryRepository seatInventoryRepository;
    private final SeatInventoryService seatInventoryService;

    public SeatLockExpiryScheduler(SeatInventoryRepository seatInventoryRepository,
                                   SeatInventoryService seatInventoryService) {
        this.seatInventoryRepository = seatInventoryRepository;
        this.seatInventoryService = seatInventoryService;
    }

    @Scheduled(fixedDelayString = "${seat.lock.sweep-interval:PT30S}")
    public void releaseExpiredLocks() {
        List<String> bookingIds = seatInventoryRepository
                .findByStatusAndLockedUntilBefore(SeatStatus.LOCKED, LocalDateTime.now()).stream()
                .map(SeatInventory::getCurrentBookingId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        for (String bookingId : bookingIds) {
            try {
                seatInventoryService.releaseExpiredLock(bookingId);
            } catch (Exception e) {
                log.warn("SeatLockExpiryScheduler:: Could not release expired lock for bookingId {}, "
                        + "will retry on next sweep: {}", bookingId, e.getMessage());
            }
        }
    }
}
