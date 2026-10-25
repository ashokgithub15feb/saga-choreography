package com.javatechie.repository;

import com.javatechie.entity.SeatInventory;
import com.javatechie.utils.enums.SeatStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface SeatInventoryRepository extends JpaRepository<SeatInventory, Long> {

    List<SeatInventory> findByShowIdAndSeatNumberIn(String showId, List<String> seatNumbers);

    List<SeatInventory> findByCurrentBookingId(String bookingId);

    List<SeatInventory> findByStatusAndLockedUntilBefore(SeatStatus status, LocalDateTime cutoff);

    List<SeatInventory> findByShowId(String showId);

    List<SeatInventory> findByStatus(SeatStatus status);

    List<SeatInventory> findByShowIdAndStatus(String showId, SeatStatus status);
}
