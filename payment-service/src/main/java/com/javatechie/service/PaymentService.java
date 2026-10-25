package com.javatechie.service;

import com.javatechie.PaymentEventsProducer;
import com.javatechie.events.SeatReservedEvent;
import com.javatechie.exception.PaymentServiceException;
import com.javatechie.exception.PoisonMessageException;
import com.javatechie.simulator.FailureSimulator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    @Autowired
    private PaymentEventsProducer eventsProducer;

    private final FailureSimulator failureSimulator;


    public void processPayment(SeatReservedEvent event) {

        // bad data can never succeed -> no retry, straight to DLT
        if (event.bookingId() == null || event.bookingId().isBlank() || event.amount() <= 0) {
            throw new PoisonMessageException("Invalid SeatReservedEvent: " + event);
        }

        // demo hook: does nothing unless a failure mode was switched on (dev profile)
        failureSimulator.maybeFail(event.bookingId());

        try {
            log.info("Processing payment for bookingId: {}", event.bookingId());

            // Simulate payment failure scenario
            if (event.amount() > 2000) {
                log.info("❌ Payment amount exceeds limit for bookingId: {}", event.bookingId());
                // failure events
                eventsProducer.publishPaymentFailureEvent(event);
                //throw new RuntimeException("Payment amount exceeds limit");
            }else{
                // success event
                eventsProducer.publishPaymentSuccessEvent(event);
                log.info("✅ Payment successful for bookingId: {}", event.bookingId());

            }


        }catch (Exception e){
            log.error("❌ Payment failed for bookingId: {}. Reason: {}", event.bookingId(), e.getMessage());
            throw new PaymentServiceException("Payment processing failed for bookingId: " + event.bookingId());
        }
    }

}
