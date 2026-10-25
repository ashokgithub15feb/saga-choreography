package com.javatechie.common;

public class KafkaConfigProperties {

    public static final String PAYMENT_EVENTS_TOPIC = "payment-events";
    public static final String PAYMENT_EVENT_GROUP = "payment-event-group";


    public static final String SEAT_RESERVED_TOPIC = "seat-reserved-topic";
    public static final String SEAT_EVENT_GROUP = "seat-event-group";

    public static final String MOVIE_BOOKING_EVENTS_TOPIC = "movie-booking-events";
    public static final String MOVIE_BOOKING_GROUP = "movie-booking-group";

    // DeadLetterPublishingRecoverer default: failed record from <topic> goes to <topic>-dlt (same partition)
    public static final String DLT_SUFFIX = "-dlt";

    // BookingLoadSimulator: booking requests that still failed after all HTTP retries
    public static final String BOOKING_REQUEST_DLT_TOPIC = "booking-request" + DLT_SUFFIX;
}
