package com.javatechie.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional outbox: an event waiting to be published to Kafka.
 * Written in the SAME database transaction as the business change (booking row), so the two can't diverge:
 * either both are committed or neither. OutboxRelay publishes unsent rows and stamps sentAt.
 */
@Entity
@Table(name = "outbox_event", indexes = @Index(name = "idx_outbox_unsent", columnList = "sentAt, createdAt"))
@Getter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    private String id;            // event id (UUID)

    private String aggregateId;   // bookingId, used as the Kafka key -> same partition -> per-booking order

    private String topic;

    private String eventType;     // fully qualified class of the payload, used to rebuild it for the JsonSerializer

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;       // event as JSON

    private Instant createdAt;

    private Instant sentAt;       // null = not published yet

    public OutboxEvent(String aggregateId, String topic, String eventType, String payload) {
        this.id = UUID.randomUUID().toString();
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }
}
