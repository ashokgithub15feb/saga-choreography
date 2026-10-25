package com.javatechie.config;

import com.javatechie.exception.PoisonMessageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;

import static com.javatechie.common.KafkaConfigProperties.BOOKING_REQUEST_DLT_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.DLT_SUFFIX;
import static com.javatechie.common.KafkaConfigProperties.MOVIE_BOOKING_EVENTS_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.PAYMENT_EVENTS_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Configuration
@Slf4j
public class KafkaConfig {


    @Bean
    public NewTopic createTransactionTopic() {
        return new NewTopic(MOVIE_BOOKING_EVENTS_TOPIC, 3, (short) 3); // Confluent Cloud requires replication factor 3
    }

    @Bean
    public NewTopic seatReservedDltTopic() {
        return new NewTopic(SEAT_RESERVED_TOPIC + DLT_SUFFIX, 3, (short) 3);
    }

    @Bean
    public NewTopic bookingRequestDltTopic() {
        return new NewTopic(BOOKING_REQUEST_DLT_TOPIC, 3, (short) 3);
    }

    @Bean
    public NewTopic paymentEventsDltTopic() {
        return new NewTopic(PAYMENT_EVENTS_TOPIC + DLT_SUFFIX, 3, (short) 3);
    }

    /**
     * Two transaction managers exist: this JPA one and Boot's KafkaTransactionManager (from transaction-id-prefix).
     * @Primary makes plain @Transactional use the database; the Kafka transaction is started by the listener container.
     */
    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    /**
     * Error handling for every @KafkaListener (Spring Boot plugs this bean into the container factory).
     * - Transient failure (DB down, timeout): retried with exponential backoff 1s -> 2s -> 4s, then DLT.
     * - Poison message (bad data, a bug): not retried, sent straight to the DLT.
     * Runs inside the listener's Kafka transaction, so "write to DLT + commit offset" is atomic.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> template) {
        // failed record from <topic> is published to <topic>-dlt (same partition) with exception headers
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // poison messages -> no retry (DeserializationException, MessageConversionException,
        // ClassCastException, ... are already non-retryable by default)
        errorHandler.addNotRetryableExceptions(PoisonMessageException.class,
                IllegalArgumentException.class, NullPointerException.class);
        errorHandler.setRetryListeners(failedRecordListener());
        return errorHandler;
    }

    // Visibility into the lifecycle of a failed record: each failed attempt, DLT hand-off, DLT failure
    private RetryListener failedRecordListener() {
        return new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Failed record {}-{}@{} key={} attempt={} : {}", record.topic(), record.partition(),
                        record.offset(), record.key(), deliveryAttempt, ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("Failed record {}-{}@{} key={} sent to {}{}", record.topic(), record.partition(),
                        record.offset(), record.key(), record.topic(), DLT_SUFFIX, ex);
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                // record is not committed -> it will be redelivered and retried again
                log.error("Could not publish failed record {}-{}@{} key={} to DLT", record.topic(),
                        record.partition(), record.offset(), record.key(), failure);
            }
        };
    }
}
