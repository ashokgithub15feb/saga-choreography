# saga-choreography
Movie Booking Saga: Choreography with Kafka Three Spring Boot services book cinema seats without a distributed transaction. Each service commits its own local transaction and announces the result as an event. The next service reacts to that event. When a step fails, earlier steps are undone by compensating actions.
