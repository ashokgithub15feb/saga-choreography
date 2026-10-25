# saga-choreography

Movie seat booking with the **Saga pattern (choreography)** on Kafka: three Spring Boot 3.5 / Java 21 services, each with its own local transaction, coordinated only through events.

Full design document with flow diagrams, a worked example, ER and class diagrams, failure scenarios and interview Q&A: **[docs/SAGA_DESIGN.html](docs/SAGA_DESIGN.html)** (open it in a browser).

| Service | Port | Database | Role |
|---|---|---|---|
| booking-service | 9191 | `booking_db` | REST API, booking status, outbox |
| seat-inventory-service | 9292 | `seat_inventory_db` | lock / reserve / release seats, lock expiry |
| payment-service | 9393 | none | simulated payment (`amount > 2000` fails) |
| movie-booking-commons | – | – | shared events, DTOs, topic names |

Flow: `BookingCreated → seats LOCKED → SeatReserved → payment → PaymentEvent → booking CONFIRMED + seats RESERVED`.
On payment failure the seats are released and the booking becomes FAILED (compensation).

## Run

```bash
export JAVA_HOME="/c/Program Files/Java/jdk-21"
export CONFLUENT_API_KEY=...  CONFLUENT_API_SECRET=...

# one time, only when moving existing data out of the old shared "javatechie" schema
mysql -uroot -proot < docs/db/split-database-per-service.sql

cd movie-booking-commons      && mvn install
cd ../seat-inventory-service  && mvn spring-boot:run
cd ../payment-service         && mvn spring-boot:run -Dspring-boot.run.profiles=dev
cd ../booking-service         && mvn spring-boot:run
```

Swagger UI: http://localhost:9191/swagger-ui.html, http://localhost:9292/swagger-ui.html, http://localhost:9393/swagger-ui.html

```bash
curl -X POST localhost:9191/booking-service/bookSeat \
  -H "Content-Type: application/json" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"userId":"u1","showId":"SHOW_101","seatIds":["A5"],"amount":500}'
# → {"reservationId":"…","status":"PENDING"}  (the saga continues asynchronously)

curl localhost:9191/booking-service/bookings/<reservationId>
# → PENDING → SEATS_LOCKED → CONFIRMED (or FAILED); unknown id → 404

curl "localhost:9292/seat-inventory-service/seats?status=AVAILABLE"
# → [{"showId":"SHOW_101","seatNumber":"A1","status":"AVAILABLE",...}, ...]
```

## Concurrency demo (BookingLoadSimulator)

A scheduler in booking-service that sends **10–15 people** at the **same 3 seats** of one show at the same moment
(first rush 20s after startup, then every 5 minutes) and checks that no seat is confirmed twice.
Shows and seats come from seat-inventory's API; users, amounts and Idempotency-Keys are random.
Some users get injected errors (503 flaky / always, 400 bad request): transient errors are **retried 3 times**
(1s, 2s, 4s, same Idempotency-Key), and anything still failing goes to the **`booking-request-dlt`** topic.

```bash
cd booking-service && mvn spring-boot:run -Dspring-boot.run.arguments=--booking.simulator.enabled=true
```

Off by default because every rush uses up seats. Reset them with:
`UPDATE seat_inventory_db.seat_inventory SET status='AVAILABLE', currentBookingId=NULL, lockedUntil=NULL, version=version+1 WHERE status<>'AVAILABLE';`

## What's implemented

- Transactional outbox (booking → Kafka) and Kafka transactions (`read_committed`, exactly-once within Kafka)
- Manual acknowledgment; `DefaultErrorHandler` with exponential backoff (1s, 2s, 4s) for transient failures; poison messages straight to `<topic>-dlt`; DLT consumer that logs failed records
- Idempotent consumers (per-service idempotency table) and `Idempotency-Key` on the REST API
- Optimistic locking (`@Version`) against double booking; booking state machine; saga timeouts (seat lock TTL, stale PENDING bookings)
- Database per service; global exception handler returning RFC 9457 `ProblemDetail`; Swagger (springdoc)
- Failure simulator in payment-service (dev profile): `POST /payment-service/simulate-failure/{NONE|FLAKY|TRANSIENT|POISON}`
- Booking status endpoint `GET /booking-service/bookings/{id}`; read-only seat endpoint `GET /seat-inventory-service/seats`
- Concurrency demo `BookingLoadSimulator`: concurrent rush, HTTP retry ×3 with backoff, `booking-request-dlt` for requests that still fail
