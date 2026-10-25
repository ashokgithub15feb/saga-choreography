-- =====================================================================================
-- Split the shared "javatechie" schema into one database per service.
--
--   booking-service        -> booking_db
--   seat-inventory-service -> seat_inventory_db
--   payment-service        -> (no database)
--
-- Copies only. javatechie is NOT changed or dropped (it also holds tables of other projects).
-- Re-runnable: CREATE ... IF NOT EXISTS + INSERT IGNORE.
-- Run it while booking-service and seat-inventory-service are STOPPED, then start them:
--   mysql -uroot -proot < docs/db/split-database-per-service.sql
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS booking_db;
CREATE DATABASE IF NOT EXISTS seat_inventory_db;

-- ---------------------------------------------------------------- booking_db
CREATE TABLE IF NOT EXISTS booking_db.booking         LIKE javatechie.booking;
CREATE TABLE IF NOT EXISTS booking_db.booking_seatids LIKE javatechie.booking_seatids;
CREATE TABLE IF NOT EXISTS booking_db.booking_seq     LIKE javatechie.booking_seq;
CREATE TABLE IF NOT EXISTS booking_db.outbox_event    LIKE javatechie.outbox_event;
CREATE TABLE IF NOT EXISTS booking_db.booking_processed_event LIKE javatechie.processed_event;

INSERT IGNORE INTO booking_db.booking         SELECT * FROM javatechie.booking;
INSERT IGNORE INTO booking_db.booking_seatids SELECT * FROM javatechie.booking_seatids;
INSERT IGNORE INTO booking_db.outbox_event    SELECT * FROM javatechie.outbox_event;

-- id generator: keep its current value so new bookings don't reuse ids
INSERT INTO booking_db.booking_seq SELECT * FROM javatechie.booking_seq
WHERE NOT EXISTS (SELECT 1 FROM booking_db.booking_seq);

-- idempotency rows written by booking-service (PAYMENT_* rows were shared and are left out on purpose)
INSERT IGNORE INTO booking_db.booking_processed_event
SELECT * FROM javatechie.processed_event
WHERE eventKey LIKE 'SEATS_LOCKED:%' OR eventKey LIKE 'SEAT_RESERVATION_FAILED:%';

-- repair: booking whose PAYMENT_COMPLETED was skipped by the shared-table bug (its seat is RESERVED)
UPDATE booking_db.booking SET status = 'CONFIRMED'
WHERE bookingCode = 'a931ed7d-03ff-4f53-8def-095a63dab4af' AND status = 'SEATS_LOCKED';

-- ---------------------------------------------------------------- seat_inventory_db
CREATE TABLE IF NOT EXISTS seat_inventory_db.seat_inventory  LIKE javatechie.seat_inventory;
CREATE TABLE IF NOT EXISTS seat_inventory_db.processed_event LIKE javatechie.processed_event;

INSERT IGNORE INTO seat_inventory_db.seat_inventory SELECT * FROM javatechie.seat_inventory;

-- idempotency rows written by seat-inventory-service
INSERT IGNORE INTO seat_inventory_db.processed_event
SELECT * FROM javatechie.processed_event
WHERE eventKey LIKE 'BOOKING_CREATED:%' OR eventKey LIKE 'PAYMENT_FAILED:%' OR eventKey LIKE 'PAYMENT_COMPLETED:%';

-- ---------------------------------------------------------------- check
SELECT 'booking_db.booking' AS tbl, COUNT(*) AS cnt FROM booking_db.booking
UNION ALL SELECT 'booking_db.outbox_event', COUNT(*) FROM booking_db.outbox_event
UNION ALL SELECT 'booking_db.booking_processed_event', COUNT(*) FROM booking_db.booking_processed_event
UNION ALL SELECT 'seat_inventory_db.seat_inventory', COUNT(*) FROM seat_inventory_db.seat_inventory
UNION ALL SELECT 'seat_inventory_db.processed_event', COUNT(*) FROM seat_inventory_db.processed_event;

-- In production also give each service its own MySQL user, allowed to touch only its own database:
--   CREATE USER 'booking_svc'@'%' IDENTIFIED BY '...';  GRANT ALL ON booking_db.*        TO 'booking_svc'@'%';
--   CREATE USER 'seat_svc'@'%'    IDENTIFIED BY '...';  GRANT ALL ON seat_inventory_db.* TO 'seat_svc'@'%';
