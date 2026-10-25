package com.javatechie;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // SeatLockExpiryScheduler
public class SeatInventoryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SeatInventoryServiceApplication.class, args);
	}

}
