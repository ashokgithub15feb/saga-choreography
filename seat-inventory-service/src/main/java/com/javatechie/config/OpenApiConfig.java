package com.javatechie.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Swagger UI: http://localhost:9292/swagger-ui.html  |  OpenAPI JSON: http://localhost:9292/v3/api-docs
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI seatInventoryServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Seat Inventory Service API")
                .description("Step B of the movie-booking saga: locks seats on BookingCreatedEvent "
                        + "and releases them on payment failure")
                .version("v1"));
    }
}
