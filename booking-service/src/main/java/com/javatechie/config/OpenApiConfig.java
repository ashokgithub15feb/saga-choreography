package com.javatechie.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Swagger UI: http://localhost:9191/swagger-ui.html  |  OpenAPI JSON: http://localhost:9191/v3/api-docs
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI bookingServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Booking Service API")
                .description("Step A of the movie-booking saga: creates a booking and publishes BookingCreatedEvent")
                .version("v1"));
    }
}
