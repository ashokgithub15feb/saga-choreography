package com.javatechie.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Swagger UI: http://localhost:9393/swagger-ui.html  |  OpenAPI JSON: http://localhost:9393/v3/api-docs
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI paymentServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Payment Service API")
                .description("Step C of the movie-booking saga: simulated payment gateway "
                        + "(failure simulator endpoints are available with the dev profile)")
                .version("v1"));
    }
}
