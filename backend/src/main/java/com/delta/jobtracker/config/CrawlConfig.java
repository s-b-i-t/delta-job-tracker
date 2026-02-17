package com.delta.jobtracker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class CrawlConfig {

    @Bean(name = "crawlExecutor", destroyMethod = "shutdown")
    public ExecutorService crawlExecutor(CrawlerProperties properties) {
        return Executors.newFixedThreadPool(properties.getGlobalConcurrency());
    }

    @Bean(name = "crawlRunExecutor", destroyMethod = "shutdown")
    public ExecutorService crawlRunExecutor() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
