package com.delta.jobtracker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DeltaJobTrackerApplication {

  public static void main(String[] args) {
    SpringApplication.run(DeltaJobTrackerApplication.class, args);
  }
}
