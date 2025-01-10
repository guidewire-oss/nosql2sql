package com.guidewire.nosql2sql;

import com.guidewire.nosql2sql.postgres.MappingConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties(value = {MappingConfiguration.class})
@RequiredArgsConstructor
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
}
