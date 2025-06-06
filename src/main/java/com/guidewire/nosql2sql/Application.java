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
    var context = SpringApplication.run(Application.class, args);
    String mode = context.getEnvironment().getProperty("runtime.mode", "service");
    if ("cli".equalsIgnoreCase(mode)) {
      // CLI command would have already run and completed. Just exit.
      System.exit(SpringApplication.exit(context, () -> 0));
    } else {
      log.info("Running in Service mode");
    }
  }
}
