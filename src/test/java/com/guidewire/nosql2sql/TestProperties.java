package com.guidewire.nosql2sql;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "testdata")
@Component
@Getter
@Setter
public class TestProperties {

  private String location;
  private List<String> tableNames;
}
