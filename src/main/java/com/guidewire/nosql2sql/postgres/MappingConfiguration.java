package com.guidewire.nosql2sql.postgres;

import java.util.Optional;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mapping")
@Data
public class MappingConfiguration {

  private String discriminatorAttributeName;
  private String partitionKeyName;
  private String sortKeyName;
  private String tableName;
  private boolean recreateTables = false;

  public Optional<String> getSortKeyName() {
    return Optional.ofNullable(sortKeyName);
  }
}
