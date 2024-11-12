package com.guidewire.nosql2sql.dynamo;

import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("aws")
@Component
@Getter
@Setter
public class AwsProperties {

  private String tableName;
  private String bucketName;
  private String optionalPrefix;

  public Optional<String> getOptionalPrefix() {
    return Optional.ofNullable(optionalPrefix);
  }

}
