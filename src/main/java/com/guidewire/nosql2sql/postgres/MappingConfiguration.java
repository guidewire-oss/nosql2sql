package com.guidewire.nosql2sql.postgres;

import java.util.Optional;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration class for mapping settings between DynamoDB and PostgreSQL.
 * This class holds the configuration properties required for the mapping process.
 */
@Component
@ConfigurationProperties(prefix = "mapping")
@Data
public class MappingConfiguration {

  private Postgresql postgresql;
  private Dynamodb dynamodb;
  private S3 s3;

  /**
   * Discriminator used to identify different record types. Each record type will be imported into a different table in PostgreSQL.
   * This should only be set when the table being exported uses a single table design.
   */
  @Deprecated
  private String discriminatorAttributeName;
  /**
   * Name of the attribute used for the partition (hash) key
   */
  @Deprecated
  private String partitionKeyName;
  /**
   * Name of the attribute used for the sort (range) key
   */
  @Deprecated
  private String sortKeyName;

  /**
   * Not used
   */
  @Deprecated
  private String tableName;
  /**
   * Name of the DynamoDB table to export
   */
  @Deprecated
  private String dynamoTableName;
  /**
   * If true, the tables in PostgreSQL will be recreated.
   */
  @Deprecated
  private boolean recreateTables = false;

  public Optional<String> getSortKeyName() {
    return Optional.ofNullable(sortKeyName);
  }

  @Data
  public static class Postgresql {

    /**
     * If true, the tables in PostgreSQL will be recreated.
     */
    private boolean recreateTables = false;

  }

  @Data
  public static class S3 {

    /**
     * Name of the S3 bucket where the export will be stored
     */
    private String bucketName;
    /**
     * Optional prefix within the S3 bucket
     */
    private Optional<String> prefix;

  }

  @Data
  public static class Dynamodb {

    /**
     * Discriminator used to identify different record types. Each record type will be imported into a different table in PostgreSQL.
     * This should only be set when the table being exported uses a single table design.
     */
    private String discriminatorAttributeName;
    /**
     * Name of the attribute used for the partition (hash) key
     */
    private String partitionKeyName;
    /**
     * Name of the attribute used for the sort (range) key
     */
    private Optional<String> sortKeyName;
    /**
     * Name of the DynamoDB table to export
     */
    private String dynamoTableName;

  }
}
