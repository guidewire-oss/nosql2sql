package com.guidewire.nosql2sql.postgres;

import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.guidewire.nosql2sql.postgres.TableMapping.ColumnMapping;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Manages the process of applying AWS DynamoDB records to a PostgreSQL database.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PostgresManager {

  private static final String SQL_DEBUG_MESSAGE = "sql = {}";

  private final S3Client s3Client;
  private final JdbcClient jdbcClient;
  private final TableMapperManager tableMapperManager = new TableMapperManager();
  private final MappingConfiguration mappingConfiguration;
  private final ObjectMapper objectMapper;

  /**
   * Applies a single AWS DynamoDB record to the PostgreSQL database.
   * @param rec The DynamoDB record to apply.
   */
  public void applyAwsRecord(Record rec) {
    var jsonOut = JsonNodeFactory.instance.objectNode();
    var image = Optional.ofNullable(rec.getDynamodb().getNewImage()).or(() -> Optional.ofNullable(rec.getDynamodb().getOldImage()));
    image.ifPresent(r -> r.forEach((k, v) -> {
      if (v.getS() != null) {
        jsonOut.put(k, v.getS());
      } else if (v.getBOOL() != null) {
        jsonOut.put(k, v.getBOOL());
      } else if (v.getN() != null) {
        jsonOut.put(k, v.getN());
      } else if (v.getM() != null) {
        jsonOut.set(k, new ObjectMapper().valueToTree(v.getM()));
      } else {
        log.warn("Key: {} with value: {} not supported", k, v);
      }
    }));
    applyToPostgres(jsonOut, convertToApplyType(rec.getEventName()));
  }

  /**
   * Applies a single record from DynamoDB to the PostgreSQL database.
   * @param jsonNode The DynamoDB record in JSON format.
   * @param applyType The type of operation to apply (INSERT, UPDATE, DELETE).
   */
  public void applyToPostgres(JsonNode jsonNode, ApplyType applyType) {

    var tableName = Optional.ofNullable(mappingConfiguration.getDynamodb().getDiscriminatorAttributeName())
        .map(attr -> jsonNode.get(attr).asText())
        .map(TableMapperManager::escapeTableName)
        // when discriminator attribute is not used, postgresql table name will match dynamo
        .orElse(mappingConfiguration.getDynamodb().getDynamoTableName());

    var tableMapping = Optional.ofNullable(tableMapperManager.getTableMapping(tableName))
        .map(tm -> maybeAddColumns(tm, jsonNode))
        .orElseGet(() -> createTable(tableMapperManager.addTableMapping(tableMapperManager.map(jsonNode, tableName))));

    // extract a map of column names and values
    var columns = tableMapping.getColumns().entrySet().stream()
        .filter(e -> jsonNode.has(e.getKey()))
        .map(e -> extractValue(jsonNode, e.getValue()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    JdbcClient.StatementSpec spec = null;
    var sql = "";
    var hasSortKey = mappingConfiguration.getDynamodb().getSortKeyName().isPresent();
    var pk = mappingConfiguration.getDynamodb().getPartitionKeyName();
    var sk = mappingConfiguration.getDynamodb().getSortKeyName().orElse("");
    var whereUniqueSql = " WHERE " + pk + " = " + "'" + columns.get(pk) + "'" + (hasSortKey ? " AND " + sk + " = " + "'" + columns.get(sk) + "'" : "");
    switch (applyType) {
      case INSERT -> {
        this.insertData(columns, tableMapping, sql, tableName, spec);
      }
      case UPDATE -> {
        this.deleteData(tableName, whereUniqueSql, false);
        this.insertData(columns, tableMapping, sql, tableName, spec);
      }
      case DELETE -> {
        this.deleteData(tableName, whereUniqueSql, true);
      }
      default -> log.error("Unknown apply type: {}", applyType);
    }

    // once a dynamo stream is supported, this method could be used to handle inserts, updates, and deletes.

    // note: we want to be able to support batching these inserts. This could be done by creating a row wrapper object containing
    // a list of table rows, each table row identifies the table and a map of column values. The wrapper can be added to a queue
    // and processed async. That would allow batching and parallel threads. Initially, each call to this
    // method just inserts a single row into a single table.

    // Note: Eventually, this could write to multiple tables to support collections of nested documents.
    // the table name would always be based on the primary table + attribute name
    // some limit on max number of nested docs is needed

  }

  private int insertData(Map<String, Object> columns, TableMapping tableMapping, String sql, String tableName, JdbcClient.StatementSpec spec) {
    var columnsNames = String.join(",", columns.keySet());
    var bindVariables = columns.keySet().stream().map(key -> ":" + key + (tableMapping.getColumn(key).getColumnType() == ColumnDataType.JSON ? "::jsonb" : "")).collect(Collectors.joining(","));

    // insert into postgres
    sql = "INSERT INTO " + tableName + " (" + columnsNames + ") VALUES (" + bindVariables + ")";
    spec = jdbcClient.sql(sql).params(columns);

    return this.runSpec(spec, sql, columns, true);
  }

  private int deleteData(String tableName, String whereUniqueSql, boolean logError) {
    var sql = "DELETE FROM " + tableName + whereUniqueSql;
    var spec = jdbcClient.sql(sql);

    return this.runSpec(spec, sql, null, logError);
  }

  private int runSpec(JdbcClient.StatementSpec spec, String sql, Map<String, Object> columns, boolean logError) {
    var returnVal = 1;
    try {
      returnVal = spec.update();
      if (logError && returnVal != 1) {
        log.error("SQL update failed! {} {}", sql, columns);
      }
    } catch (Exception e) {
      log.error("Update failed", e);
    }

    return returnVal;
  }

  private void startSync() {
    var sql = "CREATE TABLE IF NOT EXISTS DELETE_RECORDS (id varchar, sk varchar, pk varchar)";
    jdbcClient.sql(sql).update();
  }

  public TableMapping createTable(TableMapping tableMapping) {
    if (mappingConfiguration.getPostgresql().isRecreateTables()) {
      dropTable(tableMapping.getTableName());
    }
    log.info("Creating table {}", tableMapping);

    var sql = "CREATE TABLE IF NOT EXISTS " + tableMapping.getTableName() + " (" + tableMapping.getColumns().entrySet().stream()
        .map(e -> e.getKey() + " " + e.getValue().getColumnType().getDatabaseType())
        .collect(Collectors.joining(",")) + ")";
    log.debug(SQL_DEBUG_MESSAGE, sql);

    jdbcClient.sql(sql).update();

    return tableMapping;
  }

  public void dropTable(String tableName) {
    log.info("dropping table {}", tableName);
    var sql = "DROP TABLE IF EXISTS " + tableName;
    log.debug(SQL_DEBUG_MESSAGE, sql);
    jdbcClient.sql(sql).update();
  }

  @SneakyThrows
  public Stream<JsonNode> loadFromS3(String bucketName, String s3Prefix) {
    final String s3ExportDataPrefix = mappingConfiguration.getS3().getPrefix().orElse("") + mappingConfiguration.getDynamodb().getDynamoTableName();
    log.info("loading from s3://{}/{}", bucketName, s3ExportDataPrefix);
    if (s3Client.listObjectsV2(b -> b.bucket(bucketName).prefix(s3ExportDataPrefix)).contents().isEmpty()) {
      throw new IllegalArgumentException("No s3 export provided");
    }

    var readerBuilder = IonSystemBuilder.standard().getReaderBuilder();

    var ionObjects = s3Client.listObjectsV2(b -> b.bucket(bucketName).prefix(s3ExportDataPrefix))
        .contents();

    return ionObjects.stream()
        .filter(o -> o.key().endsWith("ion.gz"))
        .flatMap(o -> {
          log.debug("Adding to {} to stream", o.key());
          var inputStream = s3Client.getObjectAsBytes(b -> b.bucket(bucketName).key(o.key())).asInputStream();
          GZIPInputStream zipStream;
          try {
            zipStream = new GZIPInputStream(inputStream);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          var reader = readerBuilder.build(zipStream);
          return Stream.generate(() -> {
                try {
                  if (reader.next() != null) {
                    var sb = new StringBuilder();
                    try (var writer = IonTextWriterBuilder.json().build(sb)) {
                      writer.writeValue(reader);
                      return objectMapper.readTree(sb.toString()).get("Item");
                    }
                  } else {
                    log.debug("No more values to read for key {}", o.key());
                    return null;
                  }
                } catch (Exception e) {
                  log.error("Failed to read from s3 export on key {}", o.key(), e);
                  return null;
                }
              })
              .takeWhile(Objects::nonNull);
        });

  }

//  public Stream<JsonNode> loadFromS3(String bucketName, String s3Prefix) {
//    final String s3ExportDataPrefix = awsProperties.getOptionalPrefix().orElse("") + mappingConfiguration.getDynamoTableName();
//    log.info("loading from s3://{}/{}", bucketName, s3ExportDataPrefix);
//    if (s3Client.listObjectsV2(b -> b.bucket(bucketName).prefix(s3ExportDataPrefix)).contents().isEmpty()) {
//      throw new IllegalArgumentException("No s3 export provided");
//    }
//
//    var readerBuilder = IonSystemBuilder.standard().getReaderBuilder();
//
//    var ionObjects = s3Client.listObjectsV2(b -> b.bucket(bucketName).prefix(s3ExportDataPrefix))
//        .contents();
//
//    return ionObjects.stream()
//        .filter(o -> o.key().endsWith("ion.gz"))
//        .flatMap(o -> {
//          log.debug("Adding to {} to stream", o.key());
//          try (var responseStream = s3Client.getObjectAsBytes(b -> b.bucket(bucketName).key(o.key())).asInputStream();
//              var zipStream = new GZIPInputStream(responseStream);
//              var ionReader = readerBuilder.build(zipStream)) {
//            return Stream.generate(() -> {
//                  try {
//                    if (ionReader.next() != null) {
//                      log.debug("Current type: {}", ionReader.getType());
//                      var sw = new StringWriter();
//                      try(var writer = IonTextWriterBuilder.json().build(sw)) {
//                        writer.writeValue(ionReader);
//                        log.debug("Successfully read and converted ION to JSON for key: {}", o.key());
//                        return objectMapper.readTree(sw.toString()).get("Item");
//                      }
//                    } else {
//                      log.debug("No more values to read for key: {}", o.key());
//                      return null;
//                    }
//                  } catch (Exception e) {
//                    log.error("Failed to read from s3 export for key {}",o.key(), e);
//                    return null;
//                  }
//                })
//                .takeWhile(Objects::nonNull);
//          } catch (Exception e) {
//            log.error("Failed to read from s3 export for key {}",o.key(), e);
//            return Stream.empty();
//          }
//        });
//
//  }

  public List<JsonNode> convertS3ExportToJson(String bucketName, String s3Prefix) {
    log.info("Converting ION to JSON...");
    String s3ExportDataPrefix;
    var jsonList = new ArrayList<JsonNode>();
    var sb = new StringBuilder();

    if (s3Client.listObjectsV2(b -> b.bucket(bucketName)).contents().isEmpty()) {
      throw new IllegalArgumentException("No s3 export provided");
    } else {
      s3ExportDataPrefix = s3Prefix + "/AWSDynamoDB/data/";
    }

    try {

      s3Client.listObjectsV2(b -> b.bucket(bucketName).prefix(s3ExportDataPrefix))
          .contents()
          .forEach(o -> {
            try (var responseStream = s3Client.getObjectAsBytes(b -> b.bucket(bucketName).key(o.key())).asInputStream();
                var zipStream = new GZIPInputStream(responseStream);
                var reader = new BufferedReader(new InputStreamReader(zipStream));
                var jsonWriter = IonTextWriterBuilder.json().build(sb);
                var ionReader = IonSystemBuilder.standard().build().newReader(reader)) {
              while (ionReader.next() != null) {
                jsonWriter.writeValue(ionReader);
                jsonList.add(objectMapper.readTree(sb.toString()).get("Item"));
                sb.delete(0, sb.length());
              }
            } catch (Exception e) {
              log.error("Failed to read from s3 export", e);
            }
          });
      log.info("Conversion completed!");

      return jsonList;
    } catch (S3Exception e) {
      log.error("failed to load export from s3", e);
    }
    return Collections.emptyList();
  }

  private Entry<String, Object> extractValue(JsonNode jsonNode, ColumnMapping column) {
    var value = switch (column.getColumnType()) {
      case STRING -> jsonNode.get(column.getColumnName()).textValue();
      case NUMBER -> jsonNode.get(column.getColumnName()).numberValue();
      case BOOL -> jsonNode.get(column.getColumnName()).booleanValue();
      case JSON -> jsonNode.get(column.getColumnName()).toString();
    };
    return Map.entry(column.getColumnName(), value);
  }

  private TableMapping maybeAddColumns(TableMapping tableMapping, JsonNode jsonNode) {
    var newColumns = tableMapperManager.createNewColumns(tableMapping, jsonNode);
    if (!newColumns.isEmpty()) {
      addColumnsToTable(tableMapping, newColumns);
      newColumns.forEach(tableMapping::addColumn);
    }
    return tableMapping;
  }

  private void addColumnsToTable(TableMapping tableMapping, Set<ColumnMapping> newColumns) {
    newColumns.forEach(columnMapping -> {
      log.info("Adding {} to {}", columnMapping, tableMapping.getTableName());
      var sql = "ALTER TABLE " + tableMapping.getTableName() + " ADD COLUMN " + columnMapping.getColumnName() + " " + columnMapping.getColumnType().getDatabaseType();
      log.debug(SQL_DEBUG_MESSAGE, sql);
      jdbcClient.sql(sql).update();
    });
  }

  private ApplyType convertToApplyType(String eventType) {
    return switch (eventType) {
      case "INSERT" -> ApplyType.INSERT;
      case "MODIFY" -> ApplyType.UPDATE;
      case "REMOVE" -> ApplyType.DELETE;
      default -> throw new IllegalArgumentException("Unsupported apply type: " + eventType);
    };
  }

  public enum ApplyType {
    INSERT,
    UPDATE,
    DELETE
  }

}
