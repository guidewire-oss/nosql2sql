package com.guidewire.nosql2sql;


import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@Component
@Slf4j
public class DataLoader {

  @Autowired
  private DynamoDbClient dynamoDbClient;
  @Autowired
  private Environment environment;
  @Autowired
  private TestProperties testProperties;

  public void loadAllToDynamo() {
    // Get the location of test data from environment properties
    Path location = Paths.get(testProperties.getLocation());

    // Use try-with-resources to ensure the directory stream is closed automatically
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(location)) {
      for (Path file : stream) {
        if (file.toFile().isFile()) { // Ensure it's a file and not a directory
          String fullname = file.getFileName().toString();
          String tableName = extractTableName(fullname);
          loadTable(file, tableName);
        }
      }
    } catch (IOException e) {
      // Handle potential I/O exceptions, e.g., logging or rethrowing as a runtime exception
      throw new RuntimeException("Failed to read test data directory", e);
    }
  }

  // Helper method to extract the table name from the filename
  private String extractTableName(String fullname) {
    return fullname.replace(".csv", "").trim();
  }

  private void loadTable(Path file, String tableName) throws IOException {
    createOrReplaceTable(tableName);
    try (var reader = Files.newBufferedReader(file)) {
      var records = CSVFormat.DEFAULT.builder()
          .setHeader()
          .setSkipHeaderRecord(true)
          .build()
          .parse(reader);
      for (var inputRecord : records) {
        var attributeMap = inputRecord.toMap().entrySet().stream()
            .filter(e -> StringUtils.trimToNull(e.getValue()) != null)
            .map(e -> Map.entry(e.getKey(), e.getKey().contains("id") ? AttributeValue.fromN(e.getValue()) : AttributeValue.fromS(e.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        dynamoDbClient.putItem(b -> b.tableName(tableName)
            .item(attributeMap)
        );


      }
    }

  }

  private void createOrReplaceTable(String tableName) {
    if (dynamoDbClient.listTables().tableNames().contains(tableName)) {
      log.info("dropping table {}", tableName);
      dynamoDbClient.deleteTable(b -> b.tableName(tableName));
    }
    boolean singleTable = tableName.equals("single_table");
    log.info("creating table {}", tableName);
    if (singleTable) {
      dynamoDbClient.createTable(b -> b.tableName(tableName)
          .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
          .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
              AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build())
          .billingMode(BillingMode.PAY_PER_REQUEST)
      );
    } else {
      dynamoDbClient.createTable(b -> b.tableName(tableName)
          .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
          .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.N).build())
          .billingMode(BillingMode.PAY_PER_REQUEST)
      );
    }
  }

}
