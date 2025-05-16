package com.guidewire.nosql2sql;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.guidewire.nosql2sql.dynamo.DynamoExportJob;
import com.guidewire.nosql2sql.postgres.MappingConfiguration;
import com.guidewire.nosql2sql.postgres.PostgresManager;
import com.guidewire.nosql2sql.postgres.PostgresManager.ApplyType;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExportDescription;
import software.amazon.awssdk.services.dynamodb.model.ExportStatus;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeRequest;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
class ApplicationTests {

  private final String bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
  @Autowired
  DynamoExportJob dynamoExportJob;
  @Autowired
  private PostgresManager postgresManager;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private JdbcClient jdbcClient;
  @Autowired
  private S3Client s3Client;
  @Autowired
  private DynamoDbClient dynamoDbClient;
  @Autowired
  private DataLoader dataLoader;
  @Autowired
  private TestProperties testProperties;
  @Qualifier("mappingConfiguration")
  @Autowired
  private MappingConfiguration mappingConfiguration;

  @BeforeAll
  void setup() {
    s3Client.createBucket(b -> b.bucket(bucketName));
    dataLoader.loadAllToDynamo();
  }

  private ExportTableToPointInTimeResponse mockExport() {
    var file = new File("src/test/resources/data/s3/output.ion.gz");
    s3Client.putObject(b -> b.bucket(bucketName).key("AWSDynamoDB/data/output.ion.gz"), RequestBody.fromFile(file));

    return ExportTableToPointInTimeResponse.builder()
        .exportDescription(ExportDescription.builder()
            .exportArn("arn:aws:dynamodb:us-east-1:123456789012:table/test/export/id")
            .exportStatus(ExportStatus.COMPLETED)
            .build())
        .build();
  }

  //  @Test
  // doesn't work with localstack
  void pointInTimeExport() {
    var exportJon = new DynamoExportJob(mappingConfiguration, dynamoDbClient);
    testProperties.getTableNames().forEach(tableName -> {
      try {
        exportJon.startExport(tableName).handle((result, err) -> {
          if (err != null) {
            throw new RuntimeException(err);
          }
          return result;
        }).get(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }
    });

  }

  @Test
  void importSingleTable() {

  }

  @Test
  void applyDoesNotIncludeArray() throws Exception {
    var table = RandomStringUtils.randomAlphabetic(10);

    var input = String.format("""
        {
          "recordType": "%s",
          "columnA": "value",
          "numberColumn": 1,
          "invalidArray": ["a"],
          "jsonColumn": {
            "a": 1
          }
        }
        """, table);

    postgresManager.applyToPostgres(objectMapper.readTree(input), ApplyType.INSERT);

    Assertions.assertFalse(jdbcClient.sql("SELECT * FROM " + table + ";").query().singleRow().containsKey("invalidArray"));

    postgresManager.dropTable(table.toLowerCase());
  }

  @Test
  void applyToPostgresSucceedsWithValidTypes() throws IOException {
    var table = RandomStringUtils.randomAlphabetic(10);
    var input = String.format("""
        {
          "recordType": "%s",
          "columnA": "value",
          "numberColumn": 1,
          "jsonColumn": {
            "a": 1
          }
        }""", table);

    postgresManager.applyToPostgres(objectMapper.readTree(input), ApplyType.INSERT);

    Assertions.assertEquals(1, jdbcClient.sql("SELECT * FROM " + table + ";").query().listOfRows().size());

    postgresManager.dropTable(table.toLowerCase());
  }

  @Test
  void testS3Conversion() throws Exception {
    DynamoDbClient spyDynamoDbClient = spy(dynamoDbClient);
    doReturn(mockExport())
        .when(spyDynamoDbClient)
        .exportTableToPointInTime(any(ExportTableToPointInTimeRequest.class));

    dynamoExportJob = new DynamoExportJob(mappingConfiguration, spyDynamoDbClient);

    var dynamoTableName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    dynamoDbClient.createTable(b ->
        b.tableName(dynamoTableName)
            .provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L))
            .keySchema(
                KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("pk").build(),
                KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("sk").build())
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()
            ));

    dynamoDbClient.putItem(b -> b.tableName(dynamoTableName)
        .item(Map.of(
            "pk", AttributeValue.fromS(RandomStringUtils.randomAlphabetic(10).toLowerCase()),
            "sk", AttributeValue.fromS(RandomStringUtils.randomAlphabetic(10).toLowerCase()),
            "key", AttributeValue.fromS(RandomStringUtils.randomAlphabetic(10))
        )));

    dynamoExportJob.startExport(dynamoTableName).handle((result, err) -> {
      if (err != null) {
        throw new RuntimeException(err);
      }
      return result;
    }).get(1, TimeUnit.MINUTES);

    Assertions.assertFalse(s3Client.listObjectsV2(b -> b.bucket(bucketName)).contents().isEmpty());

    Assertions.assertDoesNotThrow(() -> postgresManager.convertS3ExportToJson(bucketName, ""));
  }

}
