package com.guidewire.nosql2sql.dynamo;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeExportRequest;
import software.amazon.awssdk.services.dynamodb.model.ExportFormat;
import software.amazon.awssdk.services.dynamodb.model.ExportStatus;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeRequest;

@Component
@Slf4j
@RequiredArgsConstructor
@Getter
@Setter
public class DynamoExportJob {

  private final AwsProperties awsProperties;
  private final DynamoDbClient dynamoDbClient;
  private final ExecutorService exportExecutor = Executors.newSingleThreadExecutor();
  private String s3ExportArn;
  private Instant exportTime;

  @PostConstruct
  private void setAwsEnvironment() {
    exportTime = Instant.now();
  }

  public CompletableFuture<Void> startExport(String tableName) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        exportToS3(tableName);
        return null;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }, exportExecutor);
  }

  private void exportToS3(String tableName) throws InterruptedException {

    var tableArn = dynamoDbClient.describeTable(b -> b.tableName(tableName)).table().tableArn();

    var exportRequest = ExportTableToPointInTimeRequest.builder()
        .tableArn(tableArn)
        .s3Bucket(awsProperties.getBucketName())
        .exportFormat(ExportFormat.ION)
        .exportTime(Instant.now())
        .build();

    log.info("Beginning export...");
    var response = dynamoDbClient.exportTableToPointInTime(exportRequest);
    s3ExportArn = response.exportDescription().exportArn();
    log.info("Export arn: {}", s3ExportArn);

    ExportStatus exportStatus = response.exportDescription().exportStatus();
    while (exportStatus == ExportStatus.IN_PROGRESS) {
      log.info("Waiting for export to finish...");
      exportStatus = dynamoDbClient.describeExport(DescribeExportRequest.builder().exportArn(s3ExportArn).build()).exportDescription().exportStatus();
      TimeUnit.SECONDS.sleep(5);
    }

    if (exportStatus == ExportStatus.FAILED) {
      log.info("Export failed: {}", response.exportDescription().failureMessage());
    } else {
      log.info("Export completed!");
    }
  }
}
