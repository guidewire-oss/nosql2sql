package com.guidewire.nosql2sql.dynamo;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.guidewire.nosql2sql.postgres.PostgresManager;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DynamoSyncingManager {

  private final PostgresManager postgresManager;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  public void startEnqueuing(List<Record> data) {

    CompletableFuture.runAsync(() -> data.forEach(postgresManager::applyAwsRecord), executor)
        .handleAsync((result, err) -> {
          if (err != null) {
            log.error("Err", err);
          }
          return result;
        });
  }
}
