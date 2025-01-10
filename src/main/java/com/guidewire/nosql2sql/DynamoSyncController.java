package com.guidewire.nosql2sql;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.guidewire.nosql2sql.dynamo.DynamoExportJob;
import com.guidewire.nosql2sql.dynamo.DynamoSyncingManager;
import java.util.List;
import java.util.concurrent.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class DynamoSyncController {

  private final DynamoExportJob dynamoExportJob;
  private final DynamoSyncingManager dynamoSyncingManager;

  private Future<?> exporter;

  @PostMapping("/api/syncData")
  public void acceptData(@RequestBody List<Record> data) {
    dynamoSyncingManager.startEnqueuing(data);
  }

  @PostMapping("/api/exportTable")
  public ResponseEntity<?> exportTable(String tableName) {
    if (exporter != null && !exporter.isDone()) {
      return ResponseEntity.badRequest().body("Export is already running");
    }
    exporter = dynamoExportJob.startExport(tableName);
    return ResponseEntity.ok().build();
  }

  @PostMapping("/api/import")
  public ResponseEntity<?> importTable() {
    log.info("starting import to postgres");

    var sw = StopWatch.createStarted();
    dynamoSyncingManager.importFromS3();

    sw.stop();
    log.info("import completed in {}", sw.formatTime());
    return ResponseEntity.ok().build();
  }

}
