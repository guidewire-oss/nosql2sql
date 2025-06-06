package com.guidewire.nosql2sql;

import com.guidewire.nosql2sql.dynamo.DynamoSyncingManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

@Slf4j
@ShellComponent
public class CliCommands {

  private final DynamoSyncingManager dynamoSyncingManager;

  public CliCommands(DynamoSyncingManager dynamoSyncingManager) {
    this.dynamoSyncingManager = dynamoSyncingManager;
  }

  @ShellMethod(value = "Import table", key = "import")
  public String importTable() {
    log.info("starting import to postgres");

    try {
      var sw = StopWatch.createStarted();
      dynamoSyncingManager.importFromS3();
      sw.stop();

      return "import completed in " + sw.formatTime();
    } catch (Exception e) {
      log.error("import failed", e);
      return "import failed: " + e.getMessage();
    }
  }
}
