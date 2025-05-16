package com.guidewire.nosql2sql;

import static org.awaitility.Awaitility.await;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.guidewire.nosql2sql.postgres.PostgresManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.utility.RandomString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
class WhenQueueIsActive {

  @Autowired
  PostgresManager postgresManager;

  @Autowired
  JdbcClient jdbcClient;

  @Autowired
  DynamoSyncController dynamoSyncController;

  @Test
  void streamEntryIsPresentInPostgres() {
    var table = RandomStringUtils.randomAlphabetic(10).toUpperCase();
    var key = RandomStringUtils.randomAlphabetic(10);
    var pk = RandomStringUtils.randomAlphabetic(10);
    var sk = RandomStringUtils.randomAlphabetic(10);
    var eventName = "INSERT";

    var dataRecords = List.of(generateRecord(eventName, pk, sk, table, key));

    dynamoSyncController.acceptData(dataRecords);

    await().until(() -> {
      try {
        return jdbcClient.sql("SELECT * FROM " + table + " WHERE pk = '" + pk + "' LIMIT 1").query().singleRow().containsValue(key);
      } catch (Exception e) {
        log.warn("Retrying. ", e);
      }
      return false;
    });
  }

  private List<Record> getRandomRecords(int n) {
    List<Record> records = new ArrayList<>();
    IntStream.range(0, n).forEach(i -> {
      var table = "SETTING_VALUE";
      var key = "test-helios-settingDef-" + RandomString.make(9);
      var pk = "test-helios-settingDef-pk-" + RandomString.make(9);
      var sk = "test-helios-settingDef-sk-" + RandomString.make(9);
      var eventName = "INSERT";

      records.add(generateRecord(table, key, pk, sk, eventName));
    });

    return records;
  }

  private Record generateRecord(String eventName, String pk, String sk, String table, String key) {
    return new Record()
        .withEventID(RandomString.make(10))
        .withEventName(eventName)
        .withEventVersion("1.1")
        .withEventSource("aws:dynamodb")
        .withAwsRegion("us-east-1")
        .withDynamodb(
            new StreamRecord()
                .withApproximateCreationDateTime(new Date())
                .withKeys(
                    Map.of(
                        "pk", new AttributeValue(pk),
                        "sk", new AttributeValue(sk)
                    )
                )
                .withNewImage(
                    Map.of(
                        "pk", new AttributeValue(pk),
                        "sk", new AttributeValue(sk),
                        "version", new AttributeValue().withN("1"),
                        "recordType", new AttributeValue(table),
                        "key", new AttributeValue(key)
                    )
                )
        );
  }

}
