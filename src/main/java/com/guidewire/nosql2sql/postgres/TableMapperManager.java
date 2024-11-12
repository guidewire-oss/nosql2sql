package com.guidewire.nosql2sql.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.guidewire.nosql2sql.postgres.TableMapping.ColumnMapping;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TableMapperManager {

  private final Map<String, TableMapping> tableMaps = new HashMap<>();

  public static String escapeTableName(String tableName) {
    return tableName.replace("&", "_");
  }

  public TableMapping addTableMapping(TableMapping tableMapping) {
    tableMaps.put(tableMapping.getTableName(), tableMapping);
    return tableMapping;
  }

  public TableMapping getTableMapping(String tableName) {
    return tableMaps.get(tableName);
  }

  public Set<ColumnMapping> createNewColumns(TableMapping tableMapping, JsonNode jsonNode) {
    Set<ColumnMapping> result = new HashSet<>();
    var unsupported = withAttributeNames(jsonNode, tableMapping.getUnsupportedColumns(), jsonNodeEntry -> {
      if (!tableMapping.hasColumn(jsonNodeEntry.getKey())) {
        result.add(ColumnMapping.builder()
            .columnName(jsonNodeEntry.getKey())
            .columnType(determineColumnDataType(jsonNodeEntry.getValue()))
            .build());
      }
    });
    unsupported.forEach(tableMapping::addUnsupportedColumn);
    return result;
  }

  private ColumnDataType determineColumnDataType(JsonNode value) {
    return switch (value.getNodeType()) {
      case BOOLEAN -> ColumnDataType.BOOL;
      case NUMBER -> ColumnDataType.NUMBER;
      case STRING -> ColumnDataType.STRING;
      case OBJECT -> ColumnDataType.JSON;
      default -> throw new IllegalArgumentException("Unsupported type " + value.getNodeType());
    };

  }

  public TableMapping map(JsonNode json, String tableName) {
    var mapping = new TableMapping(tableName);
    var unsupported = withAttributeNames(json, Set.of(), jsonNodeEntry -> {
      var columnDataType = switch (jsonNodeEntry.getValue().getNodeType()) {
        case BOOLEAN -> ColumnDataType.BOOL;
        case NUMBER -> ColumnDataType.NUMBER;
        case STRING -> ColumnDataType.STRING;
        case OBJECT -> ColumnDataType.JSON;
        default -> throw new IllegalArgumentException("Unsupported type " + jsonNodeEntry.getValue().getNodeType());
      };
      mapping.addColumn(
          ColumnMapping.builder()
              .columnName(jsonNodeEntry.getKey())
              .columnType(columnDataType)
              .build());
    });
    unsupported.forEach(mapping::addUnsupportedColumn);
    return mapping;
  }

  public Set<String> withAttributeNames(JsonNode json, Set<String> ignored, Consumer<Entry<String, JsonNode>> consumer) {
    var unsupportedAttributeNames = new HashSet<String>();
    json.fields().forEachRemaining(jsonNodeEntry -> {
      if (!ignored.contains(jsonNodeEntry.getKey())) {
        if (jsonNodeEntry.getValue().isArray()) {
          log.warn("Skipping unsupported value type attribute: {} type: {}", jsonNodeEntry.getKey(), jsonNodeEntry.getValue().getNodeType().name());
          unsupportedAttributeNames.add(jsonNodeEntry.getKey());
        } else {
          consumer.accept(jsonNodeEntry);
        }
      }
    });
    return unsupportedAttributeNames;
  }

}
