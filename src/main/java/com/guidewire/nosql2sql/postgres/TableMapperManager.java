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

/**
 * Manages the mapping of DynamoDB tables to PostgreSQL tables.
 * This class handles the creation and management of table mappings and column mappings.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TableMapperManager {

  private final Map<String, TableMapping> tableMaps = new HashMap<>();

  /**
   * Escapes the table name to ensure it is valid in PostgreSQL.
   * @param tableName The original table name.
   * @return The escaped table name.
   */
  public static String escapeTableName(String tableName) {
    return tableName.replace("&", "_");
  }

  /**
   * Adds a new table mapping to the manager.
   * @param tableMapping The table mapping to add.
   * @return The added table mapping.
   */
  public TableMapping addTableMapping(TableMapping tableMapping) {
    tableMaps.put(tableMapping.getTableName(), tableMapping);
    return tableMapping;
  }

  /**
   * Retrieves a table mapping by table name.
   * @param tableName The name of the table.
   * @return The table mapping, or null if not found.
   */
  public TableMapping getTableMapping(String tableName) {
    return tableMaps.get(tableName);
  }

  /**
   * Creates new columns for a table mapping based on a JSON node.
   * @param tableMapping The table mapping.
   * @param jsonNode The JSON node containing the data.
   * @return A set of new column mappings.
   */
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

  /**
   * Determines the column data type based on the JSON node value.
   *
   * @param value the JSON node value
   * @return the column data type
   * @throws IllegalArgumentException if the JSON node type is unsupported
   */
  private ColumnDataType determineColumnDataType(JsonNode value) {
    return switch (value.getNodeType()) {
      case BOOLEAN -> ColumnDataType.BOOL;
      case NUMBER -> ColumnDataType.NUMBER;
      case STRING -> ColumnDataType.STRING;
      case OBJECT -> ColumnDataType.JSON;
      default -> throw new IllegalArgumentException("Unsupported type " + value.getNodeType());
    };
  }

  /**
   * Maps a JSON node to a table mapping.
   *
   * @param json the JSON node containing data
   * @param tableName the name of the table
   * @return the table mapping
   */
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

  /**
   * Processes JSON node attributes with the provided consumer, ignoring specified attributes.
   *
   * @param json the JSON node containing data
   * @param ignored a set of attribute names to ignore
   * @param consumer the consumer to process each attribute
   * @return a set of unsupported attribute names
   */
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