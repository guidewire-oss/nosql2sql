package com.guidewire.nosql2sql.postgres;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents a mapping of a DynamoDB table to a PostgreSQL table.
 * This class holds the column mappings and unsupported columns for a table.
 */
@Getter
@ToString
public class TableMapping {

  private final String tableName;
  private final Map<String, ColumnMapping> columns = new HashMap<>();
  private final Set<String> unsupportedColumns = new HashSet<>();

  /**
   * Constructs a new table mapping with the specified table name.
   * @param tableName The name of the table.
   */
  public TableMapping(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Adds a column mapping to the table mapping.
   * @param columnMapping The column mapping to add.
   */
  public void addColumn(ColumnMapping columnMapping) {
    columns.put(columnMapping.getColumnName(), columnMapping);
  }

  /**
   * Adds an unsupported column to the table mapping.
   * @param columnName The name of the unsupported column.
   */
  public void addUnsupportedColumn(String columnName) {
    unsupportedColumns.add(columnName);
  }

  public ColumnMapping getColumn(String columnName) {
    return columns.get(columnName);
  }

  public boolean hasColumn(String columnName) {
    return columns.containsKey(columnName) || unsupportedColumns.contains(columnName);
  }

  @Builder
  @Getter
  @ToString
  public static class ColumnMapping {

    private String columnName;
    private ColumnDataType columnType;
  }
}
