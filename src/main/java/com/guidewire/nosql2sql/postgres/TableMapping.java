package com.guidewire.nosql2sql.postgres;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class TableMapping {

  private final String tableName;
  private final Map<String, ColumnMapping> columns = new HashMap<>();
  private final Set<String> unsupportedColumns = new HashSet<>();

  public TableMapping(String tableName) {
    this.tableName = tableName;
  }

  public void addColumn(ColumnMapping columnMapping) {
    columns.put(columnMapping.getColumnName(), columnMapping);
  }

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
