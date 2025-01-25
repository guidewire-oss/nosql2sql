package com.guidewire.nosql2sql.postgres;

import lombok.Getter;

/**
 * Represents the data types that can be used for columns in the PostgreSQL database.
 * Each enum constant corresponds to a specific PostgreSQL data type.
 */
@Getter
public enum ColumnDataType {
  STRING("varchar"),
  NUMBER("numeric"),
  BOOL("boolean"),
  JSON("jsonb");

  private final String databaseType;

  ColumnDataType(String databaseType) {
    this.databaseType = databaseType;
  }
}