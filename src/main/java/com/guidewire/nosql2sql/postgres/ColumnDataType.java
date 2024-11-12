package com.guidewire.nosql2sql.postgres;

import lombok.Getter;

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
