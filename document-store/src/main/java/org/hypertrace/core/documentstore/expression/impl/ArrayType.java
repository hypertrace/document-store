package org.hypertrace.core.documentstore.expression.impl;

import lombok.Getter;

public enum ArrayType {
  TEXT("text[]"),
  INTEGER("integer[]"),
  BOOLEAN("boolean[]"),
  DOUBLE_PRECISION("double precision[]");

  @Getter private final String postgresType;

  ArrayType(String postgresType) {
    this.postgresType = postgresType;
  }
}
