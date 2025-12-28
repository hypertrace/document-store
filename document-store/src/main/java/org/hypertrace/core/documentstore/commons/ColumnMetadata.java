package org.hypertrace.core.documentstore.commons;

import org.hypertrace.core.documentstore.expression.impl.DataType;

public interface ColumnMetadata {
  String getName();

  DataType getCanonicalType();

  String getInternalType();

  boolean isNullable();
}
