package org.hypertrace.core.documentstore.commons;

import org.hypertrace.core.documentstore.expression.impl.DataType;

public interface ColumnMetadata {

  /**
   * @return the col name
   */
  String getName();

  /**
   * @return the col's canonical type, as defined here: {@link DataType}
   */
  DataType getCanonicalType();

  /**
   * @return whether this column can be set to NULL
   */
  boolean isNullable();
}
