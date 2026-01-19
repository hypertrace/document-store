package org.hypertrace.core.documentstore.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

@Builder
@AllArgsConstructor
public class PostgresColumnMetadata implements ColumnMetadata {

  private final String colName;
  private final DataType canonicalType;
  @Getter private final PostgresDataType postgresType;
  private final boolean nullable;
  private final boolean isArray;

  @Override
  public String getName() {
    return colName;
  }

  @Override
  public DataType getCanonicalType() {
    return canonicalType;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  @Override
  public boolean isArray() {
    return isArray;
  }
}
