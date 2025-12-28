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
  private final String pgType;
  private final boolean nullable;

  @Override
  public String getName() {
    return colName;
  }

  @Override
  public DataType getCanonicalType() {
    return canonicalType;
  }

  @Override
  public String getInternalType() {
    return pgType;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }
}
