package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.function.UnaryOperator;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PostgresDocumentArrayFilterParserGetter implements PostgresArrayFilterParserGetter {

  @Override
  public UnaryOperator<PostgresSelectTypeExpressionVisitor> getParser(
      final String arraySource, final String alias, final Object rhsValue) {
    // Any LHS field name (elements) is to be prefixed with current alias (inner)
    return baseVisitor -> new PostgresIdentifierAccessingExpressionVisitor(baseVisitor, alias);
  }
}
