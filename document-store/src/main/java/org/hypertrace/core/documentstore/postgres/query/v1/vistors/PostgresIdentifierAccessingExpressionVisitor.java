package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.JSON_FIELD_ACCESSOR;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

public class PostgresIdentifierAccessingExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {
  private final String baseField;

  public PostgresIdentifierAccessingExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor, final String baseField) {
    super(baseVisitor);
    this.baseField = baseField;
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    final String parsed = expression.accept(baseVisitor);
    final String[] paths = parsed.split(DOC_PATH_SEPARATOR);
    final StringBuilder builder = new StringBuilder(baseField);

    for (final String path : paths) {
      builder.append(JSON_FIELD_ACCESSOR).append("'").append(path).append("'");
    }

    return builder.toString();
  }
}
