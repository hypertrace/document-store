package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FieldToPgColumn;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

@NoArgsConstructor
public class PostgresDataAccessorIdentifierExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {

  private Type type = Type.NUMERIC;

  public PostgresDataAccessorIdentifierExpressionVisitor(Type type) {
    this.type = type;
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor, Type type) {
    super(baseVisitor);
    this.type = type;
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresQueryParser postgresQueryParser, Type type) {
    super(postgresQueryParser);
    this.type = type;
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    FieldToPgColumn fieldToPgColumn = getPostgresQueryParser().transformField(expression);

    // If this is a JsonIdentifierExpression, use its type instead of the visitor's type
    Type typeToUse = this.type;
    if (expression instanceof JsonIdentifierExpression) {
      JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression;
      typeToUse = convertJsonTypeToPostgresType(jsonExpr.getJsonType());
    }

    return getPostgresQueryParser()
        .getPgColTransformer()
        .buildFieldAccessorWithCast(fieldToPgColumn, typeToUse);
  }

  private Type convertJsonTypeToPostgresType(String jsonType) {
    switch (jsonType) {
      case "STRING":
        return Type.STRING;
      case "STRING_ARRAY":
        return Type.STRING_ARRAY;
      default:
        throw new IllegalArgumentException("Unsupported JSON type: " + jsonType);
    }
  }
}
