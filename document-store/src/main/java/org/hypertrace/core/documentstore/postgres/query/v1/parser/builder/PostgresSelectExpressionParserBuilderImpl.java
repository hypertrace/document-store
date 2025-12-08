package org.hypertrace.core.documentstore.postgres.query.v1.parser.builder;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getType;

import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresConstantExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresDataAccessorIdentifierExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFieldIdentifierExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFunctionExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;

public class PostgresSelectExpressionParserBuilderImpl
    implements PostgresSelectExpressionParserBuilder {

  private final PostgresQueryParser postgresQueryParser;

  public PostgresSelectExpressionParserBuilderImpl(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
  }

  @Override
  public PostgresSelectTypeExpressionVisitor build(final RelationalExpression expression) {
    switch (expression.getOperator()) {
      case CONTAINS:
      case NOT_CONTAINS:
      case EXISTS:
      case NOT_EXISTS:
      case IN:
      case NOT_IN:
        return new PostgresFunctionExpressionVisitor(
            new PostgresFieldIdentifierExpressionVisitor(this.postgresQueryParser));

      case EQ:
      case NEQ:
        // For EQ/NEQ on array fields, treat like CONTAINS to use -> instead of ->>
        if (shouldSwitchToContainsFlow(expression)) {
          // Use field identifier (JSON accessor ->) for array fields
          return new PostgresFunctionExpressionVisitor(
              new PostgresFieldIdentifierExpressionVisitor(this.postgresQueryParser));
        }
      // Fall through to default for non-array fields
      default:
        return new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                this.postgresQueryParser,
                getType(expression.getRhs().accept(new PostgresConstantExpressionVisitor()))));
    }
  }

  /**
   * Checks if this is an EQ/NEQ operator on an array field.
   *
   * <p>Only converts to CONTAINS when RHS is a scalar value. If RHS is an array, we want exact
   * equality match, not containment.
   *
   * <p>Handles both:
   *
   * <ul>
   *   <li>{@link JsonIdentifierExpression} with array field type (JSONB arrays)
   *   <li>{@link ArrayIdentifierExpression} with array type (top-level array columns)
   * </ul>
   */
  private boolean shouldSwitchToContainsFlow(final RelationalExpression expression) {
    if (expression.getOperator() != EQ && expression.getOperator() != NEQ) {
      return false;
    }

    // Check if RHS is an array/iterable - if so, don't convert (since we want an exact match for
    // such cases)
    if (expression.getRhs() instanceof ConstantExpression) {
      ConstantExpression constExpr = (ConstantExpression) expression.getRhs();
      if (constExpr.getValue() instanceof Iterable) {
        return false;
      }
    }

    return isArrayField(expression.getLhs());
  }

  private boolean isArrayField(final SelectTypeExpression lhs) {
    if (lhs instanceof JsonIdentifierExpression) {
      JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) lhs;
      return jsonExpr
          .getFieldType()
          .map(
              fieldType ->
                  fieldType == JsonFieldType.BOOLEAN_ARRAY
                      || fieldType == JsonFieldType.STRING_ARRAY
                      || fieldType == JsonFieldType.NUMBER_ARRAY
                      || fieldType == JsonFieldType.OBJECT_ARRAY)
          .orElse(false);
    }
    return lhs instanceof ArrayIdentifierExpression;
  }
}
