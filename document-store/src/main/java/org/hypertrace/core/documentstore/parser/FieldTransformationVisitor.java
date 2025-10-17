package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;

/**
 * Visitor interface for transforming identifier expressions into database-specific field
 * representations.
 *
 * <p>Implementations should be database-specific (e.g., PostgresFieldTransformationVisitor,
 * MongoFieldTransformationVisitor).
 *
 * @param <T> The return type of the transformation (e.g., FieldToPgColumn for Postgres)
 */
public interface FieldTransformationVisitor<T> {

  /**
   * Visits a regular IdentifierExpression (e.g., "field.nested.path" for nested collections)
   *
   * @param expression The identifier expression
   * @return The transformed field representation
   */
  T visit(IdentifierExpression expression);

  /**
   * Visits a JsonIdentifierExpression (e.g., explicit JSONB column access for flat collections)
   *
   * @param expression The JSON identifier expression
   * @return The transformed field representation
   */
  T visit(JsonIdentifierExpression expression);
}
