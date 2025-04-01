package org.hypertrace.core.documentstore.expression.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

/**
 * A simple wrapper for referencing a physical collection/table in the FROM clause.
 */
@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFromExpression implements FromTypeExpression {
  // Currently, this supports joins on the same table/collection only. In the future, we can also add a tableName field here to support joins across tables/collections.
  String alias; // e.g. "leftTable"

  @Override
  public <T> T accept(FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
