package org.hypertrace.core.documentstore.expression.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

/**
 * Expression representing a join operation where the right side expression is a subquery. Note that
 * this currently supports a self-join only, so the collection to be joined with is implicit.
 */
@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SubQueryJoinExpression implements FromTypeExpression {
  Query subQuery;
  String subQueryAlias;
  FilterTypeExpression joinCondition;

  @Override
  public <T> T accept(FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
