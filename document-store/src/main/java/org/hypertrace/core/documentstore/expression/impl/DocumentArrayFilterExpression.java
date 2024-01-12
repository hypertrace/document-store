package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

/**
 * Expression representing a condition for filtering on document array fields
 *
 * <p>Example: If product is an array field (containing documents)<code>
 * ANY(product) [color IN ('Blue', 'Green') AND color != 'Black' AND name = 'Comb']
 * </code> can be constructed as <code>
 *   DocumentArrayFilterExpression.builder()
 *    .operator(ANY)
 *    .arraySource(IdentifierExpression.of("product"))
 *    .filter(
 *      LogicalExpression.and(
 *        RelationalExpression.of(
 *          IdentifierExpression.of("color"),
 *          IN,
 *          ConstantExpression.ofStrings("Blue", "Green")),
 *        RelationalExpression.of(
 *          IdentifierExpression.of("color"),
 *          NEQ,
 *          ConstantExpression.of("Black")),
 *        RelationalExpression.of(
 *          IdentifierExpression.of("name"),
 *          EQ,
 *          ConstantExpression.of("Comb"))
 *      )
 *    .build();
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DocumentArrayFilterExpression implements FilterTypeExpression {
  ArrayOperator operator;

  SelectTypeExpression arraySource;

  FilterTypeExpression filter;

  @SuppressWarnings("unused")
  public static class DocumentArrayFilterExpressionBuilder {
    public DocumentArrayFilterExpression build() {
      Preconditions.checkArgument(operator != null, "array operator is null");
      Preconditions.checkArgument(arraySource != null, "array source is null");
      Preconditions.checkArgument(filter != null, "filter is null");
      return new DocumentArrayFilterExpression(operator, arraySource, filter);
    }
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return String.format("%s(%s) [%s]", operator, arraySource, filter);
  }
}
