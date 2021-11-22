package org.hypertrace.core.documentstore.mongo.expression.transformer;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;

import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.SelectionSpec;

@AllArgsConstructor
public class MongoAggregateExpressionSelectionAddingTransformer
    implements SelectingExpressionVisitor {
  private final String alias;

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(AggregateExpression expression) {
    if (expression.getAggregator() == DISTINCT_COUNT) {
      // Since MongoDB doesn't support $distinctCount in aggregations, we convert this to
      // $addToSet function. So, we need to project $size(set) instead of just the alias
      SelectingExpression pairingExpression =
          FunctionExpression.builder()
              .operator(LENGTH)
              .operand(IdentifierExpression.of(alias))
              .build();
      return Optional.of(SelectionSpec.of(pairingExpression, alias));
    }

    return Optional.of(SelectionSpec.of(IdentifierExpression.of(alias)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(ConstantExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(FunctionExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(IdentifierExpression expression) {
    return Optional.empty();
  }
}
