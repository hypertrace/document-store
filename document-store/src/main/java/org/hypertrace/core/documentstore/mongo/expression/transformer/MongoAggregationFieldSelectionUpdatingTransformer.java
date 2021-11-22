package org.hypertrace.core.documentstore.mongo.expression.transformer;

import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.SelectionSpec;

@AllArgsConstructor
public class MongoAggregationFieldSelectionUpdatingTransformer
    implements SelectingExpressionVisitor {
  private final List<GroupingExpression> aggregations;
  private final SelectionSpec source;

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(AggregateExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(ConstantExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(FunctionExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(IdentifierExpression expression) {
    for (GroupingExpression group : aggregations) {
      if (expression.equals(group)) {
        String key = expression.getName();
        String identifier = MongoCollection.ID_KEY + "." + key;

        return SelectionSpec.of(IdentifierExpression.of(identifier), key);
      }
    }

    return source;
  }
}
