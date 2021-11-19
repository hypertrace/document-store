package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoAggregateExpressionParser extends MongoExpressionParser {
  private final SelectionSpec source;

  protected MongoAggregateExpressionParser(final Query query, final SelectionSpec source) {
    super(query);
    this.source = source;
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    String key;

    if (expression.getAggregator() == AggregationOperator.DISTINCT_COUNT) {
      // Since there is no direct support for DISTINCT_COUNT (along with aggregations) in MongoDB,
      // we split it into 2 steps:
      //   (a) Add to set (so that we get the distinct values)
      //   (b) Get the size of the set (so that we get the count of distinct values)
      key = "$addToSet";
      addSizeProjectionsList();
    } else {
      key = "$" + expression.getAggregator().name().toLowerCase();
    }

    SelectingExpressionParser parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoSelectingExpressionParser(query));
    Object value = expression.getExpression().parse(parser);
    return Map.of(key, value);
  }

  private void addSizeProjectionsList() {
    if (source == null) {
      return;
    }

    SelectionSpec newSpec =
        SelectionSpec.of(
            FunctionExpression.builder()
                .operator(LENGTH)
                .operand(IdentifierExpression.of(source.getAlias()))
                .build(),
            source.getAlias());

    // Code flow reaching here means we have an aggregation in selection list.
    // So, query.getSelections() will not return an empty list.
    query.getSelections().add(newSpec);
  }
}
