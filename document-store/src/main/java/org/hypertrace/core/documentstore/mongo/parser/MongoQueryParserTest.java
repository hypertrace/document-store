package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.query.AllSelection.ALL;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.query.Query;

// TODO: Remove this testing class
public class MongoQueryParserTest {
  public static void main(String[] args) {
    Query query =
        Query.builder()
            .selection(ALL)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("col1"),
                    RelationalOperator.GT,
                    ConstantExpression.of(7)))
            .build();

    FilteringExpression whereFilter = query.getFilter();
    Object parsedResponse = MongoFilteringExpressionParser.getFilterClause(whereFilter);

    // Outputs: {"$match": {"col1": {"$gt": 7}}}
    System.out.println(parsedResponse);
  }
}
