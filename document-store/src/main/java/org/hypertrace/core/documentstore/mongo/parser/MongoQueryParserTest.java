package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.ConstantExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.query.Query;

// TODO: Remove this testing class
public class MongoQueryParserTest {
  public static void main(String[] args) {
    Query query =
        Query.builder()
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("col1"),
                    RelationalOperator.GT,
                    ConstantExpression.of(7)))
            .build();

    FilteringExpression whereFilter = query.getFilter();
    MongoFilteringExpressionParser parser = new MongoFilteringExpressionParser();
    Object parsedResponse = whereFilter.parse(parser);

    // Outputs: {"$match": {"col1": {"$gt": 7}}}
    System.out.println(parsedResponse);
  }
}
