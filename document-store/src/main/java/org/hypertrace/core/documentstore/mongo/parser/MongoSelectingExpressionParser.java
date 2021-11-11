package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.function.Predicate.not;

import com.mongodb.client.model.Projections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.query.Selection;

public class MongoSelectingExpressionParser implements SelectingExpressionParser {

  @Override
  public Map<String, Object> parse(final AggregateExpression expression) {
    // AggregateExpression will be a part of the '$group' clause
    return Map.of();
  }

  @Override
  public Object parse(final ConstantExpression expression) {
    return MongoConstantExpressionParser.parse(expression);
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    return MongoFunctionExpressionParser.parse(expression);
  }

  @Override
  public String parse(final IdentifierExpression expression) {
    return MongoIdentifierExpressionParser.parse(expression);
  }

  public static Bson getSelections(final List<Selection> selections) {
    // TODO: Handle all
    MongoSelectingExpressionParser parser = new MongoSelectingExpressionParser();
    List<String> selected = selections
        .stream()
        .filter(not(Selection::isAggregation))
        .map(exp -> exp.getExpression().parse(parser).toString())
        .collect(Collectors.toList());

    return Projections.include(selected);
  }
}
