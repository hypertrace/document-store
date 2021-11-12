package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.function.Predicate.not;

import com.mongodb.BasicDBObject;
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
import org.hypertrace.core.documentstore.query.WhitelistedSelection;

public class MongoSelectingExpressionParser implements SelectingExpressionParser {

  private static final String PROJECT_CLAUSE = "$project";

  private final String identifierPrefix;

  public MongoSelectingExpressionParser() {
    this(false);
  }

  public MongoSelectingExpressionParser(final boolean prefixIdentifier) {
    this.identifierPrefix = prefixIdentifier ? "$" : "";
  }

  @Override
  public Map<String, Object> parse(final AggregateExpression expression) {
    return MongoAggregateExpressionParser.parse(expression);
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
    return identifierPrefix + MongoIdentifierExpressionParser.parse(expression);
  }

  public static Bson getSelections(final List<Selection> selections) {
    if (selections.stream().anyMatch(Selection::allColumnsSelected)) {
      return Projections.include();
    }

    MongoSelectingExpressionParser parser = new MongoSelectingExpressionParser();
    List<String> selected =
        selections.stream()
            .filter(not(Selection::isAggregation))
            .map(exp -> (WhitelistedSelection) exp)
            .map(exp -> exp.getExpression().parse(parser).toString())
            .collect(Collectors.toList());

    return Projections.include(selected);
  }

  public static BasicDBObject getProjectClause(final List<Selection> selections) {
    if (selections.stream().anyMatch(Selection::allColumnsSelected)
        || selections.stream().allMatch(Selection::isAggregation)) {
      return new BasicDBObject();
    }

    return new BasicDBObject(PROJECT_CLAUSE, getSelections(selections));
  }
}
