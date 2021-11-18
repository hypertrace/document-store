package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.function.Predicate.not;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Projections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoSelectingExpressionParser extends MongoExpressionParser
    implements SelectingExpressionParser {

  private static final String PROJECT_CLAUSE = "$project";

  private final String identifierPrefix;

  public MongoSelectingExpressionParser(final Query query) {
    this(query, false);
  }

  public MongoSelectingExpressionParser(final Query query, final boolean prefixIdentifier) {
    super(query);
    this.identifierPrefix = prefixIdentifier ? "$" : "";
  }

  @Override
  public Map<String, Object> parse(final AggregateExpression expression) {
    return new MongoAggregateExpressionParser(query).parse(expression);
  }

  @Override
  public Object parse(final ConstantExpression expression) {
    return new MongoConstantExpressionParser(query).parse(expression);
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    return new MongoFunctionExpressionParser(query).parse(expression);
  }

  @Override
  public String parse(final IdentifierExpression expression) {
    return identifierPrefix + new MongoIdentifierExpressionParser(query).parse(expression);
  }

  public static List<String> getSelections(final Query query) {
    List<SelectionSpec> selectionSpecs = query.getSelections();
    MongoSelectingExpressionParser parser = new MongoSelectingExpressionParser(query);

    return selectionSpecs.stream()
        .filter(not(SelectionSpec::isAggregation))
        .map(exp -> exp.getExpression().parse(parser).toString())
        .collect(Collectors.toList());
  }

  public static BasicDBObject getProjectClause(final Query query) {
    List<String> selections = getSelections(query);

    if (CollectionUtils.isEmpty(selections)) {
      return new BasicDBObject();
    }

    return new BasicDBObject(PROJECT_CLAUSE, Projections.include(selections));
  }
}
