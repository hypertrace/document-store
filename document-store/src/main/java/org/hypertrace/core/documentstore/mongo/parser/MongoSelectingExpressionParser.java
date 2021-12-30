package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.stream.Collectors.toMap;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public abstract class MongoSelectingExpressionParser implements SelectingExpressionVisitor {

  private static final String PROJECT_CLAUSE = "$project";

  protected final MongoSelectingExpressionParser baseParser;

  protected MongoSelectingExpressionParser() {
    this(MongoUnsupportedSelectingExpressionParser.INSTANCE);
  }

  protected MongoSelectingExpressionParser(final MongoSelectingExpressionParser baseParser) {
    this.baseParser = baseParser;
  }

  @Override
  public <T> T visit(final AggregateExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public <T> T visit(final ConstantExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public <T> T visit(final FunctionExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public <T> T visit(final IdentifierExpression expression) {
    return baseParser.visit(expression);
  }

  public static BasicDBObject getSelections(final Query query) {
    List<SelectionSpec> selectionSpecs = query.getSelections();
    MongoSelectingExpressionParser parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoIdentifierExpressionParser(new MongoFunctionExpressionParser()));

    Map<String, Object> projectionMap =
        selectionSpecs.stream()
            .map(spec -> MongoSelectingExpressionParser.parse(parser, spec))
            .flatMap(map -> map.entrySet().stream())
            .collect(
                toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    MongoSelectingExpressionParser::mergeValues));

    return new BasicDBObject(projectionMap);
  }

  public static BasicDBObject getProjectClause(final Query query) {
    BasicDBObject selections = getSelections(query);

    if (selections.isEmpty()) {
      return selections;
    }

    return new BasicDBObject(PROJECT_CLAUSE, selections);
  }

  private static Map<String, Object> parse(
      final MongoSelectingExpressionParser baseParser, final SelectionSpec spec) {
    MongoProjectionSelectingExpressionParser parser =
        new MongoProjectionSelectingExpressionParser(spec.getAlias(), baseParser);

    return spec.getExpression().visit(parser);
  }

  private static <T> T mergeValues(final T first, final T second) {
    if (first.equals(second)) {
      return second;
    }

    throw new IllegalArgumentException(
        String.format(
            "Query contains duplicate aliases with different selections: (%s, %s)", first, second));
  }
}
