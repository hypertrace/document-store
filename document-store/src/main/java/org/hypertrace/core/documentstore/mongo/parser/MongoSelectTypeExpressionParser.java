package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.stream.Collectors.toMap;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public abstract class MongoSelectTypeExpressionParser implements SelectTypeExpressionVisitor {

  private static final String PROJECT_CLAUSE = "$project";

  protected final MongoSelectTypeExpressionParser baseParser;

  protected MongoSelectTypeExpressionParser() {
    this(MongoUnsupportedSelectTypeExpressionParser.INSTANCE);
  }

  protected MongoSelectTypeExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
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
    MongoSelectTypeExpressionParser parser =
        new MongoIdentifierPrefixingSelectTypeExpressionParser(
            new MongoIdentifierExpressionParser(new MongoFunctionExpressionParser()));

    Map<String, Object> projectionMap =
        selectionSpecs.stream()
            .map(spec -> MongoSelectTypeExpressionParser.parse(parser, spec))
            .flatMap(map -> map.entrySet().stream())
            .collect(
                toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    MongoSelectTypeExpressionParser::mergeValues));

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
      final MongoSelectTypeExpressionParser baseParser, final SelectionSpec spec) {
    MongoProjectionSelectTypeExpressionParser parser =
        new MongoProjectionSelectTypeExpressionParser(spec.getAlias(), baseParser);

    return spec.getExpression().accept(parser);
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
