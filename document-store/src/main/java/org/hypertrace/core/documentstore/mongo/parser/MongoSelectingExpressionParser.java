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

public class MongoSelectingExpressionParser extends MongoExpressionParser
    implements SelectingExpressionVisitor {

  private static final String PROJECT_CLAUSE = "$project";

  public MongoSelectingExpressionParser(final Query query) {
    super(query);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return new MongoAggregateExpressionParser(query).parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final ConstantExpression expression) {
    return new MongoConstantExpressionParser(query).parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    return new MongoFunctionExpressionParser(query).parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final IdentifierExpression expression) {
    return new MongoIdentifierExpressionParser(query).parse(expression);
  }

  public static BasicDBObject getSelections(final Query query) {
    List<SelectionSpec> selectionSpecs = query.getSelections();
    MongoSelectingExpressionParser parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoNonAggregationSelectingExpressionParser(query));

    Map<String, Object> projectionMap =
        selectionSpecs.stream()
            .map(spec -> MongoSelectingExpressionParser.parse(parser, spec))
            .flatMap(map -> map.entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

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
}
