package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoFromTypeExpressionParser implements FromTypeExpressionVisitor {

  private static final String PATH_KEY = "path";
  public static final String PRESERVE_NULL_AND_EMPTY_ARRAYS = "preserveNullAndEmptyArrays";
  private static final String UNWIND_OPERATOR = "$unwind";

  private static final MongoIdentifierPrefixingParser mongoIdentifierPrefixingParser =
      new MongoIdentifierPrefixingParser(new MongoIdentifierExpressionParser());

  @SuppressWarnings("unchecked")
  @Override
  public List<BasicDBObject> visit(UnnestExpression unnestExpression) {
    String parsedIdentifierExpression =
        mongoIdentifierPrefixingParser.visit(unnestExpression.getIdentifierExpression());
    List<BasicDBObject> objects = new ArrayList<>();
    objects.add(
        new BasicDBObject(
            UNWIND_OPERATOR,
            Map.of(
                PATH_KEY,
                parsedIdentifierExpression,
                PRESERVE_NULL_AND_EMPTY_ARRAYS,
                unnestExpression.isPreserveNullAndEmptyArrays())));

    if (null != unnestExpression.getFilterTypeExpression()) {
      objects.add(
          MongoFilterTypeExpressionParser.getFilterClause(
              unnestExpression.getFilterTypeExpression()));
    }

    return objects;
  }

  public static List<BasicDBObject> getFromClauses(final Query query) {
    MongoFromTypeExpressionParser mongoFromTypeExpressionParser =
        new MongoFromTypeExpressionParser();
    return query.getFromTypeExpressions().stream()
        .flatMap(
            fromTypeExpression ->
                ((List<BasicDBObject>) fromTypeExpression.accept(mongoFromTypeExpressionParser))
                    .stream())
        .collect(Collectors.toList());
  }
}
