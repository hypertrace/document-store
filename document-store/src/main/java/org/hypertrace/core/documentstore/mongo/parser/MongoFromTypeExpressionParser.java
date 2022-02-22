package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoFromTypeExpressionParser implements FromTypeExpressionVisitor {

  private static final String PATH_KEY = "path";
  private static final String UNWIND_OPERATOR = "$unwind";

  private static final MongoIdentifierPrefixingParser mongoIdentifierPrefixingParser =
      new MongoIdentifierPrefixingParser(new MongoIdentifierExpressionParser());

  @SuppressWarnings("unchecked")
  @Override
  public BasicDBObject visit(UnnestExpression unnestExpression) {
    String parsedIdentifierExpression =
        mongoIdentifierPrefixingParser.visit(unnestExpression.getIdentifierExpression());
    return new BasicDBObject(UNWIND_OPERATOR, Map.of(PATH_KEY, parsedIdentifierExpression));
  }

  public static List<BasicDBObject> getFromClauses(final Query query) {
    MongoFromTypeExpressionParser mongoFromTypeExpressionParser =
        new MongoFromTypeExpressionParser();
    return query.getFromTypeExpressions().stream()
        .map(
            fromTypeExpression ->
                (BasicDBObject) fromTypeExpression.accept(mongoFromTypeExpressionParser))
        .collect(Collectors.toList());
  }
}
