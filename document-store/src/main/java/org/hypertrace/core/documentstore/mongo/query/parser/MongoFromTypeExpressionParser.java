package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;
import org.hypertrace.core.documentstore.mongo.query.MongoQueryExecutor;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoFromTypeExpressionParser implements FromTypeExpressionVisitor {

  private static final String PATH_KEY = "path";
  public static final String PRESERVE_NULL_AND_EMPTY_ARRAYS = "preserveNullAndEmptyArrays";
  private static final String UNWIND_OPERATOR = "$unwind";
  private static final String LOOKUP_OPERATOR = "$lookup";
  private static final String MATCH_OPERATOR = "$match";
  private static final String REPLACE_ROOT_OPERATOR = "$replaceRoot";
  private static final String EXPR_OPERATOR = "$expr";

  private static final MongoIdentifierPrefixingParser mongoIdentifierPrefixingParser =
      new MongoIdentifierPrefixingParser(new MongoIdentifierExpressionParser());

  private final MongoQueryExecutor mongoQueryExecutor;
  private MongoLetClauseBuilder mongoLetClauseBuilder;

  public MongoFromTypeExpressionParser(MongoQueryExecutor mongoQueryExecutor) {
    this.mongoQueryExecutor = mongoQueryExecutor;
  }

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

  @SuppressWarnings("unchecked")
  @Override
  public List<BasicDBObject> visit(SubQueryJoinExpression subQueryJoinExpression) {
    this.mongoLetClauseBuilder =
        new MongoLetClauseBuilder(subQueryJoinExpression.getSubQueryAlias());

    Query transformedSubQuery =
        MongoQueryTransformer.transform(subQueryJoinExpression.getSubQuery());
    List<BasicDBObject> aggregatePipeline =
        new ArrayList<>(mongoQueryExecutor.convertToAggregatePipeline(transformedSubQuery));

    String joinedResultFieldName =
        getJoinedResultFieldName(subQueryJoinExpression.getSubQueryAlias());
    // Add the lookup stage to join the subquery results with the main collection
    aggregatePipeline.add(createLookupStage(subQueryJoinExpression, joinedResultFieldName));

    // Lookup Stage puts the joined results into an array field. We need to unwind that array field
    // to get the joined results as separate documents.
    aggregatePipeline.add(createUnwindStage(joinedResultFieldName));

    // Replace root with the joined document
    aggregatePipeline.add(createReplaceRootStage(joinedResultFieldName));

    return aggregatePipeline;
  }

  private BasicDBObject createLookupStage(
      SubQueryJoinExpression subQueryJoinExpression, String joinedResultFieldName) {
    BasicDBObject lookupStage = new BasicDBObject();
    BasicDBObject lookupSpec = new BasicDBObject();

    lookupSpec.put("from", mongoQueryExecutor.getCollectionName());
    lookupSpec.put("let", subQueryJoinExpression.getJoinCondition().accept(mongoLetClauseBuilder));
    lookupSpec.put("pipeline", createLookupPipeline(subQueryJoinExpression));
    lookupSpec.put("as", joinedResultFieldName);

    lookupStage.put(LOOKUP_OPERATOR, lookupSpec);
    return lookupStage;
  }

  private List<BasicDBObject> createLookupPipeline(SubQueryJoinExpression subQueryJoinExpression) {
    List<BasicDBObject> pipeline = new ArrayList<>();
    pipeline.add(createMatchStage(subQueryJoinExpression));
    return pipeline;
  }

  private BasicDBObject createMatchStage(SubQueryJoinExpression subQueryJoinExpression) {
    BasicDBObject matchStage = new BasicDBObject();
    BasicDBObject expr = new BasicDBObject();
    expr.put(EXPR_OPERATOR, getFilterClause(subQueryJoinExpression.getJoinCondition()));
    matchStage.put(MATCH_OPERATOR, expr);
    return matchStage;
  }

  private BasicDBObject getFilterClause(FilterTypeExpression joinCondition) {
    final FilterTypeExpressionVisitor parser =
        new MongoFilterTypeExpressionParser(
            MongoRelationalFilterContext.builder()
                .location(FilterLocation.INSIDE_EXPR)
                .lhsParser(
                    new MongoDollarPrefixingIdempotentParser(new MongoIdentifierExpressionParser()))
                .rhsParser(
                    new MongoDollarPrefixingIdempotentParser(
                        new MongoAliasedIdentifierExpressionParser()))
                .build());
    final Map<String, Object> filter = joinCondition.accept(parser);
    return new BasicDBObject(filter);
  }

  private String getJoinedResultFieldName(String subQueryAlias) {
    return "__joined_result_with_" + subQueryAlias;
  }

  private BasicDBObject createUnwindStage(String joinedResultFieldName) {
    BasicDBObject unwindStage = new BasicDBObject();
    BasicDBObject unwindSpec = new BasicDBObject();

    unwindSpec.put(PATH_KEY, MongoUtils.PREFIX + joinedResultFieldName);
    unwindSpec.put(PRESERVE_NULL_AND_EMPTY_ARRAYS, true);

    unwindStage.put(UNWIND_OPERATOR, unwindSpec);
    return unwindStage;
  }

  private BasicDBObject createReplaceRootStage(String joinedResultFieldName) {
    BasicDBObject replaceRootStage = new BasicDBObject();
    BasicDBObject newRoot = new BasicDBObject();

    newRoot.put("newRoot", MongoUtils.PREFIX + joinedResultFieldName);
    replaceRootStage.put(REPLACE_ROOT_OPERATOR, newRoot);

    return replaceRootStage;
  }

  public List<BasicDBObject> getFromClauses(final Query query) {
    MongoFromTypeExpressionParser mongoFromTypeExpressionParser =
        new MongoFromTypeExpressionParser(mongoQueryExecutor);
    return query.getFromTypeExpressions().stream()
        .flatMap(
            fromTypeExpression ->
                ((List<BasicDBObject>) fromTypeExpression.accept(mongoFromTypeExpressionParser))
                    .stream())
        .collect(Collectors.toList());
  }
}
