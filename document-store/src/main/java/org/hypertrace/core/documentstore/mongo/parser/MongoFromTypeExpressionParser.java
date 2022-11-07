package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilterTypeExpressionParser.FILTER_CLAUSE;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.JoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.mongo.MongoQueryExecutor;
import org.hypertrace.core.documentstore.mongo.parser.MongoJoinConditionParser.MongoJoinParseResult;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoFromTypeExpressionParser implements FromTypeExpressionVisitor {

  public static final String PRESERVE_NULL_AND_EMPTY_ARRAYS = "preserveNullAndEmptyArrays";

  private static final String UNWIND_CLAUSE = "$unwind";
  private static final String LOOKUP_CLAUSE = "$lookup";

  private static final String PATH_KEY = "path";
  private static final String FROM_KEY = "from";
  private static final String LET_KEY = "let";
  private static final String PIPELINE_KEY = "pipeline";
  private static final String AS_KEY = "as";

  private static final MongoIdentifierPrefixingParser mongoIdentifierPrefixingParser =
      new MongoIdentifierPrefixingParser(new MongoIdentifierExpressionParser());

  @SuppressWarnings("unchecked")
  @Override
  public List<BasicDBObject> visit(final UnnestExpression unnestExpression) {
    String parsedIdentifierExpression =
        mongoIdentifierPrefixingParser.visit(unnestExpression.getIdentifierExpression());
    List<BasicDBObject> objects = new ArrayList<>();
    objects.add(
        new BasicDBObject(
            UNWIND_CLAUSE,
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
  public List<BasicDBObject> visit(final JoinExpression joinExpression) {
    final Optional<MongoJoinParseResult> filterCondition =
        Optional.ofNullable(joinExpression.getJoinCondition())
            .map(MongoJoinConditionParser::getFilter);
    final BasicDBObject lookupObject = new BasicDBObject();

    lookupObject.append(FROM_KEY, joinExpression.getJoiningCollectionName());
    lookupObject.append(AS_KEY, joinExpression.getCollectionAlias());

    final BasicDBObject letObject = new BasicDBObject();
    filterCondition.stream()
        .map(MongoJoinParseResult::getVariables)
        .flatMap(Set::stream)
        .forEach(variable -> letObject.append(encodeKey(variable), PREFIX + variable));
    if (!letObject.isEmpty()) {
      lookupObject.append(LET_KEY, letObject);
    }

    final BasicDBObject matchObject = new BasicDBObject();
    filterCondition.stream()
        .map(MongoJoinParseResult::getParsedFilter)
        .map(Map::entrySet)
        .flatMap(Set::stream)
        .forEach(entry -> matchObject.append(entry.getKey(), entry.getValue()));

    final List<BasicDBObject> pipeline =
        Stream.concat(
                Stream.of(matchObject)
                    .filter(not(BasicDBObject::isEmpty))
                    .map(value -> new BasicDBObject(FILTER_CLAUSE, value)),
                Optional.ofNullable(joinExpression.getSubQuery())
                    .map(MongoQueryExecutor::parsePipeline)
                    .stream()
                    .flatMap(List::stream))
            .collect(toUnmodifiableList());
    lookupObject.append(PIPELINE_KEY, pipeline);

    final List<BasicDBObject> joinPipeline = new ArrayList<>();

    joinPipeline.add(new BasicDBObject(LOOKUP_CLAUSE, lookupObject));
    joinPipeline.add(
        new BasicDBObject(
            UNWIND_CLAUSE,
            new BasicDBObject(PATH_KEY, PREFIX + joinExpression.getCollectionAlias())
                .append(PRESERVE_NULL_AND_EMPTY_ARRAYS, true)));

    return unmodifiableList(joinPipeline);
  }

  @SuppressWarnings("unchecked")
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
