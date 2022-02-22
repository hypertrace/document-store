package org.hypertrace.core.documentstore.mongo;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilterTypeExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.parser.MongoFilterTypeExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoGroupTypeExpressionParser.getGroupClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectTypeExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSelectTypeExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortTypeExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.parser.MongoSortTypeExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.mongo.parser.MongoFromTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {
  private static final List<Function<Query, Collection<BasicDBObject>>> aggregatePipelineFunctions =
      List.of(
          query -> Collections.singleton(getFilterClause(query, Query::getFilter)),
          MongoFromTypeExpressionParser::getFromClauses,
          query -> Collections.singleton(getGroupClause(query)),
          query -> Collections.singleton(getProjectClause(query)),
          query -> Collections.singleton(getFilterClause(query, Query::getAggregationFilter)),
          query -> Collections.singleton(getSortClause(query)),
          query -> Collections.singleton(getSkipClause(query)),
          query -> Collections.singleton(getLimitClause(query)));

  final com.mongodb.client.MongoCollection<BasicDBObject> collection;

  public MongoCursor<BasicDBObject> find(final Query query) {
    BasicDBObject filterClause = getFilter(query, Query::getFilter);
    BasicDBObject projection = getSelections(query);

    FindIterable<BasicDBObject> iterable = collection.find(filterClause).projection(projection);

    BasicDBObject sortOrders = getOrders(query);
    if (!sortOrders.isEmpty()) {
      iterable.sort(sortOrders);
    }

    applyPagination(iterable, query);

    logClauses(query, projection, filterClause, sortOrders, query.getPagination().orElse(null));

    return iterable.cursor();
  }

  public MongoCursor<BasicDBObject> aggregate(final Query originalQuery) {
    Query query = transformAndLog(originalQuery);

    List<BasicDBObject> pipeline =
        aggregatePipelineFunctions.stream()
            .flatMap(function -> function.apply(query).stream())
            .filter(not((BasicDBObject::isEmpty)))
            .collect(Collectors.toList());

    logPipeline(pipeline);
    AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);

    return iterable.cursor();
  }

  private void logClauses(
      final Query query,
      final Bson projection,
      final Bson filterClause,
      final Bson sortOrders,
      final Pagination pagination) {
    log.debug(
        "MongoDB find():\nQuery: {}\nCollection: {}\n Projections: {}\n Filter: {}\n Sorting: {}\n Pagination: {}",
        query,
        collection.getNamespace(),
        projection,
        filterClause,
        sortOrders,
        pagination);
  }

  private void logPipeline(final List<BasicDBObject> pipeline) {
    log.debug(
        "MongoDB aggregation():\n Collection: {}\n Pipeline: {}",
        collection.getNamespace(),
        pipeline);
  }

  private Query transformAndLog(Query query) {
    log.debug("MongoDB query before transformation: {}", query);
    query = MongoQueryTransformer.transform(query);
    log.debug("MongoDB query after transformation: {}", query);
    return query;
  }
}
