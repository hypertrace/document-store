package org.hypertrace.core.documentstore.mongo.query;

import static java.lang.Long.parseLong;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.COUNT_ALIAS;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.getCountClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFromTypeExpressionParser.getFromClauses;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoGroupTypeExpressionParser.getGroupClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoFromTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {
  private static final List<Function<Query, Collection<BasicDBObject>>>
      AGGREGATE_PIPELINE_FUNCTIONS =
          List.of(
              query -> singleton(getFilterClause(query, Query::getFilter)),
              MongoFromTypeExpressionParser::getFromClauses,
              query -> getPostUnwindingFilterClause(query, getFromClauses(query)),
              query -> singleton(getGroupClause(query)),
              query -> singleton(getProjectClause(query)),
              query -> singleton(getFilterClause(query, Query::getAggregationFilter)),
              query -> singleton(getSortClause(query)),
              query -> singleton(getSkipClause(query)),
              query -> singleton(getLimitClause(query)));

  private static Set<BasicDBObject> getPostUnwindingFilterClause(
      final Query query, final List<BasicDBObject> fromClauses) {
    return fromClauses.isEmpty() ? emptySet() : singleton(getFilterClause(query, Query::getFilter));
  }

  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;

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
        AGGREGATE_PIPELINE_FUNCTIONS.stream()
            .flatMap(function -> function.apply(query).stream())
            .filter(not(BasicDBObject::isEmpty))
            .collect(Collectors.toList());

    logPipeline(pipeline);

    try {
      final AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);
      return iterable.cursor();
    } catch (final MongoCommandException e) {
      log.error("Execution failed for query: {}. Aggregation Pipeline: {}", query, pipeline);
      throw e;
    }
  }

  public long count(final Query originalQuery) {
    final Query query = transformAndLog(originalQuery);

    final List<BasicDBObject> pipeline =
        Stream.concat(
                AGGREGATE_PIPELINE_FUNCTIONS.stream()
                    .flatMap(function -> function.apply(query).stream()),
                Stream.of(getCountClause()))
            .filter(not(BasicDBObject::isEmpty))
            .collect(Collectors.toList());

    logPipeline(pipeline);
    final AggregateIterable<BasicDBObject> iterable = collection.aggregate(pipeline);

    try (final MongoCursor<BasicDBObject> cursor = iterable.cursor()) {
      if (cursor.hasNext()) {
        return parseLong(cursor.next().get(COUNT_ALIAS).toString());
      }
    }

    return 0;
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
