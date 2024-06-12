package org.hypertrace.core.documentstore.mongo.query;

import static java.lang.Long.parseLong;
import static java.util.Collections.singleton;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.COUNT_ALIAS;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.getCountClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoGroupTypeExpressionParser.getGroupClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoNonProjectedSortTypeExpressionParser.getNonProjectedSortClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoFromTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {

  private static final List<Function<Query, Collection<BasicDBObject>>>
      DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS =
          List.of(
              query -> singleton(getFilterClause(query, Query::getFilter)),
              MongoFromTypeExpressionParser::getFromClauses,
              query -> singleton(getGroupClause(query)),
              query -> singleton(getProjectClause(query)),
              query -> singleton(getFilterClause(query, Query::getAggregationFilter)),
              query -> singleton(getSortClause(query)),
              query -> singleton(getSkipClause(query)),
              query -> singleton(getLimitClause(query)));

  private static final List<Function<Query, Collection<BasicDBObject>>>
      SORT_OPTIMISED_AGGREGATE_PIPELINE_FUNCTIONS =
          List.of(
              query -> singleton(getFilterClause(query, Query::getFilter)),
              MongoFromTypeExpressionParser::getFromClauses,
              query -> singleton(getNonProjectedSortClause(query)),
              query -> singleton(getSkipClause(query)),
              query -> singleton(getLimitClause(query)),
              query -> singleton(getProjectClause(query)));

  private static final Integer ZERO = Integer.valueOf(0);
  private static final MongoCursor<BasicDBObject> EMPTY_CURSOR =
      new MongoCursor<>() {
        @Override
        public void close() {
          // Do nothing
        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public BasicDBObject next() {
          throw new NoSuchElementException();
        }

        @Override
        public int available() {
          return 0;
        }

        @Override
        public BasicDBObject tryNext() {
          throw new NoSuchElementException();
        }

        @Override
        public ServerCursor getServerCursor() {
          // It is okay to throw exception since we are never invoking this method
          throw new UnsupportedOperationException();
        }

        @Override
        public ServerAddress getServerAddress() {
          // It is okay to throw exception since we are never invoking this method
          throw new UnsupportedOperationException();
        }
      };

  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;
  private final ConnectionConfig connectionConfig;

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
    if (originalQuery.getPagination().map(Pagination::getLimit).map(ZERO::equals).orElse(false)) {
      log.debug("Not executing query because of a 0 limit");
      return EMPTY_CURSOR;
    }

    Query query = transformAndLog(originalQuery);

    List<Function<Query, Collection<BasicDBObject>>> aggregatePipeline =
        DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS;
    if (connectionConfig.isSortOptimizedQueryEnabled()
        && query.getAggregations().isEmpty()
        && query.getAggregationFilter().isEmpty()
        && !isSortContainsAggregation(query)
        && !isProjectionContainsAggregation(query)) {
      aggregatePipeline = SORT_OPTIMISED_AGGREGATE_PIPELINE_FUNCTIONS;
    }

    List<BasicDBObject> pipeline =
        aggregatePipeline.stream()
            .flatMap(function -> function.apply(query).stream())
            .filter(not(BasicDBObject::isEmpty))
            .collect(toList());

    logPipeline(pipeline);

    try {
      final AggregateIterable<BasicDBObject> iterable =
          collection.aggregate(pipeline).allowDiskUse(true);
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
                DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS.stream()
                    .flatMap(function -> function.apply(query).stream()),
                Stream.of(getCountClause()))
            .filter(not(BasicDBObject::isEmpty))
            .collect(toList());

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
        "MongoDB find():\nQuery: {}\nCollection: {}\n Projections: {}\n Filter: {}\n Sorting: "
            + "{}\n Pagination: {}",
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

  private boolean isProjectionContainsAggregation(Query query) {
    return query.getSelections().stream()
        .anyMatch(
            selectionSpec ->
                selectionSpec.getExpression().getClass().equals(AggregateExpression.class));
  }

  private boolean isSortContainsAggregation(Query query) {
    Map<String, List<SelectionSpec>> aliasToSelectionMap =
        query.getSelections().stream().collect(Collectors.groupingBy(this::getAlias, toList()));
    return query.getSorts().stream()
        .anyMatch(
            sort ->
                sort.getExpression().getClass().equals(FunctionExpression.class)
                    || sort.getExpression().getClass().equals(AggregateExpression.class)
                    || isSortOnAggregatedProjection(aliasToSelectionMap, sort));
  }

  private String getAlias(SelectionSpec selectionSpec) {
    return selectionSpec.getAlias() != null
        ? selectionSpec.getAlias()
        : ((IdentifierExpression) selectionSpec.getExpression()).getName();
  }

  private boolean isSortOnAggregatedProjection(
      Map<String, List<SelectionSpec>> aliasToSelectionMap, SortingSpec sort) {
    List<SelectionSpec> selectionSpecs =
        aliasToSelectionMap.get(((IdentifierExpression) sort.getExpression()).getName());
    return selectionSpecs != null
        && selectionSpecs.stream()
            .anyMatch(
                selectionSpec ->
                    selectionSpec.getExpression().getClass().equals(FunctionExpression.class)
                        || selectionSpec
                            .getExpression()
                            .getClass()
                            .equals(AggregateExpression.class));
  }
}
