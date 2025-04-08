package org.hypertrace.core.documentstore.mongo.query;

import static java.lang.Long.parseLong;
import static java.util.Collections.singleton;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.model.options.QueryOptions.DEFAULT_QUERY_OPTIONS;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.COUNT_ALIAS;
import static org.hypertrace.core.documentstore.mongo.clause.MongoCountClauseSupplier.getCountClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.applyPagination;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilter;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoNonProjectedSortTypeExpressionParser.getNonProjectedSortClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getSelections;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getOrders;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.mongo.collection.MongoCollectionOptionsApplier;
import org.hypertrace.core.documentstore.mongo.query.parser.AliasParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoFromTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoGroupTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.transformer.MongoQueryTransformer;
import org.hypertrace.core.documentstore.parser.AggregateExpressionChecker;
import org.hypertrace.core.documentstore.parser.FunctionExpressionChecker;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;

@Slf4j
@AllArgsConstructor
public class MongoQueryExecutor {
  private final List<Function<Query, Collection<BasicDBObject>>>
      DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS =
          List.of(
              query -> singleton(getFilterClause(query, Query::getFilter)),
              query -> new MongoFromTypeExpressionParser(this).getFromClauses(query),
              MongoGroupTypeExpressionParser::getGroupClauses,
              query -> singleton(getProjectClause(query)),
              query -> singleton(getFilterClause(query, Query::getAggregationFilter)),
              query -> singleton(getSortClause(query)),
              query -> singleton(getSkipClause(query)),
              query -> singleton(getLimitClause(query)));

  private final List<Function<Query, Collection<BasicDBObject>>>
      SORT_OPTIMISED_AGGREGATE_PIPELINE_FUNCTIONS =
          List.of(
              query -> singleton(getFilterClause(query, Query::getFilter)),
              query -> new MongoFromTypeExpressionParser(this).getFromClauses(query),
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

  private final MongoCollectionOptionsApplier optionsApplier = new MongoCollectionOptionsApplier();
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
    return aggregate(originalQuery, DEFAULT_QUERY_OPTIONS);
  }

  public MongoCursor<BasicDBObject> aggregate(
      final Query originalQuery, final QueryOptions queryOptions) {
    if (originalQuery.getPagination().map(Pagination::getLimit).map(ZERO::equals).orElse(false)) {
      log.debug("Not executing query because of a 0 limit");
      return EMPTY_CURSOR;
    }

    Query query = transformAndLog(originalQuery);

    List<BasicDBObject> pipeline = convertToAggregatePipeline(query);

    logPipeline(pipeline, queryOptions);

    try {
      final com.mongodb.client.MongoCollection<BasicDBObject> collectionWithOptions =
          optionsApplier.applyOptions(connectionConfig, queryOptions, collection);
      final Duration queryTimeout = queryTimeout(connectionConfig, queryOptions);

      final AggregateIterable<BasicDBObject> iterable =
          collectionWithOptions
              .aggregate(pipeline)
              .maxTime(queryTimeout.toMillis(), MILLISECONDS)
              .allowDiskUse(true);

      return iterable.cursor();
    } catch (final Exception e) {
      log.error("Execution failed for query: {}. Aggregation Pipeline: {}", query, pipeline);
      throw e;
    }
  }

  public long count(final Query originalQuery) {
    return count(originalQuery, DEFAULT_QUERY_OPTIONS);
  }

  public long count(final Query originalQuery, final QueryOptions queryOptions) {
    final Query query = transformAndLog(originalQuery);

    final List<BasicDBObject> pipeline =
        Stream.concat(
                DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS.stream()
                    .flatMap(function -> function.apply(query).stream()),
                Stream.of(getCountClause()))
            .filter(not(BasicDBObject::isEmpty))
            .collect(toList());

    logPipeline(pipeline, queryOptions);
    final com.mongodb.client.MongoCollection<BasicDBObject> collectionWithOptions =
        optionsApplier.applyOptions(connectionConfig, queryOptions, collection);
    final Duration queryTimeout = queryTimeout(connectionConfig, queryOptions);
    final AggregateIterable<BasicDBObject> iterable =
        collectionWithOptions
            .aggregate(pipeline)
            .maxTime(queryTimeout.toMillis(), MILLISECONDS)
            .allowDiskUse(true);

    try (final MongoCursor<BasicDBObject> cursor = iterable.cursor()) {
      if (cursor.hasNext()) {
        return parseLong(cursor.next().get(COUNT_ALIAS).toString());
      }
    }

    return 0;
  }

  public String getCollectionName() {
    return collection.getNamespace().getCollectionName();
  }

  public List<BasicDBObject> convertToAggregatePipeline(Query query) {
    List<Function<Query, Collection<BasicDBObject>>> aggregatePipeline =
        getAggregationPipeline(query);

    List<BasicDBObject> pipeline =
        aggregatePipeline.stream()
            .flatMap(function -> function.apply(query).stream())
            .filter(not(BasicDBObject::isEmpty))
            .collect(toUnmodifiableList());
    return pipeline;
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

  private void logPipeline(final List<BasicDBObject> pipeline, final QueryOptions queryOptions) {
    log.debug(
        "MongoDB aggregation():\n Collection: {}\n Pipeline: {}\n QueryOptions: {}",
        collection.getNamespace(),
        pipeline,
        queryOptions);
  }

  private Query transformAndLog(Query query) {
    log.debug("MongoDB query before transformation: {}", query);
    query = MongoQueryTransformer.transform(query);
    log.debug("MongoDB query after transformation: {}", query);
    return query;
  }

  private List<Function<Query, Collection<BasicDBObject>>> getAggregationPipeline(Query query) {
    List<Function<Query, Collection<BasicDBObject>>> aggregatePipeline =
        DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS;
    if (connectionConfig
            .aggregationPipelineMode()
            .equals(AggregatePipelineMode.SORT_OPTIMIZED_IF_POSSIBLE)
        && query.getAggregations().isEmpty()
        && query.getAggregationFilter().isEmpty()
        && !isProjectionContainsAggregation(query)
        && !isSortContainsAggregation(query)) {
      log.debug("Using sort optimized aggregate pipeline functions for query: {}", query);
      aggregatePipeline = SORT_OPTIMISED_AGGREGATE_PIPELINE_FUNCTIONS;
    }
    return aggregatePipeline;
  }

  private boolean isProjectionContainsAggregation(Query query) {
    return query.getSelections().stream()
        .map(SelectionSpec::getExpression)
        .anyMatch(spec -> spec.accept(new AggregateExpressionChecker()));
  }

  private boolean isSortContainsAggregation(Query query) {
    // ideally there should be only one alias per selection,
    // in case of duplicates, we will accept the latest one
    Map<String, SelectionSpec> aliasToSelectionMap =
        query.getSelections().stream()
            .filter(spec -> this.getAlias(spec).isPresent())
            .collect(
                Collectors.toMap(
                    entry -> this.getAlias(entry).orElseThrow(), identity(), (v1, v2) -> v2));
    return query.getSorts().stream()
        .anyMatch(sort -> isSortOnAggregatedField(aliasToSelectionMap, sort));
  }

  private boolean isSortOnAggregatedField(
      Map<String, SelectionSpec> aliasToSelectionMap, SortingSpec sort) {
    boolean isFunctionExpression = sort.getExpression().accept(new FunctionExpressionChecker());
    boolean isAggregateExpression = sort.getExpression().accept(new AggregateExpressionChecker());
    return isFunctionExpression
        || isAggregateExpression
        || isSortOnAggregatedProjection(aliasToSelectionMap, sort);
  }

  private Optional<String> getAlias(SelectionSpec selectionSpec) {
    if (selectionSpec.getAlias() != null) {
      return Optional.of(selectionSpec.getAlias());
    }

    return selectionSpec.getExpression().accept(new AliasParser());
  }

  private boolean isSortOnAggregatedProjection(
      Map<String, SelectionSpec> aliasToSelectionMap, SortingSpec sort) {
    Optional<String> alias = sort.getExpression().accept(new AliasParser());
    if (alias.isEmpty()) {
      throw new UnsupportedOperationException(
          "Cannot sort by an expression that does not have an alias in selection");
    }

    SelectionSpec selectionSpec = aliasToSelectionMap.get(alias.get());
    if (selectionSpec == null) {
      return false;
    }

    Boolean isFunctionExpression =
        selectionSpec.getExpression().accept(new FunctionExpressionChecker());
    Boolean isAggregationExpression =
        selectionSpec.getExpression().accept(new AggregateExpressionChecker());
    return isFunctionExpression || isAggregationExpression;
  }

  private Duration queryTimeout(
      final ConnectionConfig connectionConfig, final QueryOptions queryOptions) {
    return Stream.of(connectionConfig.queryTimeout(), queryOptions.queryTimeout())
        .filter(Objects::nonNull)
        .min(comparing(Duration::toMillis))
        .orElseThrow();
  }
}
