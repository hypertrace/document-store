package org.hypertrace.core.documentstore.mongo.query;

import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getLimitClause;
import static org.hypertrace.core.documentstore.mongo.query.MongoPaginationHelper.getSkipClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoFilterTypeExpressionParser.getFilterClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoNonProjectedSortTypeExpressionParser.getNonProjectedSortClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser.getProjectClause;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.getSortClause;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.mongo.query.parser.AliasParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoFromTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoGroupTypeExpressionParser;
import org.hypertrace.core.documentstore.parser.AggregateExpressionChecker;
import org.hypertrace.core.documentstore.parser.FunctionExpressionChecker;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;

@Slf4j
@AllArgsConstructor
public class MongoAggregationPipelineConverter {
  private final AggregatePipelineMode aggregationPipelineMode;
  private final MongoCollection<BasicDBObject> collection;

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

  private List<Function<Query, Collection<BasicDBObject>>> getAggregationPipeline(Query query) {
    List<Function<Query, Collection<BasicDBObject>>> aggregatePipeline =
        DEFAULT_AGGREGATE_PIPELINE_FUNCTIONS;
    if (aggregationPipelineMode.equals(AggregatePipelineMode.SORT_OPTIMIZED_IF_POSSIBLE)
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
}
