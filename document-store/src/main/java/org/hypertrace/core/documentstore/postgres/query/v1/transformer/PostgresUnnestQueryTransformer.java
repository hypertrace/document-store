package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.RootExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;
import org.hypertrace.core.documentstore.query.transform.TransformedQueryBuilder;

/**
 * Accessing an item of a JSON array w/o index expression results in no match in Postgres.
 *
 * <p>As an example, { "item":"Soap", "sales":[ { "city":"delhi" } ] }. If we try to access
 * `sales.city` it will return null as there is no key at `sales`.
 *
 * <p>However, as mongo supports -EQ- operator on array field that behaves as contains internally,
 * `sales.city-EQ-delhi` is a valid filter expression. Ideally, the client should have expressed
 * contains as `sales-CONTAINS-{"city":"delhi"}`.
 *
 * <p>To support compatibility, this pre-processor moves any applied filter on the array field to
 * unnest expression filter.
 */
public class PostgresUnnestQueryTransformer implements QueryTransformer {

  @SuppressWarnings("unchecked")
  @Override
  public Query transform(Query query) {
    // Don't do any thing if there is no unnest expression
    if (query.getFromTypeExpressions().size() <= 0) {
      return query;
    }

    // Prepare an AND tree of filters. If, any OR filter, the full sub-tree needs to move.
    AndSplitFiltersCollector andSplitFiltersCollector = new AndSplitFiltersCollector();
    List<FilterTypeExpression> originalAndSplitFilters =
        query
            .getFilter()
            .map(
                filterTypeExpression ->
                    filterTypeExpression.<List<FilterTypeExpression>>accept(
                        andSplitFiltersCollector))
            .orElse(List.of());

    // check if any filter matches with unnest expression
    // if matches, move the matched one to unnest filter and remove it from main filter list
    List<FilterTypeExpression> finalAndSplitFilters = new ArrayList<>();
    List<FromTypeExpression> finalFromTypeExpressions =
        matchUnnestExpressionsWithFilters(
            query.getFromTypeExpressions(), originalAndSplitFilters, finalAndSplitFilters);

    // return same query if no change
    if (originalAndSplitFilters.size() == finalAndSplitFilters.size()) {
      return query;
    }

    // return modified query if change
    TransformedQueryBuilder transformedQueryBuilder = new TransformedQueryBuilder(query);
    Optional<FilterTypeExpression> finalFilter = prepareFinalAndFiltersChain(finalAndSplitFilters);
    transformedQueryBuilder.setFromClauses(finalFromTypeExpressions);
    if (finalFilter.isPresent()) {
      transformedQueryBuilder.setFilter(finalFilter.get());
    } else {
      transformedQueryBuilder.clearFilter();
    }
    return transformedQueryBuilder.build();
  }

  private Optional<FilterTypeExpression> prepareFinalAndFiltersChain(
      List<FilterTypeExpression> finalFilters) {
    return Optional.ofNullable(
        finalFilters.stream().reduce(null, PostgresUnnestQueryTransformer::buildAndFilter));
  }

  private static FilterTypeExpression buildAndFilter(
      FilterTypeExpression left, FilterTypeExpression right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return LogicalExpression.and(left, right);
  }

  private static List<FromTypeExpression> matchUnnestExpressionsWithFilters(
      List<FromTypeExpression> fromTypeExpressions,
      List<FilterTypeExpression> andFilters,
      List<FilterTypeExpression> finalAndFilters) {

    List<FromTypeExpression> finalFromTypeExpressions = Lists.newArrayList(fromTypeExpressions);
    FilterIdentifierProvider filterIdentifierProvider = new FilterIdentifierProvider();
    UnnestExpressionProvider unnestExpressionProvider = new UnnestExpressionProvider();

    for (FilterTypeExpression filterTypeExpression : andFilters) {
      Optional<UnnestExpression> matchedUnnestExpression =
          getMatchedUnnestExpression(
              finalFromTypeExpressions,
              filterTypeExpression,
              unnestExpressionProvider,
              filterIdentifierProvider);

      if (matchedUnnestExpression.isPresent()) {
        UnnestExpression unnestExpression =
            buildUnnestExpression(matchedUnnestExpression.get(), filterTypeExpression);
        updateModifiedUnnestExpression(
            finalFromTypeExpressions, matchedUnnestExpression.get(), unnestExpression);
      } else {
        finalAndFilters.add(filterTypeExpression);
      }
    }
    return finalFromTypeExpressions;
  }

  private static void updateModifiedUnnestExpression(
      List<FromTypeExpression> finalFromTypeExpressions,
      FromTypeExpression original,
      FromTypeExpression modified) {
    int foundAt =
        IntStream.range(0, finalFromTypeExpressions.size())
            .filter(i -> finalFromTypeExpressions.get(i).equals(original))
            .findFirst()
            .getAsInt();
    finalFromTypeExpressions.set(foundAt, modified);
  }

  private static Optional<UnnestExpression> getMatchedUnnestExpression(
      List<FromTypeExpression> fromTypeExpressions,
      FilterTypeExpression filterTypeExpression,
      UnnestExpressionProvider unnestExpressionProvider,
      FilterIdentifierProvider filterIdentifierProvider) {
    return fromTypeExpressions.stream()
        .map(
            fromTypeExpression ->
                (UnnestExpression) fromTypeExpression.accept(unnestExpressionProvider))
        .filter(
            unnestExpression ->
                filterTypeExpression.accept(
                    new FilterToUnnestExpressionMatcher(
                        filterIdentifierProvider, unnestExpression)))
        .max(Comparator.comparingInt(u -> u.getIdentifierExpression().getName().length()));
  }

  private static UnnestExpression buildUnnestExpression(
      UnnestExpression matchedUnnestExpression, FilterTypeExpression filterTypeExpression) {
    return UnnestExpression.builder()
        .identifierExpression(matchedUnnestExpression.getIdentifierExpression())
        .preserveNullAndEmptyArrays(matchedUnnestExpression.isPreserveNullAndEmptyArrays())
        .filterTypeExpression(
            buildAndFilter(filterTypeExpression, matchedUnnestExpression.getFilterTypeExpression()))
        .build();
  }

  @SuppressWarnings("unchecked")
  private static class UnnestExpressionProvider implements FromTypeExpressionVisitor {

    @Override
    public UnnestExpression visit(UnnestExpression unnestExpression) {
      return unnestExpression;
    }
  }

  @SuppressWarnings("unchecked")
  private static class FilterIdentifierProvider implements SelectTypeExpressionVisitor {

    @Override
    public List<String> visit(AggregateExpression expression) {
      return expression.getExpression().accept(this);
    }

    @Override
    public List<String> visit(ConstantExpression expression) {
      return List.of();
    }

    @Override
    public List<String> visit(DocumentConstantExpression expression) {
      return List.of();
    }

    @Override
    public List<String> visit(FunctionExpression expression) {
      return expression.getOperands().stream()
          .map(exp -> exp.<List<String>>accept(this))
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public List<String> visit(IdentifierExpression expression) {
      return List.of(expression.getName());
    }

    @Override
    public List<String> visit(final RootExpression expression) {
      throw new UnsupportedOperationException();
    }
  }

  @SuppressWarnings("unchecked")
  private static class AndSplitFiltersCollector implements FilterTypeExpressionVisitor {

    @Override
    public List<FilterTypeExpression> visit(LogicalExpression expression) {
      if (expression.getOperator().equals(LogicalOperator.AND)) {
        return expression.getOperands().stream()
            .map(
                filterTypeExpression ->
                    filterTypeExpression.<List<FilterTypeExpression>>accept(this))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
      }
      return List.of(expression);
    }

    @Override
    public List<FilterTypeExpression> visit(RelationalExpression expression) {
      return List.of(expression);
    }

    @Override
    public List<FilterTypeExpression> visit(KeyExpression expression) {
      return List.of(expression);
    }

    @Override
    public List<FilterTypeExpression> visit(final ArrayFilterExpression expression) {
      throw new UnsupportedOperationException();
    }
  }

  @SuppressWarnings("unchecked")
  private static class FilterToUnnestExpressionMatcher implements FilterTypeExpressionVisitor {
    private final UnnestExpression unnestExpression;
    private final FilterIdentifierProvider filterIdentifierProvider;

    FilterToUnnestExpressionMatcher(
        FilterIdentifierProvider filterIdentifierProvider, UnnestExpression unnestExpression) {
      this.unnestExpression = unnestExpression;
      this.filterIdentifierProvider = filterIdentifierProvider;
    }

    @Override
    public Boolean visit(LogicalExpression expression) {
      if (expression.getOperator().equals(LogicalOperator.OR)) {
        return expression.getOperands().stream()
            .filter(filterTypeExpression -> filterTypeExpression.accept(this))
            .findFirst()
            .isPresent();
      }
      return false;
    }

    @Override
    public Boolean visit(RelationalExpression expression) {
      List<String> lhs = expression.getLhs().accept(filterIdentifierProvider);
      return lhs.stream()
          .anyMatch(p -> p.contains(unnestExpression.getIdentifierExpression().getName()));
    }

    @Override
    public Boolean visit(KeyExpression expression) {
      return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Boolean visit(final ArrayFilterExpression expression) {
      throw new UnsupportedOperationException();
    }
  }
}
