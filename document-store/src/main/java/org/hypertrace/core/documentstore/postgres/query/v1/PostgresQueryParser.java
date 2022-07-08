package org.hypertrace.core.documentstore.postgres.query.v1;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresGroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;

public class PostgresQueryParser {
  private static String NOT_YET_SUPPORTED = "Not yet supported %s";
  private Builder paramsBuilder;
  private final String collection;

  public PostgresQueryParser(String collection) {
    this.collection = collection;
  }

  public Builder getParamsBuilder() {
    return paramsBuilder;
  }

  public String parse(Query query) {
    // prepare selection and form clause
    // TODO : add impl for selection + form clause for unwind
    StringBuilder sqlBuilder = new StringBuilder(String.format("SELECT * FROM %s", collection));
    paramsBuilder = Params.newBuilder();

    // selection clause
    Optional<String> selectionClause = parseSelection(query.getSelections());
    if (selectionClause.isPresent()) {
      sqlBuilder = new StringBuilder();
      sqlBuilder.append(String.format("SELECT %s FROM %s", selectionClause.get(), collection));
    }
    // where clause
    Optional<String> whereFilter = parseFilter(query.getFilter());
    if (whereFilter.isPresent()) {
      sqlBuilder.append(String.format(" WHERE %s", whereFilter.get()));
    }

    // group by
    Optional<String> groupBy = parseGroupBy(query);
    if (groupBy.isPresent()) {
      sqlBuilder.append(String.format(" GROUP BY %s", groupBy.get()));
    }

    // having
    Optional<String> having = parseHaving(query.getAggregationFilter());
    if (having.isPresent()) {
      sqlBuilder.append(String.format(" HAVING %s", having.get()));
    }

    // order by
    Optional<String> orderBy = parseOrderBy(query.getSorts());
    if (having.isPresent()) {
      sqlBuilder.append(String.format(" ORDER BY %s", orderBy.get()));
    }

    // offset and limit
    Optional<String> pagination = parsePagination(query.getPagination());
    if (having.isPresent()) {
      sqlBuilder.append(String.format(" %s", pagination.get()));
    }

    return sqlBuilder.toString();
  }

  private Optional<String> parseSelection(List<SelectionSpec> selectionSpecs) {
    return Optional.ofNullable(PostgresSelectTypeExpressionVisitor.getSelections(selectionSpecs));
  }

  private Optional<String> parseFilter(Optional<FilterTypeExpression> filterTypeExpression) {
    return filterTypeExpression.map(
        expression -> expression.accept(new PostgresFilterTypeExpressionVisitor(this)));
  }

  private Optional<String> parseGroupBy(Query query) {
    return Optional.ofNullable(PostgresGroupTypeExpressionVisitor.getGroupByClause(query));
  }

  private Optional<String> parseHaving(Optional<FilterTypeExpression> filterTypeExpression) {
    if (filterTypeExpression.isPresent()) {
      throw new UnsupportedOperationException(String.format(NOT_YET_SUPPORTED, "having clause"));
    }
    return Optional.empty();
  }

  private Optional<String> parseOrderBy(List<SortingSpec> sortingSpecs) {
    if (sortingSpecs.size() > 0) {
      throw new UnsupportedOperationException(String.format(NOT_YET_SUPPORTED, "order by clause"));
    }
    return Optional.empty();
  }

  private Optional<String> parsePagination(Optional<Pagination> pagination) {
    if (pagination.isPresent()) {
      throw new UnsupportedOperationException(
          String.format(NOT_YET_SUPPORTED, "pagination clause"));
    }
    return Optional.empty();
  }
}
