package org.hypertrace.core.documentstore.postgres.query.v1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresGroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingSpec;

public class PostgresQueryParser {
  private static String NOT_YET_SUPPORTED = "Not yet supported %s";
  private final String collection;

  @Getter private Builder paramsBuilder;

  @Getter private Query query;

  @Getter private Map<String, String> pgSelections; // alias to field name

  private void resetParser(Query query) {
    this.query = query;
    paramsBuilder = Params.newBuilder();
    pgSelections = new HashMap<>();
  }

  public PostgresQueryParser(String collection) {
    this.collection = collection;
  }

  public String parse(Query query) {
    // prepare selection and form clause
    // TODO : add impl for selection + form clause for unwind
    StringBuilder sqlBuilder = new StringBuilder();
    this.resetParser(query);

    // where clause
    Optional<String> whereFilter = parseFilter();
    if (whereFilter.isPresent()) {
      sqlBuilder.append(String.format(" WHERE %s", whereFilter.get()));
    }

    // selection clause
    Optional<String> selectionClause = parseSelection();
    if (selectionClause.isPresent()) {
      sqlBuilder.insert(0, String.format("SELECT %s FROM %s", selectionClause.get(), collection));
    } else {
      sqlBuilder.insert(0, String.format("SELECT * FROM %s", collection));
    }

    // group by
    Optional<String> groupBy = parseGroupBy();
    if (groupBy.isPresent()) {
      sqlBuilder.append(String.format(" GROUP BY %s", groupBy.get()));
    }

    // having
    Optional<String> having = parseHaving();
    if (having.isPresent()) {
      sqlBuilder.append(String.format(" HAVING %s", having.get()));
    }

    // order by
    Optional<String> orderBy = parseOrderBy();
    if (orderBy.isPresent()) {
      sqlBuilder.append(String.format(" ORDER BY %s", orderBy.get()));
    }

    // offset and limit
    Optional<String> pagination = parsePagination();
    if (pagination.isPresent()) {
      sqlBuilder.append(String.format(" %s", pagination.get()));
    }

    return sqlBuilder.toString();
  }

  private Optional<String> parseSelection() {
    return Optional.ofNullable(PostgresSelectTypeExpressionVisitor.getSelections(this));
  }

  private Optional<String> parseFilter() {
    return PostgresFilterTypeExpressionVisitor.getFilterClause(this);
  }

  private Optional<String> parseGroupBy() {
    return Optional.ofNullable(PostgresGroupTypeExpressionVisitor.getGroupByClause(this));
  }

  private Optional<String> parseHaving() {
    return PostgresFilterTypeExpressionVisitor.getAggregationFilterClause(this);
  }

  private Optional<String> parseOrderBy() {
    List<SortingSpec> sortingSpecs = this.query.getSorts();
    if (sortingSpecs.size() > 0) {
      throw new UnsupportedOperationException(String.format(NOT_YET_SUPPORTED, "order by clause"));
    }
    return Optional.empty();
  }

  private Optional<String> parsePagination() {
    Optional<Pagination> pagination = this.query.getPagination();
    if (pagination.isPresent()) {
      throw new UnsupportedOperationException(
          String.format(NOT_YET_SUPPORTED, "pagination clause"));
    }
    return Optional.empty();
  }
}
