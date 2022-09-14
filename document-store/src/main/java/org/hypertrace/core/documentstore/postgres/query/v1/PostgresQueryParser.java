package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID_ALIAS;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FieldToPgColumnTransformer;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresAggregationFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresGroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresUnnestFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresQueryParser {
  private static final String ALL_SELECTIONS = "*";

  @Getter private final String collection;
  @Getter private final Query query;

  @Setter String finalTableName;
  @Getter private final Builder paramsBuilder = Params.newBuilder();

  // map of alias name to parsed expression
  // e.g qty_count : COUNT(DISTINCT CAST(document->>'quantity' AS NUMERIC))
  @Getter private final Map<String, String> pgSelections = new HashMap<>();

  // map of the original field name to pgColumnName for unnest expression
  // e.g if sales and sales.medium are array fields,
  // unwind sales data will be available in the X column,
  // unwind sales.medium data will be available in the Y column.
  // The below map will maintain that mapping.
  @Getter private final Map<String, String> pgColumnNames = new HashMap<>();
  @Getter private final FieldToPgColumnTransformer toPgColumnTransformer;

  public PostgresQueryParser(String collection, Query query) {
    this.collection = collection;
    this.query = query;
    this.finalTableName = collection;
    toPgColumnTransformer = new FieldToPgColumnTransformer(this);
  }

  public String parse() {
    StringBuilder sqlBuilder = new StringBuilder();
    int startIndexOfSelection = 0;

    // handle from clause (it is unwind operation on array)
    Optional<String> fromClause = parseFromClause();
    if (fromClause.isPresent()) {
      startIndexOfSelection = fromClause.get().length();
      sqlBuilder.append(fromClause.get());
    }

    // handle where clause
    Optional<String> whereFilter = fromClause.isPresent() ? parseUnnestFilter() : parseFilter();
    whereFilter.ifPresent(s -> sqlBuilder.append(String.format(" WHERE %s", s)));

    // selection clause
    sqlBuilder.insert(
        startIndexOfSelection, String.format("SELECT %s FROM %s", getSelections(), finalTableName));

    // group by
    Optional<String> groupBy = parseGroupBy();
    groupBy.ifPresent(s -> sqlBuilder.append(String.format(" GROUP BY %s", s)));

    // having
    Optional<String> having = parseHaving();
    having.ifPresent(s -> sqlBuilder.append(String.format(" HAVING %s", s)));

    // order by
    Optional<String> orderBy = parseOrderBy();
    orderBy.ifPresent(s -> sqlBuilder.append(String.format(" ORDER BY %s", s)));

    // offset and limit
    Optional<String> pagination = parsePagination();
    pagination.ifPresent(s -> sqlBuilder.append(String.format(" %s", s)));

    return sqlBuilder.toString();
  }

  public String getSelections() {
    return parseSelection().orElse(ALL_SELECTIONS);
  }

  public String getSelectionsWithImplicitId() {
    return getSelections() + ", " + String.format("%s AS %s", ID, IMPLICIT_ID_ALIAS);
  }

  public Optional<String> parseSelection() {
    return Optional.ofNullable(PostgresSelectTypeExpressionVisitor.getSelections(this));
  }

  public Optional<String> parseFilter() {
    return PostgresFilterTypeExpressionVisitor.getFilterClause(this);
  }

  public Optional<String> parseUnnestFilter() {
    return PostgresUnnestFilterTypeExpressionVisitor.getFilterClause(this);
  }

  public Optional<String> parseFromClause() {
    return PostgresFromTypeExpressionVisitor.getFromClause(this);
  }

  public Optional<String> parseGroupBy() {
    return Optional.ofNullable(PostgresGroupTypeExpressionVisitor.getGroupByClause(this));
  }

  public Optional<String> parseHaving() {
    return PostgresAggregationFilterTypeExpressionVisitor.getAggregationFilterClause(this);
  }

  public Optional<String> parseOrderBy() {
    return PostgresSortTypeExpressionVisitor.getOrderByClause(this);
  }

  public Optional<String> parsePagination() {
    Optional<Pagination> pagination = this.query.getPagination();
    if (pagination.isPresent()) {
      this.paramsBuilder.addObjectParam(pagination.get().getOffset());
      this.paramsBuilder.addObjectParam(pagination.get().getLimit());
      return Optional.of("OFFSET ? LIMIT ?");
    }
    return Optional.empty();
  }
}
