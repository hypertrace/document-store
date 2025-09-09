package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.PostgresTableIdentifier;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FieldToPgColumnTransformer;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresAggregationFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresGroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresUnnestFilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.registry.PostgresColumnRegistry;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresQueryParser {

  private static final String ALL_SELECTIONS = "*";

  @Getter private final PostgresTableIdentifier tableIdentifier;
  @Getter private final Query query;
  @Getter private final String flatStructureCollectionName;
  @Getter private final PostgresColumnRegistry columnRegistry;

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

  public PostgresQueryParser(
      PostgresTableIdentifier tableIdentifier, Query query, String flatStructureCollectionName) {
    this(tableIdentifier, query, flatStructureCollectionName, null);
  }

  public PostgresQueryParser(
      PostgresTableIdentifier tableIdentifier,
      Query query,
      String flatStructureCollectionName,
      PostgresColumnRegistry columnRegistry) {
    this.tableIdentifier = tableIdentifier;
    this.query = query;
    this.flatStructureCollectionName = flatStructureCollectionName;
    this.columnRegistry = columnRegistry;
    this.finalTableName = tableIdentifier.toString();
    toPgColumnTransformer = new FieldToPgColumnTransformer(this);
  }

  public PostgresQueryParser(PostgresTableIdentifier tableIdentifier, Query query) {
    this(tableIdentifier, query, null, null);
  }

  public PostgresQueryParser(
      PostgresTableIdentifier tableIdentifier, Query query, PostgresColumnRegistry columnRegistry) {
    this(tableIdentifier, query, null, columnRegistry);
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

  public String buildSelectQueryForUpdate() {
    final String selections = getSelectionsWithImplicitId();
    final Optional<String> optionalOrderBy = parseOrderBy();
    final Optional<String> optionalFilter = parseFilter();

    final StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT ").append(selections);
    queryBuilder.append(" FROM ").append(tableIdentifier);
    optionalFilter.ifPresent(filter -> queryBuilder.append(" WHERE ").append(filter));
    optionalOrderBy.ifPresent(orderBy -> queryBuilder.append(" ORDER BY ").append(orderBy));
    queryBuilder.append(" LIMIT 1");
    queryBuilder.append(" FOR UPDATE");

    return queryBuilder.toString();
  }

  public String buildFilterClause() {
    return parseFilter().map(filter -> "WHERE " + filter).orElse("");
  }

  private String getSelections() {
    return parseSelection().orElse(ALL_SELECTIONS);
  }

  private String getSelectionsWithImplicitId() {
    return getSelections() + ", " + String.format("%s AS %s", ID, IMPLICIT_ID);
  }

  private Optional<String> parseSelection() {
    return Optional.ofNullable(PostgresSelectTypeExpressionVisitor.getSelections(this));
  }

  private Optional<String> parseFilter() {
    return PostgresFilterTypeExpressionVisitor.getFilterClause(this);
  }

  private Optional<String> parseUnnestFilter() {
    return PostgresUnnestFilterTypeExpressionVisitor.getFilterClause(this);
  }

  private Optional<String> parseFromClause() {
    return PostgresFromTypeExpressionVisitor.getFromClause(this);
  }

  private Optional<String> parseGroupBy() {
    return Optional.ofNullable(PostgresGroupTypeExpressionVisitor.getGroupByClause(this));
  }

  private Optional<String> parseHaving() {
    return PostgresAggregationFilterTypeExpressionVisitor.getAggregationFilterClause(this);
  }

  private Optional<String> parseOrderBy() {
    return PostgresSortTypeExpressionVisitor.getOrderByClause(this);
  }

  private Optional<String> parsePagination() {
    Optional<Pagination> pagination = this.query.getPagination();
    if (pagination.isPresent()) {
      this.paramsBuilder.addObjectParam(pagination.get().getOffset());
      this.paramsBuilder.addObjectParam(pagination.get().getLimit());
      return Optional.of("OFFSET ? LIMIT ?");
    }
    return Optional.empty();
  }
}
