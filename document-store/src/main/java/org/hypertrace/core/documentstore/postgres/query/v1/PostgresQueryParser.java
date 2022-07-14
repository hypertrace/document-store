package org.hypertrace.core.documentstore.postgres.query.v1;

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
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresQueryParser {
  @Getter private final String collection;
  @Getter private final Query query;

  @Setter String finalTableName;
  @Getter private final Builder paramsBuilder = Params.newBuilder();

  // map of alias name to parsed expression
  // e.g qty_count : COUNT(DISTINCT CAST(document->>'quantity' AS NUMERIC))
  @Getter private final Map<String, String> pgSelections = new HashMap<>();

  // map of original field name to pgColumnName
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

    // handle where clause if it was not handled in from clause preparation
    if (fromClause.isEmpty()) {
      Optional<String> whereFilter = parseFilter();
      if (whereFilter.isPresent()) {
        sqlBuilder.append(String.format(" WHERE %s", whereFilter.get()));
      }
    }

    // selection clause
    Optional<String> selectionClause = parseSelection();
    if (selectionClause.isPresent()) {
      sqlBuilder.insert(
          startIndexOfSelection,
          String.format("SELECT %s FROM %s", selectionClause.get(), finalTableName));
    } else {
      sqlBuilder.insert(startIndexOfSelection, String.format("SELECT * FROM %s", finalTableName));
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
