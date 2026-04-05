package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Optional;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

/**
 * Transforms the legacy {@link org.hypertrace.core.documentstore.Query} to the newer {@link Query}
 * format. Since the legacy query does not carry any type information, this class interfaces with
 * {@link SchemaRegistry} to find the type info.
 *
 * <p>This transformer handles:
 *
 * <ul>
 *   <li>Filter transformation (delegated to {@link LegacyFilterToQueryFilterTransformer})
 *   <li>Selection transformation (field names to appropriate identifier expressions)
 *   <li>OrderBy transformation (field names to sort expressions)
 *   <li>Pagination (limit/offset)
 * </ul>
 */
public class LegacyQueryToV2QueryTransformer {

  private final SchemaRegistry<? extends ColumnMetadata> schemaRegistry;
  private final String tableName;
  private final LegacyFilterToQueryFilterTransformer filterTransformer;

  public LegacyQueryToV2QueryTransformer(
      SchemaRegistry<? extends ColumnMetadata> schemaRegistry, String tableName) {
    this.schemaRegistry = schemaRegistry;
    this.tableName = tableName;
    this.filterTransformer = new LegacyFilterToQueryFilterTransformer(schemaRegistry, tableName);
  }

  /**
   * Transforms a legacy Query to the new v2 Query.
   *
   * @param legacyQuery the legacy query to transform
   * @return the transformed v2 Query
   */
  public Query transform(org.hypertrace.core.documentstore.Query legacyQuery) {
    if (legacyQuery == null) {
      return Query.builder().build();
    }

    Query.QueryBuilder builder = Query.builder();

    // Transform filter
    if (legacyQuery.getFilter() != null) {
      Filter v2Filter = filterTransformer.transform(legacyQuery.getFilter());
      if (v2Filter != null && v2Filter.getExpression() != null) {
        builder.setFilter(v2Filter.getExpression());
      }
    }

    // Transform selections
    if (legacyQuery.getSelections() != null && !legacyQuery.getSelections().isEmpty()) {
      for (String selection : legacyQuery.getSelections()) {
        builder.addSelection(createIdentifierExpression(selection));
      }
    }

    // Transform orderBy
    if (legacyQuery.getOrderBys() != null && !legacyQuery.getOrderBys().isEmpty()) {
      for (OrderBy orderBy : legacyQuery.getOrderBys()) {
        SortOrder sortOrder = orderBy.isAsc() ? SortOrder.ASC : SortOrder.DESC;
        builder.addSort(createIdentifierExpression(orderBy.getField()), sortOrder);
      }
    }

    // Set pagination
    Integer limit = legacyQuery.getLimit();
    Integer offset = legacyQuery.getOffset();
    if (limit != null && limit >= 0) {
      builder.setPagination(
          Pagination.builder()
              .offset(offset != null && offset >= 0 ? offset : 0)
              .limit(limit)
              .build());
    }

    return builder.build();
  }

  /**
   * Creates the appropriate identifier expression based on the field name and schema.
   *
   * <p>Uses the schema registry to determine if a field is:
   *
   * <ul>
   *   <li>A direct column → IdentifierExpression
   *   <li>A JSONB nested path → JsonIdentifierExpression with STRING type (default for
   *       selections/orderBy since we don't have a value to infer type from)
   * </ul>
   *
   * <p>Returns IdentifierExpression (or subclass) which implements both SelectTypeExpression and
   * SortTypeExpression, allowing use in both selections and orderBy clauses.
   */
  private IdentifierExpression createIdentifierExpression(String fieldName) {
    if (fieldName == null || fieldName.isEmpty()) {
      throw new IllegalArgumentException("Field name cannot be null or empty");
    }

    // Check if the full path is a direct column
    if (schemaRegistry.getColumnOrRefresh(tableName, fieldName).isPresent()) {
      return IdentifierExpression.of(fieldName);
    }

    // Try to find a JSONB column prefix
    Optional<String> jsonbColumn = findJsonbColumnPrefix(fieldName);
    if (jsonbColumn.isPresent()) {
      String columnName = jsonbColumn.get();
      String[] jsonPath = getNestedPath(fieldName, columnName);
      // Default to STRING for selections/orderBy since we don't have a value to infer from
      return JsonIdentifierExpression.of(columnName, JsonFieldType.STRING, jsonPath);
    }

    // Fallback: treat as direct column (will fail at query time if column doesn't exist)
    return IdentifierExpression.of(fieldName);
  }

  /**
   * Finds the JSONB column prefix for a given path by progressively checking prefixes.
   *
   * <p>For example, given path "props.inheritedAttributes.color":
   *
   * <ul>
   *   <li>If "props" is a JSONB column → returns "props"
   *   <li>If "props.inheritedAttributes" is a JSONB column → returns "props.inheritedAttributes"
   *   <li>If neither is JSONB → returns empty
   * </ul>
   */
  private Optional<String> findJsonbColumnPrefix(String path) {
    if (!path.contains(".")) {
      return Optional.empty();
    }

    String[] parts = path.split("\\.");
    StringBuilder columnBuilder = new StringBuilder(parts[0]);

    for (int i = 0; i < parts.length - 1; i++) {
      if (i > 0) {
        columnBuilder.append(".").append(parts[i]);
      }
      String candidateColumn = columnBuilder.toString();
      Optional<? extends ColumnMetadata> colMeta =
          schemaRegistry.getColumnOrRefresh(tableName, candidateColumn);

      if (colMeta.isPresent() && colMeta.get().getCanonicalType() == DataType.JSON) {
        return Optional.of(candidateColumn);
      }
    }

    return Optional.empty();
  }

  /**
   * Extracts the JSONB path portion after removing the column name prefix.
   *
   * <p>For example, if the path is "props.inheritedAttributes.color" and the column name is
   * "props", then the returned path is ["inheritedAttributes", "color"].
   */
  private String[] getNestedPath(String fullPath, String jsonbColName) {
    if (fullPath.equals(jsonbColName)) {
      return new String[0];
    }
    String nested = fullPath.substring(jsonbColName.length() + 1);
    return nested.split("\\.");
  }
}
