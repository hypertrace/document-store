package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.query.Filter;

/**
 * Transforms the legacy {@link org.hypertrace.core.documentstore.Filter} to the newer {@link
 * org.hypertrace.core.documentstore.query.Filter} format. Since the legacy filter does not carry
 * any type information, this class interfaces with {@link SchemaRegistry} to find the type info.
 */
public class LegacyFilterToQueryFilterTransformer {

  private final SchemaRegistry<? extends ColumnMetadata> schemaRegistry;
  private final String tableName;

  public LegacyFilterToQueryFilterTransformer(
      SchemaRegistry<? extends ColumnMetadata> schemaRegistry, String tableName) {
    this.schemaRegistry = schemaRegistry;
    this.tableName = tableName;
  }

  /**
   * Transforms a legacy Filter to the new query Filter.
   *
   * @param legacyFilter the legacy filter to transform
   * @return the transformed query Filter, or null if legacyFilter is null
   */
  public Filter transform(org.hypertrace.core.documentstore.Filter legacyFilter) {
    if (legacyFilter == null) {
      return null;
    }

    FilterTypeExpression expression = transformToExpression(legacyFilter);
    return Filter.builder().expression(expression).build();
  }

  /**
   * Transforms a legacy Filter to a FilterTypeExpression.
   *
   * @param legacyFilter the legacy filter to transform
   * @return the FilterTypeExpression representation
   */
  private FilterTypeExpression transformToExpression(
      org.hypertrace.core.documentstore.Filter legacyFilter) {
    if (legacyFilter.isComposite()) {
      return transformCompositeFilter(legacyFilter);
    } else {
      return transformLeafFilter(legacyFilter);
    }
  }

  /** Transforms a composite filter (AND/OR) to a LogicalExpression. */
  private FilterTypeExpression transformCompositeFilter(
      org.hypertrace.core.documentstore.Filter legacyFilter) {
    org.hypertrace.core.documentstore.Filter.Op op = legacyFilter.getOp();
    org.hypertrace.core.documentstore.Filter[] childFilters = legacyFilter.getChildFilters();

    if (childFilters == null || childFilters.length == 0) {
      throw new IllegalArgumentException("Composite filter must have child filters");
    }

    List<FilterTypeExpression> operands =
        Arrays.stream(childFilters).map(this::transformToExpression).collect(Collectors.toList());

    switch (op) {
      case AND:
        return LogicalExpression.and(operands);
      case OR:
        return LogicalExpression.or(operands);
      default:
        throw new UnsupportedOperationException("Unsupported composite operator: " + op);
    }
  }

  /** Transforms a leaf filter (comparison operators) to a RelationalExpression. */
  private FilterTypeExpression transformLeafFilter(
      org.hypertrace.core.documentstore.Filter legacyFilter) {
    String fieldName = legacyFilter.getFieldName();
    Object value = legacyFilter.getValue();
    org.hypertrace.core.documentstore.Filter.Op op = legacyFilter.getOp();

    SelectTypeExpression lhs = createIdentifierExpression(fieldName, op, value);
    RelationalOperator relationalOp = mapOperator(op);
    ConstantExpression rhs = createConstantExpression(value, op);

    return RelationalExpression.of(lhs, relationalOp, rhs);
  }

  /**
   * Creates the appropriate identifier expression based on the field name and schema.
   *
   * <p>Uses the schema registry to determine if a field is:
   *
   * <ul>
   *   <li>A direct column → IdentifierExpression
   *   <li>A JSONB nested path → JsonIdentifierExpression with inferred field type
   * </ul>
   */
  private SelectTypeExpression createIdentifierExpression(
      String fieldName, org.hypertrace.core.documentstore.Filter.Op op, Object value) {
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
      JsonFieldType fieldType = inferJsonFieldType(value);
      return JsonIdentifierExpression.of(columnName, fieldType, jsonPath);
    }

    // Fallback: treat as direct column (will fail at query time if column doesn't exist)
    return IdentifierExpression.of(fieldName);
  }

  /**
   * Finds the JSONB column that serves as the prefix for the given path.
   *
   * <p>Resolution strategy: progressively try shorter prefixes to find a JSONB column.
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

  /** Extracts the nested JSONB path from a full path given the resolved column name. */
  private String[] getNestedPath(String fullPath, String columnName) {
    if (fullPath.equals(columnName)) {
      return new String[0];
    }
    String nested = fullPath.substring(columnName.length() + 1);
    return nested.split("\\.");
  }

  /** Infers the JsonFieldType from the filter value. */
  private JsonFieldType inferJsonFieldType(Object value) {
    if (value == null) {
      return JsonFieldType.STRING; // Default to STRING
    }

    // For collections, check the first element
    if (value instanceof Collection) {
      Collection<?> collection = (Collection<?>) value;
      if (collection.isEmpty()) {
        return JsonFieldType.STRING;
      }
      Object firstElement = collection.iterator().next();
      return inferTypeFromElement(firstElement);
    }

    if (value instanceof Object[]) {
      Object[] array = (Object[]) value;
      if (array.length == 0) {
        return JsonFieldType.STRING;
      }
      return inferTypeFromElement(array[0]);
    }

    return inferTypeFromElement(value);
  }

  /** Infers JsonFieldType from a single element. */
  private JsonFieldType inferTypeFromElement(Object element) {
    if (element instanceof String) {
      return JsonFieldType.STRING;
    }
    if (element instanceof Number) {
      return JsonFieldType.NUMBER;
    }
    if (element instanceof Boolean) {
      return JsonFieldType.BOOLEAN;
    }
    throw new IllegalArgumentException(
        "Unsupported value type for JsonFieldType inference: " + element.getClass().getName());
  }

  /** Maps legacy Filter.Op to RelationalOperator. */
  private RelationalOperator mapOperator(org.hypertrace.core.documentstore.Filter.Op op) {
    switch (op) {
      case EQ:
        return RelationalOperator.EQ;
      case NEQ:
        return RelationalOperator.NEQ;
      case GT:
        return RelationalOperator.GT;
      case LT:
        return RelationalOperator.LT;
      case GTE:
        return RelationalOperator.GTE;
      case LTE:
        return RelationalOperator.LTE;
      case IN:
        return RelationalOperator.IN;
      case NOT_IN:
        return RelationalOperator.NOT_IN;
      case CONTAINS:
        return RelationalOperator.CONTAINS;
      case NOT_CONTAINS:
        return RelationalOperator.NOT_CONTAINS;
      case EXISTS:
        return RelationalOperator.EXISTS;
      case NOT_EXISTS:
        return RelationalOperator.NOT_EXISTS;
      case LIKE:
        return RelationalOperator.LIKE;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }

  /**
   * Creates a ConstantExpression from the filter value.
   *
   * <p>Handles different value types including collections for IN/NOT_IN operators.
   */
  @SuppressWarnings("unchecked")
  private ConstantExpression createConstantExpression(
      Object value, org.hypertrace.core.documentstore.Filter.Op op) {
    if (value == null) {
      // For EXISTS/NOT_EXISTS, value can be null
      return ConstantExpression.of((String) null);
    }

    // Handle collection types for IN/NOT_IN operators
    if (value instanceof Collection) {
      return createConstantExpressionFromCollection((Collection<?>) value);
    }

    if (value instanceof Object[]) {
      return createConstantExpressionFromCollection(Arrays.asList((Object[]) value));
    }

    if (value instanceof String) {
      return ConstantExpression.of((String) value);
    }

    if (value instanceof Number) {
      return ConstantExpression.of((Number) value);
    }

    if (value instanceof Boolean) {
      return ConstantExpression.of((Boolean) value);
    }

    // Fallback: convert to string
    return ConstantExpression.of(value.toString());
  }

  /** Creates a ConstantExpression from a collection of values. */
  @SuppressWarnings("unchecked")
  private ConstantExpression createConstantExpressionFromCollection(Collection<?> collection) {
    if (collection.isEmpty()) {
      throw new IllegalArgumentException("Collection cannot be empty for IN/NOT_IN operators");
    }

    Object firstElement = collection.iterator().next();

    if (firstElement instanceof String) {
      return ConstantExpression.ofStrings(
          collection.stream().map(Object::toString).collect(Collectors.toList()));
    }

    if (firstElement instanceof Number) {
      return ConstantExpression.ofNumbers(
          collection.stream().map(v -> (Number) v).collect(Collectors.toList()));
    }

    if (firstElement instanceof Boolean) {
      return ConstantExpression.ofBooleans(
          collection.stream().map(v -> (Boolean) v).collect(Collectors.toList()));
    }

    throw new IllegalArgumentException(
        "Unsupported collection element type: " + firstElement.getClass().getName());
  }
}
