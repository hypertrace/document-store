package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.DataType;

/**
 * A Document implementation where each field carries its value and type information.
 *
 * <p>For flat PostgreSQL tables, type information is needed to correctly set values in prepared
 * statements, especially for arrays and JSONB columns.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * TypedDocument typedDoc = TypedDocument.builder()
 *     .field("item", "Soap", DataType.STRING)
 *     .field("price", 99, DataType.INTEGER)
 *     .field("in_stock", true, DataType.BOOLEAN)
 *     .arrayField("tags", List.of("hygiene", "bath"), DataType.STRING)
 *     .jsonbField("props", Map.of("brand", "Dove", "size", "large"))
 *     .build();
 * }</pre>
 */
public class TypedDocument implements Document {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, TypedField> fields;

  private TypedDocument(Map<String, TypedField> fields) {
    this.fields = Collections.unmodifiableMap(fields);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toJson() {
    Map<String, Object> jsonMap = new LinkedHashMap<>();
    for (Map.Entry<String, TypedField> entry : fields.entrySet()) {
      jsonMap.put(entry.getKey(), entry.getValue().getValue());
    }
    try {
      return MAPPER.writeValueAsString(jsonMap);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize TypedDocument to JSON", e);
    }
  }

  @Override
  public DocumentType getDocumentType() {
    return DocumentType.FLAT;
  }

  /** Returns all fields in this document. */
  public Map<String, TypedField> getFields() {
    return fields;
  }

  /** Returns a specific field by name, or null if not present. */
  public TypedField getField(String fieldName) {
    return fields.get(fieldName);
  }

  /** Represents a field's value and type information. */
  public static class TypedField {
    private final Object value;
    private final FieldKind kind;
    private final DataType dataType; // For SCALAR or ARRAY element type

    private TypedField(Object value, FieldKind kind, DataType dataType) {
      this.value = value;
      this.kind = kind;
      this.dataType = dataType;
    }

    public static TypedField scalar(Object value, DataType dataType) {
      return new TypedField(value, FieldKind.SCALAR, dataType);
    }

    public static TypedField array(Collection<?> value, DataType elementType) {
      return new TypedField(value, FieldKind.ARRAY, elementType);
    }

    public static TypedField jsonb(Object value) {
      return new TypedField(value, FieldKind.JSONB, null);
    }

    public Object getValue() {
      return value;
    }

    public FieldKind getKind() {
      return kind;
    }

    /** Returns the DataType for scalar fields, or the element type for array fields. */
    public DataType getDataType() {
      return dataType;
    }

    public boolean isScalar() {
      return kind == FieldKind.SCALAR;
    }

    public boolean isArray() {
      return kind == FieldKind.ARRAY;
    }

    public boolean isJsonb() {
      return kind == FieldKind.JSONB;
    }
  }

  /** The kind of field (scalar, array, or JSONB). */
  public enum FieldKind {
    SCALAR,
    ARRAY,
    JSONB
  }

  /** Builder for TypedDocument */
  public static class Builder {
    private final Map<String, TypedField> fields = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Adds a scalar field with its value and type.
     *
     * @param name the column name
     * @param value the field value
     * @param type the data type
     * @return this builder
     */
    public Builder field(String name, Object value, DataType type) {
      fields.put(name, TypedField.scalar(value, type));
      return this;
    }

    /**
     * Adds an array field with its values and element type.
     *
     * @param name the column name
     * @param values the array values
     * @param elementType the data type of array elements
     * @return this builder
     */
    public Builder arrayField(String name, List<?> values, DataType elementType) {
      fields.put(name, TypedField.array(values, elementType));
      return this;
    }

    /**
     * Adds a JSONB field with its value (will be serialized as JSON).
     *
     * @param name the column name
     * @param value the value (typically a Map or object that can be serialized to JSON)
     * @return this builder
     */
    public Builder jsonbField(String name, Object value) {
      fields.put(name, TypedField.jsonb(value));
      return this;
    }

    public TypedDocument build() {
      return new TypedDocument(fields);
    }
  }
}
