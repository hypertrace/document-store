package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.junit.jupiter.api.Test;

/**
 * Tests for the visitor pattern implementation in field transformers. Verifies that instanceof
 * checks are not needed and transformation logic is properly encapsulated.
 */
public class FieldTransformationVisitorTest {

  @Test
  void testFlatTransformerVisitsIdentifierExpression() {
    FlatPostgresFieldTransformer transformer = new FlatPostgresFieldTransformer();
    IdentifierExpression expression = IdentifierExpression.of("column_name");
    Map<String, String> pgColMapping = new HashMap<>();

    FieldToPgColumn result = transformer.transform(expression, pgColMapping);

    assertNull(result.getTransformedField(), "Direct column should have no transformed field");
    assertEquals("\"column_name\"", result.getPgColumn(), "Column name should be quoted");
  }

  @Test
  void testFlatTransformerVisitsJsonIdentifierExpression() {
    FlatPostgresFieldTransformer transformer = new FlatPostgresFieldTransformer();
    // Using cleaner varargs syntax
    JsonIdentifierExpression expression = JsonIdentifierExpression.of("props", "brand", "name");
    Map<String, String> pgColMapping = new HashMap<>();

    FieldToPgColumn result = transformer.transform(expression, pgColMapping);

    assertEquals("brand.name", result.getTransformedField(), "Should have nested path");
    assertEquals("\"props\"", result.getPgColumn(), "Column name should be quoted");
  }

  @Test
  void testNestedTransformerVisitsIdentifierExpression() {
    NestedPostgresColTransformer transformer = new NestedPostgresColTransformer();
    IdentifierExpression expression = IdentifierExpression.of("field.nested");
    Map<String, String> pgColMapping = new HashMap<>();

    FieldToPgColumn result = transformer.transform(expression, pgColMapping);

    assertEquals("field.nested", result.getTransformedField(), "Should use document column");
    assertEquals("document", result.getPgColumn(), "Should default to document column");
  }

  @Test
  void testNestedTransformerRejectsJsonIdentifierExpression() {
    NestedPostgresColTransformer transformer = new NestedPostgresColTransformer();
    // Using varargs syntax
    JsonIdentifierExpression expression = JsonIdentifierExpression.of("props", "brand");
    Map<String, String> pgColMapping = new HashMap<>();

    // Should throw UnsupportedOperationException for nested collections
    assertThrows(
        UnsupportedOperationException.class,
        () -> transformer.transform(expression, pgColMapping),
        "JsonIdentifierExpression should not be supported for nested collections");
  }

  @Test
  void testVisitorPatternAvoidInstanceof() {
    // This test demonstrates that visitor pattern eliminates the need for instanceof checks
    FlatPostgresFieldTransformer flatTransformer = new FlatPostgresFieldTransformer();
    Map<String, String> pgColMapping = new HashMap<>();

    // Both expression types work through the same transform() method
    // No instanceof checks needed in the transformer
    IdentifierExpression expr1 = IdentifierExpression.of("column");
    // Using cleaner varargs syntax
    JsonIdentifierExpression expr2 = JsonIdentifierExpression.of("props", "field");

    // Both dispatch to the correct visit() method via polymorphism
    FieldToPgColumn result1 = flatTransformer.transform(expr1, pgColMapping);
    FieldToPgColumn result2 = flatTransformer.transform(expr2, pgColMapping);

    // Verify both produce correct results without instanceof checks
    assertNull(result1.getTransformedField());
    assertEquals("field", result2.getTransformedField());
  }
}
