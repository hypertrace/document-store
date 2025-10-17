package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonIdentifierExpressionTest {

  @Test
  void testOfWithNullJsonPath() {
    // Test with null List
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonIdentifierExpression.of("props", (List<String>) null));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testOfWithEmptyJsonPath() {
    // Test with empty List
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonIdentifierExpression.of("props", Collections.emptyList()));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testEqualsAndHashCodeWithParent() {
    JsonIdentifierExpression expr1 = JsonIdentifierExpression.of("props", "brand");
    JsonIdentifierExpression expr2 = JsonIdentifierExpression.of("props", "brand");

    // Test equality - should be equal
    assertEquals(expr1, expr2, "Expressions with same column and path should be equal");

    // Test hashCode - should be equal
    assertEquals(
        expr1.hashCode(),
        expr2.hashCode(),
        "Expressions with same column and path should have same hashCode");

    // Test with different path
    JsonIdentifierExpression expr3 = JsonIdentifierExpression.of("props", "color");
    assertNotEquals(expr1, expr3, "Expressions with different paths should not be equal");
    assertNotEquals(
        expr1.hashCode(),
        expr3.hashCode(),
        "Expressions with different paths should have different hashCode");

    // Test with different column name
    JsonIdentifierExpression expr4 = JsonIdentifierExpression.of("customAttr", "brand");
    assertNotEquals(expr1, expr4, "Expressions with different columns should not be equal");
  }

  @Test
  void testEqualsIncludesParentName() {
    // Create two expressions with same column/path
    JsonIdentifierExpression expr1 = JsonIdentifierExpression.of("props", "brand");
    JsonIdentifierExpression expr2 = JsonIdentifierExpression.of("props", "brand");

    // Verify they have the same name from parent class
    assertEquals(expr1.getName(), expr2.getName(), "Parent name should be equal");
    assertEquals("props.brand", expr1.getName(), "Generated name should be correct");

    // Verify equality includes parent name
    assertEquals(expr1, expr2, "Equality should include parent name field");
  }

  @Test
  void testToString() {
    JsonIdentifierExpression expr = JsonIdentifierExpression.of("props", "brand");

    String result = expr.toString();

    // Verify format: JsonIdentifier{name=`props.brand`, column=`props`, path=[brand]}
    assertTrue(result.startsWith("JsonIdentifier{"), "toString should start with JsonIdentifier{");
    assertTrue(result.contains("name=`props.brand`"), "toString should contain full name");
    assertTrue(result.contains("column=`props`"), "toString should contain column name");
    assertTrue(result.contains("path=[brand]"), "toString should contain JSON path");
    assertTrue(result.endsWith("}"), "toString should end with }");
  }

  @Test
  void testToStringWithDeepPath() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", "seller", "address", "city");

    String result = expr.toString();

    // Verify nested path is represented correctly
    assertTrue(
        result.contains("name=`props.seller.address.city`"),
        "toString should contain full nested name");
    assertTrue(result.contains("column=`props`"), "toString should contain column name");
    assertTrue(
        result.contains("path=[seller, address, city]"), "toString should contain full JSON path");
  }

  @Test
  void testOfVarargsWithEmptyArray() {
    // Test with explicit empty array
    String[] emptyArray = new String[0];
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> JsonIdentifierExpression.of("props", emptyArray));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testOfVarargsWithNull() {
    // Test with null varargs
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonIdentifierExpression.of("props", (String[]) null));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }
}
