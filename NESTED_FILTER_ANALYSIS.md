# Analysis: Nested JSON Field Filters for Flat PostgreSQL Collections

## Current State

### Nested Collections (Working)
For nested PostgreSQL collections where the entire document is stored in a JSONB column (`document`), filters on nested fields like `props.colors` work:

```java
// Test case: testContains (line 1527-1547)
Query query = Query.builder()
    .addSelection(IdentifierExpression.of("props.colors"))
    .setFilter(
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("props.colors"),
                    CONTAINS,
                    ConstantExpression.of("Green")))
            .build())
    .build();
```

**Generated SQL (Nested):**
```sql
SELECT document -> 'props' -> 'colors' AS "props.colors"
FROM collection
WHERE document -> 'props' -> 'colors' @> '["Green"]'::jsonb
```

### Flat Collections (Currently Not Supported)
For flat collections where `props` is a JSONB column and `item`, `price` are regular columns:

**Desired Usage:**
```java
Query query = Query.builder()
    .addSelection(JsonIdentifierExpression.of("props", List.of("colors"), "STRING_ARRAY"))
    .setFilter(
        RelationalExpression.of(
            JsonIdentifierExpression.of("props", List.of("colors"), "STRING_ARRAY"),
            CONTAINS,
            ConstantExpression.of("Green")))
    .build();
```

**Desired SQL (Flat):**
```sql
SELECT props -> 'colors' AS colors
FROM collection
WHERE props -> 'colors' @> '["Green"]'::jsonb
```

## Filter Types Analysis

### 1. CONTAINS Operator

#### **Use Case: Array Contains Primitive Value**
- **LHS:** Array field (e.g., `props.colors`, `props.brands`)
- **RHS:** Primitive string value
- **Logic:** Check if array contains the value

**For Nested Collections:**
```java
// Current implementation: PostgresContainsRelationalFilterParser
parsedLhs = "document -> 'props' -> 'colors'"
parsedRhs = "Green"
convertedRhs = "[\"Green\"]"  // Wrapped in array

SQL: document -> 'props' -> 'colors' @> '["Green"]'::jsonb
```

**For Flat Collections (Needed):**
```java
parsedLhs = "props -> 'colors'"  // From JsonIdentifierExpression
parsedRhs = "Green"
convertedRhs = "[\"Green\"]"

SQL: props -> 'colors' @> '["Green"]'::jsonb
```

**Key Insight:** The same logic works! We just need the LHS parser to handle `JsonIdentifierExpression`.

---

### 2. IN Operator

#### **Use Case: Scalar IN Array**
- **LHS:** Primitive field (e.g., `id`, `item`)
- **RHS:** Array of values
- **Logic:** Check if LHS value is in RHS array

**For Nested Collections:**
```java
// Current implementation: PostgresInRelationalFilterParser
// Handles both: scalar IN array AND array-array intersection

SQL for scalar:
((jsonb_typeof(to_jsonb(LHS)) = 'array' AND to_jsonb(LHS) @> jsonb_build_array(?))
 OR (jsonb_build_array(LHS) @> jsonb_build_array(?)))
```

**For Flat Collections (Needed):**
```java
// If LHS is JsonIdentifierExpression pointing to a scalar JSON field
parsedLhs = "props -> 'brand'"  // e.g., nested string field

SQL: Same as above, should work
```

**Key Insight:** Current IN parser handles both cases via type checking. Should work for JSON fields too.

---

### 3. NOT_CONTAINS Operator

#### **Use Case: Array Does NOT Contain Primitive Value**
- **LHS:** Array field
- **RHS:** Primitive value
- **Logic:** Check if array does NOT contain the value

**Current implementation:** `PostgresNotContainsRelationalFilterParser`
```sql
NOT (document -> 'props' -> 'colors' @> '["Green"]'::jsonb)
```

**For Flat Collections:** Same logic should work with JSON accessor.

---

### 4. Standard Relational Operators (EQ, NE, GT, LTE, etc.)

**For Nested Collections:**
```java
// PostgresStandardRelationalFilterParser
// Uses casting based on field type

parsedLhs = "document -> 'props' ->> 'price'"  // ->> for text extraction
cast = "(document -> 'props' ->> 'price')::numeric"

SQL: (document -> 'props' ->> 'price')::numeric > 100
```

**For Flat Collections (Needed):**
```java
parsedLhs = "props ->> 'price'"  // JsonIdentifierExpression
cast = "(props ->> 'price')::numeric"

SQL: (props ->> 'price')::numeric > 100
```

**Key Insight:** Standard operators should work once we have proper accessor syntax from JsonIdentifierExpression.

---

## Current Architecture

### Filter Parser Selection Logic
**File:** `PostgresRelationalFilterParserFactoryImpl.java`

```java
boolean isFirstClassField =
    postgresQueryParser.getPgColTransformer() instanceof FlatPostgresFieldTransformer;

if (expression.getOperator() == CONTAINS) {
    return isFirstClassField ? nonJsonFieldContainsParser : jsonFieldContainsParser;
} else if (expression.getOperator() == IN) {
    return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
}
```

**Problem:** This checks if the *collection* is flat, not if the *field* is a JSON field within a flat collection.

### Current Parsers

| Operator | Nested Collection (JSON) | Flat Collection (Non-JSON) |
|----------|--------------------------|----------------------------|
| CONTAINS | `PostgresContainsRelationalFilterParser` | `PostgresContainsRelationalFilterParserNonJsonField` |
| IN | `PostgresInRelationalFilterParser` | `PostgresInRelationalFilterParserNonJsonField` |
| NOT_CONTAINS | `PostgresNotContainsRelationalFilterParser` | (same) |
| Standard (EQ, GT, etc.) | `PostgresStandardRelationalFilterParser` | (same) |

---

## Gap Analysis

### What's Missing for Flat Collections + JSON Fields?

1. **Parser Selection Logic:**
   - Currently: Collection type determines parser
   - Needed: Field type determines parser
   - For `JsonIdentifierExpression` in flat collections → use JSON parsers

2. **LHS Visitor Support:**
   - `PostgresFieldIdentifierExpressionVisitor` needs to handle `JsonIdentifierExpression`
   - Should produce: `props -> 'fieldName'` or `props ->> 'fieldName'`
   - Already implemented in recent commits for SELECT, needs verification for filters

3. **No New Parsers Needed:**
   - Existing JSON parsers (`PostgresContainsRelationalFilterParser`, `PostgresInRelationalFilterParser`) should work
   - Just need to route `JsonIdentifierExpression` to JSON parsers instead of non-JSON parsers

---

## Proposed Solution

### Option 1: Field-Based Parser Selection (Recommended)

**Change:** Determine parser based on **LHS expression type**, not collection type.

```java
// In PostgresRelationalFilterParserFactoryImpl
@Override
public PostgresRelationalFilterParser parser(
    final RelationalExpression expression,
    final PostgresQueryParser postgresQueryParser) {

    // Check if LHS is a JsonIdentifierExpression (nested JSON field in flat collection)
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;

    // Check if collection is flat (all fields are first-class columns)
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer() instanceof FlatPostgresFieldTransformer;

    // Use JSON parsers for:
    // 1. Nested collections (entire document is JSON)
    // 2. JSON fields within flat collections (JsonIdentifierExpression)
    boolean useJsonParser = !isFlatCollection || isJsonField;

    if (expression.getOperator() == CONTAINS) {
        return useJsonParser ? jsonFieldContainsParser : nonJsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
        return useJsonParser ? jsonFieldInFilterParser : nonJsonFieldInFilterParser;
    }

    return parserMap.getOrDefault(
        expression.getOperator(),
        postgresStandardRelationalFilterParser);
}
```

### Option 2: Visitor Pattern (More Extensible)

**Change:** Have parsers use visitor pattern to determine field type.

```java
// LHS expression accepts a type checker visitor
boolean isJsonField = expression.getLhs().accept(new IsJsonFieldVisitor());
```

---

## Test Cases Needed

### 1. CONTAINS on JSON Array Field in Flat Collection
```java
@Test
void testFlatCollectionJsonArrayContains() {
    Query query = Query.builder()
        .addSelection(JsonIdentifierExpression.of("props", List.of("colors"), "STRING_ARRAY"))
        .setFilter(
            RelationalExpression.of(
                JsonIdentifierExpression.of("props", List.of("colors"), "STRING_ARRAY"),
                CONTAINS,
                ConstantExpression.of("Green")))
        .build();

    // Expected SQL:
    // WHERE props -> 'colors' @> '["Green"]'::jsonb
}
```

### 2. NOT_CONTAINS on JSON Array Field
```java
@Test
void testFlatCollectionJsonArrayNotContains() {
    Query query = Query.builder()
        .setFilter(
            RelationalExpression.of(
                JsonIdentifierExpression.of("props", List.of("brands"), "STRING_ARRAY"),
                NOT_CONTAINS,
                ConstantExpression.of("Brand A")))
        .build();

    // Expected SQL:
    // WHERE NOT (props -> 'brands' @> '["Brand A"]'::jsonb)
}
```

### 3. IN Operator with JSON Scalar Field
```java
@Test
void testFlatCollectionJsonScalarIn() {
    Query query = Query.builder()
        .setFilter(
            RelationalExpression.of(
                JsonIdentifierExpression.of("props", List.of("brand"), "STRING"),
                IN,
                ConstantExpression.ofStrings(List.of("Dettol", "Dove"))))
        .build();

    // Expected SQL:
    // WHERE ((jsonb_typeof(to_jsonb(props -> 'brand')) = 'array'
    //        AND to_jsonb(props -> 'brand') @> jsonb_build_array(?))
    //    OR (jsonb_build_array(props -> 'brand') @> jsonb_build_array(?)))
    //    OR ...
}
```

### 4. Standard Operator (EQ, GT) on JSON Numeric Field
```java
@Test
void testFlatCollectionJsonNumericComparison() {
    Query query = Query.builder()
        .setFilter(
            RelationalExpression.of(
                JsonIdentifierExpression.of("props", List.of("rating"), "LONG"),
                GT,
                ConstantExpression.of(4)))
        .build();

    // Expected SQL:
    // WHERE (props ->> 'rating')::numeric > 4
}
```

### 5. Nested Path (Multiple Levels)
```java
@Test
void testFlatCollectionDeepNestedFilter() {
    Query query = Query.builder()
        .setFilter(
            RelationalExpression.of(
                JsonIdentifierExpression.of("props", List.of("seller", "address", "city"), "STRING"),
                EQ,
                ConstantExpression.of("New York")))
        .build();

    // Expected SQL:
    // WHERE props -> 'seller' -> 'address' ->> 'city' = 'New York'
}
```

---

## Implementation Checklist

- [ ] 1. Update `PostgresRelationalFilterParserFactoryImpl.parser()` to check LHS expression type
- [ ] 2. Verify `PostgresFieldIdentifierExpressionVisitor` handles `JsonIdentifierExpression` for filters
- [ ] 3. Add test: CONTAINS on JSON array field
- [ ] 4. Add test: NOT_CONTAINS on JSON array field
- [ ] 5. Add test: IN operator with JSON scalar field
- [ ] 6. Add test: Standard operators (EQ, GT, LT) on JSON fields
- [ ] 7. Add test: Nested path filters (multiple levels deep)
- [ ] 8. Add consistency test: Flat vs Nested collection same filter results

---

## Key Insights

1. **No new parsers needed** - Existing JSON parsers work for flat collections too
2. **Parser selection is the key** - Need to route based on field type, not just collection type
3. **LHS visitor already handles JsonIdentifierExpression** - Recent commits added `visit(JsonIdentifierExpression)`
4. **Same SQL patterns** - `props -> 'field'` vs `document -> 'props' -> 'field'`
5. **Type casting already works** - `PostgresDataAccessorIdentifierExpressionVisitor` handles type conversion

---

## Risks & Considerations

1. **Breaking Changes:** Need to ensure existing flat collection filters (on first-class columns) still use non-JSON parsers
2. **Performance:** JSON filters might be slower than direct column filters
3. **Indexing:** May need JSONB GIN indexes on `props` column for performance
4. **Type Safety:** Ensure type parameter in JsonIdentifierExpression is used correctly for casting

---

## References

- `PostgresContainsRelationalFilterParser.java` - JSON CONTAINS logic using `@>` operator
- `PostgresInRelationalFilterParser.java` - JSON IN logic with type checking
- `PostgresRelationalFilterParserFactoryImpl.java` - Parser selection based on collection/field type
- `FlatPostgresFieldTransformer.java` - Field transformation for flat collections
- `PostgresFieldIdentifierExpressionVisitor.java` - LHS expression visitor for filters
