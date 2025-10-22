# JSONB Filter Architecture - Simple instanceof Design

## Overview

This document describes the architecture used to handle CONTAINS and IN filters on JSONB nested columns using simple, direct `instanceof` type checking.

## Architecture Components

### 1. **Type Detection via instanceof**

```java
// Simple, direct type check
boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;
```

**Purpose:** Determines if an expression represents a JSON field access (JSONB column).

**Benefits:**
- ✅ **Simple & Direct** - Immediately clear what's being checked
- ✅ **Standard Java** - Everyone understands instanceof
- ✅ **No extra methods** - Doesn't pollute interfaces
- ✅ **Type-safe** - Compile-time type checking
- ✅ **Appropriate** - Right tool for type checking

### 2. **Factory Pattern for Parser Selection**

#### `PostgresRelationalFilterParserFactoryImpl`
```java
// Location: postgres/query/v1/parser/filter/PostgresRelationalFilterParserFactoryImpl.java

public PostgresRelationalFilterParser parser(
    RelationalExpression expression, 
    PostgresQueryParser postgresQueryParser) {
    
    // Determine parser type based on collection and field type
    boolean useJsonParser = shouldUseJsonParser(expression, postgresQueryParser);
    
    if (expression.getOperator() == CONTAINS) {
        return useJsonParser ? jsonFieldContainsParser : nonJsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
        return useJsonParser ? jsonFieldInFilterParser : nonJsonFieldInFilterParser;
    }
    // ...
}

private boolean shouldUseJsonParser(
    RelationalExpression expression, 
    PostgresQueryParser postgresQueryParser) {
    
    boolean isFlatCollection = 
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;
    
    // Simple instanceof check
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;
    
    return !isFlatCollection || isJsonField;
}
```

**Decision Logic:**
```
Use JSON Parser when:
1. Nested collections (entire document is JSONB), OR
2. JSON fields within flat collections (JSONB columns)

Use Standard Parser when:
- Flat collections with first-class columns (e.g., text[], int)
```

### 3. **Parser Implementations**

#### For JSON Fields (JSONB columns)
- **`PostgresContainsRelationalFilterParser`** - Uses `@>` with JSONB casting
- **`PostgresInRelationalFilterParser`** - Uses `jsonb_typeof()` and `@>` operators

```sql
-- CONTAINS on JSON field
props @> '["value"]'::jsonb

-- IN on JSON field  
(jsonb_typeof(to_jsonb(props)) = 'array' AND to_jsonb(props) @> jsonb_build_array(?))
```

#### For Non-JSON Fields (Regular PostgreSQL arrays)
- **`PostgresContainsRelationalFilterParserNonJsonField`** - Uses array `@>` operator
- **`PostgresInRelationalFilterParserNonJsonField`** - Uses array `&&` operator

```sql
-- CONTAINS on array column
tags_unnested @> ARRAY[?]::text[]

-- IN on array column
ARRAY[tags_unnested]::text[] && ARRAY[?, ?]::text[]
```

## Design Principles

### ✅ **1. Pragmatic Simplicity**
```java
// Direct type checking - simple and clear
boolean isJsonField = expression instanceof JsonIdentifierExpression;
```

**Why instanceof is appropriate here:**
- **Clarity** - Intent is immediately obvious
- **Standard** - Well-known Java idiom
- **Appropriate** - This is exactly what instanceof is for
- **KISS** - Keep It Simple, Stupid
- **YAGNI** - You Aren't Gonna Need It (no extra abstraction)

### ✅ **2. Single Responsibility**
- **Expression classes** - Represent field access patterns
- **Factory** - Selects appropriate parser based on field type
- **Parsers** - Generate SQL for specific operators and field types
- Each class does one thing well

### ✅ **3. Type Safety**
- `instanceof` is compile-time type-safe
- Type hierarchy provides structure
- No runtime casting errors in normal flow

### ✅ **4. Easy to Extend**
New expression types can be added by:
1. Create new expression class extending `IdentifierExpression`
2. Factory automatically uses correct parser based on `instanceof` check
3. Add new `instanceof` check only if special handling needed

## Extension Points

### Adding New Operators

1. **Create parser interface:**
```java
interface PostgresMyOperatorParserInterface extends PostgresRelationalFilterParser {
    String parse(RelationalExpression expression, PostgresRelationalFilterContext context);
}
```

2. **Create JSON and non-JSON implementations:**
```java
class PostgresMyOperatorParser implements PostgresMyOperatorParserInterface { }
class PostgresMyOperatorParserNonJsonField implements PostgresMyOperatorParserInterface { }
```

3. **Update factory:**
```java
if (expression.getOperator() == MY_OPERATOR) {
    return useJsonParser ? jsonFieldMyOperatorParser : nonJsonFieldMyOperatorParser;
}
```

### Adding New Expression Types

1. **Create the new expression class:**
```java
public class MyNewExpression extends IdentifierExpression {
    // ... your implementation
}
```

2. **Add instanceof check in factory if special handling needed:**
```java
if (expression.getLhs() instanceof MyNewExpression) {
    // Use special parser for this type
}
```

That's it! No method overriding needed unless special behavior is required.

## Testing Strategy

### Unit Tests
- Factory selects correct parser based on field type
- Each parser generates correct SQL
- Type detection works correctly for all expression types

### Integration Tests
- CONTAINS filters work on JSONB columns
- IN filters work on JSONB columns
- Mixed queries with JSON and non-JSON fields
- Nested vs flat collection behavior

## Summary

**The architecture uses simple instanceof checking:**
1. ✅ Direct `instanceof JsonIdentifierExpression` type checks
2. ✅ Factory pattern for parser selection based on field type
3. ✅ Clean separation between JSON and non-JSON parsers
4. ✅ No unnecessary abstraction layers

**Key Benefits:**
- ✅ **Simple** - Immediately understandable
- ✅ **Direct** - No indirection or extra methods
- ✅ **Standard Java** - Uses well-known idioms
- ✅ **Type-Safe** - Compile-time checking
- ✅ **Appropriate** - Right tool for the job
- ✅ **No pollution** - Doesn't add methods to wrong interfaces

**Why instanceof is Best Here:**
- ✅ Type checking is exactly what `instanceof` is for
- ✅ Only 3 places in code that need the check
- ✅ Not a "type property" - just implementation detail
- ✅ Simpler than any abstraction (KISS principle)
- ✅ Easier to understand and maintain

**Comparison with Alternatives:**

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **instanceof** ✅ | Simple, direct, clear | Some say "not OOP" | **Best for this case** |
| Polymorphic Method | "More OOP" | Pollutes interfaces, over-engineered | Overkill |
| Visitor Pattern | Very extensible | Way too complex for simple check | Overkill |
| Marker Interface | Type-safe | Still uses instanceof | Adds complexity |

**When to use each pattern:**
- **instanceof** ✅ - Simple type checks (this case)
- **Polymorphic Method** - Behavior that's core to type contract
- **Visitor Pattern** - Multiple complex operations across types
- **Marker Interface** - Semantic grouping of multiple types
