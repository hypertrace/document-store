package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.exception.SchemaMismatchException;
import org.hypertrace.core.documentstore.model.options.MissingColumnStrategy;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class FlatCollectionWriteTest extends BaseWriteTest {

  private static final String FLAT_COLLECTION_NAME = "myTestFlat";
  private static final String INSERT_STATEMENTS_FILE = "query/pg_flat_collection_insert.json";

  private static Collection flatCollection;

  @BeforeAll
  public static void init() throws IOException {
    initPostgres();
    LOGGER.info("Postgres datastore initialized: {}", postgresDatastore.listCollections());

    createFlatCollectionSchema((PostgresDatastore) postgresDatastore, FLAT_COLLECTION_NAME);
    flatCollection =
        postgresDatastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
  }

  private static void executeInsertStatements() {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    try {
      String jsonContent = readFileFromResource(INSERT_STATEMENTS_FILE).orElseThrow();
      JsonNode rootNode = OBJECT_MAPPER.readTree(jsonContent);
      JsonNode statementsNode = rootNode.get("statements");

      if (statementsNode == null || !statementsNode.isArray()) {
        throw new RuntimeException("Invalid JSON format: 'statements' array not found");
      }

      try (Connection connection = pgDatastore.getPostgresClient()) {
        for (JsonNode statementNode : statementsNode) {
          String statement = statementNode.asText().trim();
          if (!statement.isEmpty()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
              preparedStatement.executeUpdate();
            } catch (Exception e) {
              LOGGER.error("Failed to execute INSERT statement: {}", e.getMessage(), e);
              throw e;
            }
          }
        }
      }
      LOGGER.info("Inserted initial data into: {}", FLAT_COLLECTION_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to execute INSERT statements: {}", e.getMessage(), e);
    }
  }

  @BeforeEach
  public void setupData() {
    // Clear and repopulate with initial data before each test
    clearTable(FLAT_COLLECTION_NAME);
    executeInsertStatements();
  }

  @AfterEach
  public void cleanup() {
    // Data is cleared in @BeforeEach, but cleanup here for safety
  }

  @AfterAll
  public static void shutdown() {
    shutdownPostgres();
  }

  @Nested
  @DisplayName("Upsert Operations")
  class UpsertTests {

    @Test
    @DisplayName("Should create new document when key doesn't exist and return true")
    void testUpsertNewDocument() throws Exception {
      String docId = generateDocId("test");

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "NewItem");
      objectNode.put("price", 99);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean isNew = flatCollection.upsert(key, document);

      assertTrue(isNew, "Should return true for new document");

      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem", rs.getString("item"));
            assertEquals(99, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should merge with existing document preserving unspecified fields")
    void testUpsertMergesWithExistingDocument() throws Exception {
      String docId = generateDocId("test");

      // First, create a document with multiple fields
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", docId);
      initialNode.put("item", "Original Item");
      initialNode.put("price", 100);
      initialNode.put("quantity", 50);
      initialNode.put("in_stock", true);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean firstResult = flatCollection.upsert(key, initialDoc);
      assertTrue(firstResult, "First upsert should create new document");

      // Now upsert with only some fields - others should be PRESERVED (merge behavior)
      ObjectNode mergeNode = OBJECT_MAPPER.createObjectNode();
      mergeNode.put("id", docId);
      mergeNode.put("item", "Updated Item");
      // price and quantity are NOT specified - they should retain their original values
      Document mergeDoc = new JSONDocument(mergeNode);

      boolean secondResult = flatCollection.upsert(key, mergeDoc);
      assertFalse(secondResult, "Second upsert should update existing document");

      // Verify merge behavior: item updated, price/quantity/in_stock preserved
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated Item", rs.getString("item"));
            // These should be PRESERVED from the original document (merge semantics)
            assertEquals(100, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
            assertTrue(rs.getBoolean("in_stock"));
          });
    }

    @Test
    @DisplayName("Upsert vs CreateOrReplace: upsert preserves, createOrReplace resets to default")
    void testUpsertVsCreateOrReplaceBehavior() throws Exception {
      String docId1 = generateDocId("test");
      String docId2 = generateDocId("test");

      // Setup: Create two identical documents
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("item", "Original Item");
      initialNode.put("price", 100);
      initialNode.put("quantity", 50);

      ObjectNode doc1 = initialNode.deepCopy();
      doc1.put("id", docId1);
      ObjectNode doc2 = initialNode.deepCopy();
      doc2.put("id", docId2);

      Key key1 = new SingleValueKey(DEFAULT_TENANT, docId1);
      Key key2 = new SingleValueKey(DEFAULT_TENANT, docId2);

      flatCollection.upsert(key1, new JSONDocument(doc1));
      flatCollection.upsert(key2, new JSONDocument(doc2));

      // Now update both with partial documents (only item field)
      ObjectNode partialUpdate = OBJECT_MAPPER.createObjectNode();
      partialUpdate.put("item", "Updated Item");

      ObjectNode partial1 = partialUpdate.deepCopy();
      partial1.put("id", docId1);
      ObjectNode partial2 = partialUpdate.deepCopy();
      partial2.put("id", docId2);

      // Use upsert for doc1 - should PRESERVE price and quantity
      flatCollection.upsert(key1, new JSONDocument(partial1));

      // Use createOrReplace for doc2 - should RESET price and quantity to NULL (default)
      flatCollection.createOrReplace(key2, new JSONDocument(partial2));

      // Verify upsert preserved original values
      queryAndAssert(
          key1,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated Item", rs.getString("item"));
            assertEquals(100, rs.getInt("price")); // PRESERVED
            assertEquals(50, rs.getInt("quantity")); // PRESERVED
          });

      // Verify createOrReplace reset to defaults
      queryAndAssert(
          key2,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated Item", rs.getString("item"));
            assertNull(rs.getObject("price")); // RESET to NULL
            assertNull(rs.getObject("quantity")); // RESET to NULL
          });
    }

    @Test
    @DisplayName("Should skip unknown fields in upsert (default SKIP strategy)")
    void testUpsertSkipsUnknownFields() throws Exception {
      String docId = generateDocId("test");

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Item with unknown");
      objectNode.put("price", 200);
      objectNode.put("unknown_field", "should be skipped");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean isNew = flatCollection.upsert(key, document);
      assertTrue(isNew);

      // Verify only known fields were inserted
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Item with unknown", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for upsertAndReturn")
    void testUpsertAndReturn() {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("_id", 100);
      objectNode.put("item", "NewItem");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "100");

      assertThrows(
          UnsupportedOperationException.class, () -> flatCollection.upsertAndReturn(key, document));
    }
  }

  @Nested
  @DisplayName("Create Operations")
  class CreateTests {

    @Test
    @DisplayName("Should create document with all supported data types")
    void testCreateWithAllDataTypes() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      String docId = generateDocId("test");

      objectNode.put("id", docId);
      objectNode.put("item", "Comprehensive Test Item");
      objectNode.put("price", 999);
      objectNode.put("quantity", "50");
      objectNode.put("big_number", 9223372036854775807L);
      objectNode.put("rating", 4.5f);
      objectNode.put("weight", 123.456789);
      objectNode.put("in_stock", true);
      objectNode.put("date", 1705315800000L);
      objectNode.put("created_date", "2024-01-15");
      objectNode.putArray("tags").add("electronics").add("sale").add("featured");
      objectNode.put("categoryTags", "single-category");
      objectNode.putArray("numbers").add(10).add(20).add(30);
      objectNode.putArray("scores").add(1.5).add(2.5).add(3.5);
      objectNode.putArray("flags").add(true).add(false).add(true);

      ObjectNode propsNode = OBJECT_MAPPER.createObjectNode();
      propsNode.put("color", "blue");
      propsNode.put("size", "large");
      propsNode.put("weight", 2.5);
      propsNode.put("warranty", true);
      propsNode.putObject("nested").put("key", "value");
      objectNode.set("props", propsNode);

      ObjectNode salesNode = OBJECT_MAPPER.createObjectNode();
      salesNode.put("total", 1000);
      salesNode.put("region", "US");
      objectNode.set("sales", salesNode);

      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = flatCollection.create(key, document);

      assertTrue(result.isSucceed());
      assertFalse(result.isPartial());
      assertTrue(result.getSkippedFields().isEmpty());

      // Verify all data types were inserted correctly
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());

            assertEquals("Comprehensive Test Item", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
            assertEquals(9223372036854775807L, rs.getLong("big_number"));
            assertEquals(4.5f, rs.getFloat("rating"), 0.01f);
            assertEquals(123.456789, rs.getDouble("weight"), 0.0001);
            assertTrue(rs.getBoolean("in_stock"));
            assertEquals(1705315800000L, rs.getTimestamp("date").getTime()); // epoch millis
            assertNotNull(rs.getDate("created_date"));

            String[] tags = (String[]) rs.getArray("tags").getArray();
            assertEquals(3, tags.length);
            assertEquals("electronics", tags[0]);
            assertEquals("sale", tags[1]);
            assertEquals("featured", tags[2]);

            // Single value auto-converted to array
            String[] categoryTags = (String[]) rs.getArray("categoryTags").getArray();
            assertEquals(1, categoryTags.length);
            assertEquals("single-category", categoryTags[0]);

            Integer[] numbers = (Integer[]) rs.getArray("numbers").getArray();
            assertEquals(3, numbers.length);
            assertEquals(10, numbers[0]);
            assertEquals(20, numbers[1]);
            assertEquals(30, numbers[2]);

            Double[] scores = (Double[]) rs.getArray("scores").getArray();
            assertEquals(3, scores.length);
            assertEquals(1.5, scores[0], 0.01);

            Boolean[] flags = (Boolean[]) rs.getArray("flags").getArray();
            assertEquals(3, flags.length);
            assertTrue(flags[0]);
            assertFalse(flags[1]);

            String propsJson = rs.getString("props");
            assertNotNull(propsJson);
            JsonNode propsResult = OBJECT_MAPPER.readTree(propsJson);
            assertEquals("blue", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());
            assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
            assertTrue(propsResult.get("warranty").asBoolean());
            assertEquals("value", propsResult.get("nested").get("key").asText());

            String salesJson = rs.getString("sales");
            assertNotNull(salesJson);
            JsonNode salesResult = OBJECT_MAPPER.readTree(salesJson);
            assertEquals(1000, salesResult.get("total").asInt());
            assertEquals("US", salesResult.get("region").asText());
          });
    }

    @Test
    @DisplayName("Should throw DuplicateDocumentException when creating with existing key")
    void testCreateDuplicateDocument() throws Exception {

      String docId = generateDocId("test");
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "dup-doc-200");
      objectNode.put("item", "First Item");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult createResult = flatCollection.create(key, document);
      Preconditions.checkArgument(
          createResult.isSucceed(),
          "Preconditions failure: Could not create doc with id: " + docId);

      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "dup-doc-200");
      objectNode2.put("item", "Second Item");
      Document document2 = new JSONDocument(objectNode2);

      assertThrows(DuplicateDocumentException.class, () -> flatCollection.create(key, document2));
    }

    @ParameterizedTest
    @DisplayName(
        "When MissingColumnStrategy is Throw, should throw an exception for unknown fields. Unknown fields are those fields that are not found in the schema but are present in the doc")
    @ArgumentsSource(MissingColumnStrategyProvider.class)
    void testUnknownFieldsAsPerMissingColumnStrategy(MissingColumnStrategy missingColumnStrategy)
        throws Exception {

      String docId = generateDocId("test");

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Item");
      objectNode.put("unknown_column", "should throw");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      if (missingColumnStrategy == MissingColumnStrategy.THROW) {
        Collection collection =
            getFlatCollectionWithStrategy(MissingColumnStrategy.THROW.toString());
        assertThrows(SchemaMismatchException.class, () -> collection.create(key, document));
        // Verify no document was inserted
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = '%s'",
                        FLAT_COLLECTION_NAME, key));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1), "Document should not exist in DB after exception");
        }
      } else {
        CreateResult result = flatCollection.create(key, document);
        // for SKIP
        assertTrue(result.isSucceed());
        // this is a partial write because unknown_column was not written to
        assertTrue(result.isPartial());
        assertTrue(result.getSkippedFields().contains("unknown_column"));

        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Item", rs.getString("item"));
            });
      }
    }

    @Test
    @DisplayName(
        "Should use default SKIP strategy when missingColumnStrategy config is empty string")
    void testEmptyMissingColumnStrategyConfigUsesDefault() throws Exception {
      Collection collectionWithEmptyStrategy = getFlatCollectionWithStrategy("");

      // Test that it uses default SKIP strategy (unknown fields are skipped, not thrown)
      String docId = generateDocId("test");
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Test Item");
      objectNode.put("unknown_field", "should be skipped with default SKIP strategy");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = collectionWithEmptyStrategy.create(key, document);

      // With default SKIP strategy, unknown fields are skipped
      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertTrue(result.getSkippedFields().contains("unknown_field"));
    }

    @Test
    @DisplayName("Should use default SKIP strategy when missingColumnStrategy config is invalid")
    void testInvalidMissingColumnStrategyConfigUsesDefault() throws Exception {
      Collection collectionWithInvalidStrategy = getFlatCollectionWithStrategy("INVALID_STRATEGY");

      String docId = generateDocId("test");
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Test Item");
      objectNode.put("unknown_field", "should be skipped with default SKIP strategy");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      CreateResult result = collectionWithInvalidStrategy.create(key, document);

      // With default SKIP strategy, unknown fields are skipped
      assertTrue(result.isSucceed());
      assertTrue(result.isPartial());
      assertTrue(result.getSkippedFields().contains("unknown_field"));
    }

    @Test
    @DisplayName("Should return failure when all fields are unknown (parsed.isEmpty)")
    void testCreateFailsWhenAllFieldsAreUnknown() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("completely_unknown_field1", "value1");
      objectNode.put("completely_unknown_field2", "value2");
      objectNode.put("another_nonexistent_column", 123);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "all-unknown-doc-700");

      CreateResult result = flatCollection.create(key, document);

      // Although no column exists in the schema, it'll create a new doc with the key as the id
      assertTrue(result.isSucceed());
      assertEquals(3, result.getSkippedFields().size());
      assertTrue(
          result
              .getSkippedFields()
              .containsAll(
                  List.of(
                      "completely_unknown_field1",
                      "completely_unknown_field2",
                      "another_nonexistent_column")));

      // Verify no row was inserted
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"id\" = '%s'",
                      FLAT_COLLECTION_NAME, key));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }

    @Test
    @DisplayName("Should refresh schema and retry on UNDEFINED_COLUMN error")
    void testCreateRefreshesSchemaOnUndefinedColumnError() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Step 1: Add a temporary column and do a create to cache the schema
      String addColumnSQL =
          String.format("ALTER TABLE \"%s\" ADD COLUMN \"temp_col\" TEXT", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addColumnSQL)) {
        ps.execute();
        LOGGER.info("Added temporary column 'temp_col' to table");
      }

      // Step 2: Create a document with the temp column to cache the schema
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "cache-schema-doc");
      objectNode1.put("item", "Item to cache schema");
      objectNode1.put("temp_col", "temp value");
      flatCollection.create(
          new SingleValueKey("default", "cache-schema-doc"), new JSONDocument(objectNode1));
      LOGGER.info("Schema cached with temp_col");

      // Step 3: DROP the column - now the cached schema is stale
      String dropColumnSQL =
          String.format("ALTER TABLE \"%s\" DROP COLUMN \"temp_col\"", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(dropColumnSQL)) {
        ps.execute();
        LOGGER.info("Dropped temp_col - schema cache is now stale");
      }

      // Step 4: Try to create with the dropped column
      // Schema registry still thinks temp_col exists, so it will include it in INSERT
      // INSERT will fail with UNDEFINED_COLUMN, triggering handlePSQLExceptionForCreate
      // which will refresh schema and retry
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "retry-doc-800");
      objectNode2.put("item", "Item after schema refresh");
      objectNode2.put("temp_col", "this column no longer exists");
      Document document = new JSONDocument(objectNode2);
      Key key = new SingleValueKey("default", "retry-doc-800");

      CreateResult result = flatCollection.create(key, document);

      // Should succeed - temp_col will be skipped (either via retry or schema refresh)
      assertTrue(result.isSucceed());
      // The dropped column should be skipped
      assertTrue(result.getSkippedFields().contains("temp_col"));

      // Verify the valid fields were inserted
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Item after schema refresh", rs.getString("item"));
          });
    }

    @ParameterizedTest
    @DisplayName("Should skip column with unparseable value and add to skippedFields")
    @ArgumentsSource(MissingColumnStrategyProvider.class)
    void testUnparsableValuesAsPerMissingColStrategy(MissingColumnStrategy missingColumnStrategy)
        throws Exception {

      String docId = generateDocId("test");

      // Try to insert a string value into an integer column with wrong type
      // The unparseable column should be skipped, not throw an exception
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", docId);
      objectNode.put("item", "Valid Item");
      objectNode.put("price", "not_a_number_at_all"); // price is INTEGER, this will fail parsing
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      if (missingColumnStrategy == MissingColumnStrategy.THROW) {
        CreateResult result =
            getFlatCollectionWithStrategy(MissingColumnStrategy.SKIP.toString())
                .create(key, document);

        // Should succeed with the valid columns, skipping the unparseable one
        assertTrue(result.isSucceed());
        assertTrue(result.isPartial());
        assertEquals(1, result.getSkippedFields().size());
        assertTrue(result.getSkippedFields().contains("price"));

        // Verify the valid fields were inserted
        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Valid Item", rs.getString("item"));
              // price should be null since it was skipped
              assertEquals(0, rs.getInt("price"));
              assertTrue(rs.wasNull());
            });
      } else {
        // SKIP strategy: unparseable value should be skipped, document created
        CreateResult result = flatCollection.create(key, document);
        assertTrue(result.isSucceed());
        assertTrue(result.isPartial());
        assertEquals(1, result.getSkippedFields().size());
        assertTrue(result.getSkippedFields().contains("price"));

        // Verify the valid fields were inserted
        queryAndAssert(
            key,
            rs -> {
              assertTrue(rs.next());
              assertEquals("Valid Item", rs.getString("item"));
              // price should be null since it was skipped
              assertEquals(0, rs.getInt("price"));
              assertTrue(rs.wasNull());
            });
      }
    }
  }

  private static Collection getFlatCollectionWithStrategy(String strategy) {
    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgresContainer.getMappedPort(5432));

    Map<String, String> configWithStrategy = new HashMap<>();
    configWithStrategy.put("url", postgresConnectionUrl);
    configWithStrategy.put("user", "postgres");
    configWithStrategy.put("password", "postgres");
    configWithStrategy.put("customParams.missingColumnStrategy", strategy);

    Datastore datastoreWithStrategy =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(configWithStrategy));

    return datastoreWithStrategy.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);
  }

  private void queryAndAssert(Key key, ResultSetConsumer consumer) throws Exception {
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
    try (Connection conn = pgDatastore.getPostgresClient();
        PreparedStatement ps =
            conn.prepareStatement(
                String.format(
                    "SELECT * FROM \"%s\" WHERE \"id\" = '%s'", FLAT_COLLECTION_NAME, key));
        ResultSet rs = ps.executeQuery()) {
      consumer.accept(rs);
    }
  }

  @FunctionalInterface
  interface ResultSetConsumer {

    void accept(ResultSet rs) throws Exception;
  }

  @Nested
  @DisplayName("CreateOrReplace Operations")
  class CreateOrReplaceTests {

    @Test
    @DisplayName(
        "Should create new document and return true. Cols not specified should be set of default NULL")
    void testCreateOrReplaceNewDocument() throws Exception {

      String docId = generateDocId("test");

      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "upsert-new-doc-100");
      objectNode.put("item", "New Upsert Item");
      objectNode.put("price", 500);
      objectNode.put("quantity", 25);
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean isNew = flatCollection.createOrReplace(key, document);

      assertTrue(isNew);

      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("New Upsert Item", rs.getString("item"));
            assertEquals(500, rs.getInt("price"));
            assertEquals(25, rs.getInt("quantity"));
            // assert on some fields that they're set to null correctly
            assertNull(rs.getObject("sales"));
            assertNull(rs.getObject("categoryTags"));
            assertNull(rs.getObject("date"));
          });
    }

    @Test
    @DisplayName("Should replace existing document and return false")
    void testCreateOrReplaceExistingDocument() throws Exception {
      String docId = generateDocId("test");
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", docId);
      initialNode.put("item", "Original Item");
      initialNode.put("price", 100);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean firstResult = flatCollection.createOrReplace(key, initialDoc);

      Preconditions.checkArgument(
          firstResult, "Preconditions failure: Could not create first document with id: " + docId);

      // Now replace with updated document
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", docId);
      updatedNode.put("item", "Updated Item");
      updatedNode.put("quantity", 50);
      Document updatedDoc = new JSONDocument(updatedNode);

      boolean secondResult = flatCollection.createOrReplace(key, updatedDoc);

      assertFalse(secondResult);

      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated Item", rs.getString("item"));
            // this should be the default since price is not present in the updated document
            assertNull(rs.getObject("price"));
            assertEquals(50, rs.getInt("quantity"));
          });
    }

    @Test
    @DisplayName("Should skip unknown fields in createOrReplace (default SKIP strategy)")
    void testCreateOrReplaceSkipsUnknownFields() throws Exception {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put("id", "upsert-skip-fields-300");
      objectNode.put("item", "Item with unknown");
      objectNode.put("price", 200);
      objectNode.put("unknown_field", "should be skipped");
      Document document = new JSONDocument(objectNode);
      Key key = new SingleValueKey("default", "upsert-skip-fields-300");

      boolean isNew = flatCollection.createOrReplace(key, document);
      assertTrue(isNew);

      // Verify only known fields were inserted
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            assertEquals("Item with unknown", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should handle JSONB fields in createOrReplace")
    void testCreateOrReplaceWithJsonbField() throws Exception {
      String docId = generateDocId("test");
      ObjectNode initialNode = OBJECT_MAPPER.createObjectNode();
      initialNode.put("id", docId);
      initialNode.put("item", "Item with props");
      ObjectNode initialProps = OBJECT_MAPPER.createObjectNode();
      initialProps.put("color", "red");
      initialProps.put("size", "small");
      initialNode.set("props", initialProps);
      Document initialDoc = new JSONDocument(initialNode);
      Key key = new SingleValueKey(DEFAULT_TENANT, docId);

      boolean wasCreated = flatCollection.createOrReplace(key, initialDoc);
      Preconditions.checkArgument(
          wasCreated, "Precondition failure: Doc could not be created with id: " + docId);

      // Update with new JSONB value
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("id", docId);
      updatedNode.put("item", "Updated Item");
      ObjectNode updatedProps = OBJECT_MAPPER.createObjectNode();
      updatedProps.put("color", "blue");
      updatedProps.put("size", "large");
      updatedProps.put("weight", 2.5);
      updatedNode.set("props", updatedProps);
      Document updatedDoc = new JSONDocument(updatedNode);

      boolean isNew = flatCollection.createOrReplace(key, updatedDoc);
      assertFalse(isNew);

      // Verify JSONB was updated
      queryAndAssert(
          key,
          rs -> {
            assertTrue(rs.next());
            JsonNode propsResult = OBJECT_MAPPER.readTree(rs.getString("props"));
            assertEquals("blue", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());
            assertEquals(2.5, propsResult.get("weight").asDouble(), 0.01);
          });
    }
  }

  @Nested
  @DisplayName("Bulk Operations")
  class BulkOperationTests {

    @Test
    @DisplayName("Should bulk upsert multiple new documents")
    void testBulkUpsertNewDocuments() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "BulkItem101");
      node1.put("price", 101);
      node1.put("quantity", 10);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-101"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "BulkItem102");
      node2.put("price", 102);
      node2.put("quantity", 20);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-102"), new JSONDocument(node2));

      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("item", "BulkItem103");
      node3.put("price", 103);
      node3.put("in_stock", true);
      ObjectNode props = OBJECT_MAPPER.createObjectNode();
      props.put("color", "red");
      props.put("size", "large");
      node3.set("props", props);
      node3.putArray("tags").add("electronics").add("sale");
      node3.putArray("numbers").add(1).add(2).add(3);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-103"), new JSONDocument(node3));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-101"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem101", rs.getString("item"));
            assertEquals(101, rs.getInt("price"));
            assertEquals(10, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-102"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem102", rs.getString("item"));
            assertEquals(102, rs.getInt("price"));
            assertEquals(20, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-103"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("BulkItem103", rs.getString("item"));
            assertEquals(103, rs.getInt("price"));
            assertTrue(rs.getBoolean("in_stock"));

            // Verify JSONB column
            JsonNode propsResult = OBJECT_MAPPER.readTree(rs.getString("props"));
            assertEquals("red", propsResult.get("color").asText());
            assertEquals("large", propsResult.get("size").asText());

            // Verify array columns
            String[] tags = (String[]) rs.getArray("tags").getArray();
            assertEquals(2, tags.length);
            assertEquals("electronics", tags[0]);

            Integer[] numbers = (Integer[]) rs.getArray("numbers").getArray();
            assertEquals(3, numbers.length);
            assertEquals(1, numbers[0]);
          });
    }

    @Test
    @DisplayName("Should bulk upsert updating existing documents")
    void testBulkUpsertUpdatesExistingDocuments() throws Exception {
      // First create some documents
      String docId1 = "bulk-update-1";
      String docId2 = "bulk-update-2";

      ObjectNode initial1 = OBJECT_MAPPER.createObjectNode();
      initial1.put("item", "Original1");
      initial1.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(initial1));

      ObjectNode initial2 = OBJECT_MAPPER.createObjectNode();
      initial2.put("item", "Original2");
      initial2.put("price", 200);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(initial2));

      // Now bulk upsert with updates
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updated1 = OBJECT_MAPPER.createObjectNode();
      updated1.put("item", "Updated1");
      updated1.put("price", 999);
      updated1.put("quantity", 50);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(updated1));

      ObjectNode updated2 = OBJECT_MAPPER.createObjectNode();
      updated2.put("item", "Updated2");
      updated2.put("price", 888);
      updated2.put("in_stock", true);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(updated2));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId1),
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated1", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
            assertEquals(50, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId2),
          rs -> {
            assertTrue(rs.next());
            assertEquals("Updated2", rs.getString("item"));
            assertEquals(888, rs.getInt("price"));
            assertTrue(rs.getBoolean("in_stock"));
          });
    }

    @Test
    @DisplayName("Should bulk upsert with mixed inserts and updates")
    void testBulkUpsertMixedInsertAndUpdate() throws Exception {
      // Create one existing document
      String existingId = "bulk-mixed-existing";
      ObjectNode existing = OBJECT_MAPPER.createObjectNode();
      existing.put("item", "ExistingItem");
      existing.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(existing));

      // Bulk upsert: update existing + insert new
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updatedExisting = OBJECT_MAPPER.createObjectNode();
      updatedExisting.put("item", "UpdatedExisting");
      updatedExisting.put("price", 555);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(updatedExisting));

      ObjectNode newDoc = OBJECT_MAPPER.createObjectNode();
      newDoc.put("item", "NewItem");
      newDoc.put("price", 777);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-mixed-new"), new JSONDocument(newDoc));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, existingId),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedExisting", rs.getString("item"));
            assertEquals(555, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-mixed-new"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem", rs.getString("item"));
            assertEquals(777, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should handle empty document map")
    void testBulkUpsertEmptyMap() {
      Map<Key, Document> emptyMap = Collections.emptyMap();
      boolean result = flatCollection.bulkUpsert(emptyMap);
      assertTrue(result);
    }

    @Test
    @DisplayName("Should handle null document map")
    void testBulkUpsertNullMap() {
      boolean result = flatCollection.bulkUpsert(null);
      assertTrue(result);
    }

    @Test
    @DisplayName("Should bulk upsert documents with different column sets")
    void testBulkUpsertDocumentsWithDifferentColumns() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "ItemWithPrice");
      node1.put("price", 100);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-1"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "ItemWithQuantity");
      node2.put("quantity", 50);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-2"), new JSONDocument(node2));

      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("item", "ItemWithAll");
      node3.put("price", 200);
      node3.put("quantity", 30);
      node3.put("in_stock", true);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "diff-cols-3"), new JSONDocument(node3));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-1"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithPrice", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
            assertEquals(0, rs.getInt("quantity"));
            assertTrue(rs.wasNull());
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithQuantity", rs.getString("item"));
            assertEquals(0, rs.getInt("price"));
            assertTrue(rs.wasNull());
            assertEquals(50, rs.getInt("quantity"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "diff-cols-3"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithAll", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
            assertEquals(30, rs.getInt("quantity"));
            assertTrue(rs.getBoolean("in_stock"));
          });
    }

    @Test
    @DisplayName("Should skip unknown fields in bulk upsert")
    void testBulkUpsertSkipsUnknownFields() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("item", "ItemWithUnknown");
      node.put("price", 100);
      node.put("unknown_field", "should be skipped");
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "bulk-unknown-field"), new JSONDocument(node));

      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertTrue(result);

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "bulk-unknown-field"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ItemWithUnknown", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should ignore documents with unknown fields when IGNORE_DOCUMENT strategy")
    void testBulkUpsertIgnoreDocumentStrategy() throws Exception {
      Collection collectionWithIgnoreStrategy =
          getFlatCollectionWithStrategy(MissingColumnStrategy.IGNORE_DOCUMENT.toString());

      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      // Doc with unknown field - should be ignored
      ObjectNode nodeWithUnknown = OBJECT_MAPPER.createObjectNode();
      nodeWithUnknown.put("item", "ShouldBeIgnored");
      nodeWithUnknown.put("price", 100);
      nodeWithUnknown.put("unknown_field", "causes ignore");
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-1"), new JSONDocument(nodeWithUnknown));

      // Doc without unknown field - should be upserted
      ObjectNode validNode = OBJECT_MAPPER.createObjectNode();
      validNode.put("item", "ValidItem");
      validNode.put("price", 200);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "ignore-doc-2"), new JSONDocument(validNode));

      boolean result = collectionWithIgnoreStrategy.bulkUpsert(bulkMap);

      assertTrue(result);

      // First doc should NOT exist (was ignored)
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-1"),
          rs -> {
            assertFalse(rs.next());
          });

      // Second doc should exist
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "ignore-doc-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("ValidItem", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return false when document has invalid JSON (IOException)")
    void testBulkUpsertWithInvalidJsonDocument() {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      // Create a Document that returns invalid JSON
      Document invalidJsonDoc =
          new Document() {
            @Override
            public String toJson() {
              return "{ invalid json without closing brace";
            }
          };

      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "invalid-json-doc"), invalidJsonDoc);

      // Should return false due to IOException during parsing
      boolean result = flatCollection.bulkUpsert(bulkMap);

      assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when batch execution fails (BatchUpdateException)")
    void testBulkUpsertBatchUpdateException() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      String addConstraintSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD CONSTRAINT price_positive CHECK (\"price\" > 0)",
              FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addConstraintSQL)) {
        ps.execute();
        LOGGER.info("Added CHECK constraint: price must be positive");
      }

      try {
        Map<Key, Document> bulkMap = new LinkedHashMap<>();

        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NegativePriceItem");
        node.put("price", -100); // Violates CHECK constraint
        bulkMap.put(
            new SingleValueKey(DEFAULT_TENANT, "negative-price-doc"), new JSONDocument(node));

        // Should return false due to BatchUpdateException
        boolean result = flatCollection.bulkUpsert(bulkMap);

        assertFalse(result);

        queryAndAssert(
            new SingleValueKey(DEFAULT_TENANT, "negative-price-doc"),
            rs -> {
              assertFalse(rs.next());
            });

      } finally {
        // Clean up: remove the CHECK constraint
        String dropConstraintSQL =
            String.format(
                "ALTER TABLE \"%s\" DROP CONSTRAINT price_positive", FLAT_COLLECTION_NAME);
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps = conn.prepareStatement(dropConstraintSQL)) {
          ps.execute();
          LOGGER.info("Removed CHECK constraint");
        }
      }
    }

    @Test
    @DisplayName("Should return empty iterator for null document map")
    void testBulkUpsertAndReturnOlderDocumentsNullMap() throws Exception {
      CloseableIterator<Document> result = flatCollection.bulkUpsertAndReturnOlderDocuments(null);
      assertFalse(result.hasNext());
      result.close();
    }

    @Test
    @DisplayName("Should return empty iterator for empty document map")
    void testBulkUpsertAndReturnOlderDocumentsEmptyMap() throws Exception {
      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(Collections.emptyMap());
      assertFalse(result.hasNext());
      result.close();
    }

    @Test
    @DisplayName("Should return empty iterator when upserting new documents (no old docs exist)")
    void testBulkUpsertAndReturnOlderDocumentsNewDocs() throws Exception {
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("item", "NewItem1");
      node1.put("price", 100);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-new-1"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("item", "NewItem2");
      node2.put("price", 200);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-new-2"), new JSONDocument(node2));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // No old documents should be returned since these are new inserts
      assertFalse(result.hasNext());
      result.close();

      // Verify documents were inserted
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-new-1"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem1", rs.getString("item"));
            assertEquals(100, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-new-2"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem2", rs.getString("item"));
            assertEquals(200, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return old documents when updating existing documents")
    void testBulkUpsertAndReturnOlderDocumentsExistingDocs() throws Exception {
      // First create some documents
      String docId1 = "return-old-existing-1";
      String docId2 = "return-old-existing-2";

      ObjectNode initial1 = OBJECT_MAPPER.createObjectNode();
      initial1.put("item", "OldItem1");
      initial1.put("price", 100);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(initial1));

      ObjectNode initial2 = OBJECT_MAPPER.createObjectNode();
      initial2.put("item", "OldItem2");
      initial2.put("price", 200);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(initial2));

      // Now bulk upsert with updates
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updated1 = OBJECT_MAPPER.createObjectNode();
      updated1.put("item", "UpdatedItem1");
      updated1.put("price", 999);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId1), new JSONDocument(updated1));

      ObjectNode updated2 = OBJECT_MAPPER.createObjectNode();
      updated2.put("item", "UpdatedItem2");
      updated2.put("price", 888);
      bulkMap.put(new SingleValueKey(DEFAULT_TENANT, docId2), new JSONDocument(updated2));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // Collect old documents
      List<Document> oldDocs = new ArrayList<>();
      while (result.hasNext()) {
        oldDocs.add(result.next());
      }
      result.close();

      // Should have 2 old documents
      assertEquals(2, oldDocs.size());

      // Verify old documents contain original values
      Map<String, JsonNode> oldDocsByItem = new HashMap<>();
      for (Document doc : oldDocs) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        oldDocsByItem.put(json.get("item").asText(), json);
      }

      assertTrue(oldDocsByItem.containsKey("OldItem1"));
      assertEquals(100, oldDocsByItem.get("OldItem1").get("price").asInt());

      assertTrue(oldDocsByItem.containsKey("OldItem2"));
      assertEquals(200, oldDocsByItem.get("OldItem2").get("price").asInt());

      // Verify documents were updated in DB
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId1),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedItem1", rs.getString("item"));
            assertEquals(999, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, docId2),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedItem2", rs.getString("item"));
            assertEquals(888, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should return only existing old documents in mixed insert/update scenario")
    void testBulkUpsertAndReturnOlderDocumentsMixed() throws Exception {
      // Create one existing document
      String existingId = "return-old-mixed-existing";

      ObjectNode existing = OBJECT_MAPPER.createObjectNode();
      existing.put("item", "ExistingItem");
      existing.put("price", 500);
      flatCollection.createOrReplace(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(existing));

      // Bulk upsert: update existing + insert new
      Map<Key, Document> bulkMap = new LinkedHashMap<>();

      ObjectNode updatedExisting = OBJECT_MAPPER.createObjectNode();
      updatedExisting.put("item", "UpdatedExisting");
      updatedExisting.put("price", 555);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, existingId), new JSONDocument(updatedExisting));

      ObjectNode newDoc = OBJECT_MAPPER.createObjectNode();
      newDoc.put("item", "NewItem");
      newDoc.put("price", 777);
      bulkMap.put(
          new SingleValueKey(DEFAULT_TENANT, "return-old-mixed-new"), new JSONDocument(newDoc));

      CloseableIterator<Document> result =
          flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap);

      // Should only return the one existing document (not the new one)
      List<Document> oldDocs = new ArrayList<>();
      while (result.hasNext()) {
        oldDocs.add(result.next());
      }
      result.close();

      assertEquals(1, oldDocs.size());

      JsonNode oldDoc = OBJECT_MAPPER.readTree(oldDocs.get(0).toJson());
      assertEquals("ExistingItem", oldDoc.get("item").asText());
      assertEquals(500, oldDoc.get("price").asInt());

      // Verify both documents exist in DB with new values
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, existingId),
          rs -> {
            assertTrue(rs.next());
            assertEquals("UpdatedExisting", rs.getString("item"));
            assertEquals(555, rs.getInt("price"));
          });

      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "return-old-mixed-new"),
          rs -> {
            assertTrue(rs.next());
            assertEquals("NewItem", rs.getString("item"));
            assertEquals(777, rs.getInt("price"));
          });
    }

    @Test
    @DisplayName("Should throw IOException when bulkUpsert fails")
    void testBulkUpsertAndReturnOlderDocumentsUpsertFailure() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Add a CHECK constraint to force upsert failure
      String addConstraintSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD CONSTRAINT price_positive_return CHECK (\"price\" > 0)",
              FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addConstraintSQL)) {
        ps.execute();
      }

      try {
        Map<Key, Document> bulkMap = new LinkedHashMap<>();

        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NegativePriceItem");
        node.put("price", -100); // Violates CHECK constraint
        bulkMap.put(new SingleValueKey(DEFAULT_TENANT, "return-old-fail"), new JSONDocument(node));

        assertThrows(
            IOException.class, () -> flatCollection.bulkUpsertAndReturnOlderDocuments(bulkMap));

      } finally {
        // Clean up: remove the CHECK constraint
        String dropConstraintSQL =
            String.format(
                "ALTER TABLE \"%s\" DROP CONSTRAINT price_positive_return", FLAT_COLLECTION_NAME);
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps = conn.prepareStatement(dropConstraintSQL)) {
          ps.execute();
        }
      }
    }
  }

  @Nested
  @DisplayName("Delete Operations")
  class DeleteTests {

    @Test
    @DisplayName("Should delete document by single key")
    void testDeleteByKey() throws Exception {
      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("id", "delete-key-test");
      node.put("item", "ToDeleteByKey");
      node.put("price", 50);
      Key key = new SingleValueKey(DEFAULT_TENANT, "delete-key-test");
      flatCollection.create(key, new JSONDocument(node));

      assertTrue(flatCollection.delete(key));
      queryAndAssert(key, rs -> assertFalse(rs.next()));
    }

    @Test
    @DisplayName("Should delete documents by multiple keys")
    void testDeleteByKeys() throws Exception {
      Key key1 = new SingleValueKey(DEFAULT_TENANT, "delete-keys-1");
      Key key2 = new SingleValueKey(DEFAULT_TENANT, "delete-keys-2");
      Key key3 = new SingleValueKey(DEFAULT_TENANT, "delete-keys-3");

      for (int i = 1; i <= 3; i++) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("id", "delete-keys-" + i);
        node.put("item", "Item" + i);
        node.put("price", i * 10);
        flatCollection.create(
            new SingleValueKey(DEFAULT_TENANT, "delete-keys-" + i), new JSONDocument(node));
      }

      // Delete keys 1 and 2, keep 3
      BulkDeleteResult result = flatCollection.delete(Set.of(key1, key2));
      assertEquals(2, result.getDeletedCount());

      queryAndAssert(key1, rs -> assertFalse(rs.next()));
      queryAndAssert(key2, rs -> assertFalse(rs.next()));
      queryAndAssert(key3, rs -> assertTrue(rs.next()));
    }

    @Test
    @DisplayName("Should delete all documents")
    void testDeleteAll() throws Exception {
      for (int i = 1; i <= 2; i++) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("id", "delete-all-" + i);
        node.put("item", "AllItem" + i);
        flatCollection.create(
            new SingleValueKey(DEFAULT_TENANT, "delete-all-" + i), new JSONDocument(node));
      }

      assertTrue(flatCollection.deleteAll());

      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format("SELECT COUNT(*) FROM \"%s\"", FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }

    @Test
    @DisplayName("Should delete with various filter types: EQ, GT, IN, legacy Filter")
    void testDeleteWithFilters() throws Exception {
      // Setup: Create documents for different filter scenarios
      // Doc 1: For EQ filter test
      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("id", "filter-eq");
      node1.put("item", "ToBeDeleted");
      node1.put("price", 100);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "filter-eq"), new JSONDocument(node1));

      // Doc 2 & 3: For GT filter test
      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("id", "filter-gt-expensive");
      node2.put("item", "Expensive");
      node2.put("price", 1000);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "filter-gt-expensive"), new JSONDocument(node2));

      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("id", "filter-gt-cheap");
      node3.put("item", "Cheap");
      node3.put("price", 10);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "filter-gt-cheap"), new JSONDocument(node3));

      // Doc 4, 5, 6: For IN filter test
      for (String fruit : List.of("Apple", "Banana", "Cherry")) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("id", "filter-in-" + fruit.toLowerCase());
        node.put("item", fruit);
        node.put("price", 50);
        flatCollection.create(
            new SingleValueKey(DEFAULT_TENANT, "filter-in-" + fruit.toLowerCase()),
            new JSONDocument(node));
      }

      // Test 1: EQ filter
      Filter eqFilter = Filter.eq("item", "ToBeDeleted");
      assertTrue(flatCollection.delete(eqFilter));
      queryAndAssert(new SingleValueKey(DEFAULT_TENANT, "filter-eq"), rs -> assertFalse(rs.next()));

      // Test 2: GT filter (price > 500)
      Filter gtFilter = new Filter(Filter.Op.GT, "price", 500);
      assertTrue(flatCollection.delete(gtFilter));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-gt-expensive"), rs -> assertFalse(rs.next()));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-gt-cheap"), rs -> assertTrue(rs.next()));

      // Test 3: IN filter
      Filter inFilter = new Filter(Filter.Op.IN, "item", List.of("Apple", "Banana"));
      assertTrue(flatCollection.delete(inFilter));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-in-apple"), rs -> assertFalse(rs.next()));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-in-banana"), rs -> assertFalse(rs.next()));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-in-cherry"), rs -> assertTrue(rs.next()));

      // Test 4: Delete the remaining Cherry item
      Filter cherryFilter = Filter.eq("item", "Cherry");
      assertTrue(flatCollection.delete(cherryFilter));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "filter-in-cherry"), rs -> assertFalse(rs.next()));
    }

    @Test
    @DisplayName("Should delete with composite AND filter and nested JSONB filter")
    void testDeleteWithCompositeAndNestedFilters() throws Exception {
      // Setup for AND filter
      ObjectNode node1 = OBJECT_MAPPER.createObjectNode();
      node1.put("id", "and-match");
      node1.put("item", "TargetItem");
      node1.put("price", 100);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "and-match"), new JSONDocument(node1));

      ObjectNode node2 = OBJECT_MAPPER.createObjectNode();
      node2.put("id", "and-nomatch");
      node2.put("item", "TargetItem");
      node2.put("price", 200);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "and-nomatch"), new JSONDocument(node2));

      // Setup for JSONB nested filter
      ObjectNode node3 = OBJECT_MAPPER.createObjectNode();
      node3.put("id", "jsonb-nike");
      node3.put("item", "Product1");
      ObjectNode props1 = OBJECT_MAPPER.createObjectNode();
      props1.put("brand", "Nike");
      node3.set("props", props1);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "jsonb-nike"), new JSONDocument(node3));

      ObjectNode node4 = OBJECT_MAPPER.createObjectNode();
      node4.put("id", "jsonb-adidas");
      node4.put("item", "Product2");
      ObjectNode props2 = OBJECT_MAPPER.createObjectNode();
      props2.put("brand", "Adidas");
      node4.set("props", props2);
      flatCollection.create(
          new SingleValueKey(DEFAULT_TENANT, "jsonb-adidas"), new JSONDocument(node4));

      // Test 1: AND filter (item = 'TargetItem' AND price = 100)
      Filter andFilter = Filter.eq("item", "TargetItem").and(Filter.eq("price", 100));
      assertTrue(flatCollection.delete(andFilter));
      queryAndAssert(new SingleValueKey(DEFAULT_TENANT, "and-match"), rs -> assertFalse(rs.next()));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "and-nomatch"), rs -> assertTrue(rs.next()));

      // Test 2: Nested JSONB filter (props.brand = 'Nike')
      Filter jsonbFilter = Filter.eq("props.brand", "Nike");
      assertTrue(flatCollection.delete(jsonbFilter));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "jsonb-nike"), rs -> assertFalse(rs.next()));
      queryAndAssert(
          new SingleValueKey(DEFAULT_TENANT, "jsonb-adidas"), rs -> assertTrue(rs.next()));
    }

    @Test
    @DisplayName("Should handle edge cases: no match returns false, null filter throws exception")
    void testDeleteEdgeCases() {
      // Test 1: No match returns false
      Filter noMatchFilter = Filter.eq("item", "NonExistentItem12345");
      assertFalse(flatCollection.delete(noMatchFilter));

      // Test 2: Null filter throws exception
      assertThrows(IllegalArgumentException.class, () -> flatCollection.delete((Filter) null));
    }

    @Test
    @DisplayName(
        "delete(Filter) should throw exception when table is dropped (schema lookup fails)")
    void testDeleteByFilterThrowsExceptionOnDroppedTable() throws Exception {
      // Create a temporary table, get collection, then drop the table to trigger exception
      String tempTable = "temp_delete_filter_test";
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Create temp table
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "CREATE TABLE \"%s\" (\"id\" TEXT PRIMARY KEY, \"item\" TEXT)", tempTable))) {
        ps.execute();
      }

      // Get collection for the temp table
      Collection tempCollection =
          postgresDatastore.getCollectionForType(tempTable, DocumentType.FLAT);

      // Drop the table - this will cause schema lookup to fail when delete is called
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(String.format("DROP TABLE \"%s\"", tempTable))) {
        ps.execute();
      }

      // With legacy filter transformer, schema lookup happens first and throws exception
      Filter filter = Filter.eq("item", "SomeValue");
      assertThrows(Exception.class, () -> tempCollection.delete(filter));
    }

    @Test
    @DisplayName("delete(Set<Key>) should return BulkDeleteResult(0) when SQLException occurs")
    void testDeleteByKeysReturnsZeroOnSQLException() throws Exception {
      // Create a temporary table, get collection, then drop the table to trigger SQLException
      String tempTable = "temp_delete_keys_test";
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Create temp table
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "CREATE TABLE \"%s\" (\"id\" TEXT PRIMARY KEY, \"item\" TEXT)", tempTable))) {
        ps.execute();
      }

      // Get collection for the temp table
      Collection tempCollection =
          postgresDatastore.getCollectionForType(tempTable, DocumentType.FLAT);

      // Insert a document to force schema caching (getPKForTable is called during create)
      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("id", "temp-key");
      node.put("item", "temp-item");
      tempCollection.create(new SingleValueKey(DEFAULT_TENANT, "temp-key"), new JSONDocument(node));

      // Drop the table to cause SQLException on delete
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(String.format("DROP TABLE \"%s\"", tempTable))) {
        ps.execute();
      }

      Set<Key> keys =
          Set.of(
              new SingleValueKey(DEFAULT_TENANT, "key1"),
              new SingleValueKey(DEFAULT_TENANT, "key2"));

      // SQLException should be caught and method should return BulkDeleteResult with 0 count
      BulkDeleteResult result = tempCollection.delete(keys);
      assertEquals(0, result.getDeletedCount());
    }

    @Test
    @DisplayName("deleteAll() should return false when SQLException occurs (dropped table)")
    void testDeleteAllReturnsFalseOnSQLException() throws Exception {
      // Create a temporary table, get collection, then drop the table to trigger SQLException
      String tempTable = "temp_delete_all_test";
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Create temp table
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "CREATE TABLE \"%s\" (\"id\" TEXT PRIMARY KEY, \"item\" TEXT)", tempTable))) {
        ps.execute();
      }

      // Get collection for the temp table
      Collection tempCollection =
          postgresDatastore.getCollectionForType(tempTable, DocumentType.FLAT);

      // Drop the table to cause SQLException on delete
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(String.format("DROP TABLE \"%s\"", tempTable))) {
        ps.execute();
      }

      // SQLException should be caught and method should return false
      assertFalse(tempCollection.deleteAll());
    }
  }

  @Nested
  @DisplayName("Update Operations")
  class UpdateTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for update with condition")
    void testUpdateWithCondition() {
      Key key = new SingleValueKey("default", "1");
      ObjectNode updatedNode = OBJECT_MAPPER.createObjectNode();
      updatedNode.put("_id", 1);
      updatedNode.put("item", "Soap");
      Document document = new JSONDocument(updatedNode);
      Filter condition = Filter.eq("price", 10);

      assertThrows(
          UnsupportedOperationException.class,
          () -> flatCollection.update(key, document, condition));
    }

    @Test
    @DisplayName("Should return empty when no document matches query")
    void testUpdateNoMatch() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("9999")))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      Optional<Document> result = flatCollection.update(query, updates, options);

      assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Should throw IOException when column does not exist")
    void testUpdateNonExistentColumn() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of(1)))
              .build();

      List<SubDocumentUpdate> updates =
          List.of(SubDocumentUpdate.of("nonexistent_column", "value"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(IOException.class, () -> flatCollection.update(query, updates, options));
    }

    @Test
    @DisplayName("Should throw IOException when nested path on non-JSONB column")
    void testUpdateNestedPathOnNonJsonbColumn() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of(1)))
              .build();

      // "item" is TEXT, not JSONB - nested path should fail
      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("item.nested", "value"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(IOException.class, () -> flatCollection.update(query, updates, options));
    }
  }

  @Nested
  @DisplayName("SubDocument Update Operations")
  class SubDocUpdateTests {

    @Nested
    @DisplayName("SET Operator Tests")
    class SetOperatorTests {

      @Test
      @DisplayName("Cases 1-4: SET all field types via bulkUpdate")
      void testSetAllFieldTypes() throws Exception {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        // Apply all updates at once: primitives, arrays, and one nested path per JSONB column
        // Note: PostgreSQL doesn't allow multiple assignments to same column in one UPDATE,
        // so we can only update one nested path per JSONB column in a single operation
        // Note: PG will throw an error if there are multiple assignments to same column in one
        // UPDATE. So we cannot set props.brand and props.colour if props is a jsonb type, for
        // example
        List<SubDocumentUpdate> updates =
            List.of(
                // Case 1: Top-level primitives
                SubDocumentUpdate.of("item", "UpdatedItem"),
                SubDocumentUpdate.of("price", 999),
                SubDocumentUpdate.of("quantity", 50),
                SubDocumentUpdate.of("in_stock", false),
                SubDocumentUpdate.of("big_number", 9999999999L),
                SubDocumentUpdate.of("rating", 4.5f),
                SubDocumentUpdate.of("weight", 123.456),
                // Case 2: Top-level arrays
                SubDocumentUpdate.of("tags", new String[] {"tag4", "tag5", "tag6"}),
                SubDocumentUpdate.of("numbers", new Integer[] {10, 20, 30}),
                SubDocumentUpdate.of("scores", new Double[] {1.1, 2.2, 3.3}),
                SubDocumentUpdate.of("flags", new Boolean[] {true, false, true}),
                // Case 3 & 4: One nested path in JSONB (props) - tests nested primitive
                SubDocumentUpdate.of("props.brand", "NewBrand"),
                // Use 'sales' JSONB column for nested array test
                SubDocumentUpdate.of(
                    "sales.regions", SubDocumentValue.of(new String[] {"US", "EU", "APAC"})));

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        // Read expected values from JSON file
        String expectedJsonContent =
            readFileFromResource("expected/set_all_field_types_expected.json").orElseThrow();
        JsonNode expectedJson = OBJECT_MAPPER.readTree(expectedJsonContent);

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          Document resultDoc = results.next();
          JsonNode resultJson = OBJECT_MAPPER.readTree(resultDoc.toJson());

          assertEquals(expectedJson, resultJson);
        }
      }

      @Test
      @DisplayName("Case 6: SET on non-existent top-level column should skip by default")
      void testSetNonExistentTopLevelColumnSkips() throws Exception {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.of("nonexistent_column1", "some_value"),
                SubDocumentUpdate.of("nonexistent_column2.value", "some_value"));
        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        // Document returned (unchanged since update was skipped)
        assertTrue(result.isPresent());

        // Verify original data is intact
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"item\" FROM \"%s\" WHERE \"id\" = '1'", FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Soap", rs.getString("item"));
        }
      }

      @Test
      @DisplayName("Case 7b: SET nested path in NULL JSONB column should create structure")
      void testSetNestedPathInNullJsonbColumn() throws Exception {
        // Row 2 has props = NULL
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("2")))
                .build();

        // In this case, props is NULL
        List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("props.newKey", "newValue"));
        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("newValue", resultJson.get("props").get("newKey").asText());

        // Verify in database
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"props\"->>'newKey' as newKey FROM \"%s\" WHERE \"id\" = '2'",
                        FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("newValue", rs.getString("newKey"));
        }
      }

      @Test
      @DisplayName("Case 7c: SET non-existent nested path in existing JSONB should create key")
      void testSetNonExistentNestedPathInExistingJsonb() throws Exception {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        // In this case, props exists but props.newAttribute doesn't exist.
        List<SubDocumentUpdate> updates =
            List.of(SubDocumentUpdate.of("props.newAttribute", "brandNewValue"));
        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> result = flatCollection.update(query, updates, options);

        assertTrue(result.isPresent());
        JsonNode resultJson = OBJECT_MAPPER.readTree(result.get().toJson());
        assertEquals("brandNewValue", resultJson.get("props").get("newAttribute").asText());
        // Existing data should be preserved
        assertEquals("Dettol", resultJson.get("props").get("brand").asText());
      }

      @Test
      @DisplayName("SET should return correct document based on ReturnDocumentType")
      void testSetReturnDocumentTypes() throws Exception {
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("4")))
                .build();

        // Test BEFORE_UPDATE - returns old value
        List<SubDocumentUpdate> updates1 = List.of(SubDocumentUpdate.of("price", 777));
        UpdateOptions beforeOptions =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.BEFORE_UPDATE).build();

        Optional<Document> beforeResult = flatCollection.update(query, updates1, beforeOptions);
        assertTrue(beforeResult.isPresent());
        JsonNode beforeJson = OBJECT_MAPPER.readTree(beforeResult.get().toJson());
        assertEquals(5, beforeJson.get("price").asInt()); // Old value

        // Verify database has new value
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"price\" FROM \"%s\" WHERE \"id\" = '4'", FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(777, rs.getInt("price"));
        }

        // Test AFTER_UPDATE - returns new value
        List<SubDocumentUpdate> updates2 = List.of(SubDocumentUpdate.of("price", 888));
        UpdateOptions afterOptions =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        Optional<Document> afterResult = flatCollection.update(query, updates2, afterOptions);
        assertTrue(afterResult.isPresent());
        JsonNode afterJson = OBJECT_MAPPER.readTree(afterResult.get().toJson());
        assertEquals(888, afterJson.get("price").asInt()); // New value

        // Test NONE - returns empty
        List<SubDocumentUpdate> updates3 = List.of(SubDocumentUpdate.of("price", 999));
        UpdateOptions noneOptions =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.NONE).build();

        Optional<Document> noneResult = flatCollection.update(query, updates3, noneOptions);
        assertFalse(noneResult.isPresent());

        // Verify database has the final value
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"price\" FROM \"%s\" WHERE \"id\" = '4'", FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(999, rs.getInt("price"));
        }
      }
    }

    @Nested
    @DisplayName("UNSET Operator Tests")
    class UnsetOperatorTests {

      @Test
      @DisplayName("Should UNSET top-level column and nested JSONB field via bulkUpdate")
      void testUnsetTopLevelAndNestedFields() throws Exception {
        // Row 1 has item="Soap" and props.brand="Dettol"
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        // UNSET both top-level column and nested JSONB field in one operation
        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: sets column to NULL
                SubDocumentUpdate.builder()
                    .subDocument("item")
                    .operator(UpdateOperator.UNSET)
                    .build(),
                // Nested JSONB: removes key from JSON object
                SubDocumentUpdate.builder()
                    .subDocument("props.brand")
                    .operator(UpdateOperator.UNSET)
                    .build(),
                // non existent columns. Shouldn't fail
                SubDocumentUpdate.builder()
                    .subDocument("nonexistentCol")
                    .operator(UpdateOperator.UNSET)
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("nonexistentCol.key")
                    .operator(UpdateOperator.UNSET)
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          Document resultDoc = results.next();
          JsonNode resultJson = OBJECT_MAPPER.readTree(resultDoc.toJson());

          // Verify top-level column is NULL
          JsonNode itemNode = resultJson.get("item");
          assertTrue(itemNode == null || itemNode.isNull());

          // Verify nested JSONB key is removed, but other keys preserved
          assertFalse(resultJson.get("props").has("brand"));
          assertEquals("M", resultJson.get("props").get("size").asText());
        }

        // Verify in database
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"item\", \"props\" FROM \"%s\" WHERE \"id\" = '1'",
                        FLAT_COLLECTION_NAME));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertNull(rs.getString("item"));
          JsonNode propsJson = OBJECT_MAPPER.readTree(rs.getString("props"));
          assertFalse(propsJson.has("brand"));
          assertEquals("M", propsJson.get("size").asText());
        }
      }
    }

    @Nested
    @DisplayName("ADD Operator Tests")
    class AddSubdocOperatorTests {

      @Test
      @DisplayName("Should ADD to all numeric types via bulkUpdate")
      void testAddAllNumericTypes() throws Exception {
        String docId = generateDocId("test");
        Key key = new SingleValueKey(DEFAULT_TENANT, docId);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NumericTestItem");
        node.put("price", 100); // INT (positive ADD)
        node.put("quantity", 50); // INT (negative ADD - decrement)
        node.put("big_number", 1000000000000L); // BIGINT
        node.put("rating", 3.5); // REAL
        node.put("weight", 50.0); // DOUBLE PRECISION
        ObjectNode sales = OBJECT_MAPPER.createObjectNode();
        sales.put("total", 200); // Nested JSONB numeric
        sales.put("count", 10);
        node.set("sales", sales);
        flatCollection.create(key, new JSONDocument(node));

        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of(key.toString())))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level INT: 100 + 5 = 105
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(5))
                    .build(),
                // Top-level INT (negative): 50 + (-15) = 35
                SubDocumentUpdate.builder()
                    .subDocument("quantity")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(-15))
                    .build(),
                // Top-level BIGINT: 1000000000000 + 500 = 1000000000500
                SubDocumentUpdate.builder()
                    .subDocument("big_number")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(500L))
                    .build(),
                // Top-level REAL: 3.5 + 1.0 = 4.5
                SubDocumentUpdate.builder()
                    .subDocument("rating")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(1.0f))
                    .build(),
                // Top-level DOUBLE: 50.0 + 2.5 = 52.5
                SubDocumentUpdate.builder()
                    .subDocument("weight")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(2.5))
                    .build(),
                // Nested JSONB: 200 + 50 = 250
                SubDocumentUpdate.builder()
                    .subDocument("sales.total")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(50))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        String expectedJsonContent =
            readFileFromResource("expected/add_all_numeric_types_expected.json").orElseThrow();
        JsonNode expectedJson = OBJECT_MAPPER.readTree(expectedJsonContent);

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          JsonNode resultJson = OBJECT_MAPPER.readTree(results.next().toJson());

          ((ObjectNode) resultJson).remove("id");
          assertEquals(expectedJson, resultJson);
        }

        // Verify in database
        PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
        try (Connection conn = pgDatastore.getPostgresClient();
            PreparedStatement ps =
                conn.prepareStatement(
                    String.format(
                        "SELECT \"price\", \"quantity\", \"big_number\", \"rating\", \"weight\", \"sales\" "
                            + "FROM \"%s\" WHERE \"id\" = '%s'",
                        FLAT_COLLECTION_NAME, key));
            ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(expectedJson.get("price").asInt(), rs.getInt("price"));
          assertEquals(expectedJson.get("quantity").asInt(), rs.getInt("quantity"));
          assertEquals(expectedJson.get("big_number").asLong(), rs.getLong("big_number"));
          assertEquals(expectedJson.get("rating").floatValue(), rs.getFloat("rating"), 0.01f);
          assertEquals(expectedJson.get("weight").asDouble(), rs.getDouble("weight"), 0.01);
          JsonNode salesJson = OBJECT_MAPPER.readTree(rs.getString("sales"));
          assertEquals(
              expectedJson.get("sales").get("total").asInt(), salesJson.get("total").asInt());
          assertEquals(
              expectedJson.get("sales").get("count").asInt(), salesJson.get("count").asInt());
        }
      }

      @Test
      @DisplayName("Should handle ADD on NULL column (treat as 0)")
      void testAddOnNullColumn() throws Exception {
        // Create a document with NULL numeric columns
        String docId = generateDocId("test");
        Key key = new SingleValueKey(DEFAULT_TENANT, docId);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "NullPriceItem");
        // price, weight are not set - will be NULL
        flatCollection.create(key, new JSONDocument(node));

        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of(key.toString())))
                .build();

        // ADD to NULL columns - COALESCE(NULL, 0) + value
        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(100))
                    .build(),
                SubDocumentUpdate.builder()
                    .subDocument("weight")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(25.5))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          JsonNode resultJson = OBJECT_MAPPER.readTree(results.next().toJson());
          assertEquals(100, resultJson.get("price").asInt());
          assertEquals(25.5, resultJson.get("weight").asDouble(), 0.01);
        }
      }

      @Test
      @DisplayName("Should throw IllegalArgumentException for non-numeric value")
      void testAddNonNumericValue() {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of("not-a-number"))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(
            IllegalArgumentException.class, () -> flatCollection.update(query, updates, options));
      }

      @Test
      @DisplayName("Should throw IllegalArgumentException for array value")
      void testAddArrayValue() {
        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of("1")))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                SubDocumentUpdate.builder()
                    .subDocument("price")
                    .operator(UpdateOperator.ADD)
                    .subDocumentValue(SubDocumentValue.of(new Integer[] {1, 2, 3}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        assertThrows(
            IllegalArgumentException.class, () -> flatCollection.update(query, updates, options));
      }
    }

    @Nested
    @DisplayName("APPEND_TO_LIST Operator Tests")
    class AppendToListOperatorTests {

      @Test
      @DisplayName("Should APPEND_TO_LIST for top-level and nested arrays via bulkUpdate")
      void testAppendToListAllCases() throws Exception {
        String docId = generateDocId("test");
        Key key = new SingleValueKey(DEFAULT_TENANT, docId);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "AppendTestItem");
        node.putArray("tags").add("tag1").add("tag2"); // Top-level array (existing)
        ObjectNode props = OBJECT_MAPPER.createObjectNode();
        props.putArray("colors").add("red").add("blue"); // Nested JSONB array (existing)
        props.put("brand", "TestBrand");
        node.set("props", props);
        ObjectNode sales = OBJECT_MAPPER.createObjectNode();
        sales.put("total", 100); // Nested JSONB without array
        node.set("sales", sales);
        flatCollection.create(key, new JSONDocument(node));

        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of(key.toString())))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level array: append to existing tags
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"newTag1", "newTag2"}))
                    .build(),
                // Nested JSONB array: append to existing props.colors
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"green", "yellow"}))
                    .build(),
                // Nested JSONB: append to non-existent array (creates it)
                SubDocumentUpdate.builder()
                    .subDocument("sales.regions")
                    .operator(UpdateOperator.APPEND_TO_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"US", "EU"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          JsonNode resultJson = OBJECT_MAPPER.readTree(results.next().toJson());

          // Verify top-level array append
          JsonNode tagsNode = resultJson.get("tags");
          assertTrue(tagsNode.isArray());
          assertEquals(4, tagsNode.size());
          assertEquals("tag1", tagsNode.get(0).asText());
          assertEquals("tag2", tagsNode.get(1).asText());
          assertEquals("newTag1", tagsNode.get(2).asText());
          assertEquals("newTag2", tagsNode.get(3).asText());

          // Verify nested JSONB array append
          JsonNode colorsNode = resultJson.get("props").get("colors");
          assertTrue(colorsNode.isArray());
          assertEquals(4, colorsNode.size());
          assertEquals("red", colorsNode.get(0).asText());
          assertEquals("blue", colorsNode.get(1).asText());
          assertEquals("green", colorsNode.get(2).asText());
          assertEquals("yellow", colorsNode.get(3).asText());

          // Verify non-existent array was created
          JsonNode regionsNode = resultJson.get("sales").get("regions");
          assertNotNull(regionsNode);
          assertTrue(regionsNode.isArray());
          assertEquals(2, regionsNode.size());
          assertEquals("US", regionsNode.get(0).asText());
          assertEquals("EU", regionsNode.get(1).asText());

          // Verify other fields preserved
          assertEquals("TestBrand", resultJson.get("props").get("brand").asText());
          assertEquals(100, resultJson.get("sales").get("total").asInt());
        }

        // todo: Add negative test cases based on Mongo's behaviour
      }
    }

    @Nested
    @DisplayName("ADD_TO_LIST_IF_ABSENT Operator Tests")
    class AddToListIfAbsentOperatorTests {

      @Test
      @DisplayName("Should ADD_TO_LIST_IF_ABSENT for top-level and nested arrays via bulkUpdate")
      void testAddToListIfAbsentAllCases() throws Exception {
        String docId = generateDocId("test");
        Key key = new SingleValueKey(DEFAULT_TENANT, docId);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "AddIfAbsentTestItem");
        node.putArray("tags").add("existing1").add("existing2"); // Top-level array
        node.putArray("numbers").add(1).add(2); // Top-level (all duplicates test)
        ObjectNode props = OBJECT_MAPPER.createObjectNode();
        props.putArray("colors").add("red").add("blue"); // Nested JSONB array
        node.set("props", props);
        flatCollection.create(key, new JSONDocument(node));

        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of(key.toString())))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: 'existing1' exists, 'newTag' is new → adds only 'newTag'
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"existing1", "newTag"}))
                    .build(),
                // Nested JSONB: 'red' exists, 'green' is new → adds only 'green'
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.ADD_TO_LIST_IF_ABSENT)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"red", "green"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          JsonNode resultJson = OBJECT_MAPPER.readTree(results.next().toJson());

          JsonNode tagsNode = resultJson.get("tags");
          assertTrue(tagsNode.isArray());
          assertEquals(3, tagsNode.size());
          Set<String> tagValues = new HashSet<>();
          tagsNode.forEach(n -> tagValues.add(n.asText()));
          assertTrue(tagValues.contains("existing1"));
          assertTrue(tagValues.contains("existing2"));
          assertTrue(tagValues.contains("newTag"));

          JsonNode colorsNode = resultJson.get("props").get("colors");
          assertTrue(colorsNode.isArray());
          assertEquals(3, colorsNode.size());
          Set<String> colorValues = new HashSet<>();
          colorsNode.forEach(n -> colorValues.add(n.asText()));
          assertTrue(colorValues.contains("red"));
          assertTrue(colorValues.contains("blue"));
          assertTrue(colorValues.contains("green"));
        }
      }
      // todo: Add a negative case to check what happens to Mongo when this operator is applied to
      // non-array columns
    }

    @Nested
    @DisplayName("REMOVE_ALL_FROM_LIST Operator Tests")
    class RemoveAllFromListOperatorTests {

      @Test
      @DisplayName("Should REMOVE_ALL_FROM_LIST for top-level and nested arrays via bulkUpdate")
      void testRemoveAllFromListAllCases() throws Exception {
        String docId = generateDocId("test");
        Key key = new SingleValueKey(DEFAULT_TENANT, docId);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        node.put("item", "RemoveTestItem");
        node.putArray("tags").add("tag1").add("tag2").add("tag3"); // Top-level: remove existing
        node.putArray("numbers").add(1).add(2).add(3); // Top-level: remove non-existent (no-op)
        ObjectNode props = OBJECT_MAPPER.createObjectNode();
        props
            .putArray("colors")
            .add("red")
            .add("blue")
            .add("green"); // Nested JSONB: remove multiple
        node.set("props", props);
        flatCollection.create(key, new JSONDocument(node));

        Query query =
            Query.builder()
                .setFilter(
                    RelationalExpression.of(
                        IdentifierExpression.of("id"),
                        RelationalOperator.EQ,
                        ConstantExpression.of(key.toString())))
                .build();

        List<SubDocumentUpdate> updates =
            List.of(
                // Top-level: remove 'tag1' → leaves tag2, tag3
                SubDocumentUpdate.builder()
                    .subDocument("tags")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"tag1"}))
                    .build(),
                // Nested JSONB: remove 'red' and 'blue' → leaves green
                SubDocumentUpdate.builder()
                    .subDocument("props.colors")
                    .operator(UpdateOperator.REMOVE_ALL_FROM_LIST)
                    .subDocumentValue(SubDocumentValue.of(new String[] {"red", "blue"}))
                    .build());

        UpdateOptions options =
            UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

        try (CloseableIterator<Document> results =
            flatCollection.bulkUpdate(query, updates, options)) {
          assertTrue(results.hasNext());
          JsonNode resultJson = OBJECT_MAPPER.readTree(results.next().toJson());

          // Verify top-level: tag1 removed, tag2 and tag3 remain
          JsonNode tagsNode = resultJson.get("tags");
          assertTrue(tagsNode.isArray());
          assertEquals(2, tagsNode.size());
          assertEquals("tag2", tagsNode.get(0).asText());
          assertEquals("tag3", tagsNode.get(1).asText());

          // Verify nested JSONB: red and blue removed, green remains
          JsonNode colorsNode = resultJson.get("props").get("colors");
          assertTrue(colorsNode.isArray());
          assertEquals(1, colorsNode.size());
          assertEquals("green", colorsNode.get(0).asText());

          // Verify numbers unchanged (no-op since we didn't update it)
          JsonNode numbersNode = resultJson.get("numbers");
          assertTrue(numbersNode.isArray());
          assertEquals(3, numbersNode.size());
        }
      }
    }
  }

  @Nested
  @DisplayName("Bulk Update Operations")
  class BulkUpdateTests {

    @Test
    @DisplayName("Should update multiple rows and return AFTER_UPDATE documents")
    void testBulkUpdateWithAfterUpdateReturn() throws Exception {
      // Filter: price > 5 should match multiple rows (IDs 1, 2, 3, 5, 6, 7, 8)
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"),
                      RelationalOperator.GT,
                      ConstantExpression.of(5)))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("quantity", 999));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      List<Document> results = new ArrayList<>();
      while (resultIterator.hasNext()) {
        results.add(resultIterator.next());
      }
      resultIterator.close();

      assertTrue(results.size() > 1, "Should return multiple updated documents");

      for (Document doc : results) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        assertEquals(999, json.get("quantity").asInt(), "All docs should have updated quantity");
      }

      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT COUNT(*) FROM \"%s\" WHERE \"quantity\" = 999",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(results.size(), rs.getInt(1), "DB should have same number of updated rows");
      }
    }

    @Test
    @DisplayName("Should update multiple rows and return BEFORE_UPDATE documents")
    void testBulkUpdateWithBeforeUpdateReturn() throws Exception {
      // First, get the original quantities for verification
      Map<String, Integer> originalQuantities = new HashMap<>();
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"id\", \"quantity\" FROM \"%s\" WHERE \"price\" > 10",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          originalQuantities.put(rs.getString("id"), rs.getInt("quantity"));
        }
      }

      // Filter: price > 10 should match a subset of rows
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"),
                      RelationalOperator.GT,
                      ConstantExpression.of(10)))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("quantity", 888));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.BEFORE_UPDATE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      List<Document> results = new ArrayList<>();
      while (resultIterator.hasNext()) {
        results.add(resultIterator.next());
      }
      resultIterator.close();

      // Verify the returned documents have the ORIGINAL quantities (before update)
      for (Document doc : results) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        String id = json.get("id").asText();
        int returnedQuantity = json.get("quantity").asInt();

        assertTrue(originalQuantities.containsKey(id), "Returned doc ID should be in original set");
        assertEquals(
            originalQuantities.get(id).intValue(),
            returnedQuantity,
            "Returned quantity should be the ORIGINAL value");
      }

      // But database should have the NEW value
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"quantity\" FROM \"%s\" WHERE \"price\" > 10",
                      FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          assertEquals(888, rs.getInt("quantity"), "DB should have the updated value");
        }
      }
    }

    @Test
    @DisplayName("Should return empty iterator when ReturnDocumentType is NONE")
    void testBulkUpdateWithNoneReturn() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 123));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.NONE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      // Should return empty iterator
      assertFalse(resultIterator.hasNext(), "Should return empty iterator for NONE return type");
      resultIterator.close();

      // But database should still be updated
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT \"price\" FROM \"%s\" WHERE \"id\" = '1'", FLAT_COLLECTION_NAME));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(123, rs.getInt("price"));
      }
    }

    @Test
    @DisplayName("Should return empty iterator when filter matches no documents")
    void testBulkUpdateNoMatchingDocuments() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("non-existent-id")))
              .build();

      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 999));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      assertFalse(resultIterator.hasNext(), "Should return empty iterator when no docs match");
      resultIterator.close();
    }

    @Test
    @DisplayName("Should update with multiple SubDocumentUpdates")
    void testBulkUpdateMultipleFields() throws Exception {
      // Update item = "Soap" (IDs 1, 5, 8)
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("Soap")))
              .build();

      // Update both price and quantity
      List<SubDocumentUpdate> updates =
          List.of(SubDocumentUpdate.of("price", 50), SubDocumentUpdate.of("quantity", 200));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      List<Document> results = new ArrayList<>();
      while (resultIterator.hasNext()) {
        results.add(resultIterator.next());
      }
      resultIterator.close();

      assertEquals(3, results.size(), "Should return 3 Soap items");

      for (Document doc : results) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        assertEquals("Soap", json.get("item").asText());
        assertEquals(50, json.get("price").asInt());
        assertEquals(200, json.get("quantity").asInt());
      }
    }

    @Test
    @DisplayName("Should update nested JSONB paths for multiple documents")
    void testBulkUpdateNestedJsonbPath() throws Exception {
      // Documents with props JSONB: IDs 1, 3, 5, 7
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("Soap")))
              .build();

      List<SubDocumentUpdate> updates =
          List.of(SubDocumentUpdate.of("props.brand", "BulkUpdatedBrand"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      List<Document> results = new ArrayList<>();
      while (resultIterator.hasNext()) {
        results.add(resultIterator.next());
      }
      resultIterator.close();

      // Verify all returned documents have updated props.brand
      for (Document doc : results) {
        JsonNode json = OBJECT_MAPPER.readTree(doc.toJson());
        JsonNode props = json.get("props");
        if (props != null && !props.isNull()) {
          assertEquals(
              "BulkUpdatedBrand", props.get("brand").asText(), "props.brand should be updated");
        }
      }
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for empty updates")
    void testBulkUpdateEmptyUpdates() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      List<SubDocumentUpdate> emptyUpdates = Collections.emptyList();

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(
          IllegalArgumentException.class,
          () -> flatCollection.bulkUpdate(query, emptyUpdates, options));
    }

    @Test
    @DisplayName("Should skip non-existent column with default SKIP strategy")
    void testBulkUpdateNonExistentColumnWithSkipStrategy() throws Exception {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      // Mix of valid and invalid (non-existent) column paths
      List<SubDocumentUpdate> updates =
          List.of(
              SubDocumentUpdate.of("price", 111),
              SubDocumentUpdate.of("nonExistentColumn", "someValue"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      // Default strategy is SKIP - should not throw, just skip the non-existent column
      CloseableIterator<Document> resultIterator =
          flatCollection.bulkUpdate(query, updates, options);

      List<Document> results = new ArrayList<>();
      while (resultIterator.hasNext()) {
        results.add(resultIterator.next());
      }
      resultIterator.close();

      assertEquals(1, results.size());
      JsonNode json = OBJECT_MAPPER.readTree(results.get(0).toJson());
      assertEquals(111, json.get("price").asInt(), "Valid column should be updated");
      assertFalse(json.has("nonExistentColumn"), "Non-existent column should not appear");
    }

    @Test
    @DisplayName("Should throw exception for non-existent column with THROW strategy")
    void testBulkUpdateNonExistentColumnWithThrowStrategy() {
      Collection collectionWithThrowStrategy =
          getFlatCollectionWithStrategy(MissingColumnStrategy.THROW.toString());

      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("id"),
                      RelationalOperator.EQ,
                      ConstantExpression.of("1")))
              .build();

      List<SubDocumentUpdate> updates =
          List.of(SubDocumentUpdate.of("nonExistentColumn", "someValue"));

      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(
          IOException.class, () -> collectionWithThrowStrategy.bulkUpdate(query, updates, options));
    }
  }

  @Nested
  @DisplayName("CreateOrReplace Schema Refresh Tests")
  class CreateOrReplaceSchemaRefreshTests {

    @Test
    @DisplayName("createOrReplace should refresh schema and retry on dropped column")
    void testCreateOrReplaceRefreshesSchemaOnDroppedColumn() throws Exception {
      PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

      // Step 1: Add a temporary column
      String addColumnSQL =
          String.format(
              "ALTER TABLE \"%s\" ADD COLUMN \"temp_upsert_col\" TEXT", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(addColumnSQL)) {
        ps.execute();
        LOGGER.info("Added temporary column 'temp_upsert_col' to table");
      }

      // Step 2: Create a document with the temp column to cache the schema
      ObjectNode objectNode1 = OBJECT_MAPPER.createObjectNode();
      objectNode1.put("id", "upsert-cache-schema-doc");
      objectNode1.put("item", "Item to cache schema");
      objectNode1.put("temp_upsert_col", "temp value");
      flatCollection.createOrReplace(
          new SingleValueKey("default", "upsert-cache-schema-doc"), new JSONDocument(objectNode1));
      LOGGER.info("Schema cached with temp_upsert_col");

      // Step 3: DROP the column - now the cached schema is stale
      String dropColumnSQL =
          String.format("ALTER TABLE \"%s\" DROP COLUMN \"temp_upsert_col\"", FLAT_COLLECTION_NAME);
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps = conn.prepareStatement(dropColumnSQL)) {
        ps.execute();
        LOGGER.info("Dropped temp_upsert_col - schema cache is now stale");
      }

      // Step 4: Try createOrReplace with the dropped column
      // Schema registry still thinks temp_upsert_col exists, so it will include it in UPSERT
      // UPSERT will fail with UNDEFINED_COLUMN, triggering handlePSQLExceptionForUpsert
      // which will refresh schema and retry
      ObjectNode objectNode2 = OBJECT_MAPPER.createObjectNode();
      objectNode2.put("id", "upsert-retry-doc");
      objectNode2.put("item", "Item after schema refresh");
      objectNode2.put("temp_upsert_col", "this column no longer exists");
      Document document = new JSONDocument(objectNode2);
      Key key = new SingleValueKey("default", "upsert-retry-doc");

      boolean result = flatCollection.createOrReplace(key, document);

      // Should succeed after schema refresh - temp_upsert_col will be skipped
      assertTrue(result);

      // Verify the valid fields were inserted
      try (Connection conn = pgDatastore.getPostgresClient();
          PreparedStatement ps =
              conn.prepareStatement(
                  String.format(
                      "SELECT * FROM \"%s\" WHERE \"id\" = '%s'", FLAT_COLLECTION_NAME, key));
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Item after schema refresh", rs.getString("item"));
      }
    }
  }

  @Nested
  @DisplayName("Drop Operations")
  class DropTests {

    @Test
    @DisplayName("Should throw UnsupportedOperationException for drop")
    void testDrop() {
      assertThrows(UnsupportedOperationException.class, () -> flatCollection.drop());
    }
  }
}
